"""
ChatGPT History Skill — collects conversation history with messages, sources, code blocks.

Verified selectors via CDP probe (2026-03-15):
- Sidebar: nav a[href*="/c/"] — regular chats, UUID in URL
- Messages: [data-message-id] with [data-message-author-role] ("user" / "assistant")
- Sources: group/footnote bg-token-bg-primary links
- Code blocks: pre code inside [data-message-id]
- Timestamps: hidden behind "..." click, format "Feb 18, 2:07 PM"
- Login: __Secure-next-auth.session-token.0 cookie on .chatgpt.com

Stop strategy: CONSECUTIVE_KNOWN
- No date headers in sidebar
- Track by conversation UUID + position
- Updated conversations move to top

Scope: Regular chats (/c/) only. Projects (/g/g-p-), GPTs (/g/g-), Group chats (/gg/) excluded.
Text only — no images, artifacts, files.

__version__ = "0.2.8"
"""
__version__ = "0.2.8"

import json
import logging
import random
import re
import time
from datetime import datetime

logger = logging.getLogger("memory_tap.skill.chatgpt")

from src.skills.base import (
    BaseSkill, SkillManifest, CollectResult, StopStrategy, RunLimits,
)
from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule
from src.cdp_client import CDPTab, CDPClient
from src.db.sync_tracker import SyncTracker
from src.human import scroll_slowly, wait_human, move_mouse


class ChatGPTHistorySkill(BaseSkill):
    """Collects ChatGPT conversation history — messages, sources, code blocks."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="chatgpt_history",
            version=__version__,
            target_url="https://chatgpt.com",
            description="Collects ChatGPT conversations — messages, sources, code blocks",
            auth_provider="openai",
            schedule_hours=3,
            login_url="https://chatgpt.com/auth/login",
            max_items_first_run=30,   # 30 conversations on first run
            max_items_per_run=0,      # unlimited for subsequent
            max_minutes_per_run=30,
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.CONSECUTIVE_KNOWN

    def _main_table_name(self) -> str:
        return "conversations"

    # ── Schema ────────────────────────────────────────────────────

    def create_schema(self, conn) -> None:
        """Create ChatGPT-specific tables in skill's own DB."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE NOT NULL,    -- UUID from /c/{uuid}
                title TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL DEFAULT '',
                is_pinned INTEGER DEFAULT 0,
                list_position INTEGER DEFAULT 0,     -- sidebar position (0 = top/newest)
                message_count INTEGER DEFAULT 0,
                discovered_date TEXT NOT NULL DEFAULT (date('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                synced_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                message_id TEXT NOT NULL,            -- data-message-id UUID
                role TEXT NOT NULL,                  -- 'user' or 'assistant'
                content TEXT NOT NULL DEFAULT '',
                thinking_block TEXT DEFAULT '',
                sources TEXT DEFAULT '',              -- JSON array of {name, url}
                code_blocks TEXT DEFAULT '',           -- JSON array of code strings
                timestamp_text TEXT DEFAULT '',       -- "Feb 18, 2:07 PM" if captured
                message_order INTEGER NOT NULL,
                synced_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (conversation_id) REFERENCES conversations(id),
                UNIQUE(conversation_id, message_id)
            );

            CREATE TABLE IF NOT EXISTS collection_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                content, thinking_block, sources,
                content='messages', content_rowid='id',
                tokenize='porter unicode61'
            );

            CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, content, thinking_block, sources)
                VALUES (new.id, new.content, new.thinking_block, new.sources);
            END;

            CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, content, thinking_block, sources)
                VALUES ('delete', old.id, old.content, old.thinking_block, old.sources);
            END;

            CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
                INSERT INTO messages_fts(messages_fts, rowid, content, thinking_block, sources)
                VALUES ('delete', old.id, old.content, old.thinking_block, old.sources);
                INSERT INTO messages_fts(rowid, content, thinking_block, sources)
                VALUES (new.id, new.content, new.thinking_block, new.sources);
            END;

            CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);
            CREATE INDEX IF NOT EXISTS idx_conv_updated ON conversations(last_updated);
        """)
        conn.commit()

    # ── Login detection ───────────────────────────────────────────

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into ChatGPT.

        Verified (2026-03-14 CDP probe):
        - Logged in: __Secure-next-auth.session-token.0 cookie, no "Log in" text
        - Not logged in: no session-token, "Log in" + "Sign up" text visible
        """
        tab.navigate("https://chatgpt.com")
        wait_human(3, 5)

        # Primary: check for session-token cookie
        result = tab._send("Network.getCookies")
        if isinstance(result, dict) and "_error" not in result:
            cookies = result.get("cookies", [])
            session_cookies = [c for c in cookies if "session-token" in c.get("name", "")]
            if session_cookies:
                logger.info("ChatGPT: logged in (session-token cookie found)")
                return True

        # Secondary: check for "Log in" text
        if tab.has_text("Log in") or tab.has_text("Sign up"):
            logger.info("ChatGPT: not logged in (Log in/Sign up text visible)")
            return False

        logger.info("ChatGPT: login unclear, treating as not logged in")
        return False

    # ── Stop signal ───────────────────────────────────────────────

    def should_stop_collecting(self, item: dict, tracker: SyncTracker) -> bool:
        """CONSECUTIVE_KNOWN stop: stop after 5 consecutive known, unchanged conversations."""
        # This is tracked in collect() via consecutive_known counter
        # The item dict has 'is_known' and 'is_updated' flags
        return item.get("_consecutive_known", 0) >= 5

    # ── Collection ────────────────────────────────────────────────

    def collect(self, tab: CDPTab, tracker: SyncTracker,
                limits: RunLimits) -> CollectResult:
        """Scan sidebar, detect new/updated conversations, collect messages."""
        result = CollectResult()
        conn = tracker._skill_conn

        # Navigate to ChatGPT
        tab.navigate("https://chatgpt.com")
        wait_human(3, 5)

        # Phase 1: Scan sidebar for conversation list
        conversations = self._scan_sidebar(tab)
        result.items_found = len(conversations)
        logger.info("Found %d conversations in sidebar", len(conversations))

        # Phase 2: Process each conversation (bottom-first to preserve sidebar order)
        # Visiting a conversation moves it to sidebar top. By visiting bottom first,
        # the last conversation we visit (originally #1) ends up back on top naturally.
        consecutive_known = 0
        collection_order = list(reversed(conversations))

        for i, conv in enumerate(collection_order):
            # Check framework limits
            if limits.should_stop():
                logger.info("Stopping: %s", limits.stop_reason)
                break

            ext_id = conv["external_id"]

            # Check if conversation exists and if it moved (= updated)
            existing = conn.execute(
                "SELECT id, list_position, message_count FROM conversations "
                "WHERE external_id = ?",
                (ext_id,),
            ).fetchone()

            if existing and existing["message_count"] > 0:
                # Known conversation — but might have new messages.
                # We can't check message count without navigating, so on first pass
                # just count it as known. The user's sidebar order changes when we
                # visit conversations, so position-based detection is unreliable.
                consecutive_known += 1
                # Update position
                conn.execute(
                    "UPDATE conversations SET list_position = ? WHERE id = ?",
                    (conv["position"], existing["id"]),
                )
                conn.commit()

                item = {"_consecutive_known": consecutive_known}
                if self.should_stop_collecting(item, tracker):
                    logger.info("Stop: 5 consecutive known conversations — reached old territory")
                    break
                continue
            else:
                consecutive_known = 0  # New or empty conversation

            # Navigate to conversation and collect messages
            try:
                messages = self._collect_conversation(tab, conv)

                # Upsert conversation
                conv_db_id = self._upsert_conversation(conn, conv, len(messages))

                # Add/update messages
                new_msg_count = self._save_messages(conn, conv_db_id, messages)

                if not existing:
                    result.items_new += 1
                elif new_msg_count > 0:
                    result.items_updated += 1

                limits.item_done()
                logger.info(
                    "Collected conversation %d/%d: '%s' (%d msgs, %d new)",
                    i + 1, len(conversations), conv["title"][:50],
                    len(messages), new_msg_count,
                )

            except Exception as e:
                logger.warning("Failed conversation %s: %s", ext_id, e)

            wait_human(2, 4)

        # No sidebar restore needed — bottom-first collection preserves order naturally

        return result

    def _restore_sidebar_order(self, tab: CDPTab, original_top: list[dict]):
        """Visit original top conversations in reverse to restore sidebar order.

        When we collect, visiting conversations pushes them to sidebar top.
        This visits the original top 5 in reverse so they end up back on top.
        """
        logger.info("Restoring sidebar order (visiting top %d conversations)", len(original_top))
        for conv in reversed(original_top):
            try:
                tab.navigate(conv["url"])
                wait_human(1, 2)
            except Exception:
                pass

    def _scan_sidebar(self, tab: CDPTab) -> list[dict]:
        """Scroll sidebar and collect all conversation entries.

        Returns list of {external_id, title, position, is_pinned, url}.
        """
        # Scroll sidebar to load all conversations — wait for new items to appear
        prev_count = 0
        no_change_count = 0
        for scroll_num in range(30):  # max 30 scrolls
            tab.js("""
                var nav = document.querySelector('nav');
                if (nav) nav.scrollTop += 400;
            """)
            wait_human(1.5, 2.5)

            # Count current conversations
            cur_count = tab.js("""
                return (function() {
                    return document.querySelectorAll('nav a[href*="/c/"]').length;
                })();
            """) or 0
            cur_count = int(cur_count)

            if cur_count > prev_count:
                no_change_count = 0
                prev_count = cur_count
                logger.info("Sidebar scroll %d: %d conversations loaded", scroll_num + 1, cur_count)
            else:
                no_change_count += 1
                if no_change_count >= 3:
                    logger.info("Sidebar scroll done: %d conversations (no new after %d scrolls)", cur_count, no_change_count)
                    break

        # Extract conversation links
        entries_raw = tab.js("""
            return (function() {
                var links = document.querySelectorAll('nav a[href*="/c/"]');
                var out = [];
                var seen = new Set();
                for (var i = 0; i < links.length; i++) {
                    var a = links[i];
                    var href = a.getAttribute('href') || '';
                    var match = href.match(/\\/c\\/([a-f0-9-]+)/);
                    if (!match || seen.has(match[1])) continue;
                    seen.add(match[1]);

                    var label = a.getAttribute('aria-label') || '';
                    var isPinned = label.toLowerCase().includes('pinned');
                    var title = a.textContent.trim();

                    out.push({
                        external_id: match[1],
                        title: title.substring(0, 300),
                        position: out.length,
                        is_pinned: isPinned,
                        url: 'https://chatgpt.com' + href,
                    });
                }
                return JSON.stringify(out);
            })();
        """)

        if entries_raw:
            try:
                return json.loads(entries_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return []

    def _collect_conversation(self, tab: CDPTab, conv: dict) -> list[dict]:
        """Navigate to conversation and extract all messages.

        Returns list of {message_id, role, content, thinking_block, sources, code_blocks, message_order}.
        """
        tab.navigate(conv["url"])
        wait_human(3, 5)

        # Wait for first messages to appear
        tab.wait_for_selector("[data-message-id]", timeout=15)

        # Scroll to top to load all messages (ChatGPT lazy-loads on scroll up)
        prev_count = 0
        for scroll_up in range(20):  # max 20 scroll-ups
            tab.js("window.scrollTo(0, 0)")
            wait_human(1, 2)
            cur_count = tab.js("""
                return document.querySelectorAll('[data-message-id]').length;
            """) or 0
            cur_count = int(cur_count)
            if cur_count == prev_count and scroll_up > 0:
                break  # no new messages loaded — all loaded
            prev_count = cur_count

        # Extract messages with role, content, sources, code
        messages_raw = tab.js("""
            return (function() {
                var msgs = document.querySelectorAll('[data-message-id]');
                var out = [];
                for (var i = 0; i < msgs.length; i++) {
                    var el = msgs[i];
                    var msgId = el.getAttribute('data-message-id') || '';
                    var role = el.getAttribute('data-message-author-role') || 'unknown';

                    // Content — main text
                    var content = el.textContent.trim();

                    // Sources — look for footnote links
                    var sourceEls = el.querySelectorAll('a[class*="footnote"], .group\\\\/footnote a');
                    var sources = [];
                    for (var s = 0; s < sourceEls.length; s++) {
                        var sa = sourceEls[s];
                        var sUrl = sa.getAttribute('href') || '';
                        var sName = sa.textContent.trim();
                        if (sUrl && sName) {
                            sources.push({name: sName, url: sUrl});
                        }
                    }

                    // Code blocks
                    var codeEls = el.querySelectorAll('pre code');
                    var codeBlocks = [];
                    for (var c = 0; c < codeEls.length; c++) {
                        var code = codeEls[c].textContent.trim();
                        if (code) codeBlocks.push(code.substring(0, 10000));
                    }

                    if (msgId && content) {
                        out.push({
                            message_id: msgId,
                            role: role,
                            content: content.substring(0, 50000),
                            thinking_block: '',
                            sources: sources.length > 0 ? JSON.stringify(sources) : '',
                            code_blocks: codeBlocks.length > 0 ? JSON.stringify(codeBlocks) : '',
                            message_order: i + 1,
                        });
                    }
                }
                return JSON.stringify(out);
            })();
        """)

        if messages_raw:
            try:
                return json.loads(messages_raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return []

    def _upsert_conversation(self, conn, conv: dict, message_count: int) -> int:
        """Insert or update conversation, return DB id."""
        existing = conn.execute(
            "SELECT id FROM conversations WHERE external_id = ?",
            (conv["external_id"],),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE conversations SET title = ?, list_position = ?, "
                "message_count = ?, is_pinned = ?, last_updated = datetime('now') "
                "WHERE id = ?",
                (conv["title"], conv["position"], message_count,
                 1 if conv.get("is_pinned") else 0, existing["id"]),
            )
            conn.commit()
            return existing["id"]
        else:
            cursor = conn.execute(
                "INSERT INTO conversations (external_id, title, url, list_position, "
                "message_count, is_pinned) VALUES (?, ?, ?, ?, ?, ?)",
                (conv["external_id"], conv["title"], conv["url"],
                 conv["position"], message_count,
                 1 if conv.get("is_pinned") else 0),
            )
            conn.commit()
            return cursor.lastrowid

    def _save_messages(self, conn, conv_db_id: int, messages: list[dict]) -> int:
        """Save messages, skipping duplicates by message_id. Returns count of new messages."""
        new_count = 0
        for msg in messages:
            existing = conn.execute(
                "SELECT id FROM messages WHERE conversation_id = ? AND message_id = ?",
                (conv_db_id, msg["message_id"]),
            ).fetchone()

            if not existing:
                conn.execute(
                    "INSERT INTO messages (conversation_id, message_id, role, content, "
                    "thinking_block, sources, code_blocks, message_order) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (conv_db_id, msg["message_id"], msg["role"],
                     msg["content"], msg.get("thinking_block", ""),
                     msg.get("sources", ""), msg.get("code_blocks", ""),
                     msg["message_order"]),
                )
                new_count += 1

        conn.commit()
        return new_count

    # ── UI Manifest ───────────────────────────────────────────────

    def get_widgets(self) -> list[WidgetDefinition]:
        return [
            WidgetDefinition(
                name="stats",
                title="ChatGPT",
                display_type="stat_cards",
                data_query="",
                refresh_seconds=600,
                size="small",
                click_action="skill_page",
            ),
            WidgetDefinition(
                name="recent",
                title="Recent ChatGPT Conversations",
                display_type="timeline",
                data_query=(
                    "SELECT id, title, url, message_count, last_updated "
                    "FROM conversations ORDER BY last_updated DESC LIMIT 8"
                ),
                refresh_seconds=300,
                size="medium",
                click_action="skill_page#conversations",
            ),
        ]

    def get_page_sections(self) -> list[PageSection]:
        return [
            PageSection(
                name="stats",
                title="Overview",
                display_type="stat_cards",
                data_query="",
                position=0,
            ),
            PageSection(
                name="pinned",
                title="Pinned Conversations",
                display_type="list",
                data_query=(
                    "SELECT id, title, url, message_count, last_updated "
                    "FROM conversations WHERE is_pinned = 1 "
                    "ORDER BY last_updated DESC"
                ),
                position=1,
                collapsible=True,
            ),
            PageSection(
                name="conversations",
                title="All Conversations",
                display_type="timeline",
                data_query=(
                    "SELECT id, title, url, message_count, last_updated, discovered_date "
                    "FROM conversations ORDER BY last_updated DESC"
                ),
                position=2,
                paginated=True,
                page_size=20,
            ),
            PageSection(
                name="search",
                title="Search Messages",
                display_type="search",
                data_query="",
                position=3,
            ),
        ]

    def get_notification_rules(self) -> list[NotificationRule]:
        return [
            NotificationRule(
                event="after_collection",
                condition="items_new > 0",
                title_template="{items_new} new ChatGPT conversations",
                message_template="Collected {items_new} new conversations, {items_updated} updated",
                level="info",
                link_to="/skill/chatgpt_history",
            ),
            NotificationRule(
                event="after_collection",
                condition="items_found == 0 and previous_count > 0",
                title_template="ChatGPT: No conversations found",
                message_template="Previously had {previous_count} conversations. Site may have changed.",
                level="warning",
                link_to="/skill/chatgpt_history",
            ),
            NotificationRule(
                event="on_login_fail",
                condition="True",
                title_template="ChatGPT: Sign-in required",
                message_template="Please sign in to ChatGPT to continue collecting",
                level="action_required",
                link_to="/settings",
            ),
        ]

    def get_stats(self, conn) -> list[dict]:
        total = conn.execute("SELECT COUNT(*) as c FROM conversations").fetchone()["c"]
        pinned = conn.execute(
            "SELECT COUNT(*) as c FROM conversations WHERE is_pinned = 1"
        ).fetchone()["c"]
        total_msgs = conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"]
        user_msgs = conn.execute(
            "SELECT COUNT(*) as c FROM messages WHERE role = 'user'"
        ).fetchone()["c"]
        return [
            {"label": "Conversations", "value": total},
            {"label": "Pinned", "value": pinned},
            {"label": "Messages", "value": total_msgs},
            {"label": "Your Messages", "value": user_msgs},
        ]

    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        rows = conn.execute(
            "SELECT m.content, m.role, m.sources, c.title, c.url, c.last_updated "
            "FROM messages_fts f "
            "JOIN messages m ON m.id = f.rowid "
            "JOIN conversations c ON c.id = m.conversation_id "
            "WHERE messages_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ).fetchall()
        return [
            {
                "type": "message",
                "title": r["title"],
                "snippet": (r["content"] or "")[:200],
                "url": r["url"],
                "date": r["last_updated"],
                "source": "chatgpt_history",
                "role": r["role"],
            }
            for r in rows
        ]
