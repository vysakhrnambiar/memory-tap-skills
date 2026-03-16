"""
Gemini History Skill — collects conversation history with messages, thinking blocks, sources.

Verified selectors via CDP probe (2026-03-15):
- Sidebar: a[href*="/app/"] with hex ID filter
- Messages: <user-query> (user), <model-response> (assistant) — trivial role detection
- Thinking blocks: <collapsible-button> with text "Thoughts"/"Show thinking" — click to expand
- Code blocks: pre code inside model-response
- Sources: inline a[href*="http"] inside model-response
- Login: SID cookie on .google.com

Stop strategy: CONSECUTIVE_KNOWN
- No date headers in sidebar
- Track by conversation hex ID

__version__ = "0.2.2"
"""
__version__ = "0.2.2"

import json
import logging
import random
import re
import time
from datetime import datetime

logger = logging.getLogger("memory_tap.skill.gemini")

from src.skills.base import (
    BaseSkill, SkillManifest, CollectResult, StopStrategy, RunLimits,
)
from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule
from src.cdp_client import CDPTab, CDPClient
from src.db.sync_tracker import SyncTracker
from src.human import scroll_slowly, wait_human, move_mouse, click_at


class GeminiHistorySkill(BaseSkill):
    """Collects Gemini conversation history — messages, thinking blocks, sources."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="gemini_history",
            version=__version__,
            target_url="https://gemini.google.com/app",
            description="Collects Gemini conversations — messages, thinking blocks, sources, code",
            auth_provider="google",
            schedule_hours=3,
            login_url="https://accounts.google.com/ServiceLogin",
            max_items_first_run=30,
            max_items_per_run=0,
            max_minutes_per_run=30,
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.CONSECUTIVE_KNOWN

    def _main_table_name(self) -> str:
        return "conversations"

    # ── Schema ────────────────────────────────────────────────────

    def create_schema(self, conn) -> None:
        """Create Gemini-specific tables in skill's own DB."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                url TEXT NOT NULL DEFAULT '',
                list_position INTEGER DEFAULT 0,
                message_count INTEGER DEFAULT 0,
                has_thinking INTEGER DEFAULT 0,
                discovered_date TEXT NOT NULL DEFAULT (date('now')),
                last_updated TEXT NOT NULL DEFAULT (datetime('now')),
                synced_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL DEFAULT '',
                thinking_block TEXT DEFAULT '',
                sources TEXT DEFAULT '',
                code_blocks TEXT DEFAULT '',
                message_order INTEGER NOT NULL,
                synced_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
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
            CREATE INDEX IF NOT EXISTS idx_conv_updated ON conversations(last_updated);
        """)
        conn.commit()

    # ── Login detection ───────────────────────────────────────────

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into Gemini.

        Verified (2026-03-14 CDP probe):
        - Logged in: SID, SSID, HSID cookies on .google.com,
                     Google Account button visible
        - Not logged in: no SID, "Sign in" text visible
        """
        tab.navigate("https://gemini.google.com/app")
        wait_human(3, 5)

        result = tab._send("Network.getCookies")
        if isinstance(result, dict) and "_error" not in result:
            cookies = result.get("cookies", [])
            sid_cookies = [
                c for c in cookies
                if c.get("name") == "SID" and ".google.com" in c.get("domain", "")
            ]
            if sid_cookies:
                logger.info("Gemini: logged in (SID cookie found)")
                return True

        if tab.has_text("Sign in"):
            logger.info("Gemini: not logged in (Sign in text visible)")
            return False

        logger.info("Gemini: login unclear, treating as not logged in")
        return False

    # ── Stop signal ───────────────────────────────────────────────

    def should_stop_collecting(self, item: dict, tracker: SyncTracker) -> bool:
        """CONSECUTIVE_KNOWN stop: stop after 5 consecutive known unchanged conversations."""
        return item.get("_consecutive_known", 0) >= 5

    # ── Collection ────────────────────────────────────────────────

    def collect(self, tab: CDPTab, tracker: SyncTracker,
                limits: RunLimits) -> CollectResult:
        """Scan sidebar, detect new/updated conversations, collect messages."""
        result = CollectResult()
        conn = tracker._skill_conn

        tab.navigate("https://gemini.google.com/app")
        wait_human(3, 5)

        # Phase 1: Scan sidebar
        conversations = self._scan_sidebar(tab)
        result.items_found = len(conversations)
        logger.info("Found %d Gemini conversations", len(conversations))

        # Phase 2: Process each
        consecutive_known = 0

        for i, conv in enumerate(conversations):
            if limits.should_stop():
                logger.info("Stopping: %s", limits.stop_reason)
                break

            ext_id = conv["external_id"]

            existing = conn.execute(
                "SELECT id, list_position, message_count FROM conversations "
                "WHERE external_id = ?",
                (ext_id,),
            ).fetchone()

            if existing and existing["message_count"] > 0:
                consecutive_known += 1
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
                consecutive_known = 0

            try:
                messages, has_thinking = self._collect_conversation(tab, conv)
                conv_db_id = self._upsert_conversation(
                    conn, conv, len(messages), has_thinking
                )
                new_msg_count = self._save_messages(conn, conv_db_id, messages)

                if not existing:
                    result.items_new += 1
                elif new_msg_count > 0:
                    result.items_updated += 1

                limits.item_done()
                logger.info(
                    "Collected Gemini %d/%d: '%s' (%d msgs, %d new, thinking=%s)",
                    i + 1, len(conversations), conv["title"][:50],
                    len(messages), new_msg_count, has_thinking,
                )

            except Exception as e:
                logger.warning("Failed Gemini conversation %s: %s", ext_id, e)

            wait_human(2, 4)

        # Courtesy: restore sidebar order by visiting original top conversations
        self._restore_sidebar_order(tab, conversations[:5])

        return result

    def _restore_sidebar_order(self, tab: CDPTab, original_top: list[dict]):
        """Visit original top conversations in reverse to restore sidebar order."""
        logger.info("Restoring sidebar order (visiting top %d conversations)", len(original_top))
        for conv in reversed(original_top):
            try:
                tab.navigate(conv["url"])
                wait_human(1, 2)
            except Exception:
                pass

    def _scan_sidebar(self, tab: CDPTab) -> list[dict]:
        """Scroll sidebar and collect conversation entries."""
        for _ in range(10):
            tab.js("""
                var sidebar = document.querySelector('nav, [role="navigation"], .sidebar');
                if (sidebar) sidebar.scrollTop += 400;
                else window.scrollTo(0, document.body.scrollHeight);
            """)
            wait_human(0.8, 1.5)

        entries_raw = tab.js("""
            return (function() {
                var links = document.querySelectorAll('a[href*="/app/"]');
                var out = [];
                var seen = new Set();
                for (var i = 0; i < links.length; i++) {
                    var a = links[i];
                    var href = a.getAttribute('href') || '';
                    var match = href.match(/\\/app\\/([a-f0-9]{10,})/);
                    if (!match || seen.has(match[1])) continue;
                    seen.add(match[1]);

                    var title = a.textContent.trim();
                    if (!title || title.length < 2) continue;

                    out.push({
                        external_id: match[1],
                        title: title.substring(0, 300),
                        position: out.length,
                        url: 'https://gemini.google.com/app/' + match[1],
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

    def _collect_conversation(self, tab: CDPTab, conv: dict) -> tuple[list[dict], bool]:
        """Navigate to conversation, extract messages + thinking blocks.

        Returns (messages_list, has_thinking).
        Gemini uses <user-query> and <model-response> — trivial role detection.
        """
        tab.navigate(conv["url"])
        wait_human(3, 5)

        tab.wait_for_selector("user-query, model-response", timeout=10)

        # Expand all thinking blocks
        # Verified (2026-03-16): button text is "Thoughts" or "Show thinking"
        # NOT all collapsible-buttons — some are "Sources used in the report"
        thinking_count = tab.js("""
            return (function() {
                var btns = document.querySelectorAll('collapsible-button');
                var count = 0;
                for (var i = 0; i < btns.length; i++) {
                    var text = btns[i].textContent.toLowerCase();
                    if (text.includes('thought') || text.includes('thinking')) {
                        btns[i].click();
                        count++;
                    }
                }
                return count;
            })();
        """) or 0

        if thinking_count:
            wait_human(1, 2)

        has_thinking = bool(thinking_count and int(thinking_count) > 0)

        # Get title from page header
        title = tab.js("""
            var h = document.querySelector('h1, [class*="title"]');
            return h ? h.textContent.trim() : '';
        """) or conv.get("title", "")

        if title:
            conv["title"] = title

        # Extract all messages
        messages_raw = tab.js("""
            return (function() {
                var out = [];
                var order = 0;
                var allEls = document.querySelectorAll('user-query, model-response');
                for (var i = 0; i < allEls.length; i++) {
                    var el = allEls[i];
                    var tag = el.tagName.toLowerCase();
                    var role = tag === 'user-query' ? 'user' : 'assistant';

                    var contentEl = el.querySelector('message-content') || el;
                    var content = contentEl.textContent.trim();

                    var thinkingText = '';
                    if (role === 'assistant') {
                        // Find thinking collapsible-button by text content
                        var allBtns = el.querySelectorAll('collapsible-button');
                        for (var b = 0; b < allBtns.length; b++) {
                            var btnText = allBtns[b].textContent.toLowerCase();
                            if (btnText.includes('thought') || btnText.includes('thinking')) {
                                // Get the sibling/parent content after the button
                                var parent = allBtns[b].parentElement;
                                if (parent) {
                                    // The thinking text is in a sibling element after the button
                                    var siblings = parent.children;
                                    for (var s = 0; s < siblings.length; s++) {
                                        if (siblings[s] !== allBtns[b] && siblings[s].tagName !== 'COLLAPSIBLE-BUTTON') {
                                            var t = siblings[s].textContent.trim();
                                            if (t.length > 10) thinkingText += t + '\n';
                                        }
                                    }
                                }
                                if (!thinkingText) thinkingText = parent ? parent.textContent.trim() : '';
                                break;
                            }
                        }
                    }

                    var sources = [];
                    if (role === 'assistant') {
                        var links = el.querySelectorAll('a[href*="http"]');
                        var seenUrls = new Set();
                        for (var s = 0; s < links.length; s++) {
                            var sUrl = links[s].getAttribute('href') || '';
                            var sName = links[s].textContent.trim();
                            if (sUrl && !seenUrls.has(sUrl) && sName.length < 200) {
                                seenUrls.add(sUrl);
                                sources.push({name: sName, url: sUrl});
                            }
                        }
                    }

                    var codeBlocks = [];
                    var codeEls = el.querySelectorAll('pre code, code');
                    for (var c = 0; c < codeEls.length; c++) {
                        var code = codeEls[c].textContent.trim();
                        if (code && code.length > 10) {
                            codeBlocks.push(code.substring(0, 10000));
                        }
                    }

                    order++;
                    if (content) {
                        out.push({
                            role: role,
                            content: content.substring(0, 50000),
                            thinking_block: thinkingText.substring(0, 20000),
                            sources: sources.length > 0 ? JSON.stringify(sources) : '',
                            code_blocks: codeBlocks.length > 0 ? JSON.stringify(codeBlocks) : '',
                            message_order: order,
                        });
                    }
                }
                return JSON.stringify(out);
            })();
        """)

        messages = []
        if messages_raw:
            try:
                messages = json.loads(messages_raw)
            except (json.JSONDecodeError, TypeError):
                pass

        return messages, has_thinking

    def _upsert_conversation(self, conn, conv: dict, message_count: int,
                              has_thinking: bool) -> int:
        """Insert or update conversation, return DB id."""
        existing = conn.execute(
            "SELECT id FROM conversations WHERE external_id = ?",
            (conv["external_id"],),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE conversations SET title = ?, list_position = ?, "
                "message_count = ?, has_thinking = ?, last_updated = datetime('now') "
                "WHERE id = ?",
                (conv["title"], conv["position"], message_count,
                 1 if has_thinking else 0, existing["id"]),
            )
            conn.commit()
            return existing["id"]
        else:
            cursor = conn.execute(
                "INSERT INTO conversations (external_id, title, url, list_position, "
                "message_count, has_thinking) VALUES (?, ?, ?, ?, ?, ?)",
                (conv["external_id"], conv["title"], conv["url"],
                 conv["position"], message_count, 1 if has_thinking else 0),
            )
            conn.commit()
            return cursor.lastrowid

    def _save_messages(self, conn, conv_db_id: int, messages: list[dict]) -> int:
        """Save messages. Gemini doesn't have per-message unique IDs,
        so we use conversation_id + message_order.

        On re-collection with new messages: append only new ones.
        """
        existing_count = conn.execute(
            "SELECT COUNT(*) as c FROM messages WHERE conversation_id = ?",
            (conv_db_id,),
        ).fetchone()["c"]

        if existing_count > 0 and len(messages) > existing_count:
            new_count = 0
            for msg in messages:
                if msg["message_order"] > existing_count:
                    conn.execute(
                        "INSERT INTO messages (conversation_id, role, content, "
                        "thinking_block, sources, code_blocks, message_order) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (conv_db_id, msg["role"], msg["content"],
                         msg.get("thinking_block", ""),
                         msg.get("sources", ""), msg.get("code_blocks", ""),
                         msg["message_order"]),
                    )
                    new_count += 1
            conn.commit()
            return new_count
        elif existing_count == 0:
            for msg in messages:
                conn.execute(
                    "INSERT INTO messages (conversation_id, role, content, "
                    "thinking_block, sources, code_blocks, message_order) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (conv_db_id, msg["role"], msg["content"],
                     msg.get("thinking_block", ""),
                     msg.get("sources", ""), msg.get("code_blocks", ""),
                     msg["message_order"]),
                )
            conn.commit()
            return len(messages)

        return 0

    # ── UI Manifest ───────────────────────────────────────────────

    def get_widgets(self) -> list[WidgetDefinition]:
        return [
            WidgetDefinition(
                name="stats",
                title="Gemini",
                display_type="stat_cards",
                data_query="",
                refresh_seconds=600,
                size="small",
                click_action="skill_page",
            ),
            WidgetDefinition(
                name="thinking",
                title="Conversations with Thinking",
                display_type="list",
                data_query=(
                    "SELECT title, url, message_count, last_updated "
                    "FROM conversations WHERE has_thinking = 1 "
                    "ORDER BY last_updated DESC LIMIT 5"
                ),
                refresh_seconds=600,
                size="medium",
                click_action="skill_page#thinking",
            ),
            WidgetDefinition(
                name="recent",
                title="Recent Gemini Conversations",
                display_type="timeline",
                data_query=(
                    "SELECT title, url, message_count, last_updated "
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
                name="thinking",
                title="Conversations with Thinking Blocks",
                display_type="list",
                data_query=(
                    "SELECT title, url, message_count, last_updated "
                    "FROM conversations WHERE has_thinking = 1 "
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
                    "SELECT title, url, message_count, last_updated, discovered_date "
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
                title_template="{items_new} new Gemini conversations",
                message_template="Collected {items_new} new conversations, {items_updated} updated",
                level="info",
                link_to="/skill/gemini_history",
            ),
            NotificationRule(
                event="after_collection",
                condition="items_found == 0 and previous_count > 0",
                title_template="Gemini: No conversations found",
                message_template="Previously had {previous_count} conversations. Site may have changed.",
                level="warning",
                link_to="/skill/gemini_history",
            ),
            NotificationRule(
                event="on_login_fail",
                condition="True",
                title_template="Gemini: Sign-in required",
                message_template="Please sign in to Gemini to continue collecting",
                level="action_required",
                link_to="/settings",
            ),
        ]

    def get_stats(self, conn) -> list[dict]:
        total = conn.execute("SELECT COUNT(*) as c FROM conversations").fetchone()["c"]
        with_thinking = conn.execute(
            "SELECT COUNT(*) as c FROM conversations WHERE has_thinking = 1"
        ).fetchone()["c"]
        total_msgs = conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"]
        user_msgs = conn.execute(
            "SELECT COUNT(*) as c FROM messages WHERE role = 'user'"
        ).fetchone()["c"]
        return [
            {"label": "Conversations", "value": total},
            {"label": "With Thinking", "value": with_thinking},
            {"label": "Messages", "value": total_msgs},
            {"label": "Your Messages", "value": user_msgs},
        ]

    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        rows = conn.execute(
            "SELECT m.content, m.role, m.thinking_block, c.title, c.url, c.last_updated "
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
                "source": "gemini_history",
                "role": r["role"],
                "has_thinking": bool(r["thinking_block"]),
            }
            for r in rows
        ]
