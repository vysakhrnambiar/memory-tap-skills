"""
ChatGPT History Skill — collects conversation history with messages and artifacts.

Flow:
1. Navigate to chatgpt.com
2. Check if logged in (sidebar with conversation list)
3. Scan conversation list — track position for update detection
4. For new/updated conversations: open each, scroll through, collect all messages
5. Download any artifacts (shown as download buttons)
6. Store in conversations + messages + artifacts tables

__version__ = "0.1.0"
"""
__version__ = "0.1.0"

import json
import logging
import os
import random
import re
import time

logger = logging.getLogger("memory_tap.skill.chatgpt")

# These imports resolve because the scheduler injects the project root into sys.path
from src.skills.base import BaseSkill, SkillManifest, CollectResult
from src.cdp_client import CDPTab
from src.db.sync_tracker import SyncTracker
from src.human import (
    scroll_slowly, scroll_to_bottom, click_at, click_element, click_text,
    wait_human, watch_page, move_mouse,
)


class ChatGPTHistorySkill(BaseSkill):
    """Collects ChatGPT conversation history."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="chatgpt_history",
            version=__version__,
            target_url="https://chatgpt.com",
            description="Collects ChatGPT conversations — messages, thinking blocks, artifacts",
            auth_provider="openai",
            schedule_hours=3,
            login_url="https://chatgpt.com/auth/login",
        )

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into ChatGPT.

        Verified detection (2026-03-14 CDP probe):
        - Logged in:  __Secure-next-auth.session-token.0 cookie on .chatgpt.com,
                      no "Log in" or "Sign up" text visible
        - Not logged in: no session-token cookie, "Log in" and "Sign up" text visible,
                         __Secure-next-auth.callback-url exists but is NOT auth
        Note: contenteditable and nav sidebar exist even when NOT logged in.
        """
        tab.navigate("https://chatgpt.com")
        wait_human(3, 5)

        # Primary: check for session-token cookie (most reliable)
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

        # If unclear — NOT logged in (never assume)
        logger.info("ChatGPT: login unclear, treating as not logged in")
        return False

    def collect(self, tab: CDPTab, tracker: SyncTracker) -> CollectResult:
        """Collect conversations from ChatGPT."""
        result = CollectResult()

        # Ensure we're on the main page with sidebar visible
        tab.navigate("https://chatgpt.com")
        wait_human(3, 5)

        # Collect conversation list from sidebar
        conversations = self._get_conversation_list(tab)
        result.items_found = len(conversations)
        logger.info("Found %d conversations in sidebar", len(conversations))

        # Process each conversation
        for i, conv in enumerate(conversations):
            external_id = conv["id"]
            position = conv["position"]

            # Check if this conversation needs updating
            if not tracker.conversation_needs_update(external_id, position):
                continue

            try:
                # Click into the conversation
                if not self._open_conversation(tab, conv):
                    continue

                wait_human(2, 4)

                # Collect all messages
                messages = self._collect_messages(tab)

                # Upsert conversation
                conv_id, is_new = tracker.upsert_conversation(
                    external_id=external_id,
                    title=conv["title"],
                    url=f"https://chatgpt.com/c/{external_id}",
                    list_position=position,
                )

                # Add new messages
                existing_count = tracker.get_message_count(conv_id)
                new_messages = [m for m in messages if m["message_order"] > existing_count]
                if new_messages:
                    tracker.add_messages(conv_id, new_messages)

                if is_new:
                    result.items_new += 1
                elif new_messages:
                    result.items_updated += 1

                logger.info(
                    "Collected conversation %d/%d: '%s' (%d messages, %d new)",
                    i + 1, len(conversations), conv["title"][:50],
                    len(messages), len(new_messages),
                )

                # Check for downloadable artifacts
                self._collect_artifacts(tab, conv_id, tracker)

            except Exception as e:
                logger.warning("Failed to collect conversation %s: %s", external_id, e)

            wait_human(1, 3)

        return result

    def _get_conversation_list(self, tab: CDPTab) -> list[dict]:
        """Extract conversation list from sidebar."""
        # Scroll sidebar to load conversations
        for _ in range(5):
            tab.js("""
                var nav = document.querySelector('nav');
                if (nav) nav.scrollTop += 300;
            """)
            wait_human(0.5, 1.0)

        entries = tab.js("""
            var items = document.querySelectorAll('nav li a, nav ol li a');
            var out = [];
            for (var i = 0; i < items.length; i++) {
                var a = items[i];
                var href = a.getAttribute('href') || '';
                // ChatGPT conversation links: /c/uuid
                var match = href.match(/\\/c\\/([a-f0-9-]+)/);
                if (match) {
                    out.push({
                        id: match[1],
                        title: a.textContent.trim().substring(0, 200),
                        position: i,
                        href: href
                    });
                }
            }
            return JSON.stringify(out);
        """)

        if entries:
            try:
                return json.loads(entries)
            except (json.JSONDecodeError, TypeError):
                pass
        return []

    def _open_conversation(self, tab: CDPTab, conv: dict) -> bool:
        """Navigate to a specific conversation."""
        href = conv.get("href", f"/c/{conv['id']}")
        tab.navigate(f"https://chatgpt.com{href}")
        wait_human(2, 4)

        # Wait for messages to appear
        return tab.wait_for_selector("[data-message-id], .agent-turn, .user-turn", timeout=10) is not None

    def _collect_messages(self, tab: CDPTab) -> list[dict]:
        """Scroll through conversation and collect all messages."""
        messages = []

        # Scroll to top first
        tab.js("window.scrollTo(0, 0)")
        wait_human(1, 2)

        # Scroll through entire conversation
        scroll_to_bottom(tab, max_scrolls=30, pause_range=(0.5, 1.5))

        # Extract all messages
        raw = tab.js("""
            var turns = document.querySelectorAll('[data-message-id], .group\\/conversation-turn');
            var out = [];
            for (var i = 0; i < turns.length; i++) {
                var turn = turns[i];
                var role = 'unknown';
                // Detect role
                if (turn.querySelector('.agent-turn, [data-message-author-role="assistant"]') ||
                    turn.classList.contains('agent-turn')) {
                    role = 'assistant';
                } else if (turn.querySelector('.user-turn, [data-message-author-role="user"]') ||
                           turn.classList.contains('user-turn')) {
                    role = 'user';
                } else {
                    // Fallback: check content structure
                    var authorEl = turn.querySelector('[data-message-author-role]');
                    if (authorEl) role = authorEl.getAttribute('data-message-author-role');
                }

                var content = turn.textContent.trim();

                // Check for thinking block
                var thinkingEl = turn.querySelector('[class*="thinking"], details summary');
                var thinking = thinkingEl ? thinkingEl.closest('details')?.textContent || '' : '';

                if (content) {
                    out.push({
                        role: role,
                        content: content.substring(0, 10000),
                        thinking_block: thinking.substring(0, 5000) || null,
                        message_order: i + 1
                    });
                }
            }
            return JSON.stringify(out);
        """)

        if raw:
            try:
                messages = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass

        return messages

    def _collect_artifacts(self, tab: CDPTab, conversation_id: int, tracker: SyncTracker):
        """Find and download any artifacts in the conversation."""
        # Look for download buttons or artifact indicators
        artifacts = tab.js("""
            var downloads = document.querySelectorAll(
                'a[download], button[aria-label*="download"], [class*="artifact"] a'
            );
            var out = [];
            for (var i = 0; i < downloads.length; i++) {
                var el = downloads[i];
                var name = el.getAttribute('download') || el.textContent.trim() || 'artifact';
                var href = el.getAttribute('href') || '';
                out.push({name: name, href: href});
            }
            return JSON.stringify(out);
        """)

        if artifacts:
            try:
                for art in json.loads(artifacts):
                    if art["name"] and art["name"] != "artifact":
                        tracker.add_artifact(
                            conversation_id=conversation_id,
                            filename=art["name"],
                            content=None,  # We store the reference, not the content
                            file_path=art.get("href"),
                        )
            except (json.JSONDecodeError, TypeError):
                pass
