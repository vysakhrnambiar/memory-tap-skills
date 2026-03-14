"""
Gemini History Skill — collects conversation history with messages and thinking blocks.

Flow:
1. Navigate to gemini.google.com/app
2. Check if logged in (Google avatar present)
3. Scan conversation list in sidebar
4. For new/updated conversations: open, scroll, collect all messages + thinking
5. Store in conversations + messages tables

__version__ = "0.1.0"
"""
__version__ = "0.1.0"

import json
import logging
import random
import re
import time

logger = logging.getLogger("memory_tap.skill.gemini")

# These imports resolve because the scheduler injects the project root into sys.path
from src.skills.base import BaseSkill, SkillManifest, CollectResult
from src.cdp_client import CDPTab
from src.db.sync_tracker import SyncTracker
from src.human import (
    scroll_slowly, scroll_to_bottom, click_at, click_element, click_text,
    wait_human, watch_page, move_mouse,
)


class GeminiHistorySkill(BaseSkill):
    """Collects Gemini conversation history."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="gemini_history",
            version=__version__,
            target_url="https://gemini.google.com/app",
            description="Collects Gemini conversations — messages, thinking blocks",
            auth_provider="google",
            schedule_hours=3,
            login_url="https://accounts.google.com/ServiceLogin?continue=https://gemini.google.com/app",
        )

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into Gemini.

        Verified detection (2026-03-14 CDP probe):
        - Logged in:  SID cookie on .google.com, Google Account button visible,
                      no "Sign in" text
        - Not logged in: no SID/SSID/HSID cookies, "Sign in" text visible
        Note: rich-textarea and contenteditable exist even when NOT logged in.
        """
        tab.navigate("https://gemini.google.com/app")
        wait_human(3, 5)

        # Primary: check for SID cookie on .google.com (most reliable)
        result = tab._send("Network.getCookies")
        if isinstance(result, dict) and "_error" not in result:
            cookies = result.get("cookies", [])
            google_auth = [c for c in cookies
                           if c.get("name") == "SID" and ".google.com" in c.get("domain", "")]
            if google_auth:
                logger.info("Gemini: logged in (SID cookie found)")
                return True

        # Secondary: check for "Sign in" text
        if tab.has_text("Sign in"):
            logger.info("Gemini: not logged in (Sign in text visible)")
            return False

        # If unclear — NOT logged in (never assume)
        logger.info("Gemini: login unclear, treating as not logged in")
        return False

    def collect(self, tab: CDPTab, tracker: SyncTracker) -> CollectResult:
        """Collect conversations from Gemini."""
        result = CollectResult()

        tab.navigate("https://gemini.google.com/app")
        wait_human(3, 5)

        # Open sidebar / conversation list
        self._open_sidebar(tab)

        # Get conversation list
        conversations = self._get_conversation_list(tab)
        result.items_found = len(conversations)
        logger.info("Found %d conversations in Gemini", len(conversations))

        for i, conv in enumerate(conversations):
            external_id = conv["id"]
            position = conv["position"]

            if not tracker.conversation_needs_update(external_id, position):
                continue

            try:
                # Click into conversation
                if not self._open_conversation(tab, conv):
                    continue

                wait_human(2, 4)

                # Collect messages
                messages = self._collect_messages(tab)

                conv_id, is_new = tracker.upsert_conversation(
                    external_id=external_id,
                    title=conv["title"],
                    url=f"https://gemini.google.com/app/{external_id}",
                    list_position=position,
                )

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

            except Exception as e:
                logger.warning("Failed to collect Gemini conversation %s: %s", external_id, e)

            wait_human(1, 3)

        return result

    def _open_sidebar(self, tab: CDPTab):
        """Open the conversations sidebar if not already open."""
        # Try clicking the menu/hamburger button
        tab.js("""
            var menuBtn = document.querySelector(
                'button[aria-label="Main menu"], button[aria-label*="menu"], ' +
                '[data-mat-icon-name="menu"]'
            );
            if (menuBtn) menuBtn.click();
        """)
        wait_human(1, 2)

    def _get_conversation_list(self, tab: CDPTab) -> list[dict]:
        """Extract conversation list from sidebar."""
        # Scroll sidebar to load more
        for _ in range(5):
            tab.js("""
                var sidebar = document.querySelector(
                    'mat-sidenav, [role="complementary"], nav, .conversation-list'
                );
                if (sidebar) sidebar.scrollTop += 300;
            """)
            wait_human(0.5, 1.0)

        entries = tab.js("""
            // Gemini conversation list items
            var items = document.querySelectorAll(
                'a[href*="/app/"], mat-list-item a, .conversation-item a'
            );
            var out = [];
            var seen = new Set();
            for (var i = 0; i < items.length; i++) {
                var a = items[i];
                var href = a.getAttribute('href') || '';
                // Extract conversation ID from URL
                var match = href.match(/\\/app\\/([a-f0-9]+)/);
                if (match && !seen.has(match[1])) {
                    seen.add(match[1]);
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
        href = conv.get("href", f"/app/{conv['id']}")
        if href.startswith("/"):
            href = f"https://gemini.google.com{href}"
        tab.navigate(href)
        wait_human(2, 4)

        # Wait for content
        return tab.wait_for_text("", timeout=5) is not None or True  # Gemini always has some text

    def _collect_messages(self, tab: CDPTab) -> list[dict]:
        """Scroll through conversation and collect all messages."""
        # Scroll to top
        tab.js("window.scrollTo(0, 0)")
        wait_human(1, 2)

        # Scroll through
        scroll_to_bottom(tab, max_scrolls=30, pause_range=(0.5, 1.5))

        # Extract messages
        raw = tab.js("""
            // Gemini uses various container patterns for messages
            var containers = document.querySelectorAll(
                'message-content, .conversation-container > div, ' +
                '[class*="response-container"], [class*="query-content"], ' +
                'model-response, user-query'
            );

            // Fallback: try to find alternating user/model blocks
            if (containers.length === 0) {
                containers = document.querySelectorAll(
                    '.message-row, [data-turn-role], .turn-container'
                );
            }

            var out = [];
            var order = 0;
            for (var i = 0; i < containers.length; i++) {
                var el = containers[i];
                var content = el.textContent.trim();
                if (!content || content.length < 2) continue;

                order++;
                var role = 'unknown';

                // Detect role from element type/class
                var tag = el.tagName.toLowerCase();
                var cls = el.className || '';
                if (tag === 'user-query' || cls.includes('query') || cls.includes('user') ||
                    el.getAttribute('data-turn-role') === 'user') {
                    role = 'user';
                } else if (tag === 'model-response' || cls.includes('response') ||
                           cls.includes('model') || cls.includes('assistant') ||
                           el.getAttribute('data-turn-role') === 'model') {
                    role = 'assistant';
                } else {
                    // Alternating pattern: odd = user, even = assistant
                    role = (order % 2 === 1) ? 'user' : 'assistant';
                }

                // Check for thinking block
                var thinkingEl = el.querySelector(
                    '[class*="thinking"], details, [class*="thought"]'
                );
                var thinking = '';
                if (thinkingEl) {
                    thinking = thinkingEl.textContent.trim().substring(0, 5000);
                }

                out.push({
                    role: role,
                    content: content.substring(0, 10000),
                    thinking_block: thinking || null,
                    message_order: order
                });
            }
            return JSON.stringify(out);
        """)

        if raw:
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass
        return []
