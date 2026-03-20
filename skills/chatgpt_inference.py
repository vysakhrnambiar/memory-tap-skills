"""
ChatGPT Inference Skill — service provider for running prompts via ChatGPT.

CHANGELOG:
  v0.2.0 (2026-03-20):
    - Added execute_prompt_with_image service
    - Image upload via #upload-photos file input + DOM.setFileInputFiles
    - Supports multiple images (list of absolute paths)
    - Prompt typed via Input.insertText (works with contenteditable)
    - Probed and verified on MKBHD channel analysis (3400 char detailed response)
  v0.1.0 (2026-03-19):
    - Initial version: execute_prompt service
    - Opens new chat, types prompt, waits for reply (copy button detection)
    - 10 minute timeout, always returns data or error
    - Tab managed per request (open/close in finally)

Verified selectors via CDP probe (2026-03-19, 2026-03-20):
- Input: #prompt-textarea (div contenteditable="true")
- Send button: button[data-testid="send-button"] (appears after text typed)
- Reply completion: button[data-testid="copy-turn-action-button"] or
                    button[data-testid="good-response-turn-action-button"]
- Reply text: [data-message-author-role="assistant"] div.markdown
- Login: __Secure-next-auth.session-token cookie on .chatgpt.com
- Image upload: #upload-photos input[type="file"] accepts image/*
- Image upload method: DOM.setFileInputFiles (no click needed)
- Prompt with images: Input.insertText after focus on input element

This skill has NO collect() — it is a service provider only.
Other skills invoke it via:
  request_service("chatgpt_inference.execute_prompt", {"prompt": "..."})
  request_service("chatgpt_inference.execute_prompt_with_image", {"prompt": "...", "image_paths": [...]})

__version__ = "0.2.0"
"""
__version__ = "0.2.0"

import json
import logging
import time

from src.cdp_client import CDPClient, CDPTab
from src.skills.base import (
    BaseSkill, SkillManifest, ServiceDefinition, StopStrategy,
    CollectResult, RunLimits,
)
from src.db.sync_tracker import SyncTracker
from src.human import wait_human

logger = logging.getLogger("memory_tap.skill.chatgpt_inf")

# Max wait for ChatGPT reply (seconds)
REPLY_TIMEOUT = 600  # 10 minutes
POLL_INTERVAL = 2    # check every 2 seconds


class ChatGPTInferenceSkill(BaseSkill):
    """Service provider — runs prompts on ChatGPT, returns replies."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="chatgpt_inference",
            version=__version__,
            target_url="https://chatgpt.com",
            description="Runs prompts on ChatGPT and returns replies. Service provider for other skills.",
            auth_provider="openai",
            schedule_hours=0,  # No scheduled runs — service only
            login_url="https://chatgpt.com/auth/login",
            max_items_first_run=0,
            max_items_per_run=0,
            max_minutes_per_run=15,
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.CONSECUTIVE_KNOWN

    def get_services(self):
        return [
            ServiceDefinition(
                name="execute_prompt",
                description="Send a raw prompt to ChatGPT, return the full reply",
                input_schema={"prompt": "str"},
                output_schema={"reply": "str"},
                max_duration_seconds=REPLY_TIMEOUT,
            ),
            ServiceDefinition(
                name="execute_prompt_with_image",
                description="Send a prompt with one or more images to ChatGPT, return the full reply",
                input_schema={"prompt": "str", "image_paths": "list[str]"},
                output_schema={"reply": "str"},
                max_duration_seconds=REPLY_TIMEOUT,
            ),
        ]

    # ── No collect — this is a service-only skill ────────────

    def create_schema(self, conn):
        """No data tables needed — inference skill doesn't store data."""
        pass

    def migrate_schema(self, conn, old_version: str, new_version: str) -> None:
        """No schema to migrate — service-only skill."""
        pass

    def collect(self, tab, tracker, limits):
        """Not used — this skill only handles service requests."""
        return CollectResult()

    def should_stop_collecting(self, item, tracker):
        return True

    def check_login(self, tab) -> bool:
        """Check if logged into ChatGPT via cookies."""
        result = tab._send("Network.getCookies")
        cookies = result.get("cookies", [])
        for c in cookies:
            if "session-token" in c.get("name", "") and ".chatgpt.com" in c.get("domain", ""):
                logger.info("ChatGPT Inference: logged in (session-token cookie found)")
                return True
        logger.info("ChatGPT Inference: not logged in")
        return False

    # ── Service handling ─────────────────────────────────────

    def handle_request(self, service_name: str, payload: dict, cdp: CDPClient) -> dict:
        """Handle a service request. Always returns dict with data or error."""
        if service_name == "execute_prompt":
            return self._execute_prompt(payload, cdp)
        if service_name == "execute_prompt_with_image":
            return self._execute_prompt_with_image(payload, cdp)
        return {"error": f"Unknown service: {service_name}"}

    def _execute_prompt(self, payload: dict, cdp: CDPClient) -> dict:
        """Send prompt to ChatGPT, wait for reply, return it."""
        prompt = payload.get("prompt", "")
        if not prompt:
            return {"error": "Empty prompt"}

        tab = None
        try:
            # Step 1: Open new tab to ChatGPT
            logger.info("Opening ChatGPT for prompt (%d chars)", len(prompt))
            tab = cdp.new_tab("https://chatgpt.com")

            # Step 2: Wait for input field
            input_found = False
            for _ in range(15):  # 30 seconds max
                check = tab.js('''
                    return document.querySelector('#prompt-textarea') ? 'found' : '';
                ''')
                if check == 'found':
                    input_found = True
                    break
                time.sleep(2)

            if not input_found:
                # Check if login issue
                has_login = tab.js('''
                    return document.body.innerText.includes('Log in') ? 'yes' : 'no';
                ''')
                if has_login == 'yes':
                    return {"error": "Not logged in to ChatGPT"}
                return {"error": "ChatGPT page failed to load — input field not found"}

            # Step 3: Verify it's a new chat (no existing messages)
            existing = tab.js('''
                return document.querySelectorAll('[data-message-author-role]').length;
            ''') or 0
            if int(existing) > 0:
                # Navigate to fresh chat
                tab.navigate("https://chatgpt.com")
                wait_human(3, 5)

            # Step 4: Type the prompt
            logger.info("Typing prompt into ChatGPT")
            # Escape the prompt for JS
            safe_prompt = prompt.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
            tab.js(f'''
                var input = document.querySelector('#prompt-textarea');
                if (input) {{
                    input.focus();
                    input.textContent = `{safe_prompt}`;
                    input.dispatchEvent(new Event('input', {{bubbles: true}}));
                }}
            ''')
            wait_human(1, 2)

            # Step 5: Verify send button appeared
            send_found = False
            for _ in range(5):
                check = tab.js('''
                    return document.querySelector('button[data-testid="send-button"]') ? 'found' : '';
                ''')
                if check == 'found':
                    send_found = True
                    break
                time.sleep(1)

            if not send_found:
                return {"error": "Send button not available after typing prompt"}

            # Step 6: Click send
            logger.info("Clicking send")
            tab.js('''
                var btn = document.querySelector('button[data-testid="send-button"]');
                if (btn) btn.click();
            ''')

            # Step 7: Poll for completion (copy button or thumbs up)
            logger.info("Waiting for reply (max %ds)...", REPLY_TIMEOUT)
            start = time.time()
            reply_complete = False

            while time.time() - start < REPLY_TIMEOUT:
                time.sleep(POLL_INTERVAL)

                done = tab.js('''
                    return (function() {
                        var copy = document.querySelector('button[data-testid="copy-turn-action-button"]');
                        var thumbUp = document.querySelector('button[data-testid="good-response-turn-action-button"]');
                        var thumbDown = document.querySelector('button[data-testid="bad-response-turn-action-button"]');
                        if (copy || thumbUp || thumbDown) return 'done';
                        return '';
                    })();
                ''')
                if done == 'done':
                    reply_complete = True
                    elapsed = time.time() - start
                    logger.info("Reply received in %.1fs", elapsed)
                    break

            if not reply_complete:
                return {"error": f"Reply timeout after {REPLY_TIMEOUT} seconds"}

            # Step 8: Extract reply text
            reply = tab.js('''
                return (function() {
                    var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                    if (msgs.length === 0) return '';
                    var last = msgs[msgs.length - 1];
                    var md = last.querySelector('.markdown');
                    return md ? md.textContent.trim() : last.textContent.trim();
                })();
            ''') or ""

            if not reply:
                return {"error": "Empty reply from ChatGPT"}

            logger.info("Reply: %s", reply[:80])
            return {"reply": reply}

        except Exception as e:
            logger.error("ChatGPT inference failed: %s", e)
            return {"error": str(e)}

        finally:
            # Step 10: Always close tab
            if tab:
                try:
                    cdp.close_tab(tab)
                except Exception:
                    pass

    def _execute_prompt_with_image(self, payload: dict, cdp: CDPClient) -> dict:
        """Send prompt with images to ChatGPT, wait for reply, return it.

        payload:
            prompt: str — the text prompt
            image_paths: list[str] — absolute paths to image files (png/jpg)
        """
        prompt = payload.get("prompt", "")
        image_paths = payload.get("image_paths", [])

        if not prompt:
            return {"error": "Empty prompt"}
        if not image_paths:
            return {"error": "No image paths provided"}

        # Validate all image files exist
        import os
        for p in image_paths:
            if not os.path.isfile(p):
                return {"error": f"Image file not found: {p}"}

        tab = None
        try:
            # Step 1: Open new tab to ChatGPT
            logger.info("Opening ChatGPT for image prompt (%d chars, %d images)",
                        len(prompt), len(image_paths))
            tab = cdp.new_tab("https://chatgpt.com")

            # Step 2: Wait for page to fully load (generous wait)
            input_found = False
            for _ in range(20):  # 40 seconds max
                check = tab.js('''
                    return document.querySelector('#prompt-textarea') ? 'found' : '';
                ''')
                if check == 'found':
                    input_found = True
                    break
                time.sleep(2)

            if not input_found:
                has_login = tab.js('''
                    return document.body.innerText.includes('Log in') ? 'yes' : 'no';
                ''')
                if has_login == 'yes':
                    return {"error": "Not logged in to ChatGPT"}
                return {"error": "ChatGPT page failed to load — input field not found"}

            # Extra settle time after page load
            wait_human(3, 5)

            # Step 3: Verify it's a new chat
            existing = tab.js('''
                return document.querySelectorAll('[data-message-author-role]').length;
            ''') or 0
            if int(existing) > 0:
                tab.navigate("https://chatgpt.com")
                wait_human(5, 8)

            # Step 4: Upload images via #upload-photos file input
            logger.info("Uploading %d image(s)", len(image_paths))
            tab._send("DOM.enable")
            doc = tab._send("DOM.getDocument")
            search = tab._send("DOM.querySelector", {
                "nodeId": doc["root"]["nodeId"],
                "selector": "#upload-photos"
            })
            file_node_id = search.get("nodeId", 0)
            if not file_node_id:
                return {"error": "Image upload input (#upload-photos) not found"}

            # Convert to absolute paths (DOM.setFileInputFiles requires absolute)
            abs_paths = [os.path.abspath(p) for p in image_paths]
            tab._send("DOM.setFileInputFiles", {
                "nodeId": file_node_id,
                "files": abs_paths
            })
            logger.info("Images set on file input")

            # Wait for images to upload and preview to appear
            wait_human(5, 8)

            # Step 5: Type the prompt using Input.insertText
            logger.info("Typing prompt")
            tab.js('''
                var el = document.querySelector('#prompt-textarea, div[contenteditable="true"]');
                if (el) el.focus();
            ''')
            wait_human(1, 2)
            tab._send("Input.insertText", {"text": prompt})
            wait_human(2, 3)

            # Step 6: Wait for send button and click
            send_found = False
            for _ in range(10):  # 20 seconds max
                check = tab.js('''
                    var btn = document.querySelector('button[data-testid="send-button"]');
                    if (btn) return 'found';
                    // Fallback: any submit-like button in form
                    var form = document.querySelector('form');
                    if (form) {
                        var btns = form.querySelectorAll('button[type="button"]');
                        for (var b of btns) {
                            var label = b.getAttribute('aria-label') || '';
                            if (label.includes('Send')) return 'found';
                        }
                    }
                    return '';
                ''')
                if check == 'found':
                    send_found = True
                    break
                time.sleep(2)

            if not send_found:
                return {"error": "Send button not available after typing prompt with image"}

            # Count existing messages BEFORE sending (to detect new ones)
            msg_count_before = int(tab.js('''
                return document.querySelectorAll('[data-message-author-role="assistant"]').length;
            ''') or 0)
            logger.info("Messages before send: %d", msg_count_before)

            logger.info("Clicking send")
            tab.js('''
                var btn = document.querySelector('button[data-testid="send-button"]');
                if (btn) { btn.click(); return; }
                var form = document.querySelector('form');
                if (form) {
                    var btns = form.querySelectorAll('button');
                    for (var b of btns) {
                        var label = b.getAttribute('aria-label') || '';
                        if (label.includes('Send')) { b.click(); return; }
                    }
                }
            ''')

            # Step 7: Poll for NEW response completion
            # Wait for: message count to increase AND stop button to disappear
            logger.info("Waiting for reply (max %ds, image analysis may take longer)...",
                        REPLY_TIMEOUT)
            start = time.time()
            reply_complete = False

            while time.time() - start < REPLY_TIMEOUT:
                time.sleep(POLL_INTERVAL)

                status = tab.js('''
                    return (function() {
                        var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                        var stop = document.querySelector('button[aria-label="Stop"], button[data-testid="stop-button"]');
                        return JSON.stringify({count: msgs.length, streaming: !!stop});
                    })();
                ''') or '{"count":0,"streaming":false}'

                import json as _json
                try:
                    st = _json.loads(status)
                except (ValueError, TypeError):
                    st = {"count": 0, "streaming": False}

                msg_count = st.get("count", 0)
                is_streaming = st.get("streaming", False)

                # New message appeared and streaming finished
                if msg_count > msg_count_before and not is_streaming:
                    reply_complete = True
                    elapsed = time.time() - start
                    logger.info("Reply received in %.1fs", elapsed)
                    break

            if not reply_complete:
                return {"error": f"Reply timeout after {REPLY_TIMEOUT} seconds"}

            # Extra wait after completion to ensure full content rendered
            wait_human(3, 5)

            # Step 8: Extract reply text (get the LAST assistant message)
            reply = tab.js('''
                return (function() {
                    var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                    if (msgs.length === 0) return '';
                    var last = msgs[msgs.length - 1];
                    var md = last.querySelector('.markdown');
                    return md ? md.textContent.trim() : last.textContent.trim();
                })();
            ''') or ""

            if not reply:
                return {"error": "Empty reply from ChatGPT"}

            logger.info("Image prompt reply: %d chars", len(reply))
            return {"reply": reply}

        except Exception as e:
            logger.error("ChatGPT image inference failed: %s", e)
            return {"error": str(e)}

        finally:
            if tab:
                try:
                    cdp.close_tab(tab)
                except Exception:
                    pass

    # ── UI (minimal — service skills don't need widgets) ─────

    def get_widgets(self):
        return []

    def get_page_sections(self):
        return []

    def get_notification_rules(self):
        return []

    def get_stats(self, conn):
        return []

    def get_search_results(self, conn, query, limit=20):
        return []
