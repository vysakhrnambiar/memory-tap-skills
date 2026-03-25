"""
ChatGPT Inference Skill — service provider for running prompts via ChatGPT.

CHANGELOG:
  v0.3.0 (2026-03-21):
    - Unified execution flow: single _execute() handles text, images, web search
    - All methods use: fresh chat enforcement, Input.insertText, mouse click send
    - Web search toggle: + → hover More → click Web search (CDP mouse events)
    - Falls back to non-web-search if toggle fails, flags in response
    - Exponential backoff response capture (3-phase: start → stop → stability)
    - Response includes web_search: true/false
  v0.2.0 (2026-03-20):
    - Added execute_prompt_with_image service
    - Image upload via #upload-photos file input + DOM.setFileInputFiles
  v0.1.0 (2026-03-19):
    - Initial version: execute_prompt service

Verified selectors via CDP probe (2026-03-19 → 2026-03-21):
- Input: #prompt-textarea (div contenteditable="true")
- Send button: button[data-testid="send-button"] — use MOUSE CLICK not JS click
- Reply text: [data-message-author-role="assistant"] div.markdown
- Login: __Secure-next-auth.session-token cookie on .chatgpt.com
- Image upload: #upload-photos input[type="file"], DOM.setFileInputFiles
- Prompt typing: Input.insertText after focus (works with contenteditable)
- Web search: + button (composer-plus-btn) → hover More (haspopup=menu) → click Web search (menuitemradio)

Services:
  execute_prompt(prompt, web_search=False)
  execute_prompt_with_image(prompt, image_paths, web_search=False)

__version__ = "0.4.0"
"""
__version__ = "0.4.0"

import json
import logging
import os
import time

import requests as _requests

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


class ChatGPTInferenceSkill(BaseSkill):
    """Service provider — runs prompts on ChatGPT, returns replies.

    Singleton: use ChatGPTInferenceSkill.instance() to get the shared instance.
    Direct API: use instance().execute_direct(payload) for in-process API calls.
    """

    _singleton = None

    @classmethod
    def instance(cls):
        """Get or create the singleton instance."""
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

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
                description="Send a prompt to ChatGPT, optionally with web search",
                input_schema={"prompt": "str", "web_search": "bool (optional)"},
                output_schema={"reply": "str", "web_search": "bool"},
                max_duration_seconds=REPLY_TIMEOUT,
            ),
            ServiceDefinition(
                name="execute_prompt_with_image",
                description="Send a prompt with images to ChatGPT, optionally with web search",
                input_schema={"prompt": "str", "image_paths": "list[str]", "web_search": "bool (optional)"},
                output_schema={"reply": "str", "web_search": "bool"},
                max_duration_seconds=REPLY_TIMEOUT,
            ),
        ]

    # ── Direct API call (no harness, no service_requests) ────

    def execute_direct(self, payload: dict) -> dict:
        """Call LLM API directly, in-process. No harness, no service_requests, no CDP.

        This is the preferred way for skills to call inference when API key is available.
        Falls back to returning error if no API key (caller should use service_requests path).

        payload:
            prompt: str (required)
            web_search: bool (optional, default False) — routes to Perplexity
            force_json: bool (optional, default True) — forces JSON response format

        Returns dict with: reply, model, tokens_in, tokens_out, cost, duration_ms, mode
        Or: error string
        """
        api_key = self._get_openrouter_key()
        if not api_key:
            return {"error": "No API key configured. Set openrouter_api_key in settings or OPENROUTER_API_KEY env var."}

        force_json = payload.get("force_json", True)
        result = self._execute_via_api(payload, force_json=force_json)

        # Log cost
        self._log_api_cost(result)

        return result

    def _log_api_cost(self, result: dict) -> None:
        """Log API call cost to core.db api_cost_log table."""
        if result.get("error") or not result.get("mode") == "api":
            return

        try:
            core_path = os.path.join(
                os.environ.get('LOCALAPPDATA', ''), 'MemoryTap', 'core.db'
            )
            import sqlite3 as _s
            conn = _s.connect(core_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_cost_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    model TEXT,
                    tokens_in INTEGER,
                    tokens_out INTEGER,
                    cost REAL,
                    duration_ms INTEGER,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            conn.execute(
                "INSERT INTO api_cost_log (model, tokens_in, tokens_out, cost, duration_ms) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    result.get("model", ""),
                    result.get("tokens_in", 0),
                    result.get("tokens_out", 0),
                    result.get("cost", 0),
                    result.get("duration_ms", 0),
                )
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.debug("Cost logging failed (non-critical): %s", e)

    def has_api_key(self) -> bool:
        """Check if an API key is configured. Skills use this to decide direct vs service_request."""
        return bool(self._get_openrouter_key())

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

    # ── CDP helpers ─────────────────────────────────────

    @staticmethod
    def _mouse_click(tab, x, y):
        """Send a proper mouse click at coordinates."""
        tab._send("Input.dispatchMouseEvent", {
            "type": "mousePressed", "x": x, "y": y,
            "button": "left", "clickCount": 1
        })
        time.sleep(0.1)
        tab._send("Input.dispatchMouseEvent", {
            "type": "mouseReleased", "x": x, "y": y,
            "button": "left", "clickCount": 1
        })

    @staticmethod
    def _mouse_move(tab, x, y):
        """Move mouse to coordinates (for hover effects)."""
        tab._send("Input.dispatchMouseEvent", {
            "type": "mouseMoved", "x": x, "y": y
        })

    @staticmethod
    def _find_element_pos(tab, js_code, max_attempts=5):
        """Find element position via JS, retry if not found."""
        for _ in range(max_attempts):
            pos = tab.js(js_code)
            if pos and pos != 'null':
                return json.loads(pos)
            time.sleep(2)
        return None

    # ── Shared flow components ─────────────────────────────

    def _open_fresh_chat(self, tab):
        """Wait for ChatGPT to load and ensure a fresh chat with zero messages.

        Raises RuntimeError if page doesn't load or can't get fresh chat.
        """
        # Wait for input field (up to 40s)
        input_found = False
        for _ in range(20):
            if tab.js("return document.querySelector('#prompt-textarea') ? 'f' : ''") == 'f':
                input_found = True
                break
            time.sleep(2)

        if not input_found:
            has_login = tab.js("return document.body.innerText.includes('Log in') ? 'yes' : 'no'")
            if has_login == 'yes':
                raise RuntimeError("Not logged in to ChatGPT")
            raise RuntimeError("ChatGPT page failed to load — input field not found")

        wait_human(3, 5)

        # Ensure zero messages (retry up to 3 times)
        for _fresh in range(3):
            existing = int(tab.js(
                "return document.querySelectorAll('[data-message-author-role]').length;"
            ) or 0)
            if existing == 0:
                return
            logger.info("Existing messages found (%d) — navigating to fresh chat", existing)
            tab.navigate("https://chatgpt.com")
            wait_human(5, 8)
            for _ in range(15):
                if tab.js("return document.querySelector('#prompt-textarea') ? 'f' : ''") == 'f':
                    break
                time.sleep(2)
            wait_human(3, 5)

        final = int(tab.js(
            "return document.querySelectorAll('[data-message-author-role]').length;"
        ) or 0)
        if final > 0:
            raise RuntimeError("Could not get a fresh ChatGPT chat after 3 attempts")

    def _upload_images(self, tab, image_paths):
        """Upload images via #upload-photos file input.

        Raises RuntimeError if upload element not found.
        """
        logger.info("Uploading %d image(s)", len(image_paths))
        tab._send("DOM.enable")
        doc = tab._send("DOM.getDocument")
        search = tab._send("DOM.querySelector", {
            "nodeId": doc["root"]["nodeId"],
            "selector": "#upload-photos"
        })
        file_node_id = search.get("nodeId", 0)
        if not file_node_id:
            raise RuntimeError("Image upload input (#upload-photos) not found")

        abs_paths = [os.path.abspath(p) for p in image_paths]
        tab._send("DOM.setFileInputFiles", {
            "nodeId": file_node_id,
            "files": abs_paths
        })
        logger.info("Images set on file input")
        wait_human(5, 8)

    def _enable_web_search(self, tab) -> bool:
        """Enable web search via + menu → hover More → click Web search.

        Uses CDP mouse events (JS click doesn't work for these menus).
        Returns True if enabled, False if failed (caller should fall back).
        """
        try:
            # Click + button
            p = self._find_element_pos(tab, """
                var btn = document.querySelector('button[data-testid="composer-plus-btn"]');
                if (!btn) return null;
                var r = btn.getBoundingClientRect();
                return JSON.stringify({x: r.x+r.width/2, y: r.y+r.height/2});
            """, max_attempts=3)
            if not p:
                logger.warning("Web search: + button not found")
                return False
            self._mouse_click(tab, p['x'], p['y'])
            time.sleep(3)

            # Hover on More (haspopup=menu — opens on hover, not click)
            p = self._find_element_pos(tab, """
                var items = document.querySelectorAll('[role="menuitem"]');
                for (var i of items) {
                    if (i.textContent.trim().startsWith('More')) {
                        var r = i.getBoundingClientRect();
                        return JSON.stringify({x: r.x+r.width/2, y: r.y+r.height/2});
                    }
                }
                return null;
            """, max_attempts=3)
            if not p:
                logger.warning("Web search: More menu item not found")
                tab.js("document.body.click()")  # dismiss menu
                return False
            self._mouse_move(tab, p['x'], p['y'])
            time.sleep(3)

            # Click Web search
            p = self._find_element_pos(tab, """
                var items = document.querySelectorAll('[role="menuitemradio"]');
                for (var i of items) {
                    if (i.textContent.trim() === 'Web search') {
                        var r = i.getBoundingClientRect();
                        return JSON.stringify({x: r.x+r.width/2, y: r.y+r.height/2});
                    }
                }
                return null;
            """, max_attempts=3)
            if not p:
                logger.warning("Web search: Web search radio item not found")
                tab.js("document.body.click()")
                return False
            self._mouse_move(tab, p['x'], p['y'])
            time.sleep(0.5)
            self._mouse_click(tab, p['x'], p['y'])
            time.sleep(2)

            # Dismiss menu
            tab.js("document.body.click()")
            time.sleep(2)

            logger.info("Web search enabled")
            return True

        except Exception as e:
            logger.warning("Web search toggle failed: %s", e)
            try:
                tab.js("document.body.click()")
            except Exception:
                pass
            return False

    def _type_prompt(self, tab, prompt):
        """Focus input and type prompt via Input.insertText."""
        logger.info("Typing prompt (%d chars)", len(prompt))
        tab.js("""
            var el = document.querySelector('#prompt-textarea, div[contenteditable="true"]');
            if (el) el.focus();
        """)
        wait_human(1, 2)
        tab._send("Input.insertText", {"text": prompt})
        wait_human(2, 3)

    def _click_send(self, tab):
        """Click send button via mouse events. Falls back to JS click.

        Raises RuntimeError if send button not found.
        """
        # Wait for send button
        send_found = False
        for _ in range(10):
            if tab.js("return document.querySelector('button[data-testid=\"send-button\"]') ? 'f' : ''") == 'f':
                send_found = True
                break
            time.sleep(2)

        if not send_found:
            raise RuntimeError("Send button not available after typing prompt")

        # Try mouse click first
        p = self._find_element_pos(tab, """
            var btn = document.querySelector('button[data-testid="send-button"]');
            if (!btn) return null;
            var r = btn.getBoundingClientRect();
            return JSON.stringify({x: r.x+r.width/2, y: r.y+r.height/2});
        """, max_attempts=3)

        if p:
            logger.info("Clicking send (mouse click)")
            self._mouse_click(tab, p['x'], p['y'])
        else:
            # Fallback to JS click
            logger.info("Clicking send (JS fallback)")
            tab.js("""
                var btn = document.querySelector('button[data-testid="send-button"]');
                if (btn) btn.click();
            """)

    def _wait_for_reply(self, tab, timeout: int = REPLY_TIMEOUT) -> str:
        """Wait for ChatGPT reply with exponential backoff stability check.

        Phase 1: Wait for streaming to start (assistant message appears)
                 Also detects "stuck thinking" — thinking dot present but no tokens for 60s
        Phase 2: Wait for streaming to stop (stop button disappears)
        Phase 3: Exponential backoff stability check (2s, 4s, 8s, 16s)
                 — response length must be stable across 2 consecutive checks

        Returns the reply text, or raises RuntimeError on timeout/stuck.
        """
        start = time.time()
        STUCK_TIMEOUT = 300  # 5 minutes of thinking with no tokens = stuck

        # Phase 1: Wait for any assistant message to appear
        logger.info("Waiting for reply to start...")
        thinking_since = None
        while time.time() - start < min(timeout, 180):
            msg_count = int(tab.js(
                "return document.querySelectorAll('[data-message-author-role=\"assistant\"]').length;"
            ) or 0)
            if msg_count > 0:
                # Check if the message has actual text or is still empty (thinking)
                msg_len = int(tab.js("""
                    var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                    if (msgs.length === 0) return 0;
                    return msgs[msgs.length - 1].textContent.trim().length;
                """) or 0)
                if msg_len > 0:
                    logger.info("Reply started (%.1fs)", time.time() - start)
                    break
                # Message exists but empty — thinking dot
                if thinking_since is None:
                    thinking_since = time.time()
                    logger.info("Thinking dot detected...")
                elif time.time() - thinking_since > STUCK_TIMEOUT:
                    raise RuntimeError(f"Stuck: thinking dot for {STUCK_TIMEOUT}s with no tokens")
            else:
                # No message at all — check for thinking indicator in DOM
                has_thinking = tab.js("""
                    var dots = document.querySelector('[class*="result-thinking"], [class*="streaming"]');
                    return dots ? 'yes' : 'no';
                """) or 'no'
                if has_thinking == 'yes':
                    if thinking_since is None:
                        thinking_since = time.time()
                        logger.info("Thinking indicator detected...")
                    elif time.time() - thinking_since > STUCK_TIMEOUT:
                        raise RuntimeError(f"Stuck: thinking for {STUCK_TIMEOUT}s with no response")

            time.sleep(2)
        else:
            raise RuntimeError("No reply started after 180 seconds")

        # Phase 2: Wait for streaming to stop (stop button disappears)
        logger.info("Streaming in progress...")
        while time.time() - start < timeout:
            has_stop = tab.js("""
                var stop = document.querySelector('button[aria-label="Stop"], button[data-testid="stop-button"]');
                return stop ? 'yes' : 'no';
            """) or 'no'
            if has_stop == 'no':
                logger.info("Streaming stopped (%.1fs)", time.time() - start)
                break
            time.sleep(2)
        else:
            raise RuntimeError(f"Reply still streaming after {timeout} seconds")

        # Phase 3: Exponential backoff stability check
        logger.info("Stability check (exponential backoff)...")
        prev_length = 0
        stable_count = 0
        backoff = 2

        while time.time() - start < timeout and backoff <= 16:
            time.sleep(backoff)
            current_length = int(tab.js("""
                var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                if (msgs.length === 0) return 0;
                var last = msgs[msgs.length - 1];
                return last.textContent.trim().length;
            """) or 0)

            if current_length == prev_length and current_length > 0:
                stable_count += 1
                logger.info("  Stable at %d chars (check %d, backoff %ds)",
                            current_length, stable_count, backoff)
                if stable_count >= 2:
                    break
            else:
                stable_count = 0
                logger.info("  Length changed: %d -> %d (backoff %ds)",
                            prev_length, current_length, backoff)

            prev_length = current_length
            backoff = min(backoff * 2, 16)

        # Final extract
        reply = tab.js("""
            return (function() {
                var msgs = document.querySelectorAll('[data-message-author-role="assistant"]');
                if (msgs.length === 0) return '';
                var last = msgs[msgs.length - 1];
                var md = last.querySelector('.markdown');
                return md ? md.textContent.trim() : last.textContent.trim();
            })();
        """) or ""

        elapsed = time.time() - start
        logger.info("Reply captured: %d chars in %.1fs", len(reply), elapsed)
        return reply

    # ── Service handling ─────────────────────────────────────

    # ── OpenRouter API Backend ──────────────────────────────────

    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    DEFAULT_API_MODEL = "google/gemini-3-pro-preview"

    def _get_inference_mode(self) -> str:
        """Get inference mode from settings. Returns 'api', 'browser', or 'auto'."""
        try:
            from src.db.core_db import get_core_connection
            conn = get_core_connection()
            # Check skill settings in core.db
            row = conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = 'chatgpt_inference' AND key = 'inference_mode'"
            ).fetchone()
            if row:
                return row[0]
        except Exception:
            pass

        # Check environment variable as fallback
        mode = os.environ.get('INFERENCE_MODE', 'auto')
        return mode

    def _get_openrouter_key(self) -> str:
        """Get OpenRouter API key from settings or environment."""
        # Try environment first (test harness uses this)
        key = os.environ.get('OPENROUTER_API_KEY', '')
        if key:
            return key

        # Try core.db settings
        try:
            from src.db.core_db import get_core_connection
            conn = get_core_connection()
            row = conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = 'chatgpt_inference' AND key = 'openrouter_api_key'"
            ).fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            pass

        return ''

    def _get_api_model(self) -> str:
        """Get the model to use for API calls."""
        model = os.environ.get('OPENROUTER_MODEL', '')
        if model:
            return model

        try:
            from src.db.core_db import get_core_connection
            conn = get_core_connection()
            row = conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = 'chatgpt_inference' AND key = 'openrouter_model'"
            ).fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            pass

        return self.DEFAULT_API_MODEL

    def _should_use_api(self) -> bool:
        """Determine if we should use API or browser."""
        mode = self._get_inference_mode()
        if mode == 'browser':
            return False
        if mode == 'api':
            return bool(self._get_openrouter_key())

        # auto: use API if key is set
        return bool(self._get_openrouter_key())

    PERPLEXITY_MODEL = "perplexity/sonar-pro-search"

    def _execute_via_api(self, payload: dict, force_json: bool = True) -> dict:
        """Execute prompt via OpenRouter API.

        Returns dict with: reply, web_search, model, tokens_in, tokens_out, cost, duration_ms
        """
        api_key = self._get_openrouter_key()
        if not api_key:
            return {"error": "OpenRouter API key not configured"}

        model = payload.get("model") or self._get_api_model()
        prompt = payload.get("prompt", "")
        want_web_search = payload.get("web_search", False)

        # Web search requested: switch to Perplexity model
        if want_web_search:
            model = self.PERPLEXITY_MODEL
            force_json = False  # Perplexity doesn't support response_format
            logger.info("[INFERENCE] Web search requested — switching to %s", model)

        if not prompt:
            return {"error": "Empty prompt"}

        logger.info("[INFERENCE] API request | model=%s | prompt=%d chars | json=%s",
                    model, len(prompt), force_json)

        start = time.time()

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://kitelight.local",
            "X-Title": "Kite Light Interest Timeline",
        }

        body = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 16384,
        }

        if force_json:
            body["response_format"] = {"type": "json_object"}

        try:
            resp = _requests.post(
                self.OPENROUTER_API_URL,
                headers=headers,
                json=body,
                timeout=120,
            )

            duration_ms = int((time.time() - start) * 1000)

            if resp.status_code != 200:
                error_text = resp.text[:200]
                logger.error("[INFERENCE] API error | status=%d | %s | model=%s",
                             resp.status_code, error_text, model)
                return {"error": f"API error {resp.status_code}: {error_text}"}

            data = resp.json()
            choice = data.get("choices", [{}])[0]
            reply = choice.get("message", {}).get("content", "")

            # Token usage and cost
            usage = data.get("usage", {})
            tokens_in = usage.get("prompt_tokens", 0)
            tokens_out = usage.get("completion_tokens", 0)

            # Cost from OpenRouter (if provided)
            cost = None
            if "usage" in data and "total_cost" in data["usage"]:
                cost = data["usage"]["total_cost"]

            logger.info(
                "[INFERENCE] API success | model=%s | %d chars | "
                "tokens=%d in/%d out | cost=$%.4f | %dms",
                model, len(reply), tokens_in, tokens_out,
                cost or 0, duration_ms
            )

            return {
                "reply": reply,
                "web_search": False,  # API doesn't do web search
                "model": model,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "cost": cost,
                "duration_ms": duration_ms,
                "mode": "api",
            }

        except _requests.Timeout:
            duration_ms = int((time.time() - start) * 1000)
            logger.error("[INFERENCE] API timeout after %dms | model=%s", duration_ms, model)
            return {"error": f"API timeout after {duration_ms}ms"}

        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            logger.error("[INFERENCE] API exception: %s | model=%s | %dms", e, model, duration_ms)
            return {"error": str(e)}

    # ── Request Router ────────────────────────────────────────

    def handle_request(self, service_name: str, payload: dict, cdp: CDPClient) -> dict:
        """Handle a service request. Routes to API or browser based on settings."""
        if service_name not in ("execute_prompt", "execute_prompt_with_image"):
            return {"error": f"Unknown service: {service_name}"}

        with_images = service_name == "execute_prompt_with_image"

        # Images always go to browser (API doesn't support file upload this way)
        if with_images:
            logger.info("[INFERENCE] Image request — using browser mode")
            return self._execute(payload, cdp, with_images=True)

        # Check if we should use API
        use_api = self._should_use_api()
        api_key = self._get_openrouter_key()
        mode = self._get_inference_mode()
        logger.info("[INFERENCE] Routing: mode=%s, api_key=%s, use_api=%s",
                    mode, f"{api_key[:8]}..." if api_key else "NONE", use_api)

        if use_api:
            result = self._execute_via_api(payload)
            if result.get("error"):
                logger.warning("[INFERENCE] API failed: %s — retrying via API",
                               result["error"])
                # Retry API (not fallback to browser)
                time.sleep(5)
                result = self._execute_via_api(payload)

            return result

        # Browser mode
        logger.info("[INFERENCE] Using browser mode (no API key or mode=browser)")
        return self._execute(payload, cdp, with_images=False)

    def _execute(self, payload: dict, cdp: CDPClient, with_images: bool,
                 _retry_count: int = 0) -> dict:
        """Unified execution: open tab → fresh chat → [images] → [web search] → type → send → wait.

        Retries up to 2 times on stuck/timeout errors (fresh tab each retry).

        payload:
            prompt: str — the text prompt (required)
            image_paths: list[str] — absolute paths to images (required if with_images=True)
            web_search: bool — enable web search (optional, default False)
        """
        MAX_RETRIES = 2
        prompt = payload.get("prompt", "")
        image_paths = payload.get("image_paths", [])
        want_web_search = payload.get("web_search", False)

        if not prompt:
            return {"error": "Empty prompt"}
        if with_images and not image_paths:
            return {"error": "No image paths provided"}
        if with_images:
            for p in image_paths:
                if not os.path.isfile(p):
                    return {"error": f"Image file not found: {p}"}

        web_search_enabled = False
        tab = None
        try:
            # Step 1: Open new tab
            logger.info("Opening ChatGPT (prompt=%d chars, images=%d, web_search=%s, attempt=%d)",
                        len(prompt), len(image_paths), want_web_search, _retry_count + 1)
            tab = cdp.new_tab("https://chatgpt.com")

            # Step 2: Fresh chat
            self._open_fresh_chat(tab)

            # Step 3: Upload images (if any)
            if with_images:
                self._upload_images(tab, image_paths)

            # Step 4: Enable web search (if requested)
            if want_web_search:
                web_search_enabled = self._enable_web_search(tab)
                if not web_search_enabled:
                    logger.warning("Web search requested but failed to enable — proceeding without")

            # Step 5: Type prompt
            self._type_prompt(tab, prompt)

            # Step 6: Click send
            self._click_send(tab)

            # Step 7: Wait for reply
            reply = self._wait_for_reply(tab)
            if not reply:
                return {"error": "Empty reply from ChatGPT", "web_search": web_search_enabled}

            logger.info("Reply: %d chars, web_search=%s", len(reply), web_search_enabled)
            return {"reply": reply, "web_search": web_search_enabled}

        except RuntimeError as e:
            error_msg = str(e)
            logger.error("ChatGPT inference failed (attempt %d): %s", _retry_count + 1, error_msg)

            # Close the failed tab
            if tab:
                try:
                    cdp.close_tab(tab)
                except Exception:
                    pass
                tab = None

            # Retry on stuck/timeout errors
            if _retry_count < MAX_RETRIES and ("Stuck" in error_msg or "timeout" in error_msg.lower()
                                                or "No reply started" in error_msg):
                logger.info("Retrying (%d/%d)...", _retry_count + 1, MAX_RETRIES)
                wait_human(3, 5)
                return self._execute(payload, cdp, with_images, _retry_count + 1)

            return {"error": error_msg, "web_search": web_search_enabled}

        except Exception as e:
            logger.error("ChatGPT inference unexpected error: %s", e)
            return {"error": str(e), "web_search": web_search_enabled}

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
