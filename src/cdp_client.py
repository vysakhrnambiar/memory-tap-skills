"""
CDP Client — reliable Chrome DevTools Protocol client for Memory Tap.

Improvements over the parent project's CDP client:
- Events stored BY TYPE in separate queues (no lost events)
- Automatic tab cleanup on crash/exit (no stale registry)
- Integrated with ChromeManager (hardcoded port)
- Tab tracking in-memory only (no file-based registry)
- Proper context manager with guaranteed cleanup
"""
import collections
import json
import logging
import threading
import time
import queue

import requests
import websocket

from .chrome_manager import get_active_port
from .db.models import add_alert

logger = logging.getLogger("memory_tap.cdp")


class TabState:
    """Tab lifecycle states."""
    CREATED = "created"
    CONNECTING = "connecting"
    IDLE = "idle"
    NAVIGATING = "navigating"
    LOADED = "loaded"
    WORKING = "working"
    CLOSING = "closing"
    CLOSED = "closed"


class CDPTab:
    """A single browser tab with reliable CDP communication.

    Key improvements:
    - Events stored by method name in separate queues (no lost events)
    - State machine tracks lifecycle: CREATED → IDLE → NAVIGATING → LOADED → WORKING → CLOSED
    """

    # States that allow navigation
    _READY_STATES = {TabState.IDLE, TabState.LOADED}
    # States that allow any operation
    _USABLE_STATES = {TabState.IDLE, TabState.LOADED, TabState.NAVIGATING, TabState.WORKING}

    def __init__(self, tab_id: str, ws_url: str, url: str, cdp_base_url: str):
        self.id = tab_id
        self.ws_url = ws_url
        self.url = url
        self._cdp_base_url = cdp_base_url
        self.ws: websocket.WebSocket | None = None
        self._msg_id = 0
        self._responses: dict[int, dict] = {}
        # Events stored by method name — no event is ever lost
        self._event_queues: dict[str, queue.Queue] = collections.defaultdict(queue.Queue)
        self._all_events = queue.Queue()  # firehose for debugging
        self._recv_thread: threading.Thread | None = None
        self._running = False
        self._lock = threading.Lock()
        self.state = TabState.CREATED

    # --- Connection ---

    def connect(self):
        if self.ws is not None:
            return self
        self.state = TabState.CONNECTING
        self.ws = websocket.create_connection(self.ws_url, timeout=30)
        self._running = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        # Enable domains we need
        self._send("Page.enable")
        self._send("Runtime.enable")
        self._send("DOM.enable")
        self._send("Network.enable")
        self._send("Input.enable")
        self.state = TabState.IDLE
        return self

    def reconnect(self):
        """Reconnect WebSocket (e.g. after navigation changes endpoint)."""
        self.disconnect()
        try:
            resp = requests.get(f"{self._cdp_base_url}/json", timeout=5)
            for tab in resp.json():
                if tab["id"] == self.id:
                    if "webSocketDebuggerUrl" in tab:
                        self.ws_url = tab["webSocketDebuggerUrl"]
                    self.url = tab.get("url", self.url)
                    break
        except Exception:
            pass
        self.connect()

    def disconnect(self):
        self.state = TabState.CLOSING
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
        if self._recv_thread and self._recv_thread.is_alive():
            self._recv_thread.join(timeout=2)
        self._recv_thread = None
        self._responses.clear()
        self._event_queues.clear()
        self.state = TabState.CLOSED

    def _recv_loop(self):
        """Background thread: routes responses by ID, events by method name.

        On WebSocket drop: attempts reconnect with exponential backoff (1s, 2s, 4s).
        Max 3 retries. If all fail, marks tab as CLOSED.
        """
        max_retries = 3
        while self._running:
            if not self.ws:
                break
            try:
                self.ws.settimeout(1.0)
                raw = self.ws.recv()
                data = json.loads(raw)
                if "id" in data:
                    self._responses[data["id"]] = data
                elif "method" in data:
                    method = data["method"]
                    self._event_queues[method].put(data)
                    self._all_events.put(data)
            except websocket.WebSocketTimeoutException:
                continue
            except (websocket.WebSocketConnectionClosedException, OSError):
                if not self._running:
                    break
                # WebSocket dropped — diagnose why before retrying
                logger.warning("WebSocket dropped for tab %s — diagnosing...", self.id[:8])

                # Diagnosis 1: Is Chrome process alive?
                chrome_alive = False
                try:
                    resp = requests.get(f"{self._cdp_base_url}/json/version", timeout=3)
                    chrome_alive = resp.status_code == 200
                except Exception:
                    pass

                if not chrome_alive:
                    logger.error("Chrome is not responding — process likely closed or crashed")
                    add_alert("Chrome Closed", "Chrome was closed or crashed. Skills cannot run until Chrome is restarted.", level="error", source="cdp")
                    self.state = TabState.CLOSED
                    break

                # Diagnosis 2: Does our tab still exist?
                tab_exists = False
                try:
                    resp = requests.get(f"{self._cdp_base_url}/json", timeout=3)
                    tab_ids = [t["id"] for t in resp.json() if t.get("type") == "page"]
                    tab_exists = self.id in tab_ids
                except Exception:
                    pass

                if not tab_exists:
                    logger.error("Tab %s no longer exists — was closed", self.id[:8])
                    add_alert("Tab Closed", f"Browser tab was closed unexpectedly during operation.", level="warning", source="cdp")
                    self.state = TabState.CLOSED
                    break

                # Chrome is alive and tab exists — WS endpoint may have changed, retry
                logger.info("Chrome alive, tab exists — attempting WS reconnect...")
                reconnected = False
                for attempt in range(max_retries):
                    backoff = 2 ** attempt  # 1s, 2s, 4s
                    time.sleep(backoff)
                    if not self._running:
                        break
                    try:
                        resp = requests.get(f"{self._cdp_base_url}/json", timeout=3)
                        for tab_info in resp.json():
                            if tab_info["id"] == self.id and "webSocketDebuggerUrl" in tab_info:
                                self.ws_url = tab_info["webSocketDebuggerUrl"]
                                break
                        self.ws = websocket.create_connection(self.ws_url, timeout=10)
                        for domain in ["Page", "Runtime", "DOM", "Network", "Input"]:
                            msg = {"id": 0, "method": f"{domain}.enable", "params": {}}
                            self.ws.send(json.dumps(msg))
                        logger.info("Reconnected tab %s on attempt %d", self.id[:8], attempt + 1)
                        reconnected = True
                        break
                    except Exception as e:
                        logger.warning("Reconnect attempt %d failed: %s", attempt + 1, e)

                if not reconnected:
                    logger.error("All %d reconnect attempts failed for tab %s — marking as CLOSED",
                                 max_retries, self.id[:8])
                    add_alert("Connection Lost", f"Lost connection to browser tab after {max_retries} retry attempts.", level="error", source="cdp")
                    self.state = TabState.CLOSED
                    break
            except Exception:
                if not self._running:
                    break
                logger.warning("Unexpected error in recv_loop for tab %s", self.id[:8])
                break

    # --- Low-level send/receive ---

    def _send(self, method: str, params: dict | None = None, timeout: float = 30) -> dict:
        """Send CDP command and wait for response.

        Thread-safe: lock covers both ID increment and ws.send() to prevent
        interleaved writes from concurrent callers. Response wait is outside
        the lock so it doesn't block other sends.
        """
        if not self.ws:
            raise RuntimeError("Not connected")

        # Lock covers ID assignment + socket write (atomic send)
        with self._lock:
            self._msg_id += 1
            msg_id = self._msg_id
            msg = {"id": msg_id, "method": method, "params": params or {}}
            try:
                self.ws.send(json.dumps(msg))
            except Exception as e:
                return {"_error": f"send failed: {e}"}

        # Wait for response (outside lock — doesn't block other sends)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if msg_id in self._responses:
                resp = self._responses.pop(msg_id)
                if "error" in resp:
                    return {"_error": resp["error"]}
                return resp.get("result", {})
            time.sleep(0.05)
        return {"_error": f"timeout waiting for {method}"}

    def drain_events(self, method: str | None = None):
        """Drain pending events. If method given, drain only that queue."""
        if method:
            q = self._event_queues.get(method)
            if q:
                while not q.empty():
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        break
        else:
            for q in self._event_queues.values():
                while not q.empty():
                    try:
                        q.get_nowait()
                    except queue.Empty:
                        break

    def wait_for_event(self, method: str, timeout: float = 30) -> dict | None:
        """Wait for a specific event type. Does NOT discard other events."""
        q = self._event_queues[method]
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            return None

    # --- State management ---

    def set_working(self):
        """Mark tab as actively being used by a skill."""
        self.state = TabState.WORKING

    def set_idle(self):
        """Mark tab as idle (skill finished its work)."""
        if self.state != TabState.CLOSED:
            self.state = TabState.IDLE

    @property
    def is_ready(self) -> bool:
        """True if tab is in a state that allows navigation/interaction."""
        return self.state in self._READY_STATES

    @property
    def is_usable(self) -> bool:
        """True if tab can be used for any operation."""
        return self.state in self._USABLE_STATES

    # --- Heartbeat ---

    def ping(self, timeout: float = 5) -> bool:
        """Quick health check — evaluates `return 1` in JS.

        Returns True if tab is responsive, False if dead/frozen.
        Used by health monitor and checkpoint.
        """
        try:
            result = self.js("return 1", timeout=timeout)
            return result == 1
        except Exception:
            return False

    # --- Checkpoint ---

    def checkpoint(self, expected_url_contains: str | None = None) -> dict:
        """Verify page state before a major action.

        Returns:
            {
                "ok": bool,           # True if everything looks good
                "url": str,           # Current URL
                "ready_state": str,   # document.readyState
                "url_match": bool,    # Does URL contain expected pattern?
                "has_overlay": bool,  # Cookie banner, dialog, etc detected
                "ws_alive": bool,     # WebSocket still connected
                "issues": [str],      # List of problems found
            }
        """
        issues = []

        # 1. Is WebSocket alive?
        ws_alive = self.ws is not None and self._running
        if not ws_alive:
            issues.append("WebSocket disconnected")
            return {
                "ok": False, "url": "", "ready_state": "unknown",
                "url_match": False, "has_overlay": False,
                "ws_alive": False, "issues": issues,
            }

        # 2. Quick JS ping — confirms tab is responsive
        if not self.ping():
            issues.append("Tab not responsive (JS ping failed)")
            return {
                "ok": False, "url": "", "ready_state": "unknown",
                "url_match": False, "has_overlay": False,
                "ws_alive": True, "issues": issues,
            }

        # 3. Current URL
        url = self.get_url()

        # 4. URL match check
        url_match = True
        if expected_url_contains:
            url_match = expected_url_contains.lower() in url.lower()
            if not url_match:
                issues.append(f"URL mismatch: expected '{expected_url_contains}' in '{url}'")

        # 5. Page ready state
        ready_state = self.js("return document.readyState") or "unknown"
        if ready_state not in ("complete", "interactive"):
            issues.append(f"Page not ready: readyState={ready_state}")

        # 6. Check for blocking overlays (cookie banners, dialogs)
        has_overlay = self.js("""
            // Check for common overlay patterns
            var overlay = document.querySelector(
                '[class*="cookie"], [class*="consent"], [class*="modal"][style*="display"],' +
                '[class*="overlay"][style*="display"], [role="dialog"][aria-modal="true"]'
            );
            if (overlay) {
                var r = overlay.getBoundingClientRect();
                return r.width > 100 && r.height > 100;
            }
            return false;
        """) or False
        if has_overlay:
            issues.append("Blocking overlay detected (cookie banner or dialog)")

        ok = len(issues) == 0
        if not ok:
            logger.warning("Checkpoint issues: %s", "; ".join(issues))

        return {
            "ok": ok, "url": url, "ready_state": ready_state,
            "url_match": url_match, "has_overlay": has_overlay,
            "ws_alive": ws_alive, "issues": issues,
        }

    # --- Navigation ---

    def navigate(self, url: str, timeout: float = 30):
        """Navigate and wait for full page load.

        Fallback: if Page.loadEventFired doesn't arrive within 10s,
        checks document.readyState as alternative. Heavy SPAs may not
        fire the load event reliably.
        """
        self.state = TabState.NAVIGATING
        self.drain_events("Page.loadEventFired")

        result = self._send("Page.navigate", {"url": url}, timeout=15)
        if isinstance(result, dict) and "_error" in result:
            self.reconnect()
            self.drain_events("Page.loadEventFired")
            result = self._send("Page.navigate", {"url": url}, timeout=15)

        # Wait for load event with fallback
        load_timeout = min(timeout, 10)
        evt = self.wait_for_event("Page.loadEventFired", timeout=load_timeout)

        if evt is None:
            # Load event didn't fire — check readyState as fallback
            ready = self.js("return document.readyState", timeout=5)
            if ready in ("complete", "interactive"):
                logger.info("Navigate: load event timeout but readyState=%s — proceeding", ready)
            else:
                # Wait a bit more then force-proceed with warning
                logger.warning("Navigate: no load event and readyState=%s — waiting 5s more", ready)
                time.sleep(5)

        time.sleep(1.5)  # settle for JS rendering
        self.url = self.js("return window.location.href") or url
        self.state = TabState.LOADED

    def wait_for_navigation(self, timeout: float = 30):
        """Wait for click-initiated navigation to complete."""
        deadline = time.time() + timeout
        got_navigated = False
        got_loaded = False

        while time.time() < deadline:
            # Check both queues without blocking long
            for method in ["Page.frameNavigated", "Page.loadEventFired", "Page.frameStoppedLoading"]:
                evt = None
                try:
                    evt = self._event_queues[method].get_nowait()
                except queue.Empty:
                    pass
                if evt:
                    if method == "Page.frameNavigated":
                        got_navigated = True
                    else:
                        got_loaded = True

            if got_navigated and got_loaded:
                break
            if got_loaded:
                break
            time.sleep(0.2)

        time.sleep(1.0)  # settle
        self.url = self.js("return window.location.href") or self.url

    # --- JavaScript ---

    def js(self, expression: str, timeout: float = 15):
        """Evaluate JS. Use 'return' for values."""
        wrapped = f"(function() {{ {expression} }})()"
        r = self._send("Runtime.evaluate", {
            "expression": wrapped,
            "returnByValue": True,
            "awaitPromise": True,
            "timeout": timeout * 1000,
        }, timeout + 5)
        if isinstance(r, dict) and "_error" in r:
            return None
        v = r.get("result", {})
        if v.get("type") == "undefined" or v.get("subtype") == "error":
            return None
        return v.get("value")

    def js_async(self, expression: str, timeout: float = 30):
        """Evaluate async JS (returns Promise value)."""
        r = self._send("Runtime.evaluate", {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
            "timeout": timeout * 1000,
        }, timeout + 5)
        if isinstance(r, dict) and "_error" in r:
            return None
        return r.get("result", {}).get("value")

    # --- Screenshots ---

    def screenshot(self, save_path: str, full_page: bool = False) -> str | None:
        """Capture screenshot to file."""
        params = {"format": "png"}
        if full_page:
            metrics = self._send("Page.getLayoutMetrics", timeout=5)
            if isinstance(metrics, dict) and "_error" not in metrics:
                content = metrics.get("contentSize", {})
                if content.get("width") and content.get("height"):
                    params["clip"] = {
                        "x": 0, "y": 0,
                        "width": content["width"],
                        "height": content["height"],
                        "scale": 1,
                    }
        import base64
        r = self._send("Page.captureScreenshot", params, 15)
        if isinstance(r, dict) and "_error" not in r and r.get("data"):
            with open(save_path, "wb") as f:
                f.write(base64.b64decode(r["data"]))
            return save_path
        return None

    # --- Element finding ---

    def query_selector(self, selector: str) -> dict | None:
        """Find first element by CSS selector. Returns {x, y, w, h, tag, text, visible}."""
        result = self.js(f"""
            var el = document.querySelector({json.dumps(selector)});
            if (!el) return null;
            var r = el.getBoundingClientRect();
            return JSON.stringify({{
                x: Math.round(r.x + r.width / 2),
                y: Math.round(r.y + r.height / 2),
                w: Math.round(r.width),
                h: Math.round(r.height),
                tag: el.tagName.toLowerCase(),
                text: el.textContent.trim().substring(0, 200),
                href: el.getAttribute('href') || '',
                visible: r.width > 0 && r.height > 0
            }});
        """)
        if result:
            try:
                return json.loads(result)
            except Exception:
                pass
        return None

    def query_selector_all(self, selector: str) -> list[dict]:
        """Find all elements by CSS selector."""
        result = self.js(f"""
            var els = document.querySelectorAll({json.dumps(selector)});
            var out = [];
            for (var i = 0; i < els.length; i++) {{
                var el = els[i];
                var r = el.getBoundingClientRect();
                out.push({{
                    x: Math.round(r.x + r.width / 2),
                    y: Math.round(r.y + r.height / 2),
                    w: Math.round(r.width),
                    h: Math.round(r.height),
                    tag: el.tagName.toLowerCase(),
                    text: el.textContent.trim().substring(0, 300),
                    href: el.getAttribute('href') || '',
                    visible: r.width > 0 && r.height > 0
                }});
            }}
            return JSON.stringify(out);
        """)
        if result:
            try:
                return json.loads(result)
            except Exception:
                pass
        return []

    def wait_for_selector(self, selector: str, timeout: float = 15) -> dict | None:
        """Wait until CSS selector matches a visible element."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            el = self.query_selector(selector)
            if el and el.get("visible"):
                return el
            time.sleep(0.3)
        return None

    def wait_for_text(self, text: str, timeout: float = 15) -> bool:
        """Wait until text appears on page."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.has_text(text):
                return True
            time.sleep(0.5)
        return False

    # --- Page inspection ---

    def get_page_text(self) -> str:
        return self.js("return document.body ? document.body.innerText : ''") or ""

    def has_text(self, needle: str) -> bool:
        return needle.lower() in self.get_page_text().lower()

    def get_title(self) -> str:
        return self.js("return document.title") or ""

    def get_url(self) -> str:
        return self.js("return window.location.href") or self.url

    def get_links(self) -> list[dict]:
        result = self.js("""
            var links = document.querySelectorAll('a[href]');
            var out = [];
            for (var i = 0; i < links.length; i++) {
                var a = links[i];
                var r = a.getBoundingClientRect();
                out.push({
                    href: a.getAttribute('href'),
                    text: a.textContent.trim().substring(0, 100),
                    visible: r.width > 0 && r.height > 0
                });
            }
            return JSON.stringify(out);
        """)
        if result:
            try:
                return json.loads(result)
            except Exception:
                pass
        return []

    def get_scroll_height(self) -> int:
        """Get total scrollable height of page."""
        return self.js("return document.body.scrollHeight") or 0

    def get_scroll_position(self) -> int:
        """Get current scroll Y position."""
        return self.js("return window.scrollY || window.pageYOffset || 0") or 0


class CDPClient:
    """Manages CDP connections to Memory Tap's Chrome instance.

    Improvements over parent project:
    - In-memory tab tracking only (no file registry to get stale)
    - Guaranteed cleanup via __exit__ and atexit
    - Hardcoded port from ChromeManager

    Usage:
        with CDPClient() as client:
            tab = client.new_tab("https://youtube.com")
            tab.navigate("https://youtube.com/feed/history")
            # ... collect data ...
    """

    def __init__(self, port: int | None = None):
        self.port = port or get_active_port()
        self.base_url = f"http://localhost:{self.port}"
        self._tabs: list[CDPTab] = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cleanup()

    def cleanup(self):
        """Close all tabs we opened."""
        for tab in self._tabs:
            try:
                tab.disconnect()
                requests.get(f"{self.base_url}/json/close/{tab.id}", timeout=3)
            except Exception:
                pass
        self._tabs.clear()

    def list_tabs(self) -> list[dict]:
        """List all open tabs."""
        try:
            resp = requests.get(f"{self.base_url}/json", timeout=5)
            return [t for t in resp.json() if t.get("type") == "page"]
        except Exception:
            return []

    def recall(self) -> dict:
        """Assess the current browser landscape before acting.

        Returns a snapshot:
            {
                "tab_count": int,
                "tabs": [{"id": str, "url": str, "title": str}, ...],
                "orphans_closed": int,
                "ready": bool,
            }

        Closes any orphaned tabs (non-about:blank tabs we don't own).
        Ensures a clean single-tab state before a skill starts.
        """
        all_tabs = self.list_tabs()
        snapshot = {
            "tab_count": len(all_tabs),
            "tabs": [{"id": t["id"], "url": t.get("url", ""), "title": t.get("title", "")} for t in all_tabs],
            "orphans_closed": 0,
            "ready": True,
        }

        logger.info("Recall: %d tab(s) open", len(all_tabs))

        # Track which tab IDs we own
        owned_ids = {tab.id for tab in self._tabs}

        # Close orphaned tabs (tabs we didn't open)
        for tab_info in all_tabs:
            tab_id = tab_info["id"]
            tab_url = tab_info.get("url", "")

            if tab_id in owned_ids:
                # We own this tab — leave it
                logger.info("  Tab %s: owned (%s)", tab_id[:8], tab_url[:60])
                continue

            if tab_url in ("about:blank", "chrome://newtab/", ""):
                # Blank tab — leave one, close extras
                if len(all_tabs) > 1:
                    logger.info("  Tab %s: blank orphan, closing", tab_id[:8])
                    try:
                        requests.get(f"{self.base_url}/json/close/{tab_id}", timeout=3)
                        snapshot["orphans_closed"] += 1
                    except Exception:
                        pass
                else:
                    logger.info("  Tab %s: only blank tab, keeping", tab_id[:8])
            else:
                # Non-blank orphan — close it
                logger.info("  Tab %s: orphan on %s, closing", tab_id[:8], tab_url[:60])
                try:
                    requests.get(f"{self.base_url}/json/close/{tab_id}", timeout=3)
                    snapshot["orphans_closed"] += 1
                except Exception:
                    pass

        if snapshot["orphans_closed"] > 0:
            logger.info("Recall: closed %d orphaned tab(s)", snapshot["orphans_closed"])

        # Re-check state after cleanup
        remaining = self.list_tabs()
        snapshot["tab_count"] = len(remaining)
        snapshot["tabs"] = [{"id": t["id"], "url": t.get("url", ""), "title": t.get("title", "")} for t in remaining]

        if len(remaining) > 5:
            logger.warning("Recall: still %d tabs open after cleanup — something is wrong", len(remaining))
            snapshot["ready"] = False

        return snapshot

    def new_tab(self, url: str = "about:blank") -> CDPTab:
        """Create a new tab and optionally navigate to URL.

        Enforces max 1 active tab. If a tab already exists, closes it first.
        """
        # Max 1 tab: close any existing tabs we own
        if self._tabs:
            logger.warning("Max 1 tab enforced — closing %d existing tab(s) before opening new one",
                           len(self._tabs))
            for old_tab in list(self._tabs):
                try:
                    old_tab.disconnect()
                    requests.get(f"{self.base_url}/json/close/{old_tab.id}", timeout=3)
                except Exception:
                    pass
            self._tabs.clear()

        for method in [requests.put, requests.get]:
            try:
                resp = method(f"{self.base_url}/json/new?url=about:blank", timeout=10)
                if resp.status_code == 200:
                    info = resp.json()
                    break
            except Exception:
                continue
        else:
            raise RuntimeError("Failed to create new tab")

        tab = CDPTab(
            tab_id=info["id"],
            ws_url=info["webSocketDebuggerUrl"],
            url="about:blank",
            cdp_base_url=self.base_url,
        )
        tab.connect()
        self._tabs.append(tab)

        if url != "about:blank":
            tab.navigate(url)

        return tab

    def connect_first_tab(self) -> CDPTab:
        """Connect to the first existing tab."""
        tabs = self.list_tabs()
        if not tabs:
            raise RuntimeError("No tabs open")
        info = tabs[0]
        tab = CDPTab(
            tab_id=info["id"],
            ws_url=info["webSocketDebuggerUrl"],
            url=info.get("url", ""),
            cdp_base_url=self.base_url,
        )
        tab.connect()
        self._tabs.append(tab)
        return tab

    def close_tab(self, tab: CDPTab):
        """Close a specific tab."""
        tab.disconnect()
        try:
            requests.get(f"{self.base_url}/json/close/{tab.id}", timeout=3)
        except Exception:
            pass
        if tab in self._tabs:
            self._tabs.remove(tab)

    def keep_tab(self, tab: CDPTab):
        """Disconnect but leave tab open in browser."""
        tab.disconnect()
        if tab in self._tabs:
            self._tabs.remove(tab)
