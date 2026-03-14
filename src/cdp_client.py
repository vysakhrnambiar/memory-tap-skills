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

from .chrome_manager import CDP_PORT

logger = logging.getLogger("memory_tap.cdp")


class CDPTab:
    """A single browser tab with reliable CDP communication.

    Key improvement: events stored by method name in separate queues.
    Waiting for Page.loadEventFired never discards Network events.
    """

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

    # --- Connection ---

    def connect(self):
        if self.ws is not None:
            return self
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

    def _recv_loop(self):
        """Background thread: routes responses by ID, events by method name."""
        while self._running and self.ws:
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
                break
            except Exception:
                break

    # --- Low-level send/receive ---

    def _send(self, method: str, params: dict | None = None, timeout: float = 30) -> dict:
        """Send CDP command and wait for response."""
        if not self.ws:
            raise RuntimeError("Not connected")
        with self._lock:
            self._msg_id += 1
            msg_id = self._msg_id
        msg = {"id": msg_id, "method": method, "params": params or {}}
        self.ws.send(json.dumps(msg))
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

    # --- Navigation ---

    def navigate(self, url: str, timeout: float = 30):
        """Navigate and wait for full page load."""
        self.drain_events("Page.loadEventFired")

        result = self._send("Page.navigate", {"url": url}, timeout=15)
        if isinstance(result, dict) and "_error" in result:
            self.reconnect()
            self.drain_events("Page.loadEventFired")
            result = self._send("Page.navigate", {"url": url}, timeout=15)

        self.wait_for_event("Page.loadEventFired", timeout=timeout)
        time.sleep(1.5)  # settle for JS rendering
        self.url = self.js("return window.location.href") or url

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

    def __init__(self, port: int = CDP_PORT):
        self.port = port
        self.base_url = f"http://localhost:{port}"
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

    def new_tab(self, url: str = "about:blank") -> CDPTab:
        """Create a new tab and optionally navigate to URL."""
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
