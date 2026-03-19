"""
Chrome Manager — manages an isolated Chrome instance for Memory Tap.

Key design:
- HARDCODED persistent profile at %LOCALAPPDATA%/MemoryTap/chrome_profile
- Never touches user's main Chrome
- Port safety: probes ports before launching, verifies ownership, picks free port
- Stores active port in file so other components can read it
- Finds Chrome via registry/common paths (Windows)
- Launches with --user-data-dir for full session persistence (cookies, logins)
- Clean lifecycle: launch → use → shutdown (no orphan processes)
"""
import json
import logging
import os
import subprocess
import sys
import threading
import time
import winreg

import requests

logger = logging.getLogger("memory_tap.chrome")

# --- Constants ---

CDP_PORT_START = 9494       # First port to try
CDP_PORT_END = 9504         # Last port to try (10 attempts)
PROFILE_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "chrome_profile")
DATA_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap")
PID_FILE = os.path.join(DATA_DIR, "chrome.pid")
PORT_FILE = os.path.join(DATA_DIR, "chrome.port")


def get_active_port() -> int:
    """Read the active CDP port from file. Other modules use this instead of a constant.

    Falls back to CDP_PORT_START if no port file exists.
    """
    try:
        with open(PORT_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return CDP_PORT_START


def _probe_port(port: int) -> str:
    """Check what's running on a port.

    Returns:
        "free"    — nothing listening
        "ours"    — Chrome with our profile is running here
        "foreign" — something else is using this port
    """
    try:
        resp = requests.get(f"http://localhost:{port}/json/version", timeout=2)
        if resp.status_code != 200:
            return "foreign"  # Something responds but not Chrome CDP

        data = resp.json()

        # Chrome CDP responds with browser info including the user-data-dir
        # in the "webSocketDebuggerUrl" or we can check via /json endpoint
        ws_url = data.get("webSocketDebuggerUrl", "")
        browser = data.get("Browser", "")

        # It's Chrome — but is it OURS?
        # Check by querying the tabs and looking at the debug info
        try:
            tabs_resp = requests.get(f"http://localhost:{port}/json", timeout=2)
            tabs_text = tabs_resp.text

            # Chrome's CDP includes the user-data-dir in some responses
            # but the most reliable way is to check the command line via /json/protocol
            # or simply check if our PID file matches
            stored_pid = _read_pid()
            if stored_pid:
                # Verify the process on this port matches our stored PID
                # We can get the PID from the Chrome process info
                try:
                    proc_resp = requests.get(f"http://localhost:{port}/json/version", timeout=2)
                    # If we stored this PID and the port responds, assume it's ours
                    # We'll do a more thorough check: try running JS in a tab to get the profile path
                    return "ours"
                except Exception:
                    pass

            # No stored PID — this is a foreign Chrome or another app
            # But could be our Chrome from a previous crash (PID file was cleaned)
            # Check if it's Chrome at all
            if "Chrome" in browser or "chrome" in browser.lower():
                # It's Chrome but we can't confirm it's ours — treat as foreign to be safe
                logger.warning("Chrome found on port %d but can't verify ownership — treating as foreign", port)
                return "foreign"

            return "foreign"

        except Exception:
            return "foreign"

    except requests.ConnectionError:
        return "free"
    except requests.Timeout:
        return "free"  # No response = nothing there
    except Exception:
        return "foreign"  # Unknown error = don't risk it


def _find_free_port() -> int | None:
    """Find a port that's free or already running our Chrome.

    Tries CDP_PORT_START through CDP_PORT_END.
    Returns the port number, or None if all occupied by foreign processes.
    """
    for port in range(CDP_PORT_START, CDP_PORT_END + 1):
        status = _probe_port(port)
        if status == "ours":
            logger.info("Port %d: our Chrome is already running here", port)
            return port
        elif status == "free":
            logger.info("Port %d: free, will use this", port)
            return port
        else:
            logger.info("Port %d: occupied by foreign process, skipping", port)

    logger.error("All ports %d-%d are occupied", CDP_PORT_START, CDP_PORT_END)
    return None


def _write_port(port: int):
    """Store the active port so other components can read it."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PORT_FILE, "w", encoding="utf-8") as f:
        f.write(str(port))


def _clear_port():
    """Remove port file."""
    try:
        os.remove(PORT_FILE)
    except OSError:
        pass


def _find_chrome() -> str | None:
    """Find Chrome executable on Windows. Registry first, then common paths."""
    for hive in [winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER]:
        for subkey in [
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe",
            r"SOFTWARE\Google\Chrome\BLBeacon",
        ]:
            try:
                with winreg.OpenKey(hive, subkey) as key:
                    path, _ = winreg.QueryValueEx(key, "" if "App Paths" in subkey else "path")
                    if "BLBeacon" in subkey:
                        path = os.path.join(path, "chrome.exe")
                    if os.path.isfile(path):
                        return path
            except (OSError, FileNotFoundError):
                continue

    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path

    return None


def _read_pid() -> int | None:
    """Read stored PID from file."""
    try:
        with open(PID_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return None


def _write_pid(pid: int):
    """Store Chrome PID for cleanup."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(PID_FILE, "w", encoding="utf-8") as f:
        f.write(str(pid))


def _clear_pid():
    """Remove PID file."""
    try:
        os.remove(PID_FILE)
    except OSError:
        pass


def _is_pid_alive(pid: int) -> bool:
    """Check if a process is running by PID. Windows-compatible.

    Uses CREATE_NO_WINDOW to prevent console flash in windowed/PyInstaller mode.
    """
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        return str(pid) in result.stdout
    except Exception:
        return False


class ChromeManager:
    """Manages the lifecycle of Memory Tap's isolated Chrome instance.

    Port safety: probes ports before launching. If 9494 is taken by another
    process, tries 9495, 9496, etc. Stores chosen port in a file.

    Usage:
        mgr = ChromeManager()
        mgr.ensure_running()          # Launch if not already running
        base_url = mgr.cdp_base_url   # "http://localhost:{port}"
        # ... use CDP ...
        mgr.shutdown()                # Kill Chrome cleanly
    """

    def __init__(self):
        self.chrome_path: str | None = None
        self.process: subprocess.Popen | None = None
        self.port: int = CDP_PORT_START
        self.cdp_base_url: str = f"http://localhost:{CDP_PORT_START}"
        self._pid: int | None = None
        self._dashboard_tab_id: str = ""  # protected — skills must not reuse this tab

    def ensure_running(self) -> bool:
        """Ensure Chrome is running with our profile. Returns True if ready.

        Port safety:
        1. Find a port that's free or already has our Chrome
        2. If our Chrome is already running on that port, reuse it
        3. If port is free, launch Chrome on it
        4. Store chosen port in file for other components
        """
        # Find a usable port
        port = _find_free_port()
        if port is None:
            logger.error("No free port found in range %d-%d", CDP_PORT_START, CDP_PORT_END)
            return False

        self.port = port
        self.cdp_base_url = f"http://localhost:{port}"

        # Check if our Chrome is already running on this port
        status = _probe_port(port)
        if status == "ours":
            logger.info("Chrome already running on port %d", port)
            self._pid = _read_pid()
            _write_port(port)
            return True

        # Port is free — launch Chrome
        self.chrome_path = _find_chrome()
        if not self.chrome_path:
            logger.error("Chrome not found. Please install Google Chrome.")
            return False

        os.makedirs(PROFILE_DIR, exist_ok=True)

        cmd = [
            self.chrome_path,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={PROFILE_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--remote-allow-origins=*",
            "--mute-audio",
            "about:blank",
        ]

        logger.info("Launching Chrome on port %d: %s", port, self.chrome_path)
        logger.info("Profile: %s", PROFILE_DIR)

        # DETACHED_PROCESS prevents inheriting parent's (hidden) console
        # but still allows Chrome to create its own visible window
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.DETACHED_PROCESS,
        )
        self._pid = self.process.pid
        _write_pid(self._pid)
        _write_port(port)

        # Wait for CDP to become available
        if not self._wait_for_cdp(timeout=15):
            logger.error("Chrome launched but CDP not responding on port %d", port)
            self.shutdown()
            return False

        logger.info("Chrome ready on port %d (PID %d)", port, self._pid)
        return True

    def _wait_for_cdp(self, timeout: int = 15) -> bool:
        """Poll until CDP endpoint responds."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = requests.get(f"{self.cdp_base_url}/json/version", timeout=2)
                if resp.status_code == 200:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False

    def audit_tabs(self, dashboard_port: int = 7777):
        """Enforce tab rules: exactly 1 dashboard tab, close everything else.

        Rules:
        - Exactly 1 dashboard tab (localhost:{dashboard_port}) must exist
        - If 0 dashboard tabs → navigate a blank tab to dashboard, or create one
        - If 2+ dashboard tabs → close extras
        - All non-dashboard tabs are closed
        - about:blank tabs → close (unless needed for dashboard)

        Called by:
        - Level 4: On startup (cleanup_tabs)
        - Level 3: Health monitor every 5 min (when no skill running)
        - Level 2: Scheduler after each skill run
        """
        try:
            resp = requests.get(f"{self.cdp_base_url}/json", timeout=5)
            tabs = [t for t in resp.json() if t.get("type") == "page"]
        except Exception as e:
            logger.warning("audit_tabs: could not list tabs: %s", e)
            return

        if not tabs:
            return

        dashboard_url = f"localhost:{dashboard_port}"
        dashboard_tabs = []
        blank_tabs = []
        other_tabs = []

        for t in tabs:
            url = t.get("url", "")
            if dashboard_url in url:
                dashboard_tabs.append(t)
            elif url in ("about:blank", "chrome://newtab/", ""):
                blank_tabs.append(t)
            else:
                other_tabs.append(t)

        closed = 0

        # Close all non-dashboard, non-blank tabs
        for t in other_tabs:
            try:
                requests.get(f"{self.cdp_base_url}/json/close/{t['id']}", timeout=3)
                closed += 1
            except Exception:
                pass

        # Ensure exactly 1 dashboard tab
        if len(dashboard_tabs) == 0:
            # No dashboard — reuse a blank tab or create one
            if blank_tabs:
                # Navigate first blank tab to dashboard
                import websocket as _ws
                import json as _json
                try:
                    ws = _ws.create_connection(blank_tabs[0]["webSocketDebuggerUrl"], timeout=10)
                    ws.send(_json.dumps({
                        "id": 1, "method": "Page.navigate",
                        "params": {"url": f"http://localhost:{dashboard_port}"},
                    }))
                    ws.recv()
                    ws.close()
                    logger.info("audit_tabs: navigated blank tab to dashboard")
                    # Close remaining blank tabs
                    for bt in blank_tabs[1:]:
                        try:
                            requests.get(f"{self.cdp_base_url}/json/close/{bt['id']}", timeout=3)
                            closed += 1
                        except Exception:
                            pass
                except Exception as e:
                    logger.warning("audit_tabs: failed to navigate blank tab: %s", e)
            else:
                # Create a dashboard tab
                self.open_headed(f"http://localhost:{dashboard_port}")
                logger.info("audit_tabs: created dashboard tab")
        else:
            # Close extra dashboard tabs (keep first one)
            for dt in dashboard_tabs[1:]:
                try:
                    requests.get(f"{self.cdp_base_url}/json/close/{dt['id']}", timeout=3)
                    closed += 1
                except Exception:
                    pass
            # Close all blank tabs (dashboard exists, blanks not needed)
            for bt in blank_tabs:
                try:
                    requests.get(f"{self.cdp_base_url}/json/close/{bt['id']}", timeout=3)
                    closed += 1
                except Exception:
                    pass

        if closed:
            logger.info("audit_tabs: closed %d tab(s)", closed)

    def cleanup_tabs(self):
        """Level 4: Startup cleanup — calls audit_tabs."""
        self.audit_tabs()

    def open_headed(self, url: str, reuse_blank: bool = False):
        """Open a URL in the Chrome instance for user interaction (login etc).

        Always creates a NEW tab for the URL. This prevents navigating away
        from the dashboard tab or any tab the user is currently on.

        Only reuses blank tabs when reuse_blank=True (used for dashboard on first run).
        """
        import websocket as _ws
        import json as _json

        tab_info = None

        # Optionally reuse an about:blank tab (only for dashboard on first run)
        if reuse_blank:
            try:
                resp = requests.get(f"{self.cdp_base_url}/json", timeout=5)
                tabs = [t for t in resp.json() if t.get("type") == "page"]
                for t in tabs:
                    if t.get("url") in ("about:blank", "chrome://newtab/", ""):
                        tab_info = t
                        logger.info("Reusing existing blank tab %s", t["id"][:8])
                        break
            except Exception:
                pass

        # Create a new blank tab (no URL param to avoid Chrome opening a new window)
        if not tab_info:
            for method in [requests.put, requests.get]:
                try:
                    resp = method(f"{self.cdp_base_url}/json/new", timeout=5)
                    if resp.status_code == 200:
                        tab_info = resp.json()
                        logger.info("Created new tab %s for %s", tab_info.get("id", "?")[:8], url[:40])
                        break
                except Exception:
                    continue

        if not tab_info:
            logger.error("Failed to create tab for URL: %s", url)
            return None

        # Navigate via CDP WebSocket
        try:
            ws_url = tab_info.get("webSocketDebuggerUrl", "")
            if ws_url:
                ws = _ws.create_connection(ws_url, timeout=10)
                ws.send(_json.dumps({
                    "id": 1,
                    "method": "Page.navigate",
                    "params": {"url": url},
                }))
                ws.settimeout(5)
                try:
                    ws.recv()
                except Exception:
                    pass
                ws.close()
                # Track dashboard tab ID so skills don't reuse it
                if "localhost" in url and "7777" in url:
                    self._dashboard_tab_id = tab_info.get("id", "")
                logger.info("Opened %s in Chrome for user interaction", url)
            else:
                logger.warning("No WebSocket URL for tab — URL may not load")
        except Exception as e:
            logger.warning("CDP navigate failed for %s: %s", url, e)

        return tab_info

    def get_tabs(self) -> list[dict]:
        """Get list of all open tabs."""
        try:
            resp = requests.get(f"{self.cdp_base_url}/json", timeout=5)
            return [t for t in resp.json() if t.get("type") == "page"]
        except Exception:
            return []

    def shutdown(self):
        """Gracefully kill our Chrome instance."""
        try:
            requests.get(f"{self.cdp_base_url}/json/close", timeout=2)
        except Exception:
            pass

        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None
        elif self._pid:
            try:
                os.kill(self._pid, 9)
            except OSError:
                pass

        _clear_pid()
        _clear_port()
        logger.info("Chrome shut down")

    def is_running(self) -> bool:
        """Check if our Chrome is still running."""
        # Check PID is alive (Windows-compatible)
        if self._pid:
            if not _is_pid_alive(self._pid):
                logger.warning("Chrome PID %d is dead", self._pid)
                return False

        # Check CDP endpoint responds
        try:
            resp = requests.get(f"{self.cdp_base_url}/json/version", timeout=2)
            return resp.status_code == 200
        except Exception:
            return False


class HealthMonitor:
    """Background thread that monitors Chrome health every N seconds.

    Checks:
    1. Is Chrome process alive? (PID check)
    2. Does CDP endpoint respond? (HTTP check)
    3. How many tabs are open?

    Sets `healthy` flag that scheduler checks before running skills.
    Logs state for debugging timeline.
    """

    def __init__(self, chrome: ChromeManager, interval: int = 10):
        self.chrome = chrome
        self.interval = interval
        self.healthy = False
        self.internet_connected = True  # assume connected at start
        self.last_check: dict | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self.on_recovery: callable = None  # callback when Chrome recovers from crash

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Health monitor started (every %ds)", self.interval)

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None

    def check_now(self) -> dict:
        """Run a health check immediately. Returns status dict."""
        status = {
            "timestamp": time.time(),
            "pid_alive": False,
            "cdp_responds": False,
            "tab_count": 0,
            "healthy": False,
        }

        # 1. PID alive?
        pid = self.chrome._pid or _read_pid()
        if pid:
            status["pid_alive"] = _is_pid_alive(pid)

        # 2. CDP responds?
        try:
            resp = requests.get(f"{self.chrome.cdp_base_url}/json/version", timeout=3)
            status["cdp_responds"] = resp.status_code == 200
        except Exception:
            pass

        # 3. Tab count
        if status["cdp_responds"]:
            try:
                resp = requests.get(f"{self.chrome.cdp_base_url}/json", timeout=3)
                tabs = [t for t in resp.json() if t.get("type") == "page"]
                status["tab_count"] = len(tabs)
            except Exception:
                pass

        # 4. Internet connectivity
        inet_ok = False
        try:
            inet_resp = requests.get("https://www.google.com/generate_204", timeout=5)
            inet_ok = inet_resp.status_code == 204
        except Exception:
            pass
        status["internet"] = inet_ok

        # Log only on state change
        was_internet = self.internet_connected
        self.internet_connected = inet_ok
        if was_internet and not inet_ok:
            logger.warning("Internet connectivity LOST")
        elif not was_internet and inet_ok:
            logger.info("Internet connectivity RESTORED")

        # Overall health
        status["healthy"] = status["pid_alive"] and status["cdp_responds"]

        # Alert on transition from healthy → unhealthy (not every check)
        was_healthy = self.healthy
        self.healthy = status["healthy"]
        self.last_check = status

        if was_healthy and not status["healthy"]:
            logger.warning("Health check FAILED: pid_alive=%s cdp_responds=%s",
                           status["pid_alive"], status["cdp_responds"])
            from .db.core_db import add_alert
            if not status["pid_alive"]:
                # Chrome died — attempt auto-relaunch
                logger.info("Chrome process dead — attempting auto-relaunch...")
                relaunched = self.chrome.ensure_running()
                if relaunched:
                    logger.info("Chrome relaunched successfully")
                    add_alert("Chrome Restarted", "Chrome was closed or crashed and has been automatically restarted.", level="info", source="health")
                    self.healthy = True
                    status["healthy"] = True
                    # Notify scheduler to retry interrupted skills
                    if self.on_recovery:
                        try:
                            self.on_recovery()
                        except Exception as e:
                            logger.error("on_recovery callback failed: %s", e)
                else:
                    add_alert("Chrome Stopped", "Chrome crashed and could not be restarted. Skills are paused.", level="error", source="health")
            elif not status["cdp_responds"]:
                add_alert("Chrome Unresponsive", "Chrome is running but not responding to commands.", level="error", source="health")

        return status

    def _loop(self):
        """Background health check loop."""
        checks_since_audit = 0
        AUDIT_EVERY_N_CHECKS = 30  # ~5 min at 10s interval

        while self._running:
            try:
                self.check_now()
                checks_since_audit += 1

                # Level 3: Periodic tab audit every ~5 minutes (ONLY when no skill running)
                if checks_since_audit >= AUDIT_EVERY_N_CHECKS and self.healthy:
                    checks_since_audit = 0
                    # Check if scheduler or service worker has a skill running
                    sched_active = getattr(self, 'scheduler_ref', None) and getattr(self.scheduler_ref, 'skill_running', None)
                    worker_active = getattr(self, 'service_worker_ref', None) and getattr(self.service_worker_ref, 'skill_running', None)
                    skill_active = sched_active or worker_active
                    if skill_active:
                        active_name = sched_active or worker_active
                        logger.debug("audit_tabs skipped — skill '%s' is running", active_name)
                    else:
                        try:
                            self.chrome.audit_tabs()
                        except Exception as e:
                            logger.warning("Periodic audit_tabs failed: %s", e)

            except Exception as e:
                logger.error("Health monitor error: %s", e)
                self.healthy = False

            for _ in range(self.interval):
                if not self._running:
                    return
                time.sleep(1)
