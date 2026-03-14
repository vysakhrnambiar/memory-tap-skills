"""
Chrome Manager — manages an isolated Chrome instance for Memory Tap.

Key design:
- HARDCODED persistent profile at %LOCALAPPDATA%/MemoryTap/chrome_profile
- Never touches user's main Chrome
- Uses a FIXED debug port (9494) — distinct from any other CDP usage
- Finds Chrome via registry/common paths (Windows)
- Launches with --user-data-dir for full session persistence (cookies, logins)
- Clean lifecycle: launch → use → shutdown (no orphan processes)
"""
import json
import logging
import os
import subprocess
import sys
import time
import winreg

import requests

logger = logging.getLogger("memory_tap.chrome")

# --- Constants (hardcoded, never configurable) ---

CDP_PORT = 9494
PROFILE_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "chrome_profile")
DATA_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap")
PID_FILE = os.path.join(DATA_DIR, "chrome.pid")


def _find_chrome() -> str | None:
    """Find Chrome executable on Windows. Registry first, then common paths."""
    # Try registry
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

    # Common install locations
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path

    return None


def _is_chrome_running() -> bool:
    """Check if our Chrome instance is already running on CDP_PORT."""
    try:
        resp = requests.get(f"http://localhost:{CDP_PORT}/json/version", timeout=2)
        return resp.status_code == 200
    except Exception:
        return False


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


class ChromeManager:
    """Manages the lifecycle of Memory Tap's isolated Chrome instance.

    Usage:
        mgr = ChromeManager()
        mgr.ensure_running()          # Launch if not already running
        base_url = mgr.cdp_base_url   # "http://localhost:9494"
        # ... use CDP ...
        mgr.shutdown()                # Kill Chrome cleanly
    """

    def __init__(self):
        self.chrome_path: str | None = None
        self.process: subprocess.Popen | None = None
        self.cdp_base_url = f"http://localhost:{CDP_PORT}"
        self._pid: int | None = None

    def ensure_running(self) -> bool:
        """Ensure Chrome is running with our profile. Returns True if ready."""
        # Already running?
        if _is_chrome_running():
            logger.info("Chrome already running on port %d", CDP_PORT)
            self._pid = _read_pid()
            return True

        # Find Chrome
        self.chrome_path = _find_chrome()
        if not self.chrome_path:
            logger.error("Chrome not found. Please install Google Chrome.")
            return False

        # Ensure profile directory exists
        os.makedirs(PROFILE_DIR, exist_ok=True)

        # Launch
        cmd = [
            self.chrome_path,
            f"--remote-debugging-port={CDP_PORT}",
            f"--user-data-dir={PROFILE_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--remote-allow-origins=*",
            # Start with a blank page — skills will navigate
            "about:blank",
        ]

        logger.info("Launching Chrome: %s", self.chrome_path)
        logger.info("Profile: %s", PROFILE_DIR)

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._pid = self.process.pid
        _write_pid(self._pid)

        # Wait for CDP to become available
        if not self._wait_for_cdp(timeout=15):
            logger.error("Chrome launched but CDP not responding on port %d", CDP_PORT)
            self.shutdown()
            return False

        logger.info("Chrome ready on port %d (PID %d)", CDP_PORT, self._pid)
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

    def open_headed(self, url: str):
        """Open a URL in the Chrome instance (makes window visible for user login).

        This navigates a new tab to the URL. The window should already be visible
        since we don't use --headless.
        """
        try:
            # Create a new tab pointing to the URL
            resp = requests.get(
                f"{self.cdp_base_url}/json/new?url={url}", timeout=5
            )
            if resp.status_code == 200:
                logger.info("Opened %s in Chrome for user interaction", url)
                return resp.json()
        except Exception as e:
            logger.error("Failed to open URL: %s", e)
        return None

    def get_tabs(self) -> list[dict]:
        """Get list of all open tabs."""
        try:
            resp = requests.get(f"{self.cdp_base_url}/json", timeout=5)
            return [t for t in resp.json() if t.get("type") == "page"]
        except Exception:
            return []

    def shutdown(self):
        """Gracefully kill our Chrome instance."""
        # Try graceful close via CDP
        try:
            requests.get(f"{self.cdp_base_url}/json/close", timeout=2)
        except Exception:
            pass

        # Kill process
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
            # Kill by stored PID (if we didn't launch it this session)
            try:
                os.kill(self._pid, 9)
            except OSError:
                pass

        _clear_pid()
        logger.info("Chrome shut down")

    def is_running(self) -> bool:
        return _is_chrome_running()
