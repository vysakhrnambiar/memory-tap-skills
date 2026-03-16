"""
Memory Tap — entry point.

Starts:
1. SQLite DB initialization
2. Chrome Manager (isolated Chrome instance)
3. Skill Updater (auto-pulls from GitHub)
4. Skill Scheduler (runs skills on schedule)
5. Dashboard (FastAPI on localhost:7777)
6. System tray icon

Usage:
    python -m src
"""
import logging
import os
import signal
import sys
import threading
import webbrowser

# PyInstaller windowed mode sets sys.stdout/stderr to None.
# Redirect to devnull so logging and uvicorn don't crash on isatty() calls.
if sys.stdout is None:
    sys.stdout = open(os.devnull, "w", encoding="utf-8")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w", encoding="utf-8")

# Setup logging first
LOG_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, "memory_tap.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("memory_tap")

from .chrome_manager import ChromeManager
from .db.core_db import init_core_db, get_setting, set_setting
from .scheduler import SkillScheduler
from .updater.skill_updater import SkillUpdater, LOCAL_SKILLS_DIR

DASHBOARD_PORT = 7777

# Global refs for tray menu actions
_chrome = None
_scheduler = None
_updater = None


def _create_tray_icon():
    """Create system tray icon with menu."""
    try:
        import pystray
        from PIL import Image, ImageDraw

        # Create a simple icon (purple circle with "M")
        size = 64
        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        # Purple circle
        draw.ellipse([2, 2, size - 2, size - 2], fill=(124, 58, 237, 255))
        # White "M" letter
        try:
            from PIL import ImageFont
            font = ImageFont.truetype("arial.ttf", 32)
        except Exception:
            font = ImageFont.load_default()
        draw.text((size // 2, size // 2), "M", fill=(255, 255, 255, 255),
                   font=font, anchor="mm")

        def on_open_dashboard(icon, item):
            webbrowser.open(f"http://localhost:{DASHBOARD_PORT}")

        def on_quit(icon, item):
            logger.info("Quit from tray")
            icon.stop()
            _shutdown()

        def on_uninstall(icon, item):
            logger.info("Uninstall from tray")
            icon.stop()
            _uninstall()

        menu = pystray.Menu(
            pystray.MenuItem("Open Dashboard", on_open_dashboard, default=True),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Uninstall Memory Tap", on_uninstall),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("Quit", on_quit),
        )

        icon = pystray.Icon("memory_tap", img, "Memory Tap", menu)
        # Run in its own thread (pystray.run() blocks on Windows)
        tray_thread = threading.Thread(target=icon.run, daemon=True)
        tray_thread.start()
        logger.info("System tray icon created")
        return icon

    except ImportError:
        logger.warning("pystray not installed — no tray icon. Install with: python -m pip install pystray Pillow")
        return None
    except Exception as e:
        logger.warning("Failed to create tray icon: %s", e)
        return None


def _register_startup():
    """Register Memory Tap to auto-start on Windows login.

    If running as exe: registers the exe path.
    If running as script: registers python -m src command.
    """
    import winreg

    try:
        if getattr(sys, 'frozen', False):
            # Running as PyInstaller exe
            exe_path = sys.executable
            cmd = f'"{exe_path}"'
        else:
            # Running as script — register python -m src
            python_exe = sys.executable
            cmd = f'"{python_exe}" -m src'

        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
            0, winreg.KEY_SET_VALUE,
        )
        winreg.SetValueEx(key, "MemoryTap", 0, winreg.REG_SZ, cmd)
        winreg.CloseKey(key)
        logger.info("Registered for Windows startup: %s", cmd)
    except Exception as e:
        logger.warning("Failed to register startup: %s", e)


def _handle_first_run(chrome: ChromeManager):
    """On first run, open dashboard in the isolated Chrome (for sign-in wizard).
    On subsequent runs, dashboard opens in user's default browser via tray icon.
    """
    # get_setting and set_setting already imported at module level from core_db

    is_first_run = get_setting("first_run_done") != "yes"

    # Always re-register startup on every launch (cleanup scripts may have removed it)
    _register_startup()

    if is_first_run:
        logger.info("First run detected")
        set_setting("first_run_done", "yes")

    # Always open dashboard in Chrome if only about:blank is open
    import time
    time.sleep(2)
    try:
        import requests as _req
        tabs_resp = _req.get(f"http://localhost:{chrome.port}/json", timeout=5)
        tabs = [t for t in tabs_resp.json() if t.get("type") == "page"]
        all_blank = all(t.get("url") in ("about:blank", "chrome://newtab/", "") for t in tabs)
        if all_blank or not tabs:
            logger.info("Opening dashboard in Chrome (all tabs blank or no tabs)")
            chrome.open_headed(f"http://localhost:{DASHBOARD_PORT}", reuse_blank=True)
        else:
            logger.info("Chrome already has tabs open — not auto-opening dashboard")
    except Exception as e:
        logger.warning("Could not check Chrome tabs, opening dashboard anyway: %s", e)
        chrome.open_headed(f"http://localhost:{DASHBOARD_PORT}", reuse_blank=True)

    if is_first_run:
        logger.info("First run setup complete")


def _uninstall():
    """Full uninstall: stop services, offer data export, remove files + registry."""
    import ctypes
    import shutil
    import winreg

    logger.info("Starting uninstall...")

    LOCALAPPDATA = os.environ.get("LOCALAPPDATA", "")
    INSTALL_DIR = os.path.join(LOCALAPPDATA, "MemoryTap")
    DB_PATH = os.path.join(INSTALL_DIR, "memory_tap.db")
    DOCUMENTS = os.path.join(os.path.expanduser("~"), "Documents")

    # Ask: keep data?
    try:
        result = ctypes.windll.user32.MessageBoxW(
            0,
            "Do you want to keep a backup of your collected data?\n\n"
            "Click YES to save a backup to your Documents folder.\n"
            "Click NO to delete everything.\n"
            "Click CANCEL to abort uninstall.",
            "Uninstall Memory Tap",
            0x23,  # YES/NO/CANCEL + question icon
        )
        if result == 2:  # CANCEL
            logger.info("Uninstall cancelled by user")
            return
        keep_data = result == 6  # YES
    except Exception:
        keep_data = True  # Default to keeping data if dialog fails

    # Stop services
    if _scheduler:
        _scheduler.stop()
    if _updater:
        _updater.stop()
    if _chrome:
        _chrome.shutdown()

    # Export data if requested
    if keep_data and os.path.isfile(DB_PATH):
        try:
            import time as _time
            backup_name = f"MemoryTap_backup_{int(_time.time())}.db"
            backup_path = os.path.join(DOCUMENTS, backup_name)
            shutil.copy2(DB_PATH, backup_path)
            logger.info("Data backed up to: %s", backup_path)
        except Exception as e:
            logger.error("Failed to backup data: %s", e)

    # Remove startup registry
    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Run",
            0, winreg.KEY_SET_VALUE,
        )
        winreg.DeleteValue(key, "MemoryTap")
        winreg.CloseKey(key)
        logger.info("Removed startup registry entry")
    except Exception:
        pass

    # Remove desktop shortcut
    shortcut = os.path.join(os.path.expanduser("~"), "Desktop", "Memory Tap.url")
    if os.path.isfile(shortcut):
        try:
            os.remove(shortcut)
        except Exception:
            pass

    # Remove install directory
    if os.path.isdir(INSTALL_DIR):
        try:
            shutil.rmtree(INSTALL_DIR, ignore_errors=True)
            logger.info("Removed %s", INSTALL_DIR)
        except Exception:
            pass

    # Show confirmation
    try:
        msg = "Memory Tap has been uninstalled."
        if keep_data:
            msg += f"\n\nYour data has been backed up to:\n{DOCUMENTS}"
        ctypes.windll.user32.MessageBoxW(0, msg, "Memory Tap Uninstalled", 0x40)
    except Exception:
        pass

    logger.info("Uninstall complete")
    os._exit(0)


def _shutdown():
    """Clean shutdown of all components."""
    logger.info("Shutting down...")
    if _scheduler:
        _scheduler.stop()
    if _updater:
        _updater.stop()
    if _chrome:
        _chrome.shutdown()
    logger.info("Goodbye!")
    os._exit(0)


def _show_splash():
    """Show a brief 'Setting up...' message while app starts.

    Uses MessageBoxTimeoutW — auto-closes after 3 seconds.
    User can click OK to dismiss early, or just wait.
    """
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxTimeoutW(
            0,
            "Setting up... this will close automatically.",
            "Memory Tap",
            0x00040040,  # MB_ICONINFORMATION | MB_TOPMOST
            0,  # language
            3000,  # auto-close after 3 seconds
        )
    except Exception:
        pass


def main():
    global _chrome, _scheduler, _updater

    # Show splash immediately so user knows something is happening
    threading.Thread(target=_show_splash, daemon=True).start()

    logger.info("=" * 50)
    logger.info("Memory Tap starting...")
    logger.info("=" * 50)

    # 1. Initialize database
    logger.info("Initializing database...")
    init_core_db()

    # 2. Start Chrome Manager
    _chrome = ChromeManager()
    logger.info("Ensuring Chrome is running...")
    if not _chrome.ensure_running():
        logger.error("Failed to start Chrome. Exiting.")
        sys.exit(1)

    # 3. Download skills from GitHub (blocking on first run, so all skills are ready)
    import time
    _updater = SkillUpdater()
    logger.info("Downloading skills from GitHub...")
    try:
        _updater.update_all()  # Blocking download — ensures all skills ready before scheduler
        logger.info("Skills downloaded")
    except Exception as e:
        logger.warning("Skill download failed: %s", e)
    _updater.start()  # Start background polling for future updates

    # 4. Start Scheduler
    _scheduler = SkillScheduler(_chrome)
    _scheduler.load_skills_from_dir(LOCAL_SKILLS_DIR)
    _scheduler.start()

    # 5. Start Dashboard
    from .dashboard.app import app, set_app_deps
    set_app_deps(_chrome, _scheduler)

    logger.info("Starting dashboard on http://localhost:%d", DASHBOARD_PORT)

    # 6. Create system tray icon
    tray_icon = _create_tray_icon()

    # 7. First-run: open dashboard in Chrome AFTER uvicorn starts
    # Must run in a thread because uvicorn.run() blocks
    def _delayed_first_run():
        """Wait for dashboard to be ready, then open it in Chrome."""
        import time
        import requests as _req
        # Wait for uvicorn to start
        for _ in range(30):
            try:
                r = _req.get(f"http://localhost:{DASHBOARD_PORT}/api/settings", timeout=2)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)
        _handle_first_run(_chrome)

    threading.Thread(target=_delayed_first_run, daemon=True).start()

    # Handle shutdown signals
    signal.signal(signal.SIGINT, lambda s, f: _shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: _shutdown())

    # Run FastAPI (blocking)
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=DASHBOARD_PORT, log_level="warning")


if __name__ == "__main__":
    main()
