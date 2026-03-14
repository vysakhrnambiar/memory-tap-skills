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

# Setup logging first
LOG_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(LOG_DIR, "memory_tap.log"),
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("memory_tap")

from .chrome_manager import ChromeManager
from .db.models import init_db
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

        menu = pystray.Menu(
            pystray.MenuItem("Open Dashboard", on_open_dashboard, default=True),
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


def main():
    global _chrome, _scheduler, _updater

    logger.info("=" * 50)
    logger.info("Memory Tap starting...")
    logger.info("=" * 50)

    # 1. Initialize database
    logger.info("Initializing database...")
    init_db()

    # 2. Start Chrome Manager
    _chrome = ChromeManager()
    logger.info("Ensuring Chrome is running...")
    if not _chrome.ensure_running():
        logger.error("Failed to start Chrome. Exiting.")
        sys.exit(1)

    # 3. Start Skill Updater (downloads skills from GitHub)
    _updater = SkillUpdater()
    _updater.start()

    # Give updater a moment to download initial skills
    import time
    time.sleep(3)

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

    # Handle shutdown signals
    signal.signal(signal.SIGINT, lambda s, f: _shutdown())
    signal.signal(signal.SIGTERM, lambda s, f: _shutdown())

    # Run FastAPI (blocking)
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=DASHBOARD_PORT, log_level="warning")


if __name__ == "__main__":
    main()
