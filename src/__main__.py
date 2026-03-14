"""
Memory Tap — entry point.

Starts:
1. SQLite DB initialization
2. Chrome Manager (isolated Chrome instance)
3. Skill Updater (auto-pulls from GitHub)
4. Skill Scheduler (runs skills on schedule)
5. Dashboard (FastAPI on localhost:7777)

Usage:
    python -m src
"""
import logging
import os
import signal
import sys
import threading

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


def main():
    logger.info("=" * 50)
    logger.info("Memory Tap starting...")
    logger.info("=" * 50)

    # 1. Initialize database
    logger.info("Initializing database...")
    init_db()

    # 2. Start Chrome Manager
    chrome = ChromeManager()
    logger.info("Ensuring Chrome is running...")
    if not chrome.ensure_running():
        logger.error("Failed to start Chrome. Exiting.")
        sys.exit(1)

    # 3. Start Skill Updater (downloads skills from GitHub)
    updater = SkillUpdater()
    updater.start()

    # Give updater a moment to download initial skills
    import time
    time.sleep(3)

    # 4. Start Scheduler
    scheduler = SkillScheduler(chrome)
    scheduler.load_skills_from_dir(LOCAL_SKILLS_DIR)
    scheduler.start()

    # 5. Start Dashboard
    from .dashboard.app import app, set_app_deps
    set_app_deps(chrome, scheduler)

    logger.info("Starting dashboard on http://localhost:%d", DASHBOARD_PORT)

    # Handle shutdown
    def shutdown(signum=None, frame=None):
        logger.info("Shutting down...")
        scheduler.stop()
        updater.stop()
        chrome.shutdown()
        logger.info("Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Run FastAPI (blocking)
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=DASHBOARD_PORT, log_level="warning")


if __name__ == "__main__":
    main()
