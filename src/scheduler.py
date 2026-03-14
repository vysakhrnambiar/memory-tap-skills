"""
Scheduler — runs skills on their configured schedule.

Runs in a background thread. Each skill has a schedule_hours setting.
Skills execute sequentially (one Chrome tab at a time).
"""
import importlib
import logging
import os
import threading
import time
from datetime import datetime, timedelta

from .cdp_client import CDPClient
from .chrome_manager import ChromeManager
from .db.models import get_connection, init_db
from .skills.base import BaseSkill

logger = logging.getLogger("memory_tap.scheduler")


class SkillScheduler:
    """Manages periodic execution of collection skills."""

    def __init__(self, chrome: ChromeManager, db_path: str | None = None):
        self.chrome = chrome
        self.db_path = db_path
        self._skills: dict[str, BaseSkill] = {}
        self._thread: threading.Thread | None = None
        self._running = False
        self._check_interval = 60  # check every 60 seconds

    def register_skill(self, skill: BaseSkill):
        """Register a skill for scheduled execution."""
        m = skill.manifest
        self._skills[m.name] = skill

        # Ensure source record exists in DB
        conn = get_connection(self.db_path)
        conn.execute(
            """INSERT OR IGNORE INTO sources (name, skill_version, target_url, schedule_hours)
               VALUES (?, ?, ?, ?)""",
            (m.name, m.version, m.target_url, m.schedule_hours),
        )
        conn.commit()
        conn.close()
        logger.info("Registered skill: %s v%s (every %dh)", m.name, m.version, m.schedule_hours)

    def load_skills_from_dir(self, skills_dir: str):
        """Load all skill .py files from a directory."""
        if not os.path.isdir(skills_dir):
            logger.warning("Skills directory not found: %s", skills_dir)
            return

        for filename in os.listdir(skills_dir):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue
            filepath = os.path.join(skills_dir, filename)
            try:
                self._load_skill_file(filepath)
            except Exception as e:
                logger.error("Failed to load skill %s: %s", filename, e)

    def _load_skill_file(self, filepath: str):
        """Load a skill from a Python file."""
        import importlib.util
        spec = importlib.util.spec_from_file_location("skill_module", filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Find the skill class (subclass of BaseSkill)
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and issubclass(attr, BaseSkill)
                    and attr is not BaseSkill):
                skill = attr()
                self.register_skill(skill)
                return

        logger.warning("No BaseSkill subclass found in %s", filepath)

    def _should_run(self, skill_name: str) -> bool:
        """Check if a skill is due to run based on its schedule."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT enabled, last_sync_at, schedule_hours, login_status FROM sources WHERE name = ?",
            (skill_name,),
        ).fetchone()
        conn.close()

        if not row or not row["enabled"]:
            return False

        if not row["last_sync_at"]:
            # Never run before — only auto-run if user has logged in at least once
            if row["login_status"] != "logged_in":
                return False  # Wait for user to sign in via dashboard first
            return True

        last_sync = datetime.fromisoformat(row["last_sync_at"])
        next_run = last_sync + timedelta(hours=row["schedule_hours"])
        return datetime.now() >= next_run

    def run_skill(self, skill_name: str) -> dict | None:
        """Run a specific skill immediately. Returns result dict."""
        skill = self._skills.get(skill_name)
        if not skill:
            logger.error("Skill not found: %s", skill_name)
            return None

        if not self.chrome.is_running():
            if not self.chrome.ensure_running():
                logger.error("Chrome not available")
                return None

        with CDPClient() as client:
            result = skill.run(client, self.db_path)
            return {
                "skill": skill_name,
                "success": result.success,
                "items_found": result.items_found,
                "items_new": result.items_new,
                "items_updated": result.items_updated,
                "error": result.error,
            }

    def start(self):
        """Start the scheduler background thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started with %d skills", len(self._skills))

    def stop(self):
        """Stop the scheduler."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._thread = None
        logger.info("Scheduler stopped")

    def _loop(self):
        """Main scheduler loop."""
        while self._running:
            for name, skill in list(self._skills.items()):
                if not self._running:
                    break
                try:
                    if self._should_run(name):
                        logger.info("Running scheduled skill: %s", name)
                        self.run_skill(name)
                except Exception as e:
                    logger.error("Scheduler error for %s: %s", name, e)

            # Wait before checking again
            for _ in range(self._check_interval):
                if not self._running:
                    break
                time.sleep(1)

    @property
    def skill_names(self) -> list[str]:
        return list(self._skills.keys())
