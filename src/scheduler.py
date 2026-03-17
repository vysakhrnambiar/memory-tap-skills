"""
Scheduler — runs skills on their configured schedule.

Runs in a background thread. Each skill has a schedule_hours setting.
Skills execute sequentially (one Chrome tab at a time).
"""
import importlib
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta

from .cdp_client import CDPClient
from .chrome_manager import ChromeManager, HealthMonitor
from .db.core_db import get_core_connection, get_skill_info, register_skill, CORE_DB_PATH, add_alert
from .db.skill_db import SkillDBManager
from .skills.base import BaseSkill

logger = logging.getLogger("memory_tap.scheduler")


class SkillScheduler:
    """Manages periodic execution of collection skills."""

    def __init__(self, chrome: ChromeManager, core_db_path: str | None = None):
        self.chrome = chrome
        self.core_db_path = core_db_path or CORE_DB_PATH
        self.skill_db_mgr = SkillDBManager(core_db_path=self.core_db_path)
        self.health = HealthMonitor(chrome)
        self._skills: dict[str, BaseSkill] = {}
        self.skill_running: str | None = None  # name of currently running skill, or None
        self._thread: threading.Thread | None = None
        self._running = False
        self._check_interval = 60  # check every 60 seconds

    def register_skill(self, skill: BaseSkill):
        """Register a skill for scheduled execution."""
        m = skill.manifest
        self._skills[m.name] = skill

        # Ensure skill DB + schema exists, register in core.db
        self.skill_db_mgr.ensure_schema(skill)
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
        """Load a skill from a Python file.

        Injects the project root into sys.path so skills can import from src.*
        Skills use: from src.skills.base import BaseSkill, etc.
        """
        import importlib.util

        # Ensure src package is importable from wherever skills are loaded
        # Find the directory that contains the 'src' package
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

        # Give each skill a unique module name to avoid collisions
        module_name = f"memory_tap_skill_{os.path.basename(filepath).replace('.py', '')}"
        spec = importlib.util.spec_from_file_location(module_name, filepath)
        module = importlib.util.module_from_spec(spec)

        try:
            spec.loader.exec_module(module)
        except Exception as e:
            logger.error("Failed to execute skill %s: %s", filepath, e)
            return

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
        conn = get_core_connection(self.core_db_path)
        row = conn.execute(
            "SELECT enabled, last_sync_at, schedule_hours, login_status FROM skill_registry WHERE name = ?",
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

        # Check health before running
        if self.health.last_check and not self.health.healthy:
            logger.warning("Skipping skill %s — Chrome is unhealthy", skill_name)
            return {"skill": skill_name, "success": False, "items_found": 0,
                    "items_new": 0, "items_updated": 0,
                    "error": "Chrome is not healthy. Check if Chrome is running."}

        if not self.chrome.is_running():
            if not self.chrome.ensure_running():
                logger.error("Chrome not available")
                return None

        # Run skill with hard kill timeout (45 min safety net)
        HARD_KILL_MINUTES = 45
        result_holder = [None]
        error_holder = [None]
        self.skill_running = skill_name

        def _run_skill():
            try:
                with CDPClient() as client:
                    result_holder[0] = skill.run(
                        client, self.skill_db_mgr, self.core_db_path
                    )
            except Exception as e:
                error_holder[0] = str(e)

        run_thread = threading.Thread(target=_run_skill, daemon=True)
        run_thread.start()
        run_thread.join(timeout=HARD_KILL_MINUTES * 60)

        if run_thread.is_alive():
            logger.error("Skill %s HARD KILLED after %d minutes", skill_name, HARD_KILL_MINUTES)
            from .db.core_db import add_alert
            add_alert(
                f"{skill_name}: Hard Kill",
                f"Skill ran for over {HARD_KILL_MINUTES} minutes and was forcefully stopped.",
                level="error", source=skill_name, db_path=self.core_db_path,
            )
            return {
                "skill": skill_name, "success": False, "items_found": 0,
                "items_new": 0, "items_updated": 0,
                "error": f"Hard killed after {HARD_KILL_MINUTES} minutes",
                "details": {},
            }

        if error_holder[0]:
            return {
                "skill": skill_name, "success": False, "items_found": 0,
                "items_new": 0, "items_updated": 0,
                "error": error_holder[0], "details": {},
            }

        self.skill_running = None

        # Level 2: Audit tabs after every skill run (success or fail)
        try:
            self.chrome.audit_tabs()
        except Exception as e:
            logger.warning("Post-skill audit_tabs failed: %s", e)

        result = result_holder[0]
        if result is None:
            return None

        return {
            "skill": skill_name,
            "success": result.success,
            "items_found": result.items_found,
            "items_new": result.items_new,
            "items_updated": result.items_updated,
            "error": result.error,
            "details": result.details,
        }

    def _get_interrupted_skills(self) -> list[str]:
        """Find skills whose last sync_log entry has status 'running' or an error.

        These are skills that were interrupted (e.g., Chrome crash) and should
        be retried after recovery.
        """
        interrupted = []
        conn = get_core_connection(self.core_db_path)
        for skill_name in self._skills:
            row = conn.execute(
                "SELECT status, error FROM sync_log "
                "WHERE skill_name = ? ORDER BY id DESC LIMIT 1",
                (skill_name,),
            ).fetchone()
            if row and (row["status"] == "running" or row["error"]):
                interrupted.append(skill_name)
        conn.close()
        return interrupted

    def retry_interrupted_skills(self):
        """Check for skills interrupted by a crash and re-run them.

        Called after Chrome auto-relaunches (health monitor detects recovery).
        Waits 30 seconds before retrying to let Chrome stabilize.
        """
        interrupted = self._get_interrupted_skills()
        if not interrupted:
            return

        logger.info("Found %d interrupted skill(s) after recovery: %s",
                     len(interrupted), ", ".join(interrupted))
        add_alert(
            "Retrying interrupted skills",
            f"Chrome recovered. Retrying: {', '.join(interrupted)}",
            level="info", source="scheduler", db_path=self.core_db_path,
        )

        # Wait 30 seconds for Chrome to stabilize
        for _ in range(30):
            if not self._running:
                return
            time.sleep(1)

        for name in interrupted:
            if not self._running:
                break
            logger.info("Retrying interrupted skill: %s", name)
            try:
                self.run_skill(name)
            except Exception as e:
                logger.error("Retry failed for %s: %s", name, e)

    def start(self):
        """Start the scheduler background thread and health monitor."""
        if self._running:
            return
        self._running = True

        # Wire up recovery callback: when health monitor detects Chrome
        # came back after a crash, retry any interrupted skills
        self.health.on_recovery = self._on_chrome_recovery
        self.health.scheduler_ref = self  # Level 3 audit checks this before closing tabs
        self.health.start()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler started with %d skills", len(self._skills))

    def _on_chrome_recovery(self):
        """Called by HealthMonitor when Chrome recovers from a crash."""
        thread = threading.Thread(
            target=self.retry_interrupted_skills, daemon=True
        )
        thread.start()

    def stop(self):
        """Stop the scheduler and health monitor."""
        self._running = False
        self.health.stop()
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

    def reload_skills(self, skills_dir: str | None = None):
        """Hot-reload all skill modules from disk.

        Called after SkillUpdater downloads new versions from GitHub.
        For each .py file in the skills directory:
        1. Re-import using importlib.reload (or fresh load if new)
        2. Re-register with updated manifest
        3. Log what was reloaded

        Existing schedule state (last_sync_at, etc.) is preserved in core.db.
        """
        if skills_dir is None:
            from .updater.skill_updater import LOCAL_SKILLS_DIR
            skills_dir = LOCAL_SKILLS_DIR

        if not os.path.isdir(skills_dir):
            logger.warning("reload_skills: directory not found: %s", skills_dir)
            return

        reloaded = []
        for filename in os.listdir(skills_dir):
            if not filename.endswith(".py") or filename.startswith("_"):
                continue

            filepath = os.path.join(skills_dir, filename)
            module_name = f"memory_tap_skill_{filename.replace('.py', '')}"

            try:
                # Check if module was previously loaded
                if module_name in sys.modules:
                    # Reload existing module
                    old_module = sys.modules[module_name]
                    module = importlib.reload(old_module)
                    logger.info("Reloaded module: %s", module_name)
                else:
                    # Fresh load (new skill file)
                    import importlib.util
                    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                    if project_root not in sys.path:
                        sys.path.insert(0, project_root)

                    spec = importlib.util.spec_from_file_location(module_name, filepath)
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = module
                    spec.loader.exec_module(module)
                    logger.info("Loaded new module: %s", module_name)

                # Find the skill class and re-register
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type) and issubclass(attr, BaseSkill)
                            and attr is not BaseSkill):
                        skill = attr()
                        old_version = None
                        if skill.manifest.name in self._skills:
                            old_version = self._skills[skill.manifest.name].manifest.version
                        self.register_skill(skill)
                        new_version = skill.manifest.version
                        if old_version and old_version != new_version:
                            logger.info("Skill %s updated: v%s -> v%s",
                                        skill.manifest.name, old_version, new_version)
                        reloaded.append(skill.manifest.name)
                        break

            except Exception as e:
                logger.error("Failed to reload skill %s: %s", filename, e)

        if reloaded:
            logger.info("Hot-reloaded %d skill(s): %s", len(reloaded), ", ".join(reloaded))
        else:
            logger.info("No skills to reload")

    @property
    def skill_names(self) -> list[str]:
        return list(self._skills.keys())
