"""
Skill Updater — auto-pulls skill updates from the official GitHub repo.

Only pulls from: https://github.com/vysakhrnambiar/memory-tap-skills
Verifies checksums before applying updates.
"""
import hashlib
import json
import logging
import os
import time
import threading

import requests

logger = logging.getLogger("memory_tap.updater")

SKILLS_REPO = "vysakhrnambiar/memory-tap-skills"
MANIFEST_URL = f"https://raw.githubusercontent.com/{SKILLS_REPO}/main/manifest.json"
RAW_BASE = f"https://raw.githubusercontent.com/{SKILLS_REPO}/main/skills"

# Local skills directory
LOCAL_SKILLS_DIR = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "skills"
)


class SkillUpdater:
    """Polls GitHub for skill updates and downloads them."""

    def __init__(self, skills_dir: str | None = None, check_hours: int = 6,
                 scheduler=None):
        self.skills_dir = skills_dir or LOCAL_SKILLS_DIR
        self.check_hours = check_hours
        self.scheduler = scheduler  # SkillScheduler ref for hot-reload
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_manifest: dict | None = None
        os.makedirs(self.skills_dir, exist_ok=True)

    def fetch_manifest(self) -> dict | None:
        """Fetch the skill manifest from GitHub."""
        try:
            resp = requests.get(MANIFEST_URL, timeout=15)
            if resp.status_code == 200:
                manifest = resp.json()
                self._last_manifest = manifest
                return manifest
            logger.warning("Manifest fetch returned %d", resp.status_code)
        except Exception as e:
            logger.error("Failed to fetch manifest: %s", e)
        return None

    def get_local_version(self, skill_name: str) -> str | None:
        """Read version from local skill's metadata comment."""
        path = os.path.join(self.skills_dir, f"{skill_name}.py")
        if not os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            # Look for __version__ = "x.y.z"
            for line in content.split("\n"):
                if line.strip().startswith("__version__"):
                    return line.split("=")[1].strip().strip('"').strip("'")
        except Exception:
            pass
        return None

    def check_updates(self) -> list[dict]:
        """Check for available updates. Returns list of updatable skills."""
        manifest = self.fetch_manifest()
        if not manifest:
            return []

        updates = []
        for skill in manifest.get("skills", []):
            name = skill["name"]
            remote_version = skill["version"]
            local_version = self.get_local_version(name)

            if local_version != remote_version:
                updates.append({
                    "name": name,
                    "local_version": local_version or "(not installed)",
                    "remote_version": remote_version,
                    "description": skill.get("description", ""),
                })

        return updates

    def download_skill(self, skill_name: str) -> bool:
        """Download a skill file from GitHub, verify checksum, save locally."""
        if not self._last_manifest:
            self.fetch_manifest()
        if not self._last_manifest:
            return False

        skill_info = None
        for s in self._last_manifest.get("skills", []):
            if s["name"] == skill_name:
                skill_info = s
                break

        if not skill_info:
            logger.error("Skill %s not found in manifest", skill_name)
            return False

        filename = skill_info.get("file", f"{skill_name}.py")
        url = f"{RAW_BASE}/{filename}"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                logger.error("Download failed for %s: HTTP %d", skill_name, resp.status_code)
                return False

            content = resp.text

            # Verify checksum if provided
            expected_checksum = skill_info.get("checksum", "")
            if expected_checksum:
                actual = hashlib.sha256(content.encode("utf-8")).hexdigest()
                if expected_checksum.startswith("sha256:"):
                    expected_checksum = expected_checksum[7:]
                if actual != expected_checksum:
                    logger.error(
                        "Checksum mismatch for %s: expected %s, got %s",
                        skill_name, expected_checksum, actual,
                    )
                    return False

            # Save
            path = os.path.join(self.skills_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)

            logger.info("Downloaded skill %s v%s", skill_name, skill_info["version"])
            return True

        except Exception as e:
            logger.error("Failed to download skill %s: %s", skill_name, e)
            return False

    def update_all(self) -> list[str]:
        """Download all available updates. Returns list of updated skill names.

        After downloading, triggers hot-reload on the scheduler (if available)
        so updated skill modules are re-imported without restarting.
        """
        updates = self.check_updates()
        updated = []
        for u in updates:
            if self.download_skill(u["name"]):
                updated.append(u["name"])

        # Hot-reload updated skills in the scheduler
        if updated and self.scheduler is not None:
            try:
                self.scheduler.reload_skills(self.skills_dir)
                logger.info("Hot-reloaded skills after update: %s", updated)
            except Exception as e:
                logger.error("Hot-reload failed after update: %s", e)

        return updated

    def start(self):
        """Start background update polling."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("Skill updater started (checking every %dh)", self.check_hours)

    def stop(self):
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self._thread = None

    def _loop(self):
        """Background polling loop."""
        # Initial check on startup
        try:
            updated = self.update_all()
            if updated:
                logger.info("Updated skills on startup: %s", updated)
        except Exception as e:
            logger.error("Initial skill update failed: %s", e)

        while self._running:
            for _ in range(self.check_hours * 3600):
                if not self._running:
                    return
                time.sleep(1)
            try:
                updated = self.update_all()
                if updated:
                    logger.info("Updated skills: %s", updated)
            except Exception as e:
                logger.error("Skill update check failed: %s", e)
