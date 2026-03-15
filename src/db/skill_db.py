"""
Skill DB Manager — manages per-skill SQLite databases.

Each skill gets its own DB file at:
    %LOCALAPPDATA%/MemoryTap/skill_data/{skill_name}.db

Skills define their own schema via create_schema(conn).
This manager handles: file creation, connections, schema versioning,
backup, and deletion.

See spec/skill_db_interface.md for full specification.
"""
import logging
import os
import shutil
import sqlite3

from .core_db import (
    get_core_connection, get_skill_info, register_skill,
    CORE_DB_PATH,
)

logger = logging.getLogger("memory_tap.db.skill")

LOCALAPPDATA = os.environ.get("LOCALAPPDATA", "")
SKILL_DATA_DIR = os.path.join(LOCALAPPDATA, "MemoryTap", "skill_data")


class SkillDBManager:
    """Manages per-skill SQLite databases.

    Usage:
        mgr = SkillDBManager()

        # On skill load — ensures DB exists + schema is current
        mgr.ensure_schema(skill)

        # During skill run — get connection
        conn = mgr.get_connection("youtube_history")
        # ... use conn ...
        conn.close()

        # Backup
        mgr.backup("youtube_history", "~/Documents/youtube_backup.db")

        # Uninstall
        mgr.delete("youtube_history")
    """

    def __init__(self, data_dir: str | None = None,
                 core_db_path: str | None = None):
        self.data_dir = data_dir or SKILL_DATA_DIR
        self.core_db_path = core_db_path or CORE_DB_PATH
        os.makedirs(self.data_dir, exist_ok=True)

    def get_db_path(self, skill_name: str) -> str:
        """Return the full path to a skill's DB file."""
        return os.path.join(self.data_dir, f"{skill_name}.db")

    def get_connection(self, skill_name: str) -> sqlite3.Connection:
        """Get a connection to a skill's DB.

        Creates the file if it doesn't exist.
        Sets WAL mode and row_factory.
        """
        path = self.get_db_path(skill_name)
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self, skill) -> bool:
        """Ensure a skill's DB exists and schema is current.

        Args:
            skill: a BaseSkill instance with manifest and create_schema()

        Flow:
        1. Check skill_registry in core.db for stored version
        2. If no entry → first time: call skill.create_schema(conn)
        3. If version changed → call skill.migrate_schema(conn, old, new)
        4. Update skill_registry with current version

        Returns True if schema was created or migrated, False if already current.
        """
        m = skill.manifest
        db_path = self.get_db_path(m.name)
        rel_path = f"skill_data/{m.name}.db"

        # Check current state in core.db
        info = get_skill_info(m.name, self.core_db_path)
        stored_version = info["version"] if info else None

        conn = self.get_connection(m.name)
        changed = False

        try:
            if stored_version is None:
                # First time — create schema
                logger.info("Creating schema for skill '%s' v%s", m.name, m.version)
                skill.create_schema(conn)
                changed = True

            elif stored_version != m.version:
                # Version changed — migrate
                logger.info("Migrating skill '%s' from v%s to v%s",
                            m.name, stored_version, m.version)
                if hasattr(skill, 'migrate_schema'):
                    skill.migrate_schema(conn, stored_version, m.version)
                else:
                    logger.warning("Skill '%s' has no migrate_schema — "
                                   "recreating schema", m.name)
                    skill.create_schema(conn)
                changed = True

            else:
                logger.debug("Skill '%s' schema is current (v%s)", m.name, m.version)

        except Exception as e:
            logger.error("Schema error for skill '%s': %s", m.name, e)
            conn.close()
            raise

        conn.close()

        # Register/update in core.db
        register_skill(
            name=m.name,
            version=m.version,
            description=m.description,
            auth_provider=m.auth_provider,
            target_url=m.target_url,
            db_path_skill=rel_path,
            login_url=m.login_url,
            tabs_needed=m.tabs_needed,
            schedule_hours=m.schedule_hours,
            db_path=self.core_db_path,
        )

        return changed

    def backup(self, skill_name: str, dest_path: str) -> bool:
        """Backup a skill's DB to a destination path.

        Uses SQLite's backup API for safe live backup.
        Returns True if successful.
        """
        src_path = self.get_db_path(skill_name)
        if not os.path.isfile(src_path):
            logger.warning("Cannot backup '%s' — DB file not found", skill_name)
            return False

        try:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            src_conn = sqlite3.connect(src_path)
            dst_conn = sqlite3.connect(dest_path)
            src_conn.backup(dst_conn)
            dst_conn.close()
            src_conn.close()
            logger.info("Backed up '%s' to %s", skill_name, dest_path)
            return True
        except Exception as e:
            logger.error("Backup failed for '%s': %s", skill_name, e)
            return False

    def delete(self, skill_name: str) -> bool:
        """Delete a skill's DB file. Used during uninstall.

        Also cleans up core.db entries for this skill.
        Returns True if deleted.
        """
        path = self.get_db_path(skill_name)

        # Delete DB file
        if os.path.isfile(path):
            try:
                os.remove(path)
                logger.info("Deleted skill DB: %s", path)
            except Exception as e:
                logger.error("Failed to delete '%s': %s", path, e)
                return False

        # Clean core.db entries
        try:
            conn = get_core_connection(self.core_db_path)
            # skill_registry uses 'name', others use 'skill_name'
            conn.execute("DELETE FROM skill_registry WHERE name = ?", (skill_name,))
            for table in ["sync_log", "tab_registry",
                           "notifications", "widget_config"]:
                conn.execute(
                    f"DELETE FROM {table} WHERE skill_name = ?", (skill_name,)
                )
            conn.commit()
            conn.close()
            logger.info("Cleaned core.db entries for '%s'", skill_name)
        except Exception as e:
            logger.error("Failed to clean core.db for '%s': %s", skill_name, e)

        return True

    def list_skill_dbs(self) -> list[dict]:
        """List all skill DB files with sizes."""
        result = []
        if not os.path.isdir(self.data_dir):
            return result
        for f in os.listdir(self.data_dir):
            if f.endswith(".db"):
                path = os.path.join(self.data_dir, f)
                result.append({
                    "name": f.replace(".db", ""),
                    "path": path,
                    "size_bytes": os.path.getsize(path),
                    "size_mb": round(os.path.getsize(path) / (1024 * 1024), 2),
                })
        return result

    def db_exists(self, skill_name: str) -> bool:
        """Check if a skill's DB file exists."""
        return os.path.isfile(self.get_db_path(skill_name))
