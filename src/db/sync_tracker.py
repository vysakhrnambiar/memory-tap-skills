"""
Sync Tracker — tracks sync state for skills.

Split architecture:
- Sync log + login status → core.db (shared infrastructure)
- Data queries (conversations, videos, etc.) → skill's own DB connection

Skills get a SyncTracker with both connections. Core operations use core.db,
data operations use the skill's own conn.
"""
import sqlite3
import logging

from .core_db import (
    get_core_connection, start_sync_log, finish_sync_log,
    update_skill_login_status, update_skill_sync,
    add_notification, CORE_DB_PATH,
)

logger = logging.getLogger("memory_tap.db.sync")


class SyncTracker:
    """Tracks sync state for a skill.

    Args:
        skill_name: name of the skill
        skill_conn: connection to the skill's own DB (for data queries)
        core_db_path: path to core.db (for sync_log, login_status)
    """

    def __init__(self, skill_name: str, skill_conn: sqlite3.Connection,
                 core_db_path: str | None = None):
        self.skill_name = skill_name
        self.conn = skill_conn              # skill's own DB
        self.core_db_path = core_db_path or CORE_DB_PATH
        self._log_id: int | None = None

    # --- Sync log (core.db) ---

    def start_sync(self) -> int:
        """Mark sync as started in core.db. Returns log ID."""
        self._log_id = start_sync_log(self.skill_name, self.core_db_path)
        return self._log_id

    def finish_sync(self, items_found: int, items_new: int, items_updated: int,
                    elapsed_minutes: float = 0, error: str | None = None,
                    stop_reason: str | None = None):
        """Mark sync as finished in core.db."""
        if not self._log_id:
            return
        status = "error" if error else "completed"
        finish_sync_log(
            self._log_id, items_found, items_new, items_updated,
            elapsed_minutes, status, error, stop_reason,
            self.core_db_path,
        )
        # Update skill registry
        update_skill_sync(
            self.skill_name, items_found, error,
            self.core_db_path,
        )

    # --- Login status (core.db) ---

    def get_login_status(self) -> str:
        """Get login status from core.db."""
        from .core_db import get_skill_info
        info = get_skill_info(self.skill_name, self.core_db_path)
        return info["login_status"] if info else "unknown"

    def set_login_status(self, status: str):
        """Update login status in core.db."""
        update_skill_login_status(self.skill_name, status, self.core_db_path)

    # --- Notifications (core.db) ---

    def notify(self, title: str, message: str, level: str = "info",
               link_to: str | None = None):
        """Push a notification to the dashboard."""
        add_notification(
            self.skill_name, title, message, level, link_to,
            self.core_db_path,
        )

    # --- Data queries (skill's own DB via self.conn) ---
    # These are generic helpers. Skills can also use self.conn directly.

    def execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute SQL on the skill's own DB."""
        return self.conn.execute(sql, params)

    def fetchone(self, sql: str, params: tuple = ()) -> dict | None:
        """Execute and fetch one row from skill's DB."""
        row = self.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def fetchall(self, sql: str, params: tuple = ()) -> list[dict]:
        """Execute and fetch all rows from skill's DB."""
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def commit(self):
        """Commit changes to skill's DB."""
        self.conn.commit()

    def item_count(self, table: str) -> int:
        """Count rows in a table in skill's DB."""
        row = self.conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
        return row["cnt"] if row else 0
