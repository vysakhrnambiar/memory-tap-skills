"""
Core DB — shared infrastructure database for Memory Tap.

Contains: settings, skill_registry, sync_log, tab_registry,
notifications, alerts, widget_config.

No skill-specific data here — that goes in per-skill DBs.
See spec/core_db_schema.md for full schema documentation.
"""
import os
import sqlite3
import logging

logger = logging.getLogger("memory_tap.db.core")

LOCALAPPDATA = os.environ.get("LOCALAPPDATA", "")
DATA_DIR = os.path.join(LOCALAPPDATA, "MemoryTap")
CORE_DB_PATH = os.path.join(DATA_DIR, "core.db")


def get_core_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Get a connection to core.db with WAL mode and row_factory."""
    path = db_path or CORE_DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_core_db(db_path: str | None = None):
    """Create all core tables. Safe to call repeatedly (IF NOT EXISTS)."""
    conn = get_core_connection(db_path)
    cur = conn.cursor()

    # --- settings ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- skill_registry ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS skill_registry (
            name TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            auth_provider TEXT NOT NULL DEFAULT '',
            target_url TEXT NOT NULL DEFAULT '',
            db_path TEXT NOT NULL,
            login_url TEXT NOT NULL DEFAULT '',
            tabs_needed INTEGER NOT NULL DEFAULT 1,
            schedule_hours INTEGER NOT NULL DEFAULT 3,
            enabled INTEGER NOT NULL DEFAULT 1,
            login_status TEXT NOT NULL DEFAULT 'unknown',
            last_sync_at TEXT,
            last_sync_items INTEGER DEFAULT 0,
            last_error TEXT,
            installed_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- sync_log ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_name TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at TEXT,
            items_found INTEGER DEFAULT 0,
            items_new INTEGER DEFAULT 0,
            items_updated INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'running',
            error TEXT,
            stop_reason TEXT,
            elapsed_minutes REAL DEFAULT 0,
            details TEXT
        )
    """)

    # --- tab_registry ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tab_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_name TEXT NOT NULL,
            tab_index INTEGER NOT NULL DEFAULT 0,
            chrome_tab_id TEXT,
            last_url TEXT NOT NULL DEFAULT 'about:blank',
            login_verified INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(skill_name, tab_index)
        )
    """)

    # --- notifications ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_name TEXT NOT NULL,
            level TEXT NOT NULL DEFAULT 'info',
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link_to TEXT,
            read INTEGER NOT NULL DEFAULT 0,
            dismissed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- alerts ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT NOT NULL DEFAULT 'warning',
            source TEXT NOT NULL DEFAULT 'system',
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            dismissed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- widget_config ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS widget_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_name TEXT NOT NULL,
            widget_name TEXT NOT NULL,
            position_x INTEGER NOT NULL DEFAULT 0,
            position_y INTEGER NOT NULL DEFAULT 0,
            width INTEGER NOT NULL DEFAULT 1,
            height INTEGER NOT NULL DEFAULT 1,
            visible INTEGER NOT NULL DEFAULT 1,
            config TEXT,
            UNIQUE(skill_name, widget_name)
        )
    """)

    # --- service_registry ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS service_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            skill_name TEXT NOT NULL,
            service_name TEXT NOT NULL,
            description TEXT DEFAULT '',
            input_schema TEXT DEFAULT '{}',
            output_schema TEXT DEFAULT '{}',
            max_duration_seconds INTEGER DEFAULT 60,
            status TEXT DEFAULT 'ready',
            UNIQUE(skill_name, service_name)
        )
    """)

    # --- service_requests ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS service_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_skill TEXT NOT NULL,
            to_skill TEXT NOT NULL,
            service_name TEXT NOT NULL,
            payload TEXT NOT NULL DEFAULT '{}',
            state TEXT NOT NULL DEFAULT 'PENDING',
            result TEXT,
            error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            claimed_at TEXT,
            completed_at TEXT,
            duration_seconds REAL
        )
    """)

    conn.commit()
    conn.close()
    logger.info("Core DB initialized: %s", db_path or CORE_DB_PATH)


# --- Settings helpers ---

def get_setting(key: str, default: str | None = None,
                db_path: str | None = None) -> str | None:
    """Read a setting value."""
    conn = get_core_connection(db_path)
    row = conn.execute(
        "SELECT value FROM settings WHERE key = ?", (key,)
    ).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str, db_path: str | None = None):
    """Write a setting value."""
    conn = get_core_connection(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now'))",
        (key, value),
    )
    conn.commit()
    conn.close()


# --- Alert helpers ---

def add_alert(title: str, message: str, level: str = "warning",
              source: str = "system", db_path: str | None = None):
    """Add a system alert."""
    conn = get_core_connection(db_path)
    conn.execute(
        "INSERT INTO alerts (level, source, title, message) VALUES (?, ?, ?, ?)",
        (level, source, title, message),
    )
    conn.commit()
    conn.close()


def get_alerts(include_dismissed: bool = False, limit: int = 20,
               db_path: str | None = None) -> list[dict]:
    """Get recent alerts."""
    conn = get_core_connection(db_path)
    if include_dismissed:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM alerts WHERE dismissed = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def dismiss_alert(alert_id: int, db_path: str | None = None):
    """Dismiss an alert."""
    conn = get_core_connection(db_path)
    conn.execute("UPDATE alerts SET dismissed = 1 WHERE id = ?", (alert_id,))
    conn.commit()
    conn.close()


# --- Notification helpers ---

def add_notification(skill_name: str, title: str, message: str,
                     level: str = "info", link_to: str | None = None,
                     db_path: str | None = None):
    """Add a skill notification."""
    conn = get_core_connection(db_path)
    conn.execute(
        "INSERT INTO notifications (skill_name, level, title, message, link_to) "
        "VALUES (?, ?, ?, ?, ?)",
        (skill_name, level, title, message, link_to),
    )
    conn.commit()
    conn.close()


def get_notifications(include_read: bool = False, limit: int = 50,
                      db_path: str | None = None) -> list[dict]:
    """Get notifications."""
    conn = get_core_connection(db_path)
    if include_read:
        rows = conn.execute(
            "SELECT * FROM notifications WHERE dismissed = 0 ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM notifications WHERE read = 0 AND dismissed = 0 "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_notification_read(notification_id: int, db_path: str | None = None):
    """Mark a notification as read."""
    conn = get_core_connection(db_path)
    conn.execute("UPDATE notifications SET read = 1 WHERE id = ?", (notification_id,))
    conn.commit()
    conn.close()


def dismiss_notification(notification_id: int, db_path: str | None = None):
    """Dismiss a notification."""
    conn = get_core_connection(db_path)
    conn.execute("UPDATE notifications SET dismissed = 1 WHERE id = ?", (notification_id,))
    conn.commit()
    conn.close()


# --- Skill Registry helpers ---

def register_skill(name: str, version: str, description: str,
                   auth_provider: str, target_url: str, db_path_skill: str,
                   login_url: str = "", tabs_needed: int = 1,
                   schedule_hours: int = 3,
                   db_path: str | None = None):
    """Register or update a skill in the registry."""
    conn = get_core_connection(db_path)
    conn.execute(
        """INSERT INTO skill_registry
           (name, version, description, auth_provider, target_url, db_path,
            login_url, tabs_needed, schedule_hours, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(name) DO UPDATE SET
               version = excluded.version,
               description = excluded.description,
               auth_provider = excluded.auth_provider,
               target_url = excluded.target_url,
               db_path = excluded.db_path,
               login_url = excluded.login_url,
               tabs_needed = excluded.tabs_needed,
               schedule_hours = excluded.schedule_hours,
               updated_at = datetime('now')""",
        (name, version, description, auth_provider, target_url, db_path_skill,
         login_url, tabs_needed, schedule_hours),
    )
    conn.commit()
    conn.close()


def get_skill_info(name: str, db_path: str | None = None) -> dict | None:
    """Get skill registry entry."""
    conn = get_core_connection(db_path)
    row = conn.execute(
        "SELECT * FROM skill_registry WHERE name = ?", (name,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_skills(db_path: str | None = None) -> list[dict]:
    """Get all registered skills."""
    conn = get_core_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM skill_registry ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_skill_login_status(name: str, status: str,
                              db_path: str | None = None):
    """Update login status for a skill."""
    conn = get_core_connection(db_path)
    conn.execute(
        "UPDATE skill_registry SET login_status = ?, updated_at = datetime('now') WHERE name = ?",
        (status, name),
    )
    conn.commit()
    conn.close()


def update_skill_sync(name: str, last_sync_items: int,
                      last_error: str | None = None,
                      db_path: str | None = None):
    """Update last sync info for a skill."""
    conn = get_core_connection(db_path)
    conn.execute(
        """UPDATE skill_registry
           SET last_sync_at = datetime('now'), last_sync_items = ?,
               last_error = ?, updated_at = datetime('now')
           WHERE name = ?""",
        (last_sync_items, last_error, name),
    )
    conn.commit()
    conn.close()


# --- Tab Registry helpers ---

def get_skill_tabs(skill_name: str, db_path: str | None = None) -> list[dict]:
    """Get all registered tabs for a skill."""
    conn = get_core_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM tab_registry WHERE skill_name = ? ORDER BY tab_index",
        (skill_name,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def upsert_skill_tab(skill_name: str, tab_index: int, chrome_tab_id: str,
                     last_url: str, login_verified: bool = False,
                     db_path: str | None = None):
    """Insert or update a tab registry entry."""
    conn = get_core_connection(db_path)
    conn.execute(
        """INSERT INTO tab_registry (skill_name, tab_index, chrome_tab_id, last_url, login_verified, updated_at)
           VALUES (?, ?, ?, ?, ?, datetime('now'))
           ON CONFLICT(skill_name, tab_index) DO UPDATE SET
               chrome_tab_id = excluded.chrome_tab_id,
               last_url = excluded.last_url,
               login_verified = excluded.login_verified,
               updated_at = datetime('now')""",
        (skill_name, tab_index, chrome_tab_id, last_url, int(login_verified)),
    )
    conn.commit()
    conn.close()


def get_all_registered_tabs(db_path: str | None = None) -> list[dict]:
    """Get all registered tabs across all skills."""
    conn = get_core_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM tab_registry ORDER BY skill_name, tab_index"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_active_skills(db_path: str | None = None) -> int:
    """Count how many distinct skills have registered tabs."""
    conn = get_core_connection(db_path)
    row = conn.execute(
        "SELECT COUNT(DISTINCT skill_name) as cnt FROM tab_registry"
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


# --- Sync Log helpers ---

def start_sync_log(skill_name: str, db_path: str | None = None) -> int:
    """Start a sync log entry. Returns log ID."""
    conn = get_core_connection(db_path)
    cur = conn.execute(
        "INSERT INTO sync_log (skill_name) VALUES (?)", (skill_name,)
    )
    log_id = cur.lastrowid
    conn.commit()
    conn.close()
    return log_id


def finish_sync_log(log_id: int, items_found: int, items_new: int,
                    items_updated: int, elapsed_minutes: float,
                    status: str = "completed", error: str | None = None,
                    stop_reason: str | None = None,
                    db_path: str | None = None):
    """Complete a sync log entry."""
    conn = get_core_connection(db_path)
    conn.execute(
        """UPDATE sync_log
           SET finished_at = datetime('now'), items_found = ?, items_new = ?,
               items_updated = ?, elapsed_minutes = ?, status = ?,
               error = ?, stop_reason = ?
           WHERE id = ?""",
        (items_found, items_new, items_updated, elapsed_minutes,
         status, error, stop_reason, log_id),
    )
    conn.commit()
    conn.close()


# --- Widget Config helpers ---

def get_widget_config(db_path: str | None = None) -> list[dict]:
    """Get all widget configs for home screen (visible only)."""
    conn = get_core_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM widget_config WHERE visible = 1 ORDER BY position_y, position_x"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_widget_config(db_path: str | None = None) -> list[dict]:
    """Get ALL widget configs including hidden ones (for customize panel)."""
    conn = get_core_connection(db_path)
    rows = conn.execute(
        "SELECT * FROM widget_config ORDER BY position_y, position_x"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_widget_config(skill_name: str, widget_name: str,
                      position_x: int = 0, position_y: int = 0,
                      width: int = 1, height: int = 1,
                      visible: bool = True, config: str | None = None,
                      db_path: str | None = None):
    """Set or update a widget's config."""
    conn = get_core_connection(db_path)
    conn.execute(
        """INSERT INTO widget_config
           (skill_name, widget_name, position_x, position_y, width, height, visible, config)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(skill_name, widget_name) DO UPDATE SET
               position_x = excluded.position_x,
               position_y = excluded.position_y,
               width = excluded.width,
               height = excluded.height,
               visible = excluded.visible,
               config = excluded.config""",
        (skill_name, widget_name, position_x, position_y, width, height,
         int(visible), config),
    )
    conn.commit()
    conn.close()
