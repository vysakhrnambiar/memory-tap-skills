"""
SQLite schema for Memory Tap — all collected data lives here.

Tables:
- settings: LLM config, API keys, schedule
- sources: registered skills and their sync status
- conversations: ChatGPT / Gemini conversations
- messages: individual messages within conversations
- artifacts: downloaded files from conversations
- youtube_videos: watched videos with descriptions + top comments
- sync_log: audit trail of what was collected and when

Full-text search via FTS5 on messages and youtube_videos.
"""
import os
import sqlite3

DB_DIR = os.path.join(os.environ.get("LOCALAPPDATA", ""), "MemoryTap")
DB_PATH = os.path.join(DB_DIR, "memory_tap.db")


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    """Get a SQLite connection with WAL mode and foreign keys."""
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | None = None):
    """Create all tables and FTS indexes. Safe to call repeatedly."""
    conn = get_connection(db_path)
    cur = conn.cursor()

    # --- Settings ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # --- Sources (registered skills) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            name TEXT PRIMARY KEY,
            skill_version TEXT NOT NULL DEFAULT '0.0.0',
            target_url TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            login_status TEXT NOT NULL DEFAULT 'unknown',
            last_sync_at TEXT,
            last_sync_items INTEGER DEFAULT 0,
            last_error TEXT,
            schedule_hours INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- Conversations (ChatGPT, Gemini) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            external_id TEXT,
            title TEXT NOT NULL,
            url TEXT,
            list_position INTEGER,
            message_count INTEGER DEFAULT 0,
            created_at TEXT,
            updated_at TEXT,
            last_synced_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(source, external_id)
        )
    """)

    # --- Messages ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            thinking_block TEXT,
            message_order INTEGER NOT NULL,
            timestamp TEXT,
            UNIQUE(conversation_id, message_order)
        )
    """)

    # --- Artifacts (downloaded files from conversations) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
            message_id INTEGER REFERENCES messages(id),
            filename TEXT NOT NULL,
            content TEXT,
            file_path TEXT,
            file_size INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- YouTube Videos ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS youtube_videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            channel TEXT,
            url TEXT NOT NULL,
            description TEXT,
            top_comment TEXT,
            top_comment_author TEXT,
            duration TEXT,
            watched_at TEXT,
            synced_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # --- Sync Log (audit trail) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            started_at TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at TEXT,
            items_found INTEGER DEFAULT 0,
            items_new INTEGER DEFAULT 0,
            items_updated INTEGER DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'running',
            error TEXT,
            details TEXT
        )
    """)

    # --- FTS5 for full-text search ---
    cur.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
            content,
            thinking_block,
            content='messages',
            content_rowid='id',
            tokenize='porter unicode61'
        )
    """)

    cur.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS youtube_fts USING fts5(
            title,
            description,
            top_comment,
            content='youtube_videos',
            content_rowid='id',
            tokenize='porter unicode61'
        )
    """)

    # Triggers to keep FTS in sync
    for table, fts, cols in [
        ("messages", "messages_fts", "content, thinking_block"),
        ("youtube_videos", "youtube_fts", "title, description, top_comment"),
    ]:
        # After insert
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table}_ai AFTER INSERT ON {table} BEGIN
                INSERT INTO {fts}(rowid, {cols}) VALUES (new.id, {', '.join(f'new.{c.strip()}' for c in cols.split(','))});
            END
        """)
        # After update
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table}_au AFTER UPDATE ON {table} BEGIN
                INSERT INTO {fts}({fts}, rowid, {cols}) VALUES ('delete', old.id, {', '.join(f'old.{c.strip()}' for c in cols.split(','))});
                INSERT INTO {fts}(rowid, {cols}) VALUES (new.id, {', '.join(f'new.{c.strip()}' for c in cols.split(','))});
            END
        """)
        # After delete
        cur.execute(f"""
            CREATE TRIGGER IF NOT EXISTS {table}_ad AFTER DELETE ON {table} BEGIN
                INSERT INTO {fts}({fts}, rowid, {cols}) VALUES ('delete', old.id, {', '.join(f'old.{c.strip()}' for c in cols.split(','))});
            END
        """)

    conn.commit()
    conn.close()


def get_setting(key: str, default: str | None = None, db_path: str | None = None) -> str | None:
    """Read a setting value."""
    conn = get_connection(db_path)
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key: str, value: str, db_path: str | None = None):
    """Write a setting value."""
    conn = get_connection(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
        (key, value),
    )
    conn.commit()
    conn.close()
