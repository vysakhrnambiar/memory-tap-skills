"""
Sync Tracker — incremental sync logic for Memory Tap.

Key strategy for detecting updates:
- ChatGPT/Gemini: conversation list position. If a conversation moved to
  the top since last sync, it was updated. Track position per conversation.
- YouTube: video_id uniqueness. If we've seen it, skip it.
- Sync log tracks every run for audit.
"""
import sqlite3
from datetime import datetime, timezone

from .models import get_connection


class SyncTracker:
    """Tracks sync state for a source (skill)."""

    def __init__(self, source: str, db_path: str | None = None):
        self.source = source
        self.db_path = db_path
        self._log_id: int | None = None

    def start_sync(self) -> int:
        """Mark sync as started. Returns log ID."""
        conn = get_connection(self.db_path)
        cur = conn.execute(
            "INSERT INTO sync_log (source) VALUES (?)",
            (self.source,),
        )
        self._log_id = cur.lastrowid
        conn.commit()
        conn.close()
        return self._log_id

    def finish_sync(self, items_found: int, items_new: int, items_updated: int,
                    error: str | None = None):
        """Mark sync as finished."""
        if not self._log_id:
            return
        conn = get_connection(self.db_path)
        status = "error" if error else "completed"
        conn.execute(
            """UPDATE sync_log
               SET finished_at = datetime('now'), items_found = ?, items_new = ?,
                   items_updated = ?, status = ?, error = ?
               WHERE id = ?""",
            (items_found, items_new, items_updated, status, error, self._log_id),
        )
        # Update source last_sync
        conn.execute(
            """UPDATE sources
               SET last_sync_at = datetime('now'), last_sync_items = ?,
                   last_error = ?
               WHERE name = ?""",
            (items_found, error, self.source),
        )
        conn.commit()
        conn.close()

    # --- Conversation tracking ---

    def get_conversation(self, external_id: str) -> dict | None:
        """Get existing conversation by external ID."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT * FROM conversations WHERE source = ? AND external_id = ?",
            (self.source, external_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def upsert_conversation(self, external_id: str, title: str, url: str | None = None,
                            list_position: int | None = None) -> tuple[int, bool]:
        """Insert or update conversation. Returns (id, is_new)."""
        conn = get_connection(self.db_path)
        existing = conn.execute(
            "SELECT id, list_position FROM conversations WHERE source = ? AND external_id = ?",
            (self.source, external_id),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE conversations
                   SET title = ?, url = COALESCE(?, url), list_position = ?,
                       last_synced_at = datetime('now')
                   WHERE id = ?""",
                (title, url, list_position, existing["id"]),
            )
            conn.commit()
            conn.close()
            return existing["id"], False
        else:
            cur = conn.execute(
                """INSERT INTO conversations (source, external_id, title, url, list_position,
                                             created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))""",
                (self.source, external_id, title, url, list_position),
            )
            conv_id = cur.lastrowid
            conn.commit()
            conn.close()
            return conv_id, True

    def conversation_needs_update(self, external_id: str, current_position: int) -> bool:
        """Check if conversation moved up in the list (was updated)."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT list_position FROM conversations WHERE source = ? AND external_id = ?",
            (self.source, external_id),
        ).fetchone()
        conn.close()
        if not row:
            return True  # new conversation
        old_pos = row["list_position"]
        if old_pos is None:
            return True
        return current_position < old_pos  # moved up = was updated

    def get_message_count(self, conversation_id: int) -> int:
        """Get number of messages already stored for a conversation."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
        conn.close()
        return row["cnt"] if row else 0

    def add_messages(self, conversation_id: int, messages: list[dict]):
        """Add messages to a conversation. Skips duplicates by message_order."""
        if not messages:
            return
        conn = get_connection(self.db_path)
        for msg in messages:
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO messages
                       (conversation_id, role, content, thinking_block, message_order, timestamp)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (conversation_id, msg["role"], msg["content"],
                     msg.get("thinking_block"), msg["message_order"],
                     msg.get("timestamp")),
                )
            except sqlite3.IntegrityError:
                pass
        # Update message count
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM messages WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()["cnt"]
        conn.execute(
            "UPDATE conversations SET message_count = ?, updated_at = datetime('now') WHERE id = ?",
            (count, conversation_id),
        )
        conn.commit()
        conn.close()

    def add_artifact(self, conversation_id: int, filename: str,
                     content: str | None = None, file_path: str | None = None,
                     message_id: int | None = None, file_size: int | None = None):
        """Store an artifact (downloaded file) from a conversation."""
        conn = get_connection(self.db_path)
        conn.execute(
            """INSERT INTO artifacts (conversation_id, message_id, filename, content, file_path, file_size)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (conversation_id, message_id, filename, content, file_path, file_size),
        )
        conn.commit()
        conn.close()

    # --- YouTube tracking ---

    def video_exists(self, video_id: str) -> bool:
        """Check if we already have this video."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT 1 FROM youtube_videos WHERE video_id = ?", (video_id,)
        ).fetchone()
        conn.close()
        return row is not None

    def add_video(self, video_id: str, title: str, channel: str | None,
                  url: str, description: str | None = None,
                  top_comment: str | None = None,
                  top_comment_author: str | None = None,
                  duration: str | None = None,
                  watched_at: str | None = None) -> int:
        """Add a YouTube video. Returns row ID."""
        conn = get_connection(self.db_path)
        cur = conn.execute(
            """INSERT OR IGNORE INTO youtube_videos
               (video_id, title, channel, url, description, top_comment,
                top_comment_author, duration, watched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (video_id, title, channel, url, description, top_comment,
             top_comment_author, duration, watched_at),
        )
        row_id = cur.lastrowid
        conn.commit()
        conn.close()
        return row_id

    # --- Login status ---

    def get_login_status(self) -> str:
        """Get login status for this source."""
        conn = get_connection(self.db_path)
        row = conn.execute(
            "SELECT login_status FROM sources WHERE name = ?", (self.source,)
        ).fetchone()
        conn.close()
        return row["login_status"] if row else "unknown"

    def set_login_status(self, status: str):
        """Update login status (logged_in, not_logged_in, unknown)."""
        conn = get_connection(self.db_path)
        conn.execute(
            "UPDATE sources SET login_status = ? WHERE name = ?",
            (status, self.source),
        )
        conn.commit()
        conn.close()
