"""
Dashboard — FastAPI web UI for Memory Tap.

Features:
- Settings page (LLM provider, API key, skill enable/disable)
- Timeline view (history by day — what was collected from sync_log in core.db)
- Skill status (from skill_registry in core.db)
- Login trigger (opens Chrome for user to sign in)
- Search across per-skill DBs
- Interest timeline visualization
"""
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ..db.models import get_connection, get_setting, set_setting

logger = logging.getLogger("memory_tap.dashboard")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
CORE_DB_PATH = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "core.db"
)
SKILL_DATA_DIR = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "skill_data"
)

app = FastAPI(title="Memory Tap", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# These get set by __main__.py at startup
_chrome_manager = None
_scheduler = None
_db_path = None


def set_app_deps(chrome_manager, scheduler, db_path=None):
    """Called by __main__ to inject dependencies."""
    global _chrome_manager, _scheduler, _db_path
    _chrome_manager = chrome_manager
    _scheduler = scheduler
    _db_path = db_path


# --- Pages ---

@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(STATIC_DIR, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


# --- Core DB helpers (skill_settings in core.db) ---

def _get_core_connection() -> sqlite3.Connection:
    """Get a connection to core.db (shared infrastructure DB)."""
    os.makedirs(os.path.dirname(CORE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(CORE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_skill_settings_table():
    """Create skill_settings table if it doesn't exist."""
    conn = _get_core_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS skill_settings (
            skill_name TEXT,
            key TEXT,
            value TEXT,
            PRIMARY KEY (skill_name, key)
        )
    """)
    conn.commit()
    conn.close()


def _get_skill_setting(skill_name: str, key: str) -> str | None:
    """Read a value from skill_settings in core.db."""
    conn = _get_core_connection()
    row = conn.execute(
        "SELECT value FROM skill_settings WHERE skill_name = ? AND key = ?",
        (skill_name, key),
    ).fetchone()
    conn.close()
    return row["value"] if row else None


def _set_skill_setting(skill_name: str, key: str, value: str):
    """Write a value to skill_settings in core.db."""
    _ensure_skill_settings_table()
    conn = _get_core_connection()
    conn.execute(
        "INSERT OR REPLACE INTO skill_settings (skill_name, key, value) VALUES (?, ?, ?)",
        (skill_name, key, value),
    )
    conn.commit()
    conn.close()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the given connection."""
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


# Settings keys that map to skill_settings in core.db
# Format: dashboard_key -> (skill_name, setting_key)
_SKILL_SETTINGS_MAP = {
    "openrouter_api_key": ("chatgpt_inference", "openrouter_api_key"),
    "interest_timeline_model": ("interest_timeline", "llm_model"),
    "garbage_detection_model": ("google_activity", "llm_model"),
}


# --- API: Settings ---

@app.get("/api/settings")
async def get_settings():
    settings = {}

    # Legacy settings from memory_tap.db (if available)
    try:
        conn = get_connection(_db_path)
        if _table_exists(conn, "settings"):
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            settings = {r["key"]: r["value"] for r in rows}
        conn.close()
    except Exception:
        logger.debug("Could not read legacy settings from memory_tap.db")

    # Skill settings from core.db skill_settings table
    _ensure_skill_settings_table()
    try:
        core_conn = _get_core_connection()
        for dashboard_key, (skill_name, setting_key) in _SKILL_SETTINGS_MAP.items():
            row = core_conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = ? AND key = ?",
                (skill_name, setting_key),
            ).fetchone()
            if row:
                settings[dashboard_key] = row["value"]
        core_conn.close()
    except Exception:
        logger.debug("Could not read skill settings from core.db")

    # Don't expose full API keys
    if "api_key" in settings:
        key = settings["api_key"]
        settings["api_key_masked"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
    if "openrouter_api_key" in settings:
        key = settings["openrouter_api_key"]
        settings["openrouter_api_key_masked"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
        del settings["openrouter_api_key"]  # Don't send raw key to frontend

    return settings


@app.post("/api/settings")
async def update_settings(request: Request):
    data = await request.json()
    _ensure_skill_settings_table()
    for key, value in data.items():
        if key and value is not None:
            if key in _SKILL_SETTINGS_MAP:
                skill_name, setting_key = _SKILL_SETTINGS_MAP[key]
                _set_skill_setting(skill_name, setting_key, str(value))
            else:
                set_setting(key, str(value), _db_path)
    return {"status": "ok"}


# --- API: Sources / Skills ---

@app.get("/api/sources")
async def get_sources():
    """Get all registered skills from core.db skill_registry."""
    try:
        conn = _get_core_connection()
        if not _table_exists(conn, "skill_registry"):
            conn.close()
            return []
        rows = conn.execute(
            "SELECT name, description, enabled, login_status, last_sync_at, "
            "last_sync_items, last_error, schedule_hours FROM skill_registry ORDER BY name"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Failed to load sources: %s", e)
        return []


@app.post("/api/sources/{name}/toggle")
async def toggle_source(name: str):
    """Toggle skill enabled/disabled in core.db skill_registry."""
    try:
        conn = _get_core_connection()
        if _table_exists(conn, "skill_registry"):
            conn.execute(
                "UPDATE skill_registry SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE name = ?",
                (name,),
            )
            conn.commit()
            row = conn.execute("SELECT enabled FROM skill_registry WHERE name = ?", (name,)).fetchone()
            conn.close()
            return {"name": name, "enabled": row["enabled"] if row else 0}
        conn.close()
    except Exception as e:
        logger.error("Failed to toggle source: %s", e)
    return {"name": name, "enabled": 0}


@app.post("/api/sources/{name}/run")
async def run_source_now(name: str):
    """Trigger immediate skill run."""
    if not _scheduler:
        return JSONResponse({"error": "Scheduler not initialized"}, status_code=500)
    result = _scheduler.run_skill(name)
    if result is None:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)
    return result


@app.post("/api/sources/{name}/login")
async def open_login(name: str):
    """Open Chrome for user to log in to a service."""
    if not _chrome_manager or not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)

    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)

    url = skill.get_login_url()
    _chrome_manager.open_headed(url)
    return {"status": "opened", "url": url}


# --- API: Timeline (history by day from sync_log in core.db) ---

@app.get("/api/timeline")
async def get_timeline(days: int = 7):
    """Get sync history grouped by day from core.db sync_log.

    Uses sync_log from core.db which tracks all skill runs
    (skill_name, items_found, items_new, status, errors, etc.).
    """
    try:
        conn = _get_core_connection()
        if not _table_exists(conn, "sync_log"):
            conn.close()
            return []

        since = (datetime.now() - timedelta(days=days)).isoformat()
        timeline = {}

        # Get sync_log entries from core.db
        logs = conn.execute(
            """SELECT skill_name, started_at, finished_at, items_found, items_new,
                      items_updated, status, error, elapsed_minutes
               FROM sync_log
               WHERE started_at >= ?
               ORDER BY started_at DESC""",
            (since,),
        ).fetchall()

        for log_entry in logs:
            day = (log_entry["started_at"] or "")[:10]
            if not day:
                continue
            if day not in timeline:
                timeline[day] = {
                    "date": day,
                    "conversations": [],
                    "videos": [],
                    "sync_runs": [],
                }
            timeline[day]["sync_runs"].append({
                "source": log_entry["skill_name"],
                "started_at": log_entry["started_at"],
                "finished_at": log_entry["finished_at"],
                "items_found": log_entry["items_found"] or 0,
                "items_new": log_entry["items_new"] or 0,
                "items_updated": log_entry["items_updated"] or 0,
                "status": log_entry["status"],
                "error": log_entry["error"],
                "elapsed_minutes": log_entry["elapsed_minutes"],
            })

        conn.close()

        # Sort by date descending
        result = sorted(timeline.values(), key=lambda x: x["date"], reverse=True)
        return result
    except Exception as e:
        logger.error("Failed to load timeline: %s", e)
        return []


# --- API: Stats ---

@app.get("/api/stats")
async def get_stats():
    """Overall collection stats from core.db."""
    stats = {
        "total_skills": 0,
        "active_skills": 0,
        "total_syncs": 0,
        "total_items_collected": 0,
        "sources": [],
    }

    try:
        conn = _get_core_connection()

        # Skill counts from skill_registry
        if _table_exists(conn, "skill_registry"):
            row = conn.execute("SELECT COUNT(*) as c FROM skill_registry").fetchone()
            stats["total_skills"] = row["c"] if row else 0

            row = conn.execute(
                "SELECT COUNT(*) as c FROM skill_registry WHERE enabled = 1"
            ).fetchone()
            stats["active_skills"] = row["c"] if row else 0

            # Per-skill info
            sources = conn.execute(
                "SELECT name, login_status, last_sync_at, last_error, enabled "
                "FROM skill_registry ORDER BY name"
            ).fetchall()
            for s in sources:
                stats["sources"].append(dict(s))

        # Sync stats from sync_log
        if _table_exists(conn, "sync_log"):
            row = conn.execute("SELECT COUNT(*) as c FROM sync_log").fetchone()
            stats["total_syncs"] = row["c"] if row else 0

            row = conn.execute(
                "SELECT COALESCE(SUM(items_new), 0) as c FROM sync_log WHERE status IN ('completed', 'success')"
            ).fetchone()
            stats["total_items_collected"] = row["c"] if row else 0

        conn.close()
    except Exception as e:
        logger.error("Failed to load stats: %s", e)

    return stats


# --- API: Search (for RAG / quick search) ---

@app.get("/api/search")
async def search(q: str, limit: int = 20):
    """Full-text search across per-skill databases."""
    results = []

    # Search in per-skill databases that have conversations/messages
    for skill_db_name in ["chatgpt_history.db", "gemini_history.db"]:
        skill_db_path = os.path.join(SKILL_DATA_DIR, skill_db_name)
        if not os.path.exists(skill_db_path):
            continue

        try:
            conn = sqlite3.connect(skill_db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row

            # Check if this DB has the expected tables
            if _table_exists(conn, "messages_fts") and _table_exists(conn, "messages") and _table_exists(conn, "conversations"):
                rows = conn.execute(
                    """SELECT m.content, m.role, c.title, c.url
                       FROM messages_fts f
                       JOIN messages m ON m.id = f.rowid
                       JOIN conversations c ON c.id = m.conversation_id
                       WHERE messages_fts MATCH ?
                       ORDER BY rank
                       LIMIT ?""",
                    (q, limit),
                ).fetchall()
                source = skill_db_name.replace("_history.db", "")
                for r in rows:
                    results.append({
                        "type": "message",
                        "source": source,
                        "conversation_title": r["title"],
                        "role": r["role"],
                        "content": (r["content"] or "")[:500],
                        "url": r["url"],
                    })
            conn.close()
        except Exception as e:
            logger.debug("Search error in %s: %s", skill_db_name, e)

    # Also try the legacy memory_tap.db if it has the tables
    try:
        conn = get_connection(_db_path)
        if _table_exists(conn, "messages_fts") and _table_exists(conn, "messages") and _table_exists(conn, "conversations"):
            rows = conn.execute(
                """SELECT m.content, m.role, m.thinking_block, c.title, c.source, c.url
                   FROM messages_fts f
                   JOIN messages m ON m.id = f.rowid
                   JOIN conversations c ON c.id = m.conversation_id
                   WHERE messages_fts MATCH ?
                   ORDER BY rank
                   LIMIT ?""",
                (q, limit),
            ).fetchall()
            for r in rows:
                results.append({
                    "type": "message",
                    "source": r["source"],
                    "conversation_title": r["title"],
                    "role": r["role"],
                    "content": (r["content"] or "")[:500],
                    "url": r["url"],
                })
        conn.close()
    except Exception:
        pass

    return results


# --- API: Conversation detail ---

@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: int):
    """Get full conversation with all messages.

    Tries per-skill DBs first, then falls back to legacy memory_tap.db.
    """
    # Try per-skill DBs
    for skill_db_name in ["chatgpt_history.db", "gemini_history.db"]:
        skill_db_path = os.path.join(SKILL_DATA_DIR, skill_db_name)
        if not os.path.exists(skill_db_path):
            continue

        try:
            conn = sqlite3.connect(skill_db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.row_factory = sqlite3.Row

            if not _table_exists(conn, "conversations"):
                conn.close()
                continue

            conv = conn.execute(
                "SELECT * FROM conversations WHERE id = ?", (conv_id,)
            ).fetchone()
            if not conv:
                conn.close()
                continue

            messages = []
            if _table_exists(conn, "messages"):
                messages = conn.execute(
                    "SELECT * FROM messages WHERE conversation_id = ? ORDER BY message_order",
                    (conv_id,),
                ).fetchall()

            artifacts = []
            if _table_exists(conn, "artifacts"):
                artifacts = conn.execute(
                    "SELECT * FROM artifacts WHERE conversation_id = ?", (conv_id,),
                ).fetchall()

            conn.close()
            return {
                "conversation": dict(conv),
                "messages": [dict(m) for m in messages],
                "artifacts": [dict(a) for a in artifacts],
            }
        except Exception:
            pass

    # Fall back to legacy memory_tap.db
    try:
        conn = get_connection(_db_path)
        if not _table_exists(conn, "conversations"):
            conn.close()
            return JSONResponse({"error": "Not found"}, status_code=404)

        conv = conn.execute(
            "SELECT * FROM conversations WHERE id = ?", (conv_id,)
        ).fetchone()
        if not conv:
            conn.close()
            return JSONResponse({"error": "Not found"}, status_code=404)

        messages = conn.execute(
            "SELECT * FROM messages WHERE conversation_id = ? ORDER BY message_order",
            (conv_id,),
        ).fetchall()

        artifacts = []
        if _table_exists(conn, "artifacts"):
            artifacts = conn.execute(
                "SELECT * FROM artifacts WHERE conversation_id = ?", (conv_id,),
            ).fetchall()

        conn.close()
        return {
            "conversation": dict(conv),
            "messages": [dict(m) for m in messages],
            "artifacts": [dict(a) for a in artifacts],
        }
    except Exception:
        return JSONResponse({"error": "Not found"}, status_code=404)


# --- Interest Timeline API ---

INTEREST_TIMELINE_DB_PATH = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "skill_data", "interest_timeline.db"
)

STRENGTH_NUMERIC = {"dominant": 5, "strong": 4, "moderate": 3, "mild": 2, "weak": 1}


def _get_interest_db() -> sqlite3.Connection | None:
    """Get connection to interest_timeline.db. Returns None if DB doesn't exist."""
    if not os.path.exists(INTEREST_TIMELINE_DB_PATH):
        return None
    conn = sqlite3.connect(INTEREST_TIMELINE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _parse_json_field(value: str | None, fallback=None):
    """Safely parse a JSON string field."""
    if not value:
        return fallback if fallback is not None else []
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return fallback if fallback is not None else []


def _parse_keywords(value: str | None) -> list:
    """Parse keywords from comma-separated or JSON string."""
    if not value:
        return []
    # Try JSON first
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass
    # Fall back to comma-separated
    return [k.strip() for k in value.split(",") if k.strip()]


@app.get("/interests", response_class=HTMLResponse)
async def interests_page():
    with open(os.path.join(STATIC_DIR, "interest_timeline.html"), "r", encoding="utf-8") as f:
        return f.read()


@app.get("/api/interests/timeline")
async def get_interest_timeline():
    """Return all interest timeline data in one call."""
    conn = _get_interest_db()
    if conn is None:
        return {"dates_processed": [], "interests": [], "daily_insights": {}}

    try:
        # Check required tables exist
        for tbl in ("processing_log", "interest_registry", "daily_interest_log", "display_log"):
            if not _table_exists(conn, tbl):
                conn.close()
                return {"dates_processed": [], "interests": [], "daily_insights": {}}

        # Get all processed dates
        dates_rows = conn.execute(
            "SELECT date FROM processing_log WHERE status IN ('success', 'complete') ORDER BY date"
        ).fetchall()
        dates_processed = [r["date"] for r in dates_rows]

        # Get all interests
        interests_rows = conn.execute(
            "SELECT * FROM interest_registry ORDER BY total_days_active DESC, canonical_name"
        ).fetchall()

        # Get all daily logs grouped by interest
        daily_rows = conn.execute(
            "SELECT * FROM daily_interest_log ORDER BY date"
        ).fetchall()
        daily_by_interest: dict[int, list] = {}
        for row in daily_rows:
            iid = row["interest_id"]
            if iid not in daily_by_interest:
                daily_by_interest[iid] = []
            daily_by_interest[iid].append(dict(row))

        # Build interest objects
        interests = []
        for ir in interests_rows:
            iid = ir["id"]
            daily_dict = {}
            for dl in daily_by_interest.get(iid, []):
                daily_dict[dl["date"]] = {
                    "strength": dl["strength"],
                    "evidence_count": dl["evidence_count"] or 0,
                    "top_evidence": _parse_json_field(dl["top_evidence"], []),
                }

            interests.append({
                "id": iid,
                "name": ir["canonical_name"],
                "category": ir["category"] or "",
                "lifecycle_status": ir["lifecycle_status"] or "emerging",
                "current_strength": ir["current_strength"] or "weak",
                "strength_numeric": STRENGTH_NUMERIC.get(ir["current_strength"] or "weak", 1),
                "first_seen": ir["first_seen"],
                "last_seen": ir["last_seen"],
                "total_days_active": ir["total_days_active"] or 1,
                "keywords": _parse_keywords(ir["keywords"]),
                "daily": daily_dict,
            })

        # Get daily insights from display_log (display_type = insight type e.g. 'contrast', 'evolution')
        daily_insights = {}
        insight_rows = conn.execute(
            "SELECT date, display_type, content FROM display_log ORDER BY date"
        ).fetchall()
        for row in insight_rows:
            content = _parse_json_field(row["content"], {})
            if isinstance(content, dict):
                insight = content.get("insight", {})
                if isinstance(insight, dict):
                    daily_insights[row["date"]] = {
                        "type": row["display_type"],
                        "headline": insight.get("headline", ""),
                        "detail": insight.get("detail", ""),
                    }
                elif isinstance(insight, str):
                    daily_insights[row["date"]] = {"type": row["display_type"], "headline": insight, "detail": ""}

        return {
            "dates_processed": dates_processed,
            "interests": interests,
            "daily_insights": daily_insights,
        }
    finally:
        conn.close()


@app.get("/api/interests/{interest_id}")
async def get_interest_detail(interest_id: int):
    """Full detail for one interest (for modal)."""
    conn = _get_interest_db()
    if conn is None:
        return JSONResponse({"error": "Interest timeline DB not found"}, status_code=404)

    try:
        # Check required tables exist
        for tbl in ("interest_registry", "daily_interest_log", "display_log"):
            if not _table_exists(conn, tbl):
                conn.close()
                return JSONResponse({"error": "Interest data not yet available"}, status_code=404)

        ir = conn.execute(
            "SELECT * FROM interest_registry WHERE id = ?", (interest_id,)
        ).fetchone()
        if not ir:
            return JSONResponse({"error": "Interest not found"}, status_code=404)

        # Daily history
        daily_rows = conn.execute(
            "SELECT date, strength, evidence_count, top_evidence FROM daily_interest_log WHERE interest_id = ? ORDER BY date",
            (interest_id,),
        ).fetchall()
        daily_history = []
        for dl in daily_rows:
            daily_history.append({
                "date": dl["date"],
                "strength": dl["strength"],
                "evidence_count": dl["evidence_count"] or 0,
                "top_evidence": _parse_json_field(dl["top_evidence"], []),
            })

        # Grounding from display_log
        grounding = []
        grounding_rows = conn.execute(
            "SELECT date, content FROM display_log WHERE display_type = 'grounding' ORDER BY date"
        ).fetchall()
        interest_name = ir["canonical_name"].lower()
        for row in grounding_rows:
            content = _parse_json_field(row["content"], {})
            # Match grounding entries that mention this interest
            content_str = json.dumps(content, ensure_ascii=False).lower() if isinstance(content, (dict, list)) else str(content).lower()
            if interest_name in content_str:
                text = ""
                if isinstance(content, dict):
                    text = content.get("grounding") or content.get("text") or content.get("context") or json.dumps(content, ensure_ascii=False)
                elif isinstance(content, list):
                    text = json.dumps(content, ensure_ascii=False)
                else:
                    text = str(content)
                grounding.append({"date": row["date"], "content": text})

        return {
            "id": interest_id,
            "name": ir["canonical_name"],
            "category": ir["category"] or "",
            "lifecycle_status": ir["lifecycle_status"] or "emerging",
            "current_strength": ir["current_strength"] or "weak",
            "first_seen": ir["first_seen"],
            "last_seen": ir["last_seen"],
            "total_days_active": ir["total_days_active"] or 1,
            "keywords": _parse_keywords(ir["keywords"]),
            "notes": ir["notes"] or "",
            "daily_history": daily_history,
            "grounding": grounding,
        }
    finally:
        conn.close()
