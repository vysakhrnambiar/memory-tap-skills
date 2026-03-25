"""
Dashboard — FastAPI web UI for Memory Tap.

Features:
- Settings page (LLM provider, API key, skill enable/disable)
- Timeline view (history by day — what was collected)
- Skill status (login status, last sync, errors)
- Login trigger (opens Chrome for user to sign in)
- RAG chat interface (talk to your collected data)
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
    # Legacy settings from memory_tap.db
    conn = get_connection(_db_path)
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    settings = {r["key"]: r["value"] for r in rows}

    # Skill settings from core.db skill_settings table
    _ensure_skill_settings_table()
    core_conn = _get_core_connection()
    for dashboard_key, (skill_name, setting_key) in _SKILL_SETTINGS_MAP.items():
        row = core_conn.execute(
            "SELECT value FROM skill_settings WHERE skill_name = ? AND key = ?",
            (skill_name, setting_key),
        ).fetchone()
        if row:
            settings[dashboard_key] = row["value"]
    core_conn.close()

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
    conn = get_connection(_db_path)
    rows = conn.execute(
        "SELECT * FROM sources ORDER BY name"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@app.post("/api/sources/{name}/toggle")
async def toggle_source(name: str):
    conn = get_connection(_db_path)
    conn.execute(
        "UPDATE sources SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE name = ?",
        (name,),
    )
    conn.commit()
    row = conn.execute("SELECT enabled FROM sources WHERE name = ?", (name,)).fetchone()
    conn.close()
    return {"name": name, "enabled": row["enabled"] if row else 0}


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


# --- API: Timeline (history by day) ---

@app.get("/api/timeline")
async def get_timeline(days: int = 7):
    """Get collected data grouped by day for the timeline view."""
    conn = get_connection(_db_path)
    since = (datetime.now() - timedelta(days=days)).isoformat()

    timeline = {}

    # Conversations updated recently
    convs = conn.execute(
        """SELECT source, title, url, message_count, updated_at, last_synced_at
           FROM conversations
           WHERE last_synced_at >= ?
           ORDER BY last_synced_at DESC""",
        (since,),
    ).fetchall()

    for c in convs:
        day = (c["last_synced_at"] or c["updated_at"] or "")[:10]
        if day not in timeline:
            timeline[day] = {"date": day, "conversations": [], "videos": [], "sync_runs": []}
        timeline[day]["conversations"].append({
            "source": c["source"],
            "title": c["title"],
            "url": c["url"],
            "message_count": c["message_count"],
            "synced_at": c["last_synced_at"],
        })

    # YouTube videos
    videos = conn.execute(
        """SELECT video_id, title, channel, url, watched_at, synced_at
           FROM youtube_videos
           WHERE synced_at >= ?
           ORDER BY synced_at DESC""",
        (since,),
    ).fetchall()

    for v in videos:
        day = (v["synced_at"] or v["watched_at"] or "")[:10]
        if day not in timeline:
            timeline[day] = {"date": day, "conversations": [], "videos": [], "sync_runs": []}
        timeline[day]["videos"].append({
            "video_id": v["video_id"],
            "title": v["title"],
            "channel": v["channel"],
            "url": v["url"],
            "watched_at": v["watched_at"],
        })

    # Sync log
    logs = conn.execute(
        """SELECT source, started_at, finished_at, items_found, items_new,
                  items_updated, status, error
           FROM sync_log
           WHERE started_at >= ?
           ORDER BY started_at DESC""",
        (since,),
    ).fetchall()

    for l in logs:
        day = (l["started_at"] or "")[:10]
        if day not in timeline:
            timeline[day] = {"date": day, "conversations": [], "videos": [], "sync_runs": []}
        timeline[day]["sync_runs"].append(dict(l))

    conn.close()

    # Sort by date descending
    result = sorted(timeline.values(), key=lambda x: x["date"], reverse=True)
    return result


# --- API: Stats ---

@app.get("/api/stats")
async def get_stats():
    """Overall collection stats."""
    conn = get_connection(_db_path)
    stats = {
        "total_conversations": conn.execute("SELECT COUNT(*) as c FROM conversations").fetchone()["c"],
        "total_messages": conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"],
        "total_videos": conn.execute("SELECT COUNT(*) as c FROM youtube_videos").fetchone()["c"],
        "total_artifacts": conn.execute("SELECT COUNT(*) as c FROM artifacts").fetchone()["c"],
        "sources": [],
    }
    sources = conn.execute("SELECT name, login_status, last_sync_at, last_error FROM sources").fetchall()
    for s in sources:
        stats["sources"].append(dict(s))
    conn.close()
    return stats


# --- API: Search (for RAG / quick search) ---

@app.get("/api/search")
async def search(q: str, limit: int = 20):
    """Full-text search across all collected data."""
    conn = get_connection(_db_path)
    results = []

    # Search messages
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
            "content": r["content"][:500],
            "url": r["url"],
        })

    # Search YouTube
    rows = conn.execute(
        """SELECT v.title, v.channel, v.url, v.description, v.top_comment
           FROM youtube_fts f
           JOIN youtube_videos v ON v.id = f.rowid
           WHERE youtube_fts MATCH ?
           ORDER BY rank
           LIMIT ?""",
        (q, limit),
    ).fetchall()
    for r in rows:
        results.append({
            "type": "youtube",
            "title": r["title"],
            "channel": r["channel"],
            "url": r["url"],
            "description": (r["description"] or "")[:300],
        })

    conn.close()
    return results


# --- API: Conversation detail ---

@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: int):
    """Get full conversation with all messages."""
    conn = get_connection(_db_path)
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

    artifacts = conn.execute(
        "SELECT * FROM artifacts WHERE conversation_id = ?", (conv_id,),
    ).fetchall()

    conn.close()
    return {
        "conversation": dict(conv),
        "messages": [dict(m) for m in messages],
        "artifacts": [dict(a) for a in artifacts],
    }


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
