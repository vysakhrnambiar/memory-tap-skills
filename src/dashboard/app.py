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
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from ..db.models import get_connection, get_setting, set_setting

logger = logging.getLogger("memory_tap.dashboard")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

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


# --- API: Settings ---

@app.get("/api/settings")
async def get_settings():
    conn = get_connection(_db_path)
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    settings = {r["key"]: r["value"] for r in rows}
    # Don't expose full API key
    if "api_key" in settings:
        key = settings["api_key"]
        settings["api_key_masked"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
    return settings


@app.post("/api/settings")
async def update_settings(request: Request):
    data = await request.json()
    for key, value in data.items():
        if key and value is not None:
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
