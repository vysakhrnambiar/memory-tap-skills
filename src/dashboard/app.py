"""
Dashboard — FastAPI platform UI for Memory Tap.

Platform architecture: dashboard provides generic infrastructure,
skills define their own widgets, pages, and notifications.

See spec/dashboard_design.md for full specification.
"""
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from ..db.core_db import (
    get_core_connection, get_setting, set_setting,
    get_alerts, dismiss_alert, get_all_skills, get_skill_info,
    get_notifications, mark_notification_read, dismiss_notification,
    get_widget_config, set_widget_config,
    update_skill_login_status, CORE_DB_PATH,
)

logger = logging.getLogger("memory_tap.dashboard")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
SCREENSHOTS_DIR = os.path.join(
    os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "logs", "screenshots"
)

app = FastAPI(title="Memory Tap", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Injected by __main__.py at startup
_chrome_manager = None
_scheduler = None
_db_path = None


def set_app_deps(chrome_manager, scheduler, db_path=None):
    """Called by __main__ to inject dependencies."""
    global _chrome_manager, _scheduler, _db_path
    _chrome_manager = chrome_manager
    _scheduler = scheduler
    _db_path = db_path or CORE_DB_PATH


# ============================================================
# Pages
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(STATIC_DIR, "index.html"), "r", encoding="utf-8") as f:
        return f.read()


# ============================================================
# API: Settings
# ============================================================

@app.get("/api/settings")
async def api_get_settings():
    conn = get_core_connection(_db_path)
    rows = conn.execute("SELECT key, value FROM settings").fetchall()
    conn.close()
    settings = {r["key"]: r["value"] for r in rows}
    if "api_key" in settings:
        key = settings["api_key"]
        settings["api_key_masked"] = key[:8] + "..." + key[-4:] if len(key) > 12 else "***"
    return settings


@app.post("/api/settings")
async def api_update_settings(request: Request):
    data = await request.json()
    for key, value in data.items():
        if key and value is not None:
            set_setting(key, str(value), _db_path)
    return {"status": "ok"}


# ============================================================
# API: Skills (registry, management)
# ============================================================

@app.get("/api/skills")
async def api_get_skills():
    """Get all registered skills with status."""
    skills = get_all_skills(_db_path)
    # Enrich with runtime info from scheduler
    if _scheduler:
        for s in skills:
            skill = _scheduler._skills.get(s["name"])
            if skill:
                s["auth_provider"] = skill.manifest.auth_provider
                s["login_url"] = skill.get_login_url()
    return skills


@app.get("/api/skills/{name}")
async def api_get_skill(name: str):
    """Get a single skill's info."""
    info = get_skill_info(name, _db_path)
    if not info:
        return JSONResponse({"error": "Skill not found"}, status_code=404)
    return info


@app.post("/api/skills/{name}/toggle")
async def api_toggle_skill(name: str):
    conn = get_core_connection(_db_path)
    conn.execute(
        "UPDATE skill_registry SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END WHERE name = ?",
        (name,),
    )
    conn.commit()
    row = conn.execute("SELECT enabled FROM skill_registry WHERE name = ?", (name,)).fetchone()
    conn.close()
    return {"name": name, "enabled": row["enabled"] if row else 0}


@app.post("/api/skills/{name}/run")
async def api_run_skill(name: str):
    """Trigger immediate skill run."""
    if not _scheduler:
        return JSONResponse({"error": "Scheduler not initialized"}, status_code=500)
    result = _scheduler.run_skill(name)
    if result is None:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)
    return result


@app.post("/api/skills/{name}/login")
async def api_open_login(name: str):
    """Open Chrome for user to log in."""
    if not _chrome_manager or not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)
    url = skill.get_login_url()
    _chrome_manager.open_headed(url)
    return {"status": "opened", "url": url}


@app.post("/api/skills/{name}/check_login")
async def api_check_login(name: str):
    """Quick login check via CDP."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)
    from ..cdp_client import CDPClient
    try:
        with CDPClient() as client:
            tab = client.new_tab(skill.manifest.target_url)
            logged_in = skill.check_login(tab)
            client.close_tab(tab)
            update_skill_login_status(name, "logged_in" if logged_in else "not_logged_in", _db_path)
            return {"name": name, "logged_in": logged_in}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/skills/check_updates")
async def api_check_updates():
    """Check GitHub for skill updates."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    from ..updater.skill_updater import SkillUpdater, LOCAL_SKILLS_DIR
    updater = SkillUpdater()
    updates = updater.check_updates()
    if updates:
        updated = updater.update_all()
        _scheduler.load_skills_from_dir(LOCAL_SKILLS_DIR)
        return {"updated": updated, "available": [u["name"] for u in updates]}
    return {"updated": [], "available": []}


@app.get("/api/auth_providers")
async def api_get_auth_providers():
    """Get sign-in status grouped by auth provider."""
    if not _scheduler:
        return []
    providers: dict[str, dict] = {}
    conn = get_core_connection(_db_path)
    rows = conn.execute("SELECT name, login_status FROM skill_registry").fetchall()
    conn.close()
    db_status = {r["name"]: r["login_status"] for r in rows}
    for name, skill in _scheduler._skills.items():
        provider = skill.manifest.auth_provider or name
        if provider not in providers:
            providers[provider] = {
                "provider": provider,
                "login_url": skill.get_login_url(),
                "skills": [],
                "all_logged_in": True,
            }
        status = db_status.get(name, "unknown")
        providers[provider]["skills"].append({"name": name, "login_status": status})
        if status != "logged_in":
            providers[provider]["all_logged_in"] = False
    return list(providers.values())


# ============================================================
# API: Widgets (home screen)
# ============================================================

@app.get("/api/widgets")
async def api_get_widgets():
    """Get all visible widget definitions + configs."""
    if not _scheduler:
        return []

    configs = get_widget_config(_db_path)
    config_map = {(c["skill_name"], c["widget_name"]): c for c in configs}

    widgets = []
    for name, skill in _scheduler._skills.items():
        try:
            for w in skill.get_widgets():
                cfg = config_map.get((name, w.name))
                widgets.append({
                    "skill_name": name,
                    **w.to_dict(),
                    "position_x": cfg["position_x"] if cfg else 0,
                    "position_y": cfg["position_y"] if cfg else len(widgets),
                    "visible": cfg["visible"] if cfg else 1,
                })
        except Exception as e:
            logger.warning("Failed to get widgets from %s: %s", name, e)

    return sorted(widgets, key=lambda w: (w["position_y"], w["position_x"]))


@app.get("/api/widgets/{skill_name}/{widget_name}/data")
async def api_get_widget_data(skill_name: str, widget_name: str):
    """Get data for a specific widget by running its query on the skill's DB."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)

    skill = _scheduler._skills.get(skill_name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)

    # Find the widget definition
    widget_def = None
    try:
        for w in skill.get_widgets():
            if w.name == widget_name:
                widget_def = w
                break
    except Exception:
        pass

    if not widget_def:
        return JSONResponse({"error": "Widget not found"}, status_code=404)

    # If no query, use get_stats()
    if widget_def.data_query is None:
        try:
            conn = _scheduler.skill_db_mgr.get_connection(skill_name)
            data = skill.get_stats(conn)
            conn.close()
            return {"data": data, "display_type": widget_def.display_type}
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    # Run query on skill's DB
    try:
        conn = _scheduler.skill_db_mgr.get_connection(skill_name)
        rows = conn.execute(widget_def.data_query).fetchall()
        conn.close()
        return {"data": [dict(r) for r in rows], "display_type": widget_def.display_type}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ============================================================
# API: Skill Pages
# ============================================================

@app.get("/api/skills/{name}/page")
async def api_get_skill_page(name: str):
    """Get skill page definition (sections)."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)
    try:
        sections = [s.to_dict() for s in skill.get_page_sections()]
        return {"skill_name": name, "sections": sections}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/skills/{name}/sections/{section}/data")
async def api_get_section_data(name: str, section: str, page: int = 1):
    """Get data for a skill page section."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)

    # Find the section
    section_def = None
    try:
        for s in skill.get_page_sections():
            if s.name == section:
                section_def = s
                break
    except Exception:
        pass

    if not section_def:
        return JSONResponse({"error": "Section not found"}, status_code=404)

    if section_def.data_query is None:
        try:
            conn = _scheduler.skill_db_mgr.get_connection(name)
            data = skill.get_stats(conn)
            conn.close()
            return {"data": data, "display_type": section_def.display_type}
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    try:
        conn = _scheduler.skill_db_mgr.get_connection(name)
        query = section_def.data_query
        if section_def.paginated:
            offset = (page - 1) * section_def.page_size
            query += f" LIMIT {section_def.page_size} OFFSET {offset}"
        rows = conn.execute(query).fetchall()
        conn.close()
        return {
            "data": [dict(r) for r in rows],
            "display_type": section_def.display_type,
            "page": page,
            "page_size": section_def.page_size if section_def.paginated else 0,
        }
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/skills/{name}/stats")
async def api_get_skill_stats(name: str):
    """Get skill stats (stat cards)."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)
    try:
        conn = _scheduler.skill_db_mgr.get_connection(name)
        data = skill.get_stats(conn)
        conn.close()
        return data
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/skills/{name}/search")
async def api_search_skill(name: str, q: str, limit: int = 20):
    """Search within a skill's data."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)
    try:
        conn = _scheduler.skill_db_mgr.get_connection(name)
        results = skill.get_search_results(conn, q, limit)
        conn.close()
        return results
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ============================================================
# API: Cross-Skill Search
# ============================================================

@app.get("/api/search")
async def api_global_search(q: str, limit: int = 20):
    """Search across all skills."""
    if not _scheduler:
        return []
    results = []
    per_skill_limit = max(5, limit // max(1, len(_scheduler._skills)))
    for name, skill in _scheduler._skills.items():
        try:
            conn = _scheduler.skill_db_mgr.get_connection(name)
            skill_results = skill.get_search_results(conn, q, per_skill_limit)
            for r in skill_results:
                r["skill_name"] = name
            results.extend(skill_results)
            conn.close()
        except Exception as e:
            logger.warning("Search failed for %s: %s", name, e)
    return results[:limit]


# ============================================================
# API: Notifications
# ============================================================

@app.get("/api/notifications")
async def api_get_notifications(all: bool = False):
    return get_notifications(include_read=all, db_path=_db_path)


@app.post("/api/notifications/{nid}/read")
async def api_read_notification(nid: int):
    mark_notification_read(nid, _db_path)
    return {"status": "ok"}


@app.post("/api/notifications/{nid}/dismiss")
async def api_dismiss_notification(nid: int):
    dismiss_notification(nid, _db_path)
    return {"status": "ok"}


@app.post("/api/notifications/read_all")
async def api_read_all_notifications():
    conn = get_core_connection(_db_path)
    conn.execute("UPDATE notifications SET read = 1 WHERE read = 0")
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ============================================================
# API: Alerts (system-level)
# ============================================================

@app.get("/api/alerts")
async def api_get_alerts():
    return get_alerts(db_path=_db_path)


@app.post("/api/alerts/{alert_id}/dismiss")
async def api_dismiss_alert(alert_id: int):
    dismiss_alert(alert_id, db_path=_db_path)
    return {"status": "ok"}


# ============================================================
# API: Screenshots
# ============================================================

@app.get("/api/screenshots/{filename}")
async def api_get_screenshot(filename: str):
    safe_name = os.path.basename(filename)
    path = os.path.join(SCREENSHOTS_DIR, safe_name)
    if os.path.isfile(path):
        return FileResponse(path, media_type="image/png")
    return JSONResponse({"error": "Not found"}, status_code=404)


@app.get("/api/screenshots")
async def api_list_screenshots():
    if not os.path.isdir(SCREENSHOTS_DIR):
        return []
    files = sorted(
        [f for f in os.listdir(SCREENSHOTS_DIR) if f.endswith(".png")],
        reverse=True,
    )
    return [{"filename": f, "url": f"/api/screenshots/{f}"} for f in files[:20]]


# ============================================================
# API: System Health
# ============================================================

@app.get("/api/system/health")
async def api_system_health():
    """System health info for settings page."""
    health = {
        "chrome_running": False,
        "chrome_port": None,
        "chrome_pid": None,
        "health_monitor": None,
        "skills_loaded": 0,
        "db_sizes": [],
    }

    if _chrome_manager:
        health["chrome_running"] = _chrome_manager.is_running()
        health["chrome_port"] = _chrome_manager.port
        health["chrome_pid"] = _chrome_manager._pid

    if _scheduler:
        health["skills_loaded"] = len(_scheduler._skills)
        if _scheduler.health.last_check:
            health["health_monitor"] = _scheduler.health.last_check
        # DB sizes
        try:
            health["db_sizes"] = _scheduler.skill_db_mgr.list_skill_dbs()
        except Exception:
            pass

    return health
