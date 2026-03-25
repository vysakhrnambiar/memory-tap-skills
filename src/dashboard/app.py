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
    get_widget_config, get_all_widget_config, set_widget_config,
    update_skill_login_status, CORE_DB_PATH,
)
from ..rag.search import search_all as rag_search_all
from ..rag.chat import chat as rag_chat

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

# Track login tabs so we can close them after successful login
# Maps auth_provider -> chrome_tab_id
_login_tabs: dict[str, str] = {}


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
            if key == "consent_accepted":
                logger.info("API: User accepted indemnity/consent")
            elif key == "api_key":
                logger.info("API: API key updated")
            else:
                logger.info("API: Setting updated: %s", key)
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


@app.post("/api/skills/{name}/schedule")
async def api_set_schedule(name: str, request: Request):
    """Update skill run frequency."""
    body = await request.json()
    hours = body.get("schedule_hours")
    if hours is None:
        return {"error": "schedule_hours required"}
    hours = float(hours)
    conn = get_core_connection(_db_path)
    conn.execute(
        "UPDATE skill_registry SET schedule_hours = ? WHERE name = ?",
        (hours, name),
    )
    conn.commit()
    conn.close()
    logger.info("API: schedule(%s) set to %.2f hours", name, hours)
    return {"name": name, "schedule_hours": hours}


@app.get("/api/skills/{name}/skill-settings")
async def api_get_skill_settings(name: str):
    """Get a skill's configurable settings with current values."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)

    # Get setting definitions from skill
    settings_defs = skill.get_configurable_settings()
    if not settings_defs:
        return []

    # Get current values from skill's DB
    try:
        conn = _scheduler.skill_db_mgr.get_connection(name)
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        current_values = {r["key"]: r["value"] for r in rows}
        conn.close()
    except Exception:
        current_values = {}

    result = []
    for s in settings_defs:
        result.append({
            "key": s.key,
            "label": s.label,
            "setting_type": s.setting_type,
            "default": s.default,
            "options": s.options,
            "description": s.description,
            "min_value": s.min_value,
            "max_value": s.max_value,
            "value": current_values.get(s.key, s.default),
        })
    return result


@app.post("/api/skills/{name}/skill-settings")
async def api_set_skill_settings(name: str, request: Request):
    """Update a skill's configurable settings."""
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": "Skill not found"}, status_code=404)

    # Validate against declared settings
    allowed_keys = {s.key for s in skill.get_configurable_settings()}
    body = await request.json()
    updated = {}

    try:
        conn = _scheduler.skill_db_mgr.get_connection(name)
        for key, value in body.items():
            if key not in allowed_keys:
                continue
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                (key, str(value)),
            )
            updated[key] = str(value)
            logger.info("API: skill_setting(%s, %s) set to %s", name, key, value)

        # If backfill_depth changed, reset backfill_completed so it re-runs
        if "backfill_depth_days" in updated:
            conn.execute(
                "UPDATE settings SET value = 'false', updated_at = datetime('now') "
                "WHERE key = 'backfill_completed'"
            )
            logger.info("API: Reset backfill_completed for %s (depth changed)", name)

        conn.commit()
        conn.close()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    return {"name": name, "updated": updated}


_running_skills: dict = {}  # name -> threading.Thread

@app.post("/api/skills/{name}/run")
async def api_run_skill(name: str):
    """Trigger immediate skill run in background thread."""
    logger.info("API: run_skill(%s) — user triggered manual run", name)
    if not _scheduler:
        return JSONResponse({"error": "Scheduler not initialized"}, status_code=500)
    if name in _running_skills and _running_skills[name].is_alive():
        return JSONResponse({"status": "already_running", "message": f"{name} is already running"})

    import threading

    def _run_in_bg():
        try:
            result = _scheduler.run_skill(name)
            if result:
                logger.info("API: run_skill(%s) — completed: found=%s, new=%s, error=%s",
                            name, result.get("items_found"), result.get("items_new"), result.get("error"))
        except Exception as e:
            logger.error("API: run_skill(%s) — background error: %s", name, e)
        finally:
            _running_skills.pop(name, None)

    t = threading.Thread(target=_run_in_bg, daemon=True)
    _running_skills[name] = t
    t.start()
    return {"status": "started", "message": f"{name} is now running in background"}

@app.get("/api/skills/{name}/running")
async def api_skill_running(name: str):
    """Check if a skill is currently running."""
    running = name in _running_skills and _running_skills[name].is_alive()
    return {"running": running}


@app.post("/api/skills/{skill_name}/dismiss/{item_type}/{item_id}")
async def api_dismiss_item(skill_name: str, item_type: str, item_id: str):
    """Dismiss an item (e.g., dismiss a video from 'unfinished' list)."""
    logger.info("API: dismiss %s/%s from %s", item_type, item_id, skill_name)
    if not _scheduler or not _scheduler.skill_db_mgr:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    try:
        conn = _scheduler.skill_db_mgr.get_connection(skill_name)
        if item_type == "unfinished_video":
            conn.execute(
                "UPDATE videos SET dismissed_unfinished = 1 WHERE video_id = ?",
                (item_id,),
            )
            conn.commit()
        conn.close()
        return {"status": "dismissed"}
    except Exception as e:
        logger.error("Dismiss error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/skills/{name}/login")
async def api_open_login(name: str):
    """Open Chrome for user to log in."""
    if not _chrome_manager or not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)
    url = skill.get_login_url()
    logger.info("API: open_login(%s) — opening %s in new Chrome tab", name, url)
    tab_info = _chrome_manager.open_headed(url)
    # Store tab ID so we can close it after successful login
    if tab_info and tab_info.get("id"):
        provider = skill.manifest.auth_provider or name
        _login_tabs[provider] = tab_info["id"]
        logger.info("API: open_login(%s) — stored login tab %s for provider %s",
                     name, tab_info["id"][:8], provider)
    return {"status": "opened", "url": url}


@app.post("/api/skills/{name}/check_login")
async def api_check_login(name: str):
    """Cookie-only login check — does NOT navigate, open, or close any tabs.

    Connects to an existing tab via raw WebSocket, checks cookies, disconnects.
    The tab stays alive and untouched.
    """
    if not _scheduler:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    skill = _scheduler._skills.get(name)
    if not skill:
        return JSONResponse({"error": f"Skill '{name}' not found"}, status_code=404)

    import requests as _req
    import websocket as _ws
    import json as _json

    try:
        port = None
        if _chrome_manager:
            port = _chrome_manager.port
        if not port:
            logger.warning("check_login(%s): Chrome not running", name)
            return JSONResponse({"error": "Chrome not running"}, status_code=500)

        # Find the login tab (the one on the target site) or any tab
        tabs_resp = _req.get(f"http://localhost:{port}/json", timeout=5)
        tabs = [t for t in tabs_resp.json() if t.get("type") == "page"]
        if not tabs:
            logger.warning("check_login(%s): No tabs available", name)
            return JSONResponse({"error": "No tabs"}, status_code=500)

        # Find best tab to check cookies on — match by auth provider domain
        # Google: any *.google.com tab (accounts, myaccount, gemini, youtube)
        # OpenAI: any *.chatgpt.com or *.openai.com tab
        provider = skill.manifest.auth_provider
        provider_domains = {
            "google": [".google.com", "youtube.com"],
            "openai": ["chatgpt.com", "openai.com"],
        }
        match_domains = provider_domains.get(provider, [])
        target_domain = skill.manifest.target_url.split("//")[-1].split("/")[0]
        if target_domain not in match_domains:
            match_domains.append(target_domain)

        tab_info = None
        # First: tab on a matching domain
        for t in tabs:
            tab_url = t.get("url") or ""
            for domain in match_domains:
                if domain in tab_url:
                    tab_info = t
                    break
            if tab_info:
                break
        # Second: any non-localhost tab
        if not tab_info:
            for t in tabs:
                tab_url = t.get("url") or ""
                if "localhost" not in tab_url and "about:blank" not in tab_url:
                    tab_info = t
                    break
        # Last resort: first tab
        if not tab_info:
            tab_info = tabs[0]

        logger.info("check_login(%s): checking cookies on tab %s (%s)",
                     name, tab_info["id"][:8], tab_info.get("url", "")[:40])

        # Raw WebSocket — connect, get cookies, disconnect. Do NOT close tab.
        ws_url = tab_info.get("webSocketDebuggerUrl", "")
        if not ws_url:
            return JSONResponse({"error": "No WebSocket URL for tab"}, status_code=500)

        ws = _ws.create_connection(ws_url, timeout=10)
        # Enable Network domain
        ws.send(_json.dumps({"id": 1, "method": "Network.enable", "params": {}}))
        ws.recv()
        # Get cookies
        ws.send(_json.dumps({"id": 2, "method": "Network.getCookies", "params": {}}))
        resp = _json.loads(ws.recv())
        # Disable Network domain and disconnect (tab stays alive)
        ws.send(_json.dumps({"id": 3, "method": "Network.disable", "params": {}}))
        try:
            ws.recv()
        except Exception:
            pass
        ws.close()

        cookies = resp.get("result", {}).get("cookies", [])

        # Check based on auth provider
        provider = skill.manifest.auth_provider
        logged_in = False

        if provider == "google":
            logged_in = any(
                c.get("name") == "SID" and ".google.com" in c.get("domain", "")
                for c in cookies
            )
        elif provider == "openai":
            logged_in = any(
                "session-token" in c.get("name", "")
                for c in cookies
            )
        else:
            logged_in = any(target_domain in c.get("domain", "") for c in cookies)

        status = "logged_in" if logged_in else "not_logged_in"
        logger.info("check_login(%s): result=%s (checked %d cookies, provider=%s)",
                     name, status, len(cookies), provider)
        update_skill_login_status(name, status, _db_path)

        # If logged in, close the login tab (no longer needed)
        if logged_in and provider and _chrome_manager:
            login_tab_id = _login_tabs.pop(provider, None)
            if login_tab_id:
                try:
                    import requests as _close_req
                    _close_req.get(
                        f"http://localhost:{_chrome_manager.port}/json/close/{login_tab_id}",
                        timeout=3,
                    )
                    logger.info("check_login(%s): closed login tab %s for provider %s",
                                name, login_tab_id[:8], provider)
                except Exception as e:
                    logger.warning("check_login(%s): failed to close login tab: %s", name, e)

        # If logged in, mark ALL skills with the same auth provider as logged in
        # (signing into Google covers YouTube + Gemini, signing into OpenAI covers ChatGPT)
        if logged_in and provider and _scheduler:
            for sname, sskill in _scheduler._skills.items():
                if sname != name and sskill.manifest.auth_provider == provider:
                    logger.info("check_login(%s): also marking %s as logged_in (same provider: %s)",
                                name, sname, provider)
                    update_skill_login_status(sname, "logged_in", _db_path)

        return {"name": name, "logged_in": logged_in}

    except Exception as e:
        logger.error("check_login(%s) failed: %s", name, e)
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


@app.get("/api/widgets/config")
async def api_get_widget_config():
    """Get ALL widget configs (including hidden) for customize panel."""
    if not _scheduler:
        return []

    all_configs = get_all_widget_config(_db_path)
    config_map = {(c["skill_name"], c["widget_name"]): c for c in all_configs}

    result = []
    for name, skill in _scheduler._skills.items():
        try:
            for w in skill.get_widgets():
                cfg = config_map.get((name, w.name))
                result.append({
                    "skill_name": name,
                    "widget_name": w.name,
                    "title": w.title,
                    "enabled": cfg["visible"] if cfg else 1,
                    "position": cfg["position_y"] if cfg else len(result),
                })
        except Exception as e:
            logger.warning("Failed to get widgets from %s: %s", name, e)

    return sorted(result, key=lambda w: w["position"])


@app.post("/api/widgets/config")
async def api_save_widget_config(request: Request):
    """Save widget visibility + order from customize panel.

    Body: {widgets: [{skill_name, widget_name, enabled, position}, ...]}
    """
    data = await request.json()
    widgets = data.get("widgets", [])
    for w in widgets:
        set_widget_config(
            skill_name=w["skill_name"],
            widget_name=w["widget_name"],
            position_y=w.get("position", 0),
            visible=bool(w.get("enabled", True)),
            db_path=_db_path,
        )
    return {"status": "ok", "count": len(widgets)}


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

    # If no query (None or empty string), use get_stats()
    if not widget_def.data_query:
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


@app.get("/api/skills/{skill_name}/conversations/{conv_id}/messages")
async def api_get_messages(skill_name: str, conv_id: int):
    """Get messages for a conversation (ChatGPT/Gemini)."""
    if not _scheduler or not _scheduler.skill_db_mgr:
        return JSONResponse({"error": "Not initialized"}, status_code=500)
    try:
        conn = _scheduler.skill_db_mgr.get_connection(skill_name)
        messages = conn.execute(
            "SELECT role, content, thinking_block, sources, code_blocks, message_order "
            "FROM messages WHERE conversation_id = ? ORDER BY message_order",
            (conv_id,),
        ).fetchall()
        conn.close()
        return {
            "messages": [
                {
                    "role": r["role"],
                    "content": r["content"],
                    "thinking_block": r["thinking_block"] or "",
                    "sources": r["sources"] or "",
                    "code_blocks": r["code_blocks"] or "",
                    "order": r["message_order"],
                }
                for r in messages
            ]
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
# API: RAG Search + Chat
# ============================================================

@app.get("/api/rag/search")
async def api_rag_search(q: str, limit: int = 20):
    """Search across all skill DBs using FTS5 (RAG search module)."""
    if not q.strip():
        return []
    try:
        results = rag_search_all(q, limit=limit)
        return results
    except Exception as e:
        logger.error("RAG search error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/chat")
async def api_chat(request: Request):
    """Chat with your collected data using LLM + RAG context.

    Body: {message: str, history: [{role, content}, ...]}
    Returns: {response: str, sources: [{source, title, url, type}], error: str|null}
    """
    data = await request.json()
    message = data.get("message", "").strip()
    history = data.get("history", [])

    if not message:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    import asyncio
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: rag_chat(
            user_message=message,
            conversation_history=history,
            core_db_path=_db_path,
        ),
    )
    return result


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
        "internet": True,
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
        if hasattr(_scheduler, 'health') and _scheduler.health:
            if _scheduler.health.last_check:
                health["health_monitor"] = _scheduler.health.last_check
            health["internet"] = getattr(_scheduler.health, 'internet_connected', True)
        # DB sizes
        try:
            health["db_sizes"] = _scheduler.skill_db_mgr.list_skill_dbs()
        except Exception:
            pass

    return health
