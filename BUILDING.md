# Building Memory Tap from Source

## Prerequisites

- Python 3.10+ (tested with 3.11)
- Google Chrome installed
- Windows 10/11

## Development Setup

```powershell
# Clone
git clone https://github.com/vysakhrnambiar/memory-tap-skills.git
cd memory-tap-skills

# Install dependencies
python -m pip install -r requirements.txt

# Run from source
python -m src
```

## Building the Exe

```powershell
# Install PyInstaller
python -m pip install pyinstaller

# Build (syntax-checks first, then builds)
python build_exe.py

# Output: dist/MemoryTap.exe (~31 MB)
```

### What the Build Does

1. `build_exe.py` syntax-checks all `src/**/*.py` files
2. Runs PyInstaller with `memory_tap.spec`
3. Bundles: Python 3.11 runtime, all dependencies, all source code, static HTML/CSS/JS
4. Excludes: tkinter, matplotlib, numpy, pandas (not needed, saves ~20 MB)
5. Output: single `dist/MemoryTap.exe` — no installer, no extraction, just run it

### Key Files

| File | Purpose |
|------|---------|
| `memory_tap.py` | PyInstaller entry point — thin wrapper that calls `src.__main__.main()` |
| `memory_tap.spec` | PyInstaller spec — defines what to bundle, hidden imports, exclusions |
| `build_exe.py` | Build script — syntax check + PyInstaller + size report |

### How the Exe Finds Its Files

When running as a PyInstaller bundle:
- `sys._MEIPASS` points to the temp extraction directory
- `memory_tap.py` adds this to `sys.path` so `from src.xxx import` works
- Static files (HTML/CSS/JS) are bundled as data files and served by FastAPI from the extracted path

## Architecture Overview

```
MemoryTap.exe
    |
    |-- [__main__.py] Entry point
    |       |-- Init SQLite database
    |       |-- Start Chrome Manager (isolated Chrome, CDP port 9494-9504)
    |       |-- Start Skill Updater (polls GitHub every 6h)
    |       |-- Start Scheduler (runs skills on schedule)
    |       |-- Start Health Monitor (checks Chrome every 10s)
    |       |-- Start Dashboard (FastAPI on localhost:7777)
    |       |-- Create Tray Icon (pystray)
    |       |-- First run: open dashboard in isolated Chrome
    |
    |-- [chrome_manager.py] Chrome lifecycle
    |       |-- Port safety: probes 9494-9504, avoids conflicts
    |       |-- Persistent profile at %LOCALAPPDATA%/MemoryTap/chrome_profile
    |       |-- Health monitor: PID alive + CDP responds + tab count
    |       |-- Crash recovery: auto-relaunch on PID death
    |
    |-- [cdp_client.py] Chrome DevTools Protocol
    |       |-- Events by type (no lost events)
    |       |-- Tab state machine: CREATED→IDLE→NAVIGATING→LOADED→WORKING→CLOSED
    |       |-- Thread-safe WebSocket send
    |       |-- Auto-reconnect on WS drop (diagnose first: Chrome alive? Tab exists?)
    |       |-- Tab recall: clean orphans before each skill
    |       |-- Max 1 tab enforcement
    |       |-- Checkpoint: verify URL, readyState, overlays, WS health
    |       |-- Ping/heartbeat
    |       |-- Page load fallback (readyState check after 10s)
    |
    |-- [human.py] Human-like interaction
    |       |-- Bezier curve mouse movements
    |       |-- Momentum scrolling
    |       |-- Variable typing speed
    |       |-- Watch page simulation (mouse drift)
    |
    |-- [skills/base.py] Skill interface
    |       |-- BaseSkill: check_login() + collect()
    |       |-- Auth provider grouping (google, openai)
    |       |-- Zero-items warning + GitHub issue reporting
    |       |-- Error screenshots on every failure
    |
    |-- [db/models.py] SQLite + FTS5
    |       |-- 8 tables: settings, sources, conversations, messages,
    |       |   artifacts, youtube_videos, sync_log, alerts
    |       |-- FTS5 full-text search on messages + videos
    |       |-- Auto-sync triggers
    |
    |-- [dashboard/app.py] FastAPI on :7777
    |       |-- Timeline (history by day)
    |       |-- Sources with auth provider grouping
    |       |-- Alerts (polls every 15s)
    |       |-- Search (FTS5)
    |       |-- Screenshots API
    |       |-- Settings
    |
    |-- [dashboard/static/index.html] Dark theme UI
    |       |-- Indemnity/risk screen (first run)
    |       |-- Sign-in wizard (step by step per auth provider)
    |       |-- Timeline, sources, search, settings tabs
    |       |-- Alert notifications
    |
    |-- [rag/] RAG chat
    |       |-- FTS5 search + context assembly
    |       |-- Gemini 2.5 Flash via OpenRouter
    |
    |-- [updater/] Skill auto-update
    |       |-- Polls GitHub every 6h
    |       |-- SHA-256 checksum verification

Skills (downloaded from GitHub, not bundled):
    |-- youtube_history.py — watch history, descriptions, top comments
    |-- chatgpt_history.py — conversations, thinking blocks, artifacts
    |-- gemini_history.py — conversations, thinking blocks
```

## Login Detection (verified via live CDP probe)

| Site | Method | Cookie/Indicator |
|------|--------|-----------------|
| YouTube | DOM check | `button#avatar-btn` visible + no "Sign in" text |
| ChatGPT | Cookie check | `__Secure-next-auth.session-token.0` on `.chatgpt.com` |
| Gemini | Cookie check | `SID` on `.google.com` |

## Skill Development

Standard method (always follow this):
1. Launch Chrome via CDP
2. Navigate to target site NOT logged in — observe cookies, URL, DOM
3. Ask user to log in
4. Navigate again LOGGED IN — compare
5. Write skill based on real observations, never guesses

See `memory/skill-development-method.md` for details.
