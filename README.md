# Memory Tap

> Personal knowledge collector. Runs in the background, collects your ChatGPT, Gemini, and YouTube history into a local searchable database. Chat with your data using LLM-powered RAG.

## What It Does

1. Launches an isolated Chrome instance (separate from your main browser)
2. You sign into your Google account once
3. Skills navigate like a human — slow scrolling, mouse movements, realistic pauses
4. Everything is stored locally in SQLite with full-text search
5. Ask questions about your collected data using Gemini 2.5 Flash via OpenRouter

## Install

```powershell
git clone https://github.com/vysakhrnambiar/memory-tap-skills.git
cd memory-tap-skills
python install.py
```

The installer will:
- Install Python dependencies
- Create the local database
- Download the latest skills
- Register Memory Tap to start on Windows login
- Create a desktop shortcut
- Offer to launch immediately

## Quick Start (after install)

```powershell
python -m src
# Or double-click: start.bat
# Dashboard: http://localhost:7777
```

## Available Skills

| Skill | Site | What It Collects |
|-------|------|-----------------|
| `youtube_history` | youtube.com | Watch history, video descriptions, top comments |
| `chatgpt_history` | chatgpt.com | Conversations, messages, thinking blocks, artifact downloads |
| `gemini_history` | gemini.google.com | Conversations, messages, thinking blocks |

Skills auto-update from this repository every 6 hours.

## Architecture

```
[Tray App]
    |-- [Scheduler] -----> runs skills every N hours
    |-- [Chrome Manager] -> isolated Chrome profile, CDP connection
    |-- [Skill Engine] --> loads skills, executes with human-like CDP
    |-- [Data Store] ----> SQLite + FTS5 full-text search
    |-- [Dashboard] -----> FastAPI + static HTML (localhost:7777)
    |-- [RAG Chat] ------> FTS5 search + LLM via OpenRouter
    |-- [Skill Updater] -> polls this GitHub repo, auto-updates skills
```

## Dashboard

- **Timeline**: See what was collected, organized by day
- **Sources**: Enable/disable skills, trigger manual runs, sign in
- **Search**: Full-text search across all collected data
- **Settings**: Configure LLM provider and API key

## Project Structure

```
memory-tap-skills/
    install.py               -- One-step installer
    pyproject.toml            -- Package definition
    requirements.txt          -- Python dependencies
    manifest.json             -- Skill registry (auto-update source)
    skills/                   -- Collection skills (pure CDP navigation)
        youtube_history.py
        chatgpt_history.py
        gemini_history.py
    src/                      -- Core application
        __main__.py           -- Entry point
        chrome_manager.py     -- Isolated Chrome lifecycle
        cdp_client.py         -- Chrome DevTools Protocol client
        human.py              -- Human-like interaction (Bezier mouse, scroll)
        scheduler.py          -- Periodic skill execution
        db/
            models.py         -- SQLite schema + FTS5
            sync_tracker.py   -- Incremental sync logic
        skills/
            base.py           -- Skill interface
        dashboard/
            app.py            -- FastAPI dashboard
            static/
                index.html    -- Dark theme UI
        rag/
            search.py         -- Full-text search
            chat.py           -- LLM chat via OpenRouter
        updater/
            skill_updater.py  -- GitHub auto-update
```

## Requirements

- Windows 10/11
- Python 3.10+
- Google Chrome installed
- OpenRouter API key (only for RAG chat — collection works without it)

## How Skills Work

Skills are pure Python scripts that navigate websites using Chrome DevTools Protocol (CDP). No APIs, no LLM needed for collection — just human-like browser automation.

When a website changes its layout, we update the skill here and your local copy auto-updates within 6 hours.

## Security

- All data stays on your machine (local SQLite)
- Skills only download from this repository
- Chrome runs with an isolated profile (never touches your main browser)
- No telemetry, no tracking

## License

MIT
