# Memory Tap

> Personal knowledge collector. Runs in the background, collects your ChatGPT, Gemini, and YouTube history into a local searchable database. Chat with your data using LLM-powered RAG.

## Download

**[Download MemoryTap.exe](https://github.com/vysakhrnambiar/memory-tap-skills/releases)** (31 MB, Windows 10/11)

Double-click to run. No Python needed. No installer wizard. It just works.

## What It Does

1. Launches an **isolated Chrome instance** (separate from your main browser — never touches your data)
2. You sign into your accounts (Google, ChatGPT) inside this Chrome — **one-time setup**
3. Skills navigate like a human — slow scrolling, mouse movements, realistic pauses
4. Everything is stored in a **local SQLite database** on your machine — nothing is uploaded anywhere
5. Ask questions about your collected data using Gemini 2.5 Flash via OpenRouter (optional)

## First Run

1. Double-click `MemoryTap.exe`
2. A purple **"M" icon** appears in your system tray (near the clock)
3. An isolated Chrome window opens showing the **Memory Tap dashboard**
4. Read and accept the **risk acknowledgment** (explains exactly what the app does)
5. Follow the **sign-in wizard** — sign into Google (covers YouTube + Gemini) and ChatGPT
6. Done — Memory Tap collects automatically every 3 hours

After reboot, Memory Tap **auto-starts** — you'll see the tray icon.

## Dashboard (localhost:7777)

| Tab | What It Shows |
|-----|--------------|
| **Timeline** | Your collected data organized by day |
| **Sources** | Skill status, login state, "Run Now" button, grouped by auth provider |
| **Search** | Full-text search across all collected data |
| **Settings** | LLM provider + API key for RAG chat |

**Alerts** appear at the top when something needs attention (Chrome crashed, site changed, login expired).

## Available Skills

| Skill | Site | What It Collects |
|-------|------|-----------------|
| `youtube_history` | youtube.com | Watch history, video descriptions, top comments |
| `chatgpt_history` | chatgpt.com | Conversations, messages, thinking blocks, artifact downloads |
| `gemini_history` | gemini.google.com | Conversations, messages, thinking blocks |

Skills **auto-update** from this repository every 6 hours. When a website changes its layout, we update the skill here and your local copy updates automatically.

## How It Works

```
MemoryTap.exe
    |-- Tray Icon (Open Dashboard / Uninstall / Quit)
    |-- Chrome Manager (isolated profile, port 9494-9504)
    |-- Skill Scheduler (runs every 3h, one tab at a time)
    |-- Health Monitor (checks Chrome every 10s, auto-restarts on crash)
    |-- Dashboard (FastAPI on localhost:7777)
    |-- Skill Updater (polls GitHub every 6h)
    |-- RAG Chat (FTS5 search + LLM via OpenRouter)
```

### Robustness Features

- **Port safety** — probes ports before launching, never conflicts with other Chrome instances
- **Tab recall** — cleans orphaned tabs before each skill run
- **Login detection** — cookie-based (verified via live CDP probe), never assumes logged in
- **Health monitor** — background thread checks Chrome every 10s, auto-relaunches on crash
- **Auto-reconnect** — diagnoses why WebSocket dropped (Chrome dead? Tab closed?) before retrying
- **Max 1 tab** — hard limit, impossible to accumulate tabs
- **Pre-step checkpoint** — verifies URL, page state, overlays before each action
- **Error screenshots** — saved on every failure for debugging
- **Zero-items warning** — detects possible site changes, offers one-click GitHub issue reporting
- **Page load fallback** — readyState check if load event doesn't fire (heavy SPAs)

## Uninstall

Right-click tray icon → **Uninstall Memory Tap**

- Asks if you want to **keep a backup** of your collected data (saves to Documents folder)
- Removes: all files, Chrome profile, startup registry, desktop shortcut
- Your backup `.db` file can be re-imported later

## Requirements

- **Windows 10/11**
- **Google Chrome** installed
- **OpenRouter API key** (only for RAG chat — collection works without it)

## Building from Source

See [BUILDING.md](BUILDING.md) for development setup and build instructions.

```powershell
git clone https://github.com/vysakhrnambiar/memory-tap-skills.git
cd memory-tap-skills
python -m pip install -r requirements.txt
python -m src          # Run from source
python build_exe.py    # Build exe
```

## Security & Privacy

- **All data stays on your machine** — local SQLite, no uploads, no telemetry
- **Open source** — inspect every line of code in this repo
- **Isolated Chrome** — separate profile, never touches your main browser
- Skills only download from **this repository** (hardcoded)
- Screenshots on errors contain no personal data — only page layout for debugging

## Reporting Issues

If a skill stops working (website changed its layout):
1. Dashboard shows a warning with **"Report Issue"** button
2. Click it → opens a pre-filled GitHub issue (skill name + version, no personal data)
3. We update the skill, your copy auto-updates within 6 hours

## License

MIT
