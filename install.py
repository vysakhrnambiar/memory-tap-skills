"""
Memory Tap Installer — run this once to set everything up.

What it does:
1. Creates %LOCALAPPDATA%/MemoryTap/ directory structure
2. Initializes SQLite database with all tables + FTS5
3. Downloads latest skills from GitHub
4. Registers Memory Tap to start on Windows login
5. Creates a desktop shortcut
6. Launches the dashboard

Usage:
    python install.py
"""
import ctypes
import os
import shutil
import sqlite3
import subprocess
import sys
import winreg


LOCALAPPDATA = os.environ.get("LOCALAPPDATA", os.path.expanduser("~\\AppData\\Local"))
INSTALL_DIR = os.path.join(LOCALAPPDATA, "MemoryTap")
SKILLS_DIR = os.path.join(INSTALL_DIR, "skills")
LOGS_DIR = os.path.join(INSTALL_DIR, "logs")
CHROME_PROFILE = os.path.join(INSTALL_DIR, "chrome_profile")
DB_PATH = os.path.join(INSTALL_DIR, "memory_tap.db")

# Where this script lives = project root
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

STARTUP_REG_KEY = r"SOFTWARE\Microsoft\Windows\CurrentVersion\Run"
APP_NAME = "MemoryTap"


def print_step(n, total, msg):
    print(f"\n  [{n}/{total}] {msg}")


def create_directories():
    """Create all required directories."""
    for d in [INSTALL_DIR, SKILLS_DIR, LOGS_DIR, CHROME_PROFILE]:
        os.makedirs(d, exist_ok=True)
        print(f"    Created: {d}")


def install_dependencies():
    """Install Python dependencies."""
    req_file = os.path.join(PROJECT_ROOT, "requirements.txt")
    if os.path.isfile(req_file):
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", req_file, "-q"],
        )
        print("    Dependencies installed")
    else:
        print("    No requirements.txt found, skipping")


def init_database():
    """Initialize SQLite database."""
    # Import from our source
    sys.path.insert(0, PROJECT_ROOT)
    from src.db.models import init_db
    init_db(DB_PATH)
    print(f"    Database created: {DB_PATH}")

    # Verify tables
    conn = sqlite3.connect(DB_PATH)
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    conn.close()
    print(f"    Tables: {', '.join(tables)}")


def download_skills():
    """Download latest skills from GitHub."""
    try:
        import requests
    except ImportError:
        print("    requests not installed yet, skipping skill download")
        return

    manifest_url = "https://raw.githubusercontent.com/vysakhrnambiar/memory-tap-skills/main/manifest.json"
    raw_base = "https://raw.githubusercontent.com/vysakhrnambiar/memory-tap-skills/main/skills"

    try:
        resp = requests.get(manifest_url, timeout=15)
        if resp.status_code != 200:
            print(f"    Failed to fetch manifest (HTTP {resp.status_code})")
            return

        manifest = resp.json()
        skills = manifest.get("skills", [])
        print(f"    Found {len(skills)} skills in manifest")

        # Save manifest locally
        import json
        with open(os.path.join(SKILLS_DIR, "manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        for skill in skills:
            filename = skill.get("file", f"{skill['name']}.py")
            url = f"{raw_base}/{filename}"
            try:
                r = requests.get(url, timeout=30)
                if r.status_code == 200:
                    path = os.path.join(SKILLS_DIR, filename)
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(r.text)
                    print(f"    Downloaded: {skill['name']} v{skill['version']}")
                else:
                    print(f"    Failed to download {skill['name']}: HTTP {r.status_code}")
            except Exception as e:
                print(f"    Failed to download {skill['name']}: {e}")

    except Exception as e:
        print(f"    Skill download failed: {e}")


def register_startup():
    """Register Memory Tap to start on Windows login."""
    # Build the command to run
    python_exe = sys.executable
    main_script = os.path.join(PROJECT_ROOT, "src", "__main__.py")
    cmd = f'"{python_exe}" -m src'

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, STARTUP_REG_KEY,
            0, winreg.KEY_SET_VALUE,
        )
        winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ, cmd)
        winreg.CloseKey(key)
        print(f"    Registered startup: {cmd}")
    except Exception as e:
        print(f"    Failed to register startup: {e}")
        print(f"    You can manually add to startup later")


def create_desktop_shortcut():
    """Create a desktop shortcut to open the dashboard."""
    try:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        shortcut_path = os.path.join(desktop, "Memory Tap.url")
        with open(shortcut_path, "w", encoding="utf-8") as f:
            f.write("[InternetShortcut]\n")
            f.write("URL=http://localhost:7777\n")
            f.write("IconIndex=0\n")
        print(f"    Desktop shortcut created: {shortcut_path}")
    except Exception as e:
        print(f"    Failed to create shortcut: {e}")


def create_bat_launcher():
    """Create a .bat file to launch Memory Tap easily."""
    bat_path = os.path.join(INSTALL_DIR, "start_memory_tap.bat")
    python_exe = sys.executable
    with open(bat_path, "w", encoding="utf-8") as f:
        f.write(f'@echo off\n')
        f.write(f'cd /d "{PROJECT_ROOT}"\n')
        f.write(f'"{python_exe}" -m src\n')
    print(f"    Launcher created: {bat_path}")

    # Also create one in project root for convenience
    local_bat = os.path.join(PROJECT_ROOT, "start.bat")
    with open(local_bat, "w", encoding="utf-8") as f:
        f.write(f'@echo off\n')
        f.write(f'cd /d "{PROJECT_ROOT}"\n')
        f.write(f'"{python_exe}" -m src\n')
    print(f"    Local launcher: {local_bat}")


def verify_chrome():
    """Check if Chrome is installed."""
    chrome_paths = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
    ]
    for p in chrome_paths:
        if os.path.isfile(p):
            print(f"    Chrome found: {p}")
            return True

    # Try registry
    try:
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe",
        )
        path, _ = winreg.QueryValueEx(key, "")
        winreg.CloseKey(key)
        if os.path.isfile(path):
            print(f"    Chrome found: {path}")
            return True
    except Exception:
        pass

    print("    WARNING: Chrome not found! Memory Tap requires Google Chrome.")
    print("    Please install Chrome from https://www.google.com/chrome/")
    return False


def main():
    print()
    print("=" * 55)
    print("   Memory Tap Installer")
    print("   Personal Knowledge Collector")
    print("=" * 55)
    print()
    print(f"  Install location: {INSTALL_DIR}")
    print(f"  Project source:   {PROJECT_ROOT}")

    total_steps = 8

    print_step(1, total_steps, "Creating directories...")
    create_directories()

    print_step(2, total_steps, "Checking Chrome...")
    verify_chrome()

    print_step(3, total_steps, "Installing Python dependencies...")
    install_dependencies()

    print_step(4, total_steps, "Initializing database...")
    init_database()

    print_step(5, total_steps, "Downloading skills from GitHub...")
    download_skills()

    print_step(6, total_steps, "Registering Windows startup...")
    register_startup()

    print_step(7, total_steps, "Creating shortcuts and launchers...")
    create_desktop_shortcut()
    create_bat_launcher()

    print_step(8, total_steps, "Verifying installation...")
    # Quick verify
    checks = {
        "Database": os.path.isfile(DB_PATH),
        "Skills dir": os.path.isdir(SKILLS_DIR),
        "Chrome profile dir": os.path.isdir(CHROME_PROFILE),
        "Logs dir": os.path.isdir(LOGS_DIR),
    }
    # Check if any skills were downloaded
    skill_files = [f for f in os.listdir(SKILLS_DIR) if f.endswith(".py")]
    checks["Skills downloaded"] = len(skill_files) > 0

    all_ok = True
    for check, passed in checks.items():
        status = "OK" if passed else "FAIL"
        print(f"    {check}: {status}")
        if not passed:
            all_ok = False

    print()
    if all_ok:
        print("  Installation complete!")
    else:
        print("  Installation completed with warnings (see above)")

    print()
    print("  To start Memory Tap:")
    print(f'    cd "{PROJECT_ROOT}"')
    print(f"    python -m src")
    print()
    print("  Or double-click: start.bat")
    print("  Dashboard: http://localhost:7777")
    print()

    # Ask to launch now
    try:
        answer = input("  Start Memory Tap now? [Y/n] ").strip().lower()
        if answer in ("", "y", "yes"):
            print("  Starting Memory Tap...")
            os.chdir(PROJECT_ROOT)
            subprocess.Popen([sys.executable, "-m", "src"])
            print("  Memory Tap is running! Open http://localhost:7777")
            print()
            # Open browser
            import webbrowser
            import time
            time.sleep(3)
            webbrowser.open("http://localhost:7777")
    except (KeyboardInterrupt, EOFError):
        print("\n  Skipped. Run 'python -m src' to start later.")


if __name__ == "__main__":
    main()
