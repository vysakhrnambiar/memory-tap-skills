# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for Memory Tap.

Builds a single-file windowed exe that bundles:
- Python runtime + all dependencies
- All src/ code
- Static HTML/CSS/JS dashboard
- No console window (windowed mode)

Build command:
    pyinstaller memory_tap.spec

Output:
    dist/MemoryTap.exe
"""

import os

block_cipher = None

# Collect all source files
src_tree = []
for root, dirs, files in os.walk('src'):
    # Skip __pycache__
    dirs[:] = [d for d in dirs if d != '__pycache__']
    for f in files:
        if f.endswith(('.py', '.html', '.css', '.js', '.png', '.ico')):
            full = os.path.join(root, f)
            # dest is relative path preserving directory structure
            dest = os.path.dirname(full)
            src_tree.append((full, dest))

a = Analysis(
    ['memory_tap.py'],
    pathex=['.'],
    binaries=[],
    datas=src_tree,
    hiddenimports=[
        'src',
        'src.__main__',
        'src.chrome_manager',
        'src.cdp_client',
        'src.human',
        'src.scheduler',
        'src.db',
        'src.db.models',
        'src.db.sync_tracker',
        'src.skills',
        'src.skills.base',
        'src.dashboard',
        'src.dashboard.app',
        'src.rag',
        'src.rag.search',
        'src.rag.chat',
        'src.updater',
        'src.updater.skill_updater',
        'uvicorn',
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        'fastapi',
        'starlette',
        'pystray',
        'pystray._win32',
        'PIL',
        'PIL.Image',
        'PIL.ImageDraw',
        'PIL.ImageFont',
        'websocket',
        'requests',
        'sqlite3',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'pytest',
        'IPython',
        'notebook',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='MemoryTap',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # No console window — windowed mode
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
