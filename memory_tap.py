"""
Memory Tap — PyInstaller entry point.

Thin wrapper that calls src.__main__.main().
All logic lives in src/ — this file just bootstraps the path for PyInstaller.
"""
import os
import sys

# When running from PyInstaller bundle, __file__ points to the temp extract dir.
# Add it to sys.path so 'from src.xxx import' works.
if getattr(sys, 'frozen', False):
    # Running as PyInstaller exe
    bundle_dir = sys._MEIPASS
    sys.path.insert(0, bundle_dir)
else:
    # Running as script
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.__main__ import main

if __name__ == "__main__":
    main()
