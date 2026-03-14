"""
Build script for Memory Tap exe.

Usage:
    python build_exe.py

Output:
    dist/MemoryTap.exe

Requirements:
    python -m pip install pyinstaller
"""
import os
import subprocess
import sys


def main():
    print("=" * 50)
    print("  Building Memory Tap exe")
    print("=" * 50)

    # Ensure we're in the right directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    print(f"  Working dir: {script_dir}")

    # Check PyInstaller
    try:
        import PyInstaller
        print(f"  PyInstaller: {PyInstaller.__version__}")
    except ImportError:
        print("  ERROR: PyInstaller not installed")
        print("  Run: python -m pip install pyinstaller")
        sys.exit(1)

    # Syntax check all source files first
    print("\n  Checking source files...")
    import py_compile
    import glob
    errors = []
    for f in glob.glob("src/**/*.py", recursive=True):
        try:
            py_compile.compile(f, doraise=True)
        except py_compile.PyCompileError as e:
            errors.append(str(e))
    if errors:
        print(f"  SYNTAX ERRORS in {len(errors)} file(s):")
        for e in errors:
            print(f"    {e}")
        sys.exit(1)
    print("  All source files OK")

    # Build
    print("\n  Building with PyInstaller...")
    result = subprocess.run(
        [sys.executable, "-m", "PyInstaller", "memory_tap.spec", "--clean", "-y"],
        capture_output=False,
    )

    if result.returncode != 0:
        print("\n  BUILD FAILED")
        sys.exit(1)

    # Check output
    exe_path = os.path.join("dist", "MemoryTap.exe")
    if os.path.isfile(exe_path):
        size_mb = os.path.getsize(exe_path) / (1024 * 1024)
        print(f"\n  BUILD SUCCESS")
        print(f"  Output: {os.path.abspath(exe_path)}")
        print(f"  Size: {size_mb:.1f} MB")
    else:
        print(f"\n  ERROR: {exe_path} not found")
        sys.exit(1)


if __name__ == "__main__":
    main()
