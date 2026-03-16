"""Live test: YouTube skill against probe Chrome on port 9777.

Runs the skill's collect() method against real YouTube with real sessions.
Stores data in a temp DB, then reports what was collected.
"""
import sys
import os
import sqlite3
import tempfile
import json
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Force UTF-8 output
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from skills.youtube_history import YouTubeHistorySkill
from src.cdp_client import CDPClient
from src.skills.base import RunLimits

print("=" * 60)
print("LIVE TEST: YouTube Skill")
print("=" * 60)

# Create temp skill DB
db_path = os.path.join(tempfile.gettempdir(), 'live_test_youtube.db')
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

skill = YouTubeHistorySkill()
skill.create_schema(conn)

print(f"\nSkill: {skill.manifest.name} v{skill.manifest.version}")
print(f"DB: {db_path}")
print(f"Chrome: port 9777 (probe)")

# Connect to probe Chrome
print("\n--- Phase 1: Login Check ---")

with CDPClient(port=9777) as client:
    tab = client.new_tab("about:blank")
    logged_in = skill.check_login(tab)
    print(f"Logged in: {logged_in}")

    if not logged_in:
        print("ERROR: Not logged in. Cannot proceed.")
        client.close_tab(tab)
        conn.close()
        os.remove(db_path)
        sys.exit(1)

    # Run collection with small limits for testing
    print("\n--- Phase 2: Collection (limited to 10 items, 5 min) ---")

    # Create a mock tracker that uses our temp DB
    class MockTracker:
        _skill_conn = conn

        def item_count(self, table):
            return conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]

    tracker = MockTracker()

    # Realistic limits: 5 videos, 10 minutes, generous scroll budget
    limits = RunLimits(
        max_items=5,
        max_minutes=10,
        max_scrolls_before_pause=10,
        pause_seconds_min=3,
        pause_seconds_max=8,
        max_scroll_sessions=20,
    )

    try:
        result = skill.collect(tab, tracker, limits)
        print(f"\n--- Phase 3: Results ---")
        print(f"Items found: {result.items_found}")
        print(f"Items new: {result.items_new}")
        print(f"Items updated: {result.items_updated}")
        print(f"Error: {result.error}")
        print(f"Details: {result.details}")
    except Exception as e:
        print(f"\nERROR during collection: {e}")
        import traceback
        traceback.print_exc()

    client.close_tab(tab)

# Check what's in the DB
print("\n--- Phase 4: DB Contents ---")

videos = conn.execute("SELECT video_id, title, channel, duration, watch_percent, watched_date FROM videos ORDER BY watched_date DESC").fetchall()
print(f"\nVideos: {len(videos)}")
for v in videos[:10]:
    title = (v["title"] or "")[:60]
    pct = v["watch_percent"] or 0
    print(f"  [{pct:3d}%] {title}")
    print(f"        channel={v['channel'] or '?'}, duration={v['duration'] or '?'}, date={v['watched_date'] or '?'}")

shorts = conn.execute("SELECT short_id, title, watched_date FROM shorts").fetchall()
print(f"\nShorts: {len(shorts)}")
for s in shorts[:5]:
    print(f"  {(s['title'] or '?')[:60]} ({s['watched_date'] or '?'})")

# FTS test
print("\n--- Phase 5: FTS Search Test ---")
if videos:
    first_title = videos[0]["title"] or ""
    if first_title:
        words = [w for w in first_title.split() if len(w) > 3]
        if words:
            search_word = words[0]
            results = skill.get_search_results(conn, search_word)
            print(f"Search '{search_word}': {len(results)} results")

# Stats
print("\n--- Phase 6: Stats ---")
stats = skill.get_stats(conn)
for s in stats:
    print(f"  {s['label']}: {s['value']}")

print(f"\n--- DONE ---")
print(f"DB kept at: {db_path}")

conn.close()
