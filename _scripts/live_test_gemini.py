"""Live test: Gemini skill against probe Chrome on port 9777."""
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

if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from skills.gemini_history import GeminiHistorySkill
from src.cdp_client import CDPClient
from src.skills.base import RunLimits

print("=" * 60)
print("LIVE TEST: Gemini Skill")
print("=" * 60)

db_path = os.path.join(tempfile.gettempdir(), 'live_test_gemini.db')
if os.path.exists(db_path):
    os.remove(db_path)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

skill = GeminiHistorySkill()
skill.create_schema(conn)

print(f"\nSkill: {skill.manifest.name} v{skill.manifest.version}")
print(f"DB: {db_path}")

with CDPClient(port=9777) as client:
    print("\n--- Phase 1: Login Check ---")
    tab = client.new_tab("about:blank")
    logged_in = skill.check_login(tab)
    print(f"Logged in: {logged_in}")

    if not logged_in:
        print("ERROR: Not logged in.")
        client.close_tab(tab)
        conn.close()
        os.remove(db_path)
        sys.exit(1)

    print("\n--- Phase 2: Collection (5 conversations, 10 min) ---")

    class MockTracker:
        _skill_conn = conn
        def item_count(self, table):
            return conn.execute(f"SELECT COUNT(*) as c FROM {table}").fetchone()["c"]

    tracker = MockTracker()

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
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()

    client.close_tab(tab)

# Check DB
print("\n--- Phase 4: DB Contents ---")
convs = conn.execute(
    "SELECT external_id, title, message_count, has_thinking, discovered_date, last_updated "
    "FROM conversations ORDER BY last_updated DESC"
).fetchall()
print(f"\nConversations: {len(convs)}")
for c in convs[:10]:
    title = (c["title"] or "")[:60]
    thinking = " [THINKING]" if c["has_thinking"] else ""
    print(f"  [{c['message_count']} msgs{thinking}] {title}")
    print(f"        discovered={c['discovered_date']}")

total_msgs = conn.execute("SELECT COUNT(*) as c FROM messages").fetchone()["c"]
thinking_msgs = conn.execute("SELECT COUNT(*) as c FROM messages WHERE thinking_block != ''").fetchone()["c"]
print(f"\nTotal messages: {total_msgs}")
print(f"Messages with thinking blocks: {thinking_msgs}")

# Sample conversation
if convs:
    first_id = conn.execute("SELECT id FROM conversations LIMIT 1").fetchone()["id"]
    msgs = conn.execute(
        "SELECT role, content, thinking_block, sources, code_blocks FROM messages "
        "WHERE conversation_id = ? ORDER BY message_order LIMIT 4",
        (first_id,)
    ).fetchall()
    print(f"\nSample messages:")
    for m in msgs:
        content = (m["content"] or "")[:100]
        print(f"  [{m['role']}] {content}")
        if m["thinking_block"]:
            print(f"        thinking: {m['thinking_block'][:80]}...")
        if m["sources"]:
            print(f"        sources: {m['sources'][:80]}")
        if m["code_blocks"]:
            print(f"        code: yes")

# FTS
print("\n--- Phase 5: FTS Search ---")
if total_msgs > 0:
    first_msg = conn.execute("SELECT content FROM messages WHERE role='user' LIMIT 1").fetchone()
    if first_msg and first_msg["content"]:
        words = [w for w in first_msg["content"].split() if len(w) > 4]
        if words:
            search_word = words[0]
            results = skill.get_search_results(conn, search_word)
            print(f"Search '{search_word[:20]}': {len(results)} results")

# Stats
print("\n--- Phase 6: Stats ---")
stats = skill.get_stats(conn)
for s in stats:
    print(f"  {s['label']}: {s['value']}")

print(f"\n--- DONE ---")
print(f"DB: {db_path}")
conn.close()
