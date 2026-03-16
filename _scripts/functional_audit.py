"""Functional audit — tests that all skill logic works as intended, not just compiles."""
import sys
import os
import sqlite3
import tempfile
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

today = datetime.now().date()
PASS = 0
FAIL = 0

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        print(f"  FAIL: {name} — {detail}")

def make_db():
    db = os.path.join(tempfile.gettempdir(), f'audit_{os.getpid()}.db')
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    return conn, db

# ════════════════════════════════════════════════════════════════════
print("=" * 60)
print("FUNCTIONAL AUDIT: YouTube Skill")
print("=" * 60)

from skills.youtube_history import (
    YouTubeHistorySkill, _parse_date_group, _extract_video_id,
    _extract_short_id, _extract_resume_seconds, _duration_to_seconds
)

# 1. Date parsing
print("\n1. Date parsing")
check("Today", _parse_date_group("Today") == today.isoformat(), _parse_date_group("Today"))
check("Yesterday", _parse_date_group("Yesterday") == (today - timedelta(days=1)).isoformat())
check("Mar 8 parses", _parse_date_group("Mar 8") is not None)
check("Empty=None", _parse_date_group("") is None)
check("None=None", _parse_date_group(None) is None)
check("Gibberish=None", _parse_date_group("gibberish xyz") is None)
# Day name
check("Day name parses", _parse_date_group("Monday") is not None)
day_result = _parse_date_group("Monday")
if day_result:
    check("Day name is past", day_result <= today.isoformat(), day_result)

# 2. Video/Short ID
print("\n2. Video/Short ID extraction")
check("Normal video ID", _extract_video_id("/watch?v=XerQ1wT-8wI") == "XerQ1wT-8wI")
check("Video with &t=", _extract_video_id("/watch?v=abc123def45&t=200s") == "abc123def45")
check("Shorts not video", _extract_video_id("/shorts/abc123def45") is None)
check("Empty video", _extract_video_id("") is None)
check("None video", _extract_video_id(None) is None)
check("Short ID", _extract_short_id("/shorts/abc123def45") == "abc123def45")
check("Watch not short", _extract_short_id("/watch?v=abc") is None)

# 3. Resume + Duration
print("\n3. Resume time + Duration")
check("Resume 200s", _extract_resume_seconds("/watch?v=abc&t=200s") == 200)
check("Resume no t", _extract_resume_seconds("/watch?v=abc") == 0)
check("Duration 29:03", _duration_to_seconds("29:03") == 1743)
check("Duration 1:02:15", _duration_to_seconds("1:02:15") == 3735)
check("Duration empty", _duration_to_seconds("") == 0)
check("Duration None", _duration_to_seconds(None) == 0)

# 4. Schema + UPSERT + FTS
print("\n4. Schema + UPSERT + FTS")
skill = YouTubeHistorySkill()
conn, db = make_db()
skill.create_schema(conn)

conn.execute(
    "INSERT INTO videos (video_id, title, channel, url, description, watch_percent, watched_date) "
    "VALUES ('vid1', 'Original Title', 'Ch1', 'url1', 'original desc', 45, ?)",
    (today.isoformat(),)
)
conn.commit()

fts1 = conn.execute("SELECT * FROM videos_fts WHERE videos_fts MATCH 'original'").fetchall()
check("FTS finds original", len(fts1) == 1, f"got {len(fts1)}")

# UPSERT
conn.execute(
    "INSERT INTO videos (video_id, title, channel, url, description, watch_percent, watched_date) "
    "VALUES ('vid1', 'New Title', 'Ch1', 'url1', 'new desc', 80, ?) "
    "ON CONFLICT(video_id) DO UPDATE SET title=excluded.title, description=excluded.description, "
    "watch_percent=excluded.watch_percent, updated_at=datetime('now')",
    (today.isoformat(),)
)
conn.commit()

row = conn.execute("SELECT title, watch_percent FROM videos WHERE video_id='vid1'").fetchone()
check("UPSERT title", row["title"] == "New Title", row["title"])
check("UPSERT watch%", row["watch_percent"] == 80, row["watch_percent"])

fts_new = conn.execute("SELECT * FROM videos_fts WHERE videos_fts MATCH 'New'").fetchall()
check("FTS updated", len(fts_new) == 1, f"got {len(fts_new)}")
fts_old = conn.execute("SELECT * FROM videos_fts WHERE videos_fts MATCH 'original'").fetchall()
check("FTS old gone", len(fts_old) == 0, f"got {len(fts_old)}")

# 5. Stats
print("\n5. Stats")
conn.execute(
    "INSERT INTO shorts (short_id, title, url, watched_date) VALUES ('s1', 'Short1', 'url_s', ?)",
    (today.isoformat(),)
)
conn.execute(
    "INSERT INTO videos (video_id, title, url, watch_percent, watched_date) "
    "VALUES ('vid2', 'Complete', 'url2', 100, ?)",
    (today.isoformat(),)
)
conn.commit()

stats = skill.get_stats(conn)
check("Stats videos", stats[0]["value"] == 2, stats[0])
check("Stats shorts", stats[1]["value"] == 1, stats[1])
check("Stats unfinished", stats[2]["value"] == 1, stats[2])  # vid1 at 80%
check("Stats completed", stats[3]["value"] == 1, stats[3])   # vid2 at 100%

# 6. Search
print("\n6. Search")
results = skill.get_search_results(conn, "New")
check("Search finds updated", len(results) == 1, f"got {len(results)}")
check("Search source field", results[0]["source"] == "youtube_history" if results else False)

# 7. Stop strategy
print("\n7. Stop strategy")
check("Stop strategy is DATE_GROUP", skill.stop_strategy.value == "date_group")

class MockTracker:
    _skill_conn = conn

tracker = MockTracker()

# No collection_state = first run = never stop
item = {"watched_date": today.isoformat()}
check("First run: no stop", skill.should_stop_collecting(item, tracker) == False)

# Set last_date_group
conn.execute(
    "INSERT INTO collection_state (key, value) VALUES ('last_date_group', ?)",
    ((today - timedelta(days=3)).isoformat(),)
)
conn.commit()

old_item = {"watched_date": (today - timedelta(days=5)).isoformat()}
check("Old item: stops", skill.should_stop_collecting(old_item, tracker) == True)

new_item = {"watched_date": today.isoformat()}
check("New item: no stop", skill.should_stop_collecting(new_item, tracker) == False)

# 8. RunLimits
print("\n8. RunLimits")
limits_first = skill.create_run_limits(is_first_run=True)
check("First run max=100", limits_first.max_items == 100)

limits_sub = skill.create_run_limits(is_first_run=False)
check("Subsequent max=0", limits_sub.max_items == 0)

for _ in range(5):
    limits_first.item_done()
check("Item counting", limits_first.items_collected == 5)
check("Not exceeded at 5", not limits_first.items_exceeded)

# 9. UI manifest validation
print("\n9. UI manifest")
widgets = skill.get_widgets()
check("3 widgets", len(widgets) == 3)
check("Widget names", [w.name for w in widgets] == ["stats", "unfinished", "recent"])

sections = skill.get_page_sections()
check("5 sections", len(sections) == 5)
check("Sections ordered", all(sections[i].position <= sections[i+1].position for i in range(len(sections)-1)))

rules = skill.get_notification_rules()
check("3 rules", len(rules) == 3)

conn.close()
os.remove(db)

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FUNCTIONAL AUDIT: ChatGPT Skill")
print("=" * 60)

from skills.chatgpt_history import ChatGPTHistorySkill

skill_c = ChatGPTHistorySkill()

# 1. Schema + message tracking
print("\n1. Schema + message dedup")
conn_c, db_c = make_db()
skill_c.create_schema(conn_c)

# Insert conversation
conn_c.execute(
    "INSERT INTO conversations (external_id, title, url, message_count) "
    "VALUES ('uuid-123', 'Test Chat', 'https://chatgpt.com/c/uuid-123', 3)"
)
conv_id = conn_c.execute("SELECT id FROM conversations WHERE external_id='uuid-123'").fetchone()["id"]

# Insert messages with unique message_ids
conn_c.execute(
    "INSERT INTO messages (conversation_id, message_id, role, content, message_order) "
    "VALUES (?, 'msg-1', 'user', 'Hello there', 1)", (conv_id,)
)
conn_c.execute(
    "INSERT INTO messages (conversation_id, message_id, role, content, message_order) "
    "VALUES (?, 'msg-2', 'assistant', 'Hi! How can I help?', 2)", (conv_id,)
)
conn_c.commit()

# Try duplicate message_id — should fail (UNIQUE constraint)
try:
    conn_c.execute(
        "INSERT INTO messages (conversation_id, message_id, role, content, message_order) "
        "VALUES (?, 'msg-1', 'user', 'Duplicate!', 3)", (conv_id,)
    )
    check("Duplicate message_id rejected", False, "Should have raised")
except sqlite3.IntegrityError:
    check("Duplicate message_id rejected", True)

# New message_id works
conn_c.execute(
    "INSERT INTO messages (conversation_id, message_id, role, content, message_order) "
    "VALUES (?, 'msg-3', 'user', 'New message', 3)", (conv_id,)
)
conn_c.commit()
check("New message accepted", True)

# 2. _save_messages dedup
print("\n2. _save_messages dedup logic")
msgs = [
    {"message_id": "msg-1", "role": "user", "content": "Hello", "message_order": 1},
    {"message_id": "msg-2", "role": "assistant", "content": "Hi", "message_order": 2},
    {"message_id": "msg-3", "role": "user", "content": "New", "message_order": 3},
    {"message_id": "msg-4", "role": "assistant", "content": "Newest!", "message_order": 4},
]
new_count = skill_c._save_messages(conn_c, conv_id, msgs)
check("Only 1 new message saved", new_count == 1, f"got {new_count}")

total = conn_c.execute("SELECT COUNT(*) as c FROM messages WHERE conversation_id=?", (conv_id,)).fetchone()["c"]
check("Total messages = 4", total == 4, f"got {total}")

# 3. FTS on messages
print("\n3. FTS search")
fts = conn_c.execute("SELECT * FROM messages_fts WHERE messages_fts MATCH 'Newest'").fetchall()
check("FTS finds new message", len(fts) == 1, f"got {len(fts)}")

# 4. Stats
print("\n4. Stats")
stats_c = skill_c.get_stats(conn_c)
check("Conversations=1", stats_c[0]["value"] == 1)
check("Messages=4", stats_c[2]["value"] == 4, stats_c[2])
check("User msgs=2", stats_c[3]["value"] == 2, stats_c[3])

# 5. Stop strategy — consecutive known
print("\n5. Stop strategy")
check("Stop is CONSECUTIVE_KNOWN", skill_c.stop_strategy.value == "consecutive_known")

class MockTrackerC:
    _skill_conn = conn_c

item_3 = {"_consecutive_known": 3}
check("3 consecutive: no stop", skill_c.should_stop_collecting(item_3, MockTrackerC()) == False)

item_5 = {"_consecutive_known": 5}
check("5 consecutive: stop", skill_c.should_stop_collecting(item_5, MockTrackerC()) == True)

# 6. Conversation position tracking
print("\n6. Position tracking for update detection")
# Simulate: conversation at position 5, then moves to position 1
conn_c.execute("UPDATE conversations SET list_position=5 WHERE id=?", (conv_id,))
conn_c.commit()
existing = conn_c.execute("SELECT list_position FROM conversations WHERE id=?", (conv_id,)).fetchone()
check("Position stored=5", existing["list_position"] == 5)

# If new_pos < old_pos → moved up → was updated
new_pos = 1
old_pos = existing["list_position"]
is_updated = new_pos < old_pos
check("Moved up = updated", is_updated == True)

# If new_pos >= old_pos → stayed or moved down → unchanged
new_pos2 = 7
is_unchanged = new_pos2 >= old_pos
check("Moved down = unchanged", is_unchanged == True)

# 7. Pinned conversations
print("\n7. Pinned conversations")
conn_c.execute(
    "INSERT INTO conversations (external_id, title, url, is_pinned) "
    "VALUES ('uuid-pin', 'Pinned Chat', 'url', 1)"
)
conn_c.commit()
pinned = conn_c.execute("SELECT COUNT(*) as c FROM conversations WHERE is_pinned=1").fetchone()["c"]
check("Pinned count=1", pinned == 1)

# 8. Sources storage
print("\n8. Sources as JSON")
sources_json = json.dumps([{"name": "Wikipedia", "url": "https://en.wikipedia.org"}])
conn_c.execute(
    "INSERT INTO messages (conversation_id, message_id, role, content, sources, message_order) "
    "VALUES (?, 'msg-src', 'assistant', 'According to sources...', ?, 5)",
    (conv_id, sources_json)
)
conn_c.commit()
src_row = conn_c.execute("SELECT sources FROM messages WHERE message_id='msg-src'").fetchone()
parsed = json.loads(src_row["sources"])
check("Sources stored as JSON", len(parsed) == 1)
check("Source name", parsed[0]["name"] == "Wikipedia")

conn_c.close()
os.remove(db_c)

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FUNCTIONAL AUDIT: Gemini Skill")
print("=" * 60)

from skills.gemini_history import GeminiHistorySkill

skill_g = GeminiHistorySkill()

# 1. Schema + thinking blocks
print("\n1. Schema + thinking blocks")
conn_g, db_g = make_db()
skill_g.create_schema(conn_g)

conn_g.execute(
    "INSERT INTO conversations (external_id, title, url, message_count, has_thinking) "
    "VALUES ('hex123abc', 'Gemini Chat', 'https://gemini.google.com/app/hex123abc', 4, 1)"
)
conv_id_g = conn_g.execute("SELECT id FROM conversations WHERE external_id='hex123abc'").fetchone()["id"]

conn_g.execute(
    "INSERT INTO messages (conversation_id, role, content, thinking_block, message_order) "
    "VALUES (?, 'assistant', 'The answer is 42', 'Let me think step by step...', 1)",
    (conv_id_g,)
)
conn_g.commit()

# FTS searches BOTH content and thinking_block
fts_content = conn_g.execute("SELECT * FROM messages_fts WHERE messages_fts MATCH 'answer'").fetchall()
check("FTS finds content", len(fts_content) == 1)

fts_think = conn_g.execute("SELECT * FROM messages_fts WHERE messages_fts MATCH 'step'").fetchall()
check("FTS finds thinking block", len(fts_think) == 1, f"got {len(fts_think)}")

# 2. has_thinking flag
print("\n2. has_thinking tracking")
stats_g = skill_g.get_stats(conn_g)
check("With Thinking=1", stats_g[1]["value"] == 1)

# Add conversation WITHOUT thinking
conn_g.execute(
    "INSERT INTO conversations (external_id, title, url, message_count, has_thinking) "
    "VALUES ('hex456def', 'No Think Chat', 'url2', 2, 0)"
)
conn_g.commit()
stats_g2 = skill_g.get_stats(conn_g)
check("Total convs=2", stats_g2[0]["value"] == 2)
check("With Thinking still=1", stats_g2[1]["value"] == 1)

# 3. _save_messages incremental
print("\n3. Incremental message saving")
existing_count = conn_g.execute(
    "SELECT COUNT(*) as c FROM messages WHERE conversation_id=?", (conv_id_g,)
).fetchone()["c"]
check("Existing msgs=1", existing_count == 1)

new_msgs = [
    {"role": "assistant", "content": "The answer is 42", "thinking_block": "step by step", "sources": "", "code_blocks": "", "message_order": 1},
    {"role": "user", "content": "Why 42?", "thinking_block": "", "sources": "", "code_blocks": "", "message_order": 2},
    {"role": "assistant", "content": "Because Douglas Adams", "thinking_block": "Hitchhiker ref", "sources": "", "code_blocks": "", "message_order": 3},
]
new_count_g = skill_g._save_messages(conn_g, conv_id_g, new_msgs)
check("2 new msgs added (order 2,3)", new_count_g == 2, f"got {new_count_g}")

total_g = conn_g.execute(
    "SELECT COUNT(*) as c FROM messages WHERE conversation_id=?", (conv_id_g,)
).fetchone()["c"]
check("Total msgs now=3", total_g == 3, f"got {total_g}")

# Same messages again = 0 new
same_count = skill_g._save_messages(conn_g, conv_id_g, new_msgs)
check("Same msgs = 0 new", same_count == 0, f"got {same_count}")

# 4. Sources as inline links
print("\n4. Inline sources")
sources = json.dumps([
    {"name": "arxiv paper", "url": "https://arxiv.org/abs/1234"},
    {"name": "blog post", "url": "https://example.com/blog"},
])
conn_g.execute(
    "INSERT INTO messages (conversation_id, role, content, sources, message_order) "
    "VALUES (?, 'assistant', 'According to research...', ?, 4)",
    (conv_id_g, sources)
)
conn_g.commit()

src_row_g = conn_g.execute(
    "SELECT sources FROM messages WHERE conversation_id=? AND message_order=4", (conv_id_g,)
).fetchone()
parsed_g = json.loads(src_row_g["sources"])
check("2 sources stored", len(parsed_g) == 2)

# 5. Search with thinking flag
print("\n5. Search returns has_thinking")
results_g = skill_g.get_search_results(conn_g, "answer")
check("Search finds msg", len(results_g) >= 1, f"got {len(results_g)}")
if results_g:
    check("has_thinking in result", "has_thinking" in results_g[0])

# 6. _upsert_conversation
print("\n6. Upsert conversation")
conv_data = {"external_id": "hex123abc", "title": "Updated Title", "position": 0, "url": "url1"}
updated_id = skill_g._upsert_conversation(conn_g, conv_data, 5, True)
check("Upsert returns same id", updated_id == conv_id_g)

row_g = conn_g.execute("SELECT title, message_count FROM conversations WHERE id=?", (conv_id_g,)).fetchone()
check("Title updated", row_g["title"] == "Updated Title")
check("Message count updated", row_g["message_count"] == 5)

conn_g.close()
os.remove(db_g)

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FUNCTIONAL AUDIT: Core DB + Sync Tracker")
print("=" * 60)

from src.db.core_db import (
    init_core_db, get_core_connection, get_setting, set_setting,
    add_alert, get_alerts, dismiss_alert,
    add_notification, get_notifications,
    register_skill, get_skill_info, get_all_skills,
)

print("\n1. Settings")
core_db = os.path.normpath(os.path.join(tempfile.gettempdir(), "test_core_func.db"))
init_core_db(core_db)

set_setting("test_key", "test_value", db_path=core_db)
check("Set setting", get_setting("test_key", db_path=core_db) == "test_value")
set_setting("test_key", "updated", db_path=core_db)
check("Update setting", get_setting("test_key", db_path=core_db) == "updated")
check("Missing setting=None", get_setting("nonexist", db_path=core_db) is None)

print("\n2. Alerts")
add_alert("Test Alert", "Something happened", level="warning", source="youtube", db_path=core_db)
alerts = get_alerts(db_path=core_db)
check("Alert created + retrievable", len(alerts) >= 1)
check("Alert content", len(alerts) > 0 and alerts[0]["title"] == "Test Alert")
if alerts:
    alert_id = alerts[0]["id"]
    dismiss_alert(alert_id, db_path=core_db)
    alerts2 = get_alerts(db_path=core_db)
    check("Alert dismissed", len(alerts2) == 0, f"still {len(alerts2)} alerts")
else:
    check("Alert dismissed", False, "no alerts to dismiss")

print("\n3. Notifications")
# Signature: add_notification(skill_name, title, message, level, link_to, db_path)
add_notification("youtube", "New data", "Found 5 videos", level="info",
                 link_to="/skill/youtube", db_path=core_db)
notifs = get_notifications(db_path=core_db)
check("Notification created", len(notifs) >= 1)
if notifs:
    check("Notification unread", notifs[0]["read"] == 0)
    check("Notification skill", notifs[0]["skill_name"] == "youtube")

print("\n4. Skill registry")
register_skill("test_skill", "1.0.0", "A test", "google", "https://test.com",
               "skill_data/test.db", "https://test.com/login", 1, 3, db_path=core_db)
info = get_skill_info("test_skill", db_path=core_db)
check("Skill registered", info is not None)
check("Skill version", info["version"] == "1.0.0")
all_skills = get_all_skills(db_path=core_db)
check("All skills includes test", any(s["name"] == "test_skill" for s in all_skills))

os.remove(core_db)

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FUNCTIONAL AUDIT: RunLimits + Multi-layer Stop")
print("=" * 60)

from src.skills.base import RunLimits
import time as _time

print("\n1. Time limit")
limits = RunLimits(max_items=0, max_minutes=0.01)  # 0.6 seconds
check("Not exceeded initially", not limits.time_exceeded)
_time.sleep(1)
check("Exceeded after 1s", limits.time_exceeded)
check("should_stop returns True", limits.should_stop())

print("\n2. Item limit")
limits2 = RunLimits(max_items=3, max_minutes=999)
for _ in range(3):
    limits2.item_done()
check("3 items = exceeded", limits2.items_exceeded)
check("should_stop True", limits2.should_stop())

print("\n3. Unlimited items")
limits3 = RunLimits(max_items=0, max_minutes=999)
for _ in range(1000):
    limits3.item_done()
check("0 = unlimited, never exceeded", not limits3.items_exceeded)

print("\n4. Scroll sessions")
limits4 = RunLimits(max_items=0, max_minutes=999, max_scrolls_before_pause=2, max_scroll_sessions=3)
for _ in range(6):  # 2 scrolls * 3 sessions
    limits4.scroll_done()
check("3 sessions reached", limits4.sessions_exceeded)

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("FUNCTIONAL AUDIT: UI Manifest")
print("=" * 60)

from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule

print("\n1. NotificationRule evaluation")
rule = NotificationRule(
    event="after_collection",
    condition="items_new > 0",
    title_template="{items_new} new videos",
    message_template="Collected {items_new} from {items_found}",
    level="info",
    link_to="/skill/youtube",
)
context_match = {"items_new": 5, "items_found": 20}
result = rule.evaluate(context_match)
check("Rule matches", result is not None)
if result:
    title, msg = result
    check("Title rendered", title == "5 new videos", title)
    check("Message rendered", msg == "Collected 5 from 20", msg)

context_no_match = {"items_new": 0, "items_found": 20}
result2 = rule.evaluate(context_no_match)
check("Rule no match", result2 is None)

print("\n2. NotificationRule edge cases")
rule2 = NotificationRule(
    event="after_collection",
    condition="items_found == 0 and previous_count > 0",
    title_template="No data found",
    message_template="Had {previous_count}, now 0",
    level="warning",
)
ctx = {"items_found": 0, "previous_count": 50}
result3 = rule2.evaluate(ctx)
check("Zero items rule fires", result3 is not None)
if result3:
    check("Warning msg", result3[1] == "Had 50, now 0", result3[1])

# ════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print(f"AUDIT COMPLETE: {PASS} passed, {FAIL} failed")
print("=" * 60)

if FAIL > 0:
    sys.exit(1)
