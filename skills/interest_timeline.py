"""
Interest Timeline Skill — extracts, tracks, and evolves user interests from activity data.

Reads enriched data from google_activity.db (tagged, sessioned, time-contexted).
Runs daily LLM pipeline via ChatGPT inference service.
Maintains interest registry with lifecycle tracking, families, and branching.

Pipeline:
  1. Find unprocessed days (compare google_activity vs processing_log)
  2. For each day: format data -> Call 1 (extract interests) -> Call 2a/2b (match registry) -> update
  3. Volume-triggered registry review (merge duplicates, create families)

CHANGELOG:
  v0.1.0 (2026-03-23): Initial build — schema, parser, Call 1, Call 2a/2b, pipeline

__version__ = "0.1.0"
"""
__version__ = "0.1.0"

import json
import logging
import os
import re
import sqlite3 as _sqlite3
import time
from datetime import datetime

logger = logging.getLogger("memory_tap.skill.interest_timeline")

from src.skills.base import (
    BaseSkill, SkillManifest, CollectResult, StopStrategy, RunLimits, SkillSetting,
)
from src.db.sync_tracker import SyncTracker
from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule
from src.cdp_client import CDPTab

# ── Constants ──────────────────────────────────────────────────

GOOGLE_ACTIVITY_DB_NAME = "google_activity.db"
MAX_RETRIES = 10          # max retry attempts per call
CALL1_TIMEOUT = 600       # 10 min for large prompts
CALL2_TIMEOUT = 600       # 10 min — Call 3 parallel submits 4 requests, sequential harness needs headroom
RETRY_BACKOFF_BASE = 15   # base wait between retries (seconds)
RETRY_BACKOFF_MAX = 300   # max wait between retries (5 min)
REGISTRY_REVIEW_THRESHOLD = 50  # trigger review when this many interests since last review


class InterestTimelineSkill(BaseSkill):
    """Extracts and tracks user interests from Google Activity data."""

    # Overridable for testing
    CORE_DB_PATH = None
    GOOGLE_ACTIVITY_DB_PATH = None
    DEFAULT_LLM_MODEL = "google/gemini-3-pro-preview"  # best quality for interest extraction

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="interest_timeline",
            version=__version__,
            target_url="",  # No website — reads from google_activity.db
            description="Extracts and tracks user interests from activity data",
            auth_provider="",
            schedule_hours=6,
            needs_browser=False,
            max_items_first_run=0,
            max_items_per_run=0,
            max_minutes_per_run=720,
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.TIME_LIMIT

    def _main_table_name(self) -> str:
        return "interest_registry"

    # ── Schema ────────────────────────────────────────────────

    def create_schema(self, conn) -> None:
        """Create tables for interest tracking."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS interest_registry (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                canonical_name    TEXT NOT NULL,
                category          TEXT,
                sub_topic         TEXT,
                first_seen        TEXT NOT NULL,
                last_seen         TEXT NOT NULL,
                total_days_active INTEGER DEFAULT 1,
                lifecycle_status  TEXT DEFAULT 'emerging',
                max_strength      TEXT,
                current_strength  TEXT,
                notes             TEXT,
                keywords          TEXT,
                parent_id         INTEGER,
                related_ids       TEXT,
                family_id         INTEGER,
                created_at        TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_registry_lifecycle ON interest_registry(lifecycle_status);
            CREATE INDEX IF NOT EXISTS idx_registry_family ON interest_registry(family_id);
            CREATE INDEX IF NOT EXISTS idx_registry_last_seen ON interest_registry(last_seen);

            CREATE TABLE IF NOT EXISTS interest_families (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                family_name     TEXT NOT NULL,
                description     TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS daily_interest_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL,
                interest_id     INTEGER NOT NULL,
                strength        TEXT NOT NULL,
                evidence_count  INTEGER,
                top_evidence    TEXT,
                notes           TEXT,
                FOREIGN KEY (interest_id) REFERENCES interest_registry(id)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_log_date ON daily_interest_log(date);
            CREATE INDEX IF NOT EXISTS idx_daily_log_interest ON daily_interest_log(interest_id);

            CREATE TABLE IF NOT EXISTS processing_log (
                date              TEXT PRIMARY KEY,
                status            TEXT NOT NULL,
                call1_response    TEXT,
                call2_response    TEXT,
                interests_found   INTEGER,
                interests_matched INTEGER,
                interests_new     INTEGER,
                processed_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS weekly_narratives (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                week_start      TEXT NOT NULL,
                week_end        TEXT NOT NULL,
                narrative       TEXT,
                interests_emerged TEXT,
                interests_faded   TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS interest_redirects (
                old_id        INTEGER NOT NULL,
                new_id        INTEGER NOT NULL,
                merge_reason  TEXT,
                merged_on     TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_redirects_old ON interest_redirects(old_id);

            CREATE TABLE IF NOT EXISTS user_context (
                key           TEXT PRIMARY KEY,
                value         TEXT NOT NULL,
                source        TEXT DEFAULT 'user_answer',
                asked_at      TEXT,
                answered_at   TEXT
            );

            CREATE TABLE IF NOT EXISTS display_log (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                date          TEXT NOT NULL,
                display_type  TEXT,
                content       TEXT,
                question_id   INTEGER,
                created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS questions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                question      TEXT NOT NULL,
                context       TEXT,
                status        TEXT DEFAULT 'pending',
                answer        TEXT,
                related_interest_ids TEXT,
                asked_on      TEXT NOT NULL DEFAULT (datetime('now')),
                answered_on   TEXT
            );

            CREATE TABLE IF NOT EXISTS settings (
                key             TEXT PRIMARY KEY,
                value           TEXT NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            INSERT OR IGNORE INTO settings (key, value) VALUES
                ('last_review_interest_count', '0'),
                ('last_review_date', ''),
                ('last_5_days_matching_rates', '[]'),
                ('llm_model', 'google/gemini-3-pro-preview');
        """)
        conn.commit()

    def migrate_schema(self, conn, old_version: str, new_version: str) -> None:
        """Handle schema changes between versions."""
        pass  # v0.1.0 — initial version

    def check_login(self, tab: CDPTab) -> bool:
        """No login needed — reads from local DB."""
        return True

    def should_stop_collecting(self, item: dict, tracker) -> bool:
        """Not used — this skill doesn't collect via scrolling."""
        return False

    # ── Interest ID Resolution ─────────────────────────────────

    def _resolve_interest_id(self, conn, interest_id: int, max_depth: int = 5) -> int:
        """Follow redirect chain to find the current survivor ID.

        Chain-safe: if survivor was later merged again, follows the chain.
        Returns the final living ID, or the original if no redirect exists.
        """
        current = interest_id
        for _ in range(max_depth):
            redirect = conn.execute(
                "SELECT new_id FROM interest_redirects WHERE old_id = ?",
                (current,)
            ).fetchone()
            if not redirect:
                return current
            current = redirect[0]
        return current

    # ── Response Parser ───────────────────────────────────────

    def _parse_chatgpt_json(self, raw_response: str) -> list[dict] | dict | None:
        """Extract JSON from ChatGPT reply text.

        Handles known formats:
          - Plain JSON array/object
          - "JSON" or "json" prefix
          - Markdown ```json ... ``` blocks
          - Explanation text before/after JSON
          - Nested in {"reply": "..."} wrapper

        Returns parsed JSON or None if unparsable.
        """
        if not raw_response or not raw_response.strip():
            return None

        text = raw_response.strip()

        # Step 1: If wrapped in {"reply": ...}, extract the reply
        if text.startswith('{') and '"reply"' in text[:50]:
            try:
                outer = json.loads(text)
                text = outer.get('reply', text)
            except (json.JSONDecodeError, TypeError):
                pass

        text = text.strip()

        # Step 2: Check for ChatGPT error messages
        error_phrases = [
            "exceeded the maximum length",
            "I can't process",
            "I'm unable to",
            "too long",
        ]
        for phrase in error_phrases:
            if phrase.lower() in text.lower():
                logger.warning(f"ChatGPT returned error: {text[:100]}")
                return None

        # Step 3: Strip known prefixes
        for prefix in ['JSON', 'json', '```json', '```']:
            if text.startswith(prefix):
                text = text[len(prefix):].strip()

        # Step 4: Strip trailing markdown fence
        if text.endswith('```'):
            text = text[:-3].strip()

        # Step 5: Try direct parse
        try:
            result = json.loads(text)
            if isinstance(result, (list, dict)):
                return result
        except json.JSONDecodeError:
            pass

        # Step 6: Find JSON array or object in the text
        # Look for first [ ... ] or { ... }
        for start_char, end_char in [('[', ']'), ('{', '}')]:
            start_idx = text.find(start_char)
            if start_idx == -1:
                continue

            # Find the matching closing bracket
            depth = 0
            for i in range(start_idx, len(text)):
                if text[i] == start_char:
                    depth += 1
                elif text[i] == end_char:
                    depth -= 1
                    if depth == 0:
                        candidate = text[start_idx:i + 1]
                        try:
                            result = json.loads(candidate)
                            if isinstance(result, (list, dict)):
                                return result
                        except json.JSONDecodeError:
                            break

        logger.warning(f"Could not parse JSON from response ({len(raw_response)} chars)")
        return None

    # ── ChatGPT Communication ─────────────────────────────────

    def _get_core_db_path(self):
        """Get path to core.db for service requests."""
        if self.CORE_DB_PATH:
            return self.CORE_DB_PATH
        return os.path.join(os.environ.get('LOCALAPPDATA', ''), 'MemoryTap', 'core.db')

    def _get_google_activity_db_path(self):
        """Get path to google_activity.db."""
        if self.GOOGLE_ACTIVITY_DB_PATH:
            return self.GOOGLE_ACTIVITY_DB_PATH
        # In production, skill DBs are in skill_data/ next to core.db
        base = os.path.dirname(self._get_core_db_path())
        return os.path.join(base, 'skill_data', GOOGLE_ACTIVITY_DB_NAME)

    def _get_llm_model(self, conn) -> str:
        """Get LLM model. Priority: core.db skill_settings > per-skill settings > class default."""
        # 1. Check core.db skill_settings (set via dashboard)
        try:
            core_db = self._get_core_db_path()
            core_conn = _sqlite3.connect(core_db)
            row = core_conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = 'interest_timeline' AND key = 'llm_model'"
            ).fetchone()
            core_conn.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
        # 2. Check per-skill settings table
        try:
            row = conn.execute(
                "SELECT value FROM settings WHERE key = 'llm_model'"
            ).fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
        return self.DEFAULT_LLM_MODEL

    def _chatgpt_request(self, prompt: str, timeout: int = CALL1_TIMEOUT,
                         web_search: bool = False, conn=None) -> str:
        """Send prompt to ChatGPT inference via service request. Returns reply text.

        This is the ONLY way interest_timeline talks to ChatGPT.
        All LLM interaction is handled by the chatgpt_inference skill via the harness.
        """
        model = self._get_llm_model(conn) if conn else self.DEFAULT_LLM_MODEL
        payload_dict = {"prompt": prompt, "web_search": web_search, "model": model}
        payload_str = json.dumps(payload_dict, ensure_ascii=False)

        core_db = self._get_core_db_path()
        conn = _sqlite3.connect(core_db)
        conn.execute(
            "INSERT INTO service_requests (from_skill, to_skill, service_name, payload, state, created_at) "
            "VALUES (?, ?, ?, ?, 'PENDING', datetime('now'))",
            ("interest_timeline", "chatgpt_inference", "execute_prompt", payload_str)
        )
        conn.commit()
        req_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()

        logger.info(f"Service request {req_id}: execute_prompt ({len(prompt)} chars, web_search={web_search})")

        # Poll for result
        for _ in range(timeout):
            conn = _sqlite3.connect(core_db)
            conn.row_factory = _sqlite3.Row
            row = conn.execute(
                "SELECT state, result, error FROM service_requests WHERE id = ?",
                (req_id,)
            ).fetchone()
            conn.close()

            if row["state"] == "COMPLETED":
                result = json.loads(row["result"]) if row["result"] else {}
                reply = result.get("reply", "")
                logger.info(f"Service request {req_id}: COMPLETED ({len(reply)} chars)")
                return reply
            elif row["state"] in ("FAILED", "TIMEOUT"):
                error = row["error"] or "Unknown error"
                logger.error(f"Service request {req_id}: {row['state']} - {error}")
                return ""

            time.sleep(1)

        logger.error(f"Service request {req_id}: timed out after {timeout}s")
        return ""

    # ── Prompt Formatting ─────────────────────────────────────

    def _format_day_prompt(self, ga_conn, target_date: str) -> str:
        """Format one day's activity data into the Call 1 prompt.

        Reads from google_activity.db (tagged, sessioned entries).
        Returns the full prompt string.
        """
        # Get baseline skip rate
        skip_row = ga_conn.execute(
            "SELECT value FROM settings WHERE key = 'avg_skip_rate'"
        ).fetchone()
        skip_rate = float(skip_row[0]) if skip_row else 0.0

        # Get sessions
        sessions = ga_conn.execute("""
            SELECT session_id,
                   MIN(timestamp) as start_ts, MAX(timestamp) as end_ts,
                   COUNT(*) as total,
                   SUM(CASE WHEN tag IN ('SHORT-WATCHED','SHORT-SKIPPED') THEN 1 ELSE 0 END) as shorts_total,
                   SUM(CASE WHEN tag = 'SHORT-SKIPPED' THEN 1 ELSE 0 END) as shorts_skipped,
                   SUM(CASE WHEN entry_type = 'Watched' THEN 1 ELSE 0 END) as watched,
                   SUM(CASE WHEN entry_type = 'Searched' THEN 1 ELSE 0 END) as searched
            FROM activities WHERE date = ? AND tag != 'GARBAGE'
            GROUP BY session_id ORDER BY session_id
        """, (target_date,)).fetchall()

        # Build data section
        data_lines = []
        for s in sessions:
            sid = s[0]   # session_id
            start = s[1][11:16] if s[1] else '?'
            end = s[2][11:16] if s[2] else '?'
            watched_count = s[6]
            searched_count = s[7]

            if watched_count == 0 and searched_count == 0:
                continue

            skip_note = ''
            shorts_total = s[4]
            if shorts_total >= 5:
                session_skip = round(s[5] / shorts_total * 100)
                diff = session_skip - skip_rate
                if diff > 10:
                    skip_note = f' | shorts skip rate {session_skip}% (ELEVATED vs baseline {skip_rate:.0f}%)'
                elif diff < -10:
                    skip_note = f' | shorts skip rate {session_skip}% (LOW vs baseline {skip_rate:.0f}%)'

            time_ctx_row = ga_conn.execute(
                'SELECT time_context FROM activities WHERE date = ? AND session_id = ? LIMIT 1',
                (target_date, sid)
            ).fetchone()
            time_ctx = time_ctx_row[0] if time_ctx_row else '?'

            data_lines.append(
                f'--- Session {sid} ({start}-{end}, {time_ctx}, '
                f'{watched_count} videos, {searched_count} searches{skip_note}) ---'
            )

            # Watched entries
            entries = ga_conn.execute("""
                SELECT tag, video_title, channel, watch_pct, actual_minutes_watched,
                       duration, channel_summary, multi_session
                FROM activities
                WHERE date = ? AND session_id = ? AND entry_type = 'Watched' AND tag != 'GARBAGE'
                ORDER BY timestamp
            """, (target_date, sid)).fetchall()

            for e in entries:
                title = (e[1] or '?')[:70]
                ch = e[2] or '?'
                pct = e[3]
                mins = f"{e[4]:.1f}min" if e[4] else '?'
                dur = e[5] or '?'
                ch_sum = f" -- {e[6][:40]}" if e[6] else ''
                multi = ' [MULTI-SESSION]' if e[7] else ''
                data_lines.append(
                    f"[{e[0]}] {title} [{pct}%/{mins}] (ch: {ch}{ch_sum}) [{dur}]{multi}"
                )

            # Search entries
            searches = ga_conn.execute("""
                SELECT query_text FROM activities
                WHERE date = ? AND session_id = ? AND entry_type = 'Searched'
                ORDER BY timestamp
            """, (target_date, sid)).fetchall()

            for sq in searches:
                data_lines.append(f'[SEARCH] "{sq[0]}"')

            data_lines.append('')

        data_section = '\n'.join(data_lines)

        # Build prompt
        prompt = (
            "You are analyzing one day of a user's digital activity to extract their INTERESTS.\n"
            "\n"
            f"DATE: {target_date}\n"
            "\n"
            "=== UNDERSTANDING THE DATA ===\n"
            "\n"
            "ENTRY TAGS:\n"
            "- [STRONG] = User actively chose this video and watched it fully (100%). Strong interest signal.\n"
            "- [MODERATE] = User chose this video but didn't finish (25-99%). Still interested.\n"
            "- [SKIP] = User opened but left quickly (<25%). Weak or no interest. BUT check actual_minutes_watched\n"
            "  -- a [SKIP] with 4.2min watched is very different from 0.1min.\n"
            "- [SHORT-WATCHED] = YouTube Shorts auto-play feed. User did NOT choose this. Watching fully means\n"
            "  interesting enough to not swipe past. WEAKER than [STRONG].\n"
            "  CRITICAL: Shorts are CONSUMPTION PATTERN, never an interest on their own.\n"
            "  Only count shorts toward an interest if the user ALSO has [STRONG], [MODERATE], or [SEARCH]\n"
            "  entries on the SAME topic. 80 comedy shorts watched != interest in comedy. But 3 comedy shorts\n"
            "  + a 20-min comedy video watched fully = interest in that comedy style.\n"
            "- [SHORT-SKIPPED] = YouTube Shorts auto-play. User swiped past. NEGATIVE signal -- topic did NOT\n"
            "  hold attention.\n"
            "  CRITICAL: Shorts are CONSUMPTION PATTERN, never an interest on their own.\n"
            "  NEVER create an interest backed ONLY by [SHORT-WATCHED] entries. Shorts alone = consumption\n"
            "  pattern, not interest. If shorts are the only evidence, do NOT include that interest.\n"
            "  NEVER merge shorts channels with long-form channels just because the theme seems similar.\n"
            "  A Hindi/English POV meme shorts channel (auto-played) is NOT the same interest as a Malayalam\n"
            "  storytelling channel the user actively chose. Different language = different interest.\n"
            "  Different format (shorts vs long-form) = different engagement level. Keep them separate.\n"
            "  If the shorts channel has no long-form match, it's consumption pattern -- drop it.\n"
            "- [SEARCH] = Google search query. STRONGEST signal -- user actively typed this. Direct intent.\n"
            "\n"
            "ACTUAL MINUTES WATCHED:\n"
            "- Watch percentage alone is misleading. 14% of a 41-minute video = 5.8 minutes genuine watching.\n"
            "  100% of a 12-second short = 0.2 minutes passive viewing.\n"
            "- Use actual_minutes_watched to judge engagement depth, not percentage.\n"
            "\n"
            "MULTI-SESSION FLAG:\n"
            "- Video appeared on multiple different days (5+ min videos only). User RETURNED to this content.\n"
            "- Patterns: all 100% on different days = replays (emotional attachment). Same partial % = keeps\n"
            "  trying. Increasing % = installment viewing.\n"
            "\n"
            "CHANNEL CONTEXT (after \"--\"):\n"
            "- Channel type reveals language preference and cultural lens.\n"
            "- \"Iran conflict\" via Malayalam news is different from via English news.\n"
            "\n"
            "SESSIONS:\n"
            "- Grouped by 15-minute inactivity gaps. Header shows time range, time of day, counts.\n"
            "- What the user consumed TOGETHER in a session tells you more than individual entries.\n"
            "\n"
            "SHORTS SKIP RATE:\n"
            f"- User's historical baseline: {skip_rate:.1f}%\n"
            "- ELEVATED = more distracted than usual, weaker signals.\n"
            "- LOW = highly engaged, even SHORT-WATCHED entries carry more weight.\n"
            "\n"
            "TIME CONTEXT:\n"
            "- late-night / early-morning / morning / lunch / afternoon / evening / night\n"
            "- If timing adds meaning, factor it in. Geopolitics at 2am = deeply engaged. Same at 8am = routine.\n"
            "\n"
            "=== WHAT IS NOT AN INTEREST ===\n"
            "\n"
            "- Payment flows (3D Secure, OAuth, bank redirects) = not interest in that service\n"
            "- Package tracking searches = tracking a delivery, not interested in logistics\n"
            "- Navigational searches (single-word: 'gemini', 'chatgpt', 'youtube', 'gmail') = LIKELY just\n"
            "  typing a URL. Still include as 'weak' interest -- Call 2 will decide if it's a pattern over time.\n"
            "- Notification-triggered searches = not active curiosity\n"
            "- Bank/tax portals = using a service, NOT researching it\n"
            "- Videos with '#live', '#livestream', '#shortslivestream' in title showing 100% at 30+ min\n"
            "  = likely background live stream, not genuine focused viewing. Treat with suspicion.\n"
            "- RULE: Distinguish USING a service vs RESEARCHING a service\n"
            "\n"
            "=== OUTPUT FORMAT ===\n"
            "\n"
            "Return ONLY a JSON array. No other text, no explanation, no markdown.\n"
            "\n"
            "Each interest object:\n"
            "{\n"
            '  "interest": "<specific descriptive name>",\n'
            '  "strength": "strong|moderate|weak",\n'
            '  "evidence_count": <number>,\n'
            '  "top_evidence": ["<title 1>", "<title 2>", "<title 3>"],\n'
            '  "category": "<broad category>",\n'
            '  "sub_topic": "<narrower grouping>",\n'
            '  "notes": "<context: multi-session, time pattern, search+video correlation>"\n'
            "}\n"
            "\n"
            "CRITICAL NAMING RULES:\n"
            "- BAD: \"Entertainment\", \"News\", \"Cooking\", \"Technology\", \"Geopolitics\"\n"
            "- GOOD: \"Iran-Israel military escalation -- following missile strikes and Hormuz tensions via Malayalam + English news\"\n"
            "- GOOD: \"Kerala nostalgia -- late-night Malayalam comedy scenes and Karikku\"\n"
            "- GOOD: \"Genetic testing startups in India -- researching full screening packages\"\n"
            "- Name must be specific enough that reading it 30 days later tells you EXACTLY what the user was into.\n"
            "- Include: specific events, channel names, cultural context, what makes this UNIQUE to this person.\n"
            "- Group related entries into ONE interest with a rich name.\n"
            "\n"
            "STRENGTH:\n"
            "- strong: Multiple [STRONG] + [SEARCH] on same topic, OR [MULTI-SESSION] with high minutes\n"
            "- moderate: Few [STRONG]/[MODERATE] entries on same topic\n"
            "- weak: Single search or single entry with low minutes. Still include it -- Call 2 decides\n"
            "  over time if this is a real interest or a one-off. Do NOT drop single data points.\n"
            "- NEVER create an interest backed ONLY by [SHORT-WATCHED] entries. Shorts alone = consumption\n"
            "  pattern, not interest. If shorts are the only evidence, do NOT include that interest.\n"
            "\n"
            "=== TODAY'S ACTIVITY DATA ===\n"
            "\n"
            f"{data_section}"
        )

        return prompt

    # ── Call 2a: Match Against Active Registry ────────────────

    def _format_call2a_prompt(self, conn, today_interests: list[dict]) -> str:
        """Format Call 2a prompt: match today's interests against active registry."""
        # Get active interests (last seen within 30 days)
        active = conn.execute("""
            SELECT id, canonical_name, category, sub_topic, total_days_active,
                   current_strength, notes, last_seen
            FROM interest_registry
            WHERE lifecycle_status IN ('emerging', 'active', 'core', 'fading')
            ORDER BY last_seen DESC
        """).fetchall()

        if not active:
            # No registry yet — everything is new
            return ""

        # Get recent evidence for each active interest
        registry_lines = []
        for a in active:
            evidence_rows = conn.execute("""
                SELECT top_evidence FROM daily_interest_log
                WHERE interest_id = ? ORDER BY date DESC LIMIT 3
            """, (a[0],)).fetchall()

            evidence_items = []
            for er in evidence_rows:
                if er[0]:
                    try:
                        evidence_items.extend(json.loads(er[0]))
                    except (json.JSONDecodeError, TypeError):
                        pass
            evidence_str = ', '.join(f'"{e}"' for e in evidence_items[:5])

            registry_lines.append(
                f"[ID:{a[0]}] \"{a[1]}\"\n"
                f"  Category: {a[2] or '?'} | Sub: {a[3] or '?'} | "
                f"Active {a[4]} days | Last: {a[7]} | Strength: {a[5] or '?'}\n"
                f"  Recent evidence: {evidence_str}\n"
                f"  Notes: {a[6] or 'none'}"
            )

        registry_section = '\n\n'.join(registry_lines)

        # Format today's interests
        today_lines = []
        for i, interest in enumerate(today_interests):
            today_lines.append(
                f"{i+1}. \"{interest['interest']}\" ({interest['strength']})\n"
                f"   Evidence: {json.dumps(interest.get('top_evidence', []), ensure_ascii=False)}"
            )
        today_section = '\n'.join(today_lines)

        prompt = (
            "You are matching today's extracted interests against an existing interest registry.\n"
            "\n"
            "=== EXISTING REGISTRY ===\n"
            "\n"
            f"{registry_section}\n"
            "\n"
            "=== TODAY'S INTERESTS ===\n"
            "\n"
            f"{today_section}\n"
            "\n"
            "=== TASK ===\n"
            "\n"
            "For EACH of today's interests, decide:\n"
            "- MATCH [ID] -- same interest as an existing registry entry. Optionally suggest updated name if it evolved.\n"
            "- NEW -- genuinely different, should be a new registry entry.\n"
            "- BRANCH [ID] -- related to existing entry but user is developing a SEPARATE sub-interest.\n"
            "  Create new entry linked to parent.\n"
            "\n"
            "IMPORTANT RULES:\n"
            "- 'Iran war' and 'Iranian food' are DIFFERENT interests despite sharing 'Iran'.\n"
            "- 'Kerala comedy' and 'Kerala politics' are DIFFERENT despite sharing 'Kerala'.\n"
            "- Match by WHAT the user is engaging with, not by keyword overlap.\n"
            "- Use the evidence titles to understand what each interest actually IS.\n"
            "- If an interest is evolving (was 'missile strikes', now 'Hormuz naval blockade'),\n"
            "  MATCH it but suggest an updated canonical name.\n"
            "\n"
            "Return ONLY a JSON array:\n"
            "[\n"
            "  {\n"
            '    "today_index": 1,\n'
            '    "decision": "MATCH|NEW|BRANCH",\n'
            '    "registry_id": <ID or null>,\n'
            '    "updated_name": "<new canonical name if evolved, else null>",\n'
            '    "reason": "<brief explanation>"\n'
            "  }\n"
            "]\n"
        )
        return prompt

    # ── Call 2b: Search Dormant Registry ──────────────────────

    def _find_dormant_candidates(self, conn, interest: dict, max_candidates: int = 10) -> list:
        """Keyword search dormant interests for potential matches."""
        # Extract keywords from interest name + evidence
        text = interest.get('interest', '') + ' ' + ' '.join(interest.get('top_evidence', []))
        # Simple keyword extraction: split on non-alphanumeric, filter short words
        words = re.findall(r'[a-zA-Z\u0D00-\u0D7F]{3,}', text.lower())
        keywords = list(set(words))

        if not keywords:
            return []

        # Search dormant interests by keyword overlap in canonical_name, keywords, notes
        dormant = conn.execute("""
            SELECT id, canonical_name, category, sub_topic, total_days_active,
                   last_seen, notes, keywords as kw_field
            FROM interest_registry
            WHERE lifecycle_status = 'dormant'
        """).fetchall()

        candidates = []
        for d in dormant:
            # Check keyword overlap
            dormant_text = f"{d[1] or ''} {d[3] or ''} {d[6] or ''} {d[7] or ''}".lower()
            overlap = sum(1 for kw in keywords if kw in dormant_text)
            if overlap >= 2:  # At least 2 keyword matches
                candidates.append({
                    'id': d[0],
                    'canonical_name': d[1],
                    'category': d[2],
                    'sub_topic': d[3],
                    'total_days_active': d[4],
                    'last_seen': d[5],
                    'notes': d[6],
                    'overlap_score': overlap,
                })

        # Sort by overlap score, return top N
        candidates.sort(key=lambda x: -x['overlap_score'])
        return candidates[:max_candidates]

    def _format_call2b_prompt(self, interest: dict, dormant_candidates: list) -> str:
        """Format Call 2b prompt: check if unmatched interest matches a dormant one."""
        candidates_lines = []
        for c in dormant_candidates:
            candidates_lines.append(
                f"[ID:{c['id']}] \"{c['canonical_name']}\"\n"
                f"  Category: {c['category'] or '?'} | Active {c['total_days_active']} days | "
                f"Last seen: {c['last_seen']}\n"
                f"  Notes: {c['notes'] or 'none'}"
            )

        candidates_text = '\n\n'.join(candidates_lines)

        prompt = (
            "A new interest was found today that didn't match any ACTIVE registry entries.\n"
            "Check if it matches any of these DORMANT (older) interests.\n"
            "\n"
            f"TODAY'S INTEREST:\n"
            f"\"{interest['interest']}\" ({interest['strength']})\n"
            f"Evidence: {json.dumps(interest.get('top_evidence', []), ensure_ascii=False)}\n"
            "\n"
            "DORMANT CANDIDATES:\n\n"
            f"{candidates_text}\n"
            "\n"
            "DECIDE:\n"
            "- RESUMPTION [ID] -- user is resuming this old interest. Reactivate it, update name if evolved.\n"
            "- RELATED [ID] -- connected but distinct. Create new entry, link as related.\n"
            "- UNRELATED -- no match. Create as brand new interest.\n"
            "\n"
            "Return ONLY JSON:\n"
            "{\n"
            '  "decision": "RESUMPTION|RELATED|UNRELATED",\n'
            '  "dormant_id": <ID or null>,\n'
            '  "updated_name": "<new name if evolved, else null>",\n'
            '  "reason": "<brief explanation>"\n'
            "}\n"
        )
        return prompt

    # ── Registry Updates ──────────────────────────────────────

    def _extract_keywords(self, interest: dict) -> str:
        """Extract searchable keywords from interest name + evidence."""
        text = interest.get('interest', '') + ' ' + ' '.join(interest.get('top_evidence', []))
        words = re.findall(r'[a-zA-Z\u0D00-\u0D7F]{3,}', text.lower())
        unique = sorted(set(words))
        return ' '.join(unique)

    def _update_lifecycle(self, conn, interest_id: int) -> None:
        """Update lifecycle_status based on rules."""
        row = conn.execute(
            "SELECT total_days_active, first_seen, last_seen, lifecycle_status "
            "FROM interest_registry WHERE id = ?",
            (interest_id,)
        ).fetchone()

        if not row:
            return

        days_active = row[0]
        last_seen = row[2]
        old_status = row[3]

        # Days since last seen
        try:
            last_dt = datetime.strptime(last_seen, '%Y-%m-%d')
            days_since = (datetime.now() - last_dt).days
        except (ValueError, TypeError):
            days_since = 0

        # Determine new status
        if days_since > 30:
            new_status = 'dormant'
        elif days_since > 14:
            if old_status in ('active', 'core'):
                new_status = 'fading'
            else:
                new_status = 'dormant'
        elif days_active >= 10:
            new_status = 'core'
        elif days_active >= 4:
            new_status = 'active'
        else:
            new_status = 'emerging'

        if new_status != old_status:
            conn.execute(
                "UPDATE interest_registry SET lifecycle_status = ?, updated_at = datetime('now') "
                "WHERE id = ?",
                (new_status, interest_id)
            )

    def _create_interest(self, conn, interest: dict, target_date: str,
                         parent_id: int | None = None) -> int:
        """Create a new interest in the registry. Returns the new ID."""
        keywords = self._extract_keywords(interest)
        strength = interest.get('strength', 'weak')

        conn.execute("""
            INSERT INTO interest_registry
            (canonical_name, category, sub_topic, first_seen, last_seen,
             total_days_active, lifecycle_status, max_strength, current_strength,
             notes, keywords, parent_id)
            VALUES (?, ?, ?, ?, ?, 1, 'emerging', ?, ?, ?, ?, ?)
        """, (
            interest['interest'],
            interest.get('category', ''),
            interest.get('sub_topic', ''),
            target_date, target_date,
            strength, strength,
            interest.get('notes', ''),
            keywords,
            parent_id,
        ))
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # If branched, update parent's related_ids
        if parent_id:
            parent_related = conn.execute(
                "SELECT related_ids FROM interest_registry WHERE id = ?",
                (parent_id,)
            ).fetchone()
            related = json.loads(parent_related[0]) if parent_related and parent_related[0] else []
            related.append(new_id)
            conn.execute(
                "UPDATE interest_registry SET related_ids = ?, updated_at = datetime('now') WHERE id = ?",
                (json.dumps(related), parent_id)
            )

        return new_id

    def _update_existing_interest(self, conn, interest_id: int, interest: dict,
                                  target_date: str, updated_name: str | None = None) -> None:
        """Update an existing registry entry with today's data."""
        strength = interest.get('strength', 'weak')

        row = conn.execute(
            "SELECT last_seen, total_days_active, max_strength FROM interest_registry WHERE id = ?",
            (interest_id,)
        ).fetchone()

        if not row:
            return

        # Increment days active only if this is a new day
        new_days = row[1]
        if row[0] != target_date:
            new_days += 1

        # Update max strength
        strength_order = {'weak': 0, 'moderate': 1, 'strong': 2}
        max_str = row[2] or 'weak'
        if strength_order.get(strength, 0) > strength_order.get(max_str, 0):
            max_str = strength

        # Update keywords with new evidence
        new_keywords = self._extract_keywords(interest)
        old_kw_row = conn.execute(
            "SELECT keywords FROM interest_registry WHERE id = ?", (interest_id,)
        ).fetchone()
        old_kw = old_kw_row[0] if old_kw_row and old_kw_row[0] else ''
        merged_kw = ' '.join(sorted(set((old_kw + ' ' + new_keywords).split())))

        updates = {
            'last_seen': target_date,
            'total_days_active': new_days,
            'current_strength': strength,
            'max_strength': max_str,
            'keywords': merged_kw,
            'updated_at': "datetime('now')",
        }

        if updated_name:
            updates['canonical_name'] = updated_name

        # Append to notes
        if interest.get('notes'):
            old_notes = conn.execute(
                "SELECT notes FROM interest_registry WHERE id = ?", (interest_id,)
            ).fetchone()
            old = old_notes[0] if old_notes and old_notes[0] else ''
            updates['notes'] = f"{old} | {target_date}: {interest['notes']}" if old else interest['notes']

        set_clause = ', '.join(
            f"{k} = ?" if k != 'updated_at' else f"{k} = datetime('now')"
            for k in updates
        )
        values = [v for k, v in updates.items() if k != 'updated_at']
        values.append(interest_id)

        conn.execute(
            f"UPDATE interest_registry SET {set_clause} WHERE id = ?",
            values
        )

    def _log_daily_interest(self, conn, target_date: str, interest_id: int,
                            interest: dict) -> None:
        """Write daily interest log entry."""
        conn.execute("""
            INSERT INTO daily_interest_log (date, interest_id, strength, evidence_count, top_evidence, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            target_date,
            interest_id,
            interest.get('strength', 'weak'),
            interest.get('evidence_count', 0),
            json.dumps(interest.get('top_evidence', []), ensure_ascii=False),
            interest.get('notes', ''),
        ))

    # ── Daily Pipeline ────────────────────────────────────────

    def _process_one_day(self, conn, ga_conn, target_date: str) -> dict:
        """Process one day: Call 1 -> Call 2a -> Call 2b -> update registry.

        Returns dict with stats: {interests_found, interests_matched, interests_new, error}
        """
        stats = {'interests_found': 0, 'interests_matched': 0, 'interests_new': 0, 'error': None}

        # ── Call 1: Extract interests ──
        prompt = self._format_day_prompt(ga_conn, target_date)
        if not prompt or len(prompt) < 100:
            logger.info(f"Day {target_date}: no data to process")
            stats['error'] = 'no_data'
            return stats

        logger.info(f"Day {target_date}: sending Call 1 ({len(prompt)} chars)")

        raw_response = None
        parsed_interests = None

        for attempt in range(MAX_RETRIES):
            raw_response = self._chatgpt_request(prompt, timeout=CALL1_TIMEOUT)
            if not raw_response:
                logger.warning(f"Day {target_date}: Call 1 attempt {attempt+1} got empty response")
            else:
                parsed_interests = self._parse_chatgpt_json(raw_response)
                if parsed_interests and isinstance(parsed_interests, list):
                    break
                logger.warning(f"Day {target_date}: Call 1 attempt {attempt+1} unparsable")

            # Exponential backoff
            wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
            logger.info(f"Day {target_date}: retrying Call 1 in {wait}s (attempt {attempt+2}/{MAX_RETRIES})")
            time.sleep(wait)

        if not parsed_interests or not isinstance(parsed_interests, list):
            logger.error(f"Day {target_date}: Call 1 failed after {MAX_RETRIES} attempts")
            conn.execute(
                "INSERT OR REPLACE INTO processing_log (date, status, call1_response, processed_at) "
                "VALUES (?, 'blocked', ?, datetime('now'))",
                (target_date, raw_response or '')
            )
            conn.commit()
            stats['error'] = 'call1_blocked'
            return stats

        stats['interests_found'] = len(parsed_interests)
        logger.info(f"Day {target_date}: Call 1 found {len(parsed_interests)} interests")

        # ── Call 2a: Match against active registry ──
        call2a_prompt = self._format_call2a_prompt(conn, parsed_interests)

        matched_decisions = {}  # today_index -> {decision, registry_id, updated_name}

        if call2a_prompt:
            logger.info(f"Day {target_date}: sending Call 2a ({len(call2a_prompt)} chars)")

            parsed_2a = None
            for attempt in range(MAX_RETRIES):
                raw_2a = self._chatgpt_request(call2a_prompt, timeout=CALL2_TIMEOUT)
                if raw_2a:
                    parsed_2a = self._parse_chatgpt_json(raw_2a)
                    if parsed_2a and isinstance(parsed_2a, list):
                        break
                    logger.warning(f"Day {target_date}: Call 2a attempt {attempt+1} unparsable")
                else:
                    logger.warning(f"Day {target_date}: Call 2a attempt {attempt+1} empty")

                wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
                logger.info(f"Day {target_date}: retrying Call 2a in {wait}s")
                time.sleep(wait)

            if not parsed_2a or not isinstance(parsed_2a, list):
                logger.error(f"Day {target_date}: Call 2a failed after {MAX_RETRIES} attempts")
                conn.execute(
                    "INSERT OR REPLACE INTO processing_log (date, status, call1_response, processed_at) "
                    "VALUES (?, 'blocked', ?, datetime('now'))",
                    (target_date, raw_response or '')
                )
                conn.commit()
                stats['error'] = 'call2a_blocked'
                return stats

            for decision in parsed_2a:
                idx = decision.get('today_index', 0) - 1  # 1-based to 0-based
                if 0 <= idx < len(parsed_interests):
                    matched_decisions[idx] = decision
        else:
            logger.info(f"Day {target_date}: no active registry, all interests are NEW")

        # ── Process each interest ──
        call2_responses = []
        for i, interest in enumerate(parsed_interests):
            decision = matched_decisions.get(i)

            if decision and decision.get('decision') == 'MATCH' and decision.get('registry_id'):
                # Matched to existing
                rid = decision['registry_id']
                self._update_existing_interest(
                    conn, rid, interest, target_date,
                    updated_name=decision.get('updated_name')
                )
                self._log_daily_interest(conn, target_date, rid, interest)
                self._update_lifecycle(conn, rid)
                stats['interests_matched'] += 1
                logger.info(f"  MATCH [{rid}]: {interest['interest'][:50]}")

            elif decision and decision.get('decision') == 'BRANCH' and decision.get('registry_id'):
                # Branch from existing
                parent_id = decision['registry_id']
                new_id = self._create_interest(conn, interest, target_date, parent_id=parent_id)
                self._log_daily_interest(conn, target_date, new_id, interest)
                stats['interests_new'] += 1
                logger.info(f"  BRANCH [{new_id}] from [{parent_id}]: {interest['interest'][:50]}")

            else:
                # Unmatched — try dormant search (Call 2b)
                dormant_candidates = self._find_dormant_candidates(conn, interest)

                if dormant_candidates:
                    call2b_prompt = self._format_call2b_prompt(interest, dormant_candidates)
                    raw_2b = self._chatgpt_request(call2b_prompt, timeout=CALL2_TIMEOUT)
                    parsed_2b = self._parse_chatgpt_json(raw_2b) if raw_2b else None
                    call2_responses.append(raw_2b)

                    if parsed_2b and isinstance(parsed_2b, dict):
                        d2b = parsed_2b.get('decision', 'UNRELATED')
                        d2b_id = parsed_2b.get('dormant_id')

                        if d2b == 'RESUMPTION' and d2b_id:
                            self._update_existing_interest(
                                conn, d2b_id, interest, target_date,
                                updated_name=parsed_2b.get('updated_name')
                            )
                            self._log_daily_interest(conn, target_date, d2b_id, interest)
                            self._update_lifecycle(conn, d2b_id)
                            stats['interests_matched'] += 1
                            logger.info(f"  RESUMPTION [{d2b_id}]: {interest['interest'][:50]}")
                            continue

                        elif d2b == 'RELATED' and d2b_id:
                            new_id = self._create_interest(conn, interest, target_date)
                            # Link as related
                            old_related = conn.execute(
                                "SELECT related_ids FROM interest_registry WHERE id = ?",
                                (d2b_id,)
                            ).fetchone()
                            related = json.loads(old_related[0]) if old_related and old_related[0] else []
                            related.append(new_id)
                            conn.execute(
                                "UPDATE interest_registry SET related_ids = ? WHERE id = ?",
                                (json.dumps(related), d2b_id)
                            )
                            self._log_daily_interest(conn, target_date, new_id, interest)
                            stats['interests_new'] += 1
                            logger.info(f"  RELATED [{new_id}] to dormant [{d2b_id}]: {interest['interest'][:50]}")
                            continue

                # Truly new
                new_id = self._create_interest(conn, interest, target_date)
                self._log_daily_interest(conn, target_date, new_id, interest)
                stats['interests_new'] += 1
                logger.info(f"  NEW [{new_id}]: {interest['interest'][:50]}")

        conn.commit()

        # ── Call 3: Daily Intelligence ──
        call3_result = self._run_daily_intelligence(conn, target_date, parsed_interests)
        if call3_result:
            display = call3_result.get('display', {})
            logger.info(f"Day {target_date}: Display [{display.get('type')}] "
                         f"{display.get('headline', '')[:60]}")
        else:
            logger.warning(f"Day {target_date}: Call 3 failed — no display generated")

        # Log processing
        conn.execute(
            "INSERT OR REPLACE INTO processing_log "
            "(date, status, call1_response, call2_response, interests_found, interests_matched, interests_new, processed_at) "
            "VALUES (?, 'complete', ?, ?, ?, ?, ?, datetime('now'))",
            (
                target_date,
                raw_response or '',
                json.dumps(call2_responses, ensure_ascii=False) if call2_responses else '',
                stats['interests_found'],
                stats['interests_matched'],
                stats['interests_new'],
            )
        )
        conn.commit()

        return stats

    # ── Main Collection (Backfill + Daily) ────────────────────

    def collect(self, tab: CDPTab, tracker: SyncTracker, limits: RunLimits) -> CollectResult:
        """Run the interest extraction pipeline.

        Finds unprocessed days in google_activity.db and processes them oldest-first.
        """
        conn = tracker.conn
        ga_db_path = self._get_google_activity_db_path()

        if not os.path.exists(ga_db_path):
            logger.error(f"Google Activity DB not found: {ga_db_path}")
            return CollectResult(error="google_activity.db not found")

        ga_conn = _sqlite3.connect(ga_db_path)
        ga_conn.row_factory = _sqlite3.Row

        # Find days that have activity data but no processing
        unprocessed = ga_conn.execute("""
            SELECT cd.date
            FROM collection_days cd
            WHERE cd.status = 'complete'
            AND cd.date NOT IN (
                SELECT date FROM processing_log WHERE status = 'complete'
            )
            ORDER BY cd.date ASC
        """).fetchall()

        # The processing_log is in OUR db, not google_activity's
        # So we need to check our own DB
        our_processed = set()
        our_blocked = set()
        try:
            for row in conn.execute("SELECT date, status FROM processing_log"):
                if row[1] == 'complete':
                    our_processed.add(row[0])
                elif row[1] == 'blocked':
                    our_blocked.add(row[0])
        except Exception:
            pass

        # Get all complete collection days from google_activity
        all_days = [r[0] for r in ga_conn.execute(
            "SELECT date FROM collection_days WHERE status = 'complete' ORDER BY date ASC"
        ).fetchall()]

        # Include blocked days for retry + unprocessed days
        days_to_process = [d for d in all_days if d not in our_processed]

        if not days_to_process:
            logger.info("All days already processed")
            ga_conn.close()
            return CollectResult(items_found=0, items_new=0)

        logger.info(f"Days to process: {len(days_to_process)} "
                     f"(oldest: {days_to_process[0]}, newest: {days_to_process[-1]})")
        if our_blocked:
            logger.info(f"Previously blocked days (will retry): {sorted(our_blocked)}")

        total_found = 0
        total_new = 0
        total_matched = 0

        for i, target_date in enumerate(days_to_process):
            is_retry = target_date in our_blocked
            logger.info(f"=== Day {i+1}/{len(days_to_process)}: {target_date}"
                         f"{' (RETRY)' if is_retry else ''} ===")

            try:
                stats = self._process_one_day(conn, ga_conn, target_date)

                if stats['error'] and 'blocked' in stats['error']:
                    # Day is blocked — STOP processing further days
                    logger.error(
                        f"Day {target_date}: BLOCKED — stopping pipeline. "
                        f"Later days need this day's registry. Will retry next run."
                    )
                    break

                total_found += stats['interests_found']
                total_new += stats['interests_new']
                total_matched += stats['interests_matched']

                logger.info(
                    f"Day {target_date}: {stats['interests_found']} found, "
                    f"{stats['interests_matched']} matched, {stats['interests_new']} new"
                )
            except Exception as e:
                logger.error(f"Day {target_date}: exception - {e}")
                conn.execute(
                    "INSERT OR REPLACE INTO processing_log (date, status, processed_at) "
                    "VALUES (?, 'blocked', datetime('now'))",
                    (target_date,)
                )
                conn.commit()
                # STOP — don't skip days
                logger.error(f"Day {target_date}: BLOCKED by exception — stopping pipeline")
                break

            # Cooldown between days
            if i < len(days_to_process) - 1:
                import random
                cooldown = random.uniform(5, 15)
                logger.info(f"Cooldown: {cooldown:.0f}s")
                time.sleep(cooldown)

        # Update all lifecycle statuses
        all_interests = conn.execute("SELECT id FROM interest_registry").fetchall()
        for row in all_interests:
            self._update_lifecycle(conn, row[0])
        conn.commit()

        ga_conn.close()

        return CollectResult(
            items_found=total_found,
            items_new=total_new,
            items_updated=total_matched,
        )

    # ── Call 3: Daily Intelligence (split into 3a/3b/3c) ────────

    def _build_registry_section(self, conn) -> str:
        """Build the full registry text used by all Call 3 sub-calls."""
        interests = conn.execute("""
            SELECT id, canonical_name, category, sub_topic, lifecycle_status,
                   current_strength, total_days_active, first_seen, last_seen, notes
            FROM interest_registry ORDER BY total_days_active DESC
        """).fetchall()

        lines = []
        for i in interests:
            evidence_rows = conn.execute("""
                SELECT date, strength, top_evidence
                FROM daily_interest_log WHERE interest_id = ?
                ORDER BY date
            """, (i[0],)).fetchall()

            all_evidence = []
            daily_entries = []
            for e in evidence_rows:
                if e[2]:
                    try:
                        all_evidence.extend(json.loads(e[2]))
                    except (json.JSONDecodeError, TypeError):
                        pass
                daily_entries.append(f"{e[0]}:{e[1]}")

            evidence_str = ', '.join(f'"{t}"' for t in all_evidence[:15])
            daily_str = ', '.join(daily_entries)

            lines.append(
                f'[{i[0]}] "{i[1]}"\n'
                f'  Category: {i[2]} | Status: {i[4]} | Strength: {i[5]} | '
                f'Days: {i[6]} | {i[7]} -> {i[8]}\n'
                f'  Daily: {daily_str}\n'
                f'  Evidence: {evidence_str}\n'
                f'  Notes: {(i[9] or "none")[:150]}'
            )

        return '\n\n'.join(lines)

    def _build_today_section(self, today_interests: list[dict]) -> str:
        """Format today's interests for prompts."""
        lines = []
        for ti in today_interests:
            lines.append(
                f'"{ti["interest"]}" ({ti.get("strength", "?")}) '
                f'evidence: {json.dumps(ti.get("top_evidence", []), ensure_ascii=False)}'
            )
        return '\n'.join(lines)

    # ── Call 3a: Interest Status ──

    def _run_call3a_interest_status(self, conn, target_date: str,
                                     registry_section: str,
                                     today_section: str) -> dict | None:
        """Call 3a: classify today's interest status (appeared/strengthened/steady/etc)."""
        prompt = (
            f"You are analyzing which interests are active today ({target_date}) "
            "and what changed.\n\n"
            "=== FULL INTEREST REGISTRY ===\n\n"
            f"{registry_section}\n\n"
            "=== TODAY'S INTERESTS (just processed) ===\n\n"
            f"{today_section}\n\n"
            "=== TASK ===\n\n"
            "For each interest that was active TODAY, classify its status:\n"
            "- 'appeared': brand new, first time ever\n"
            "- 'strengthened': existed before, got stronger today\n"
            "- 'steady': same as recent days\n"
            "- 'weakened': was stronger before, weaker today\n"
            "- 'returned': was dormant/fading, came back today\n\n"
            "Also list interests that were recently active (last 7 days) but ABSENT today.\n\n"
            "Return ONLY JSON:\n"
            "{\n"
            '  "today_interests": [\n'
            '    {\n'
            '      "interest_id": <registry ID>,\n'
            '      "name": "<interest name>",\n'
            '      "today_status": "appeared|strengthened|steady|weakened|returned",\n'
            '      "today_strength": "strong|moderate|weak",\n'
            '      "streak_days": <consecutive days active>,\n'
            '      "total_days": <total days ever active>,\n'
            '      "brief": "<one-line what happened today>"\n'
            '    }\n'
            '  ],\n'
            '  "absent_interests": [\n'
            '    {\n'
            '      "interest_id": <registry ID>,\n'
            '      "name": "<interest name>",\n'
            '      "last_seen": "<date>",\n'
            '      "days_absent": <number>,\n'
            '      "was_strength": "<strength when last active>"\n'
            '    }\n'
            '  ]\n'
            "}\n"
        )

        for attempt in range(MAX_RETRIES):
            raw = self._chatgpt_request(prompt, timeout=CALL2_TIMEOUT)
            if raw:
                parsed = self._parse_chatgpt_json(raw)
                if parsed and isinstance(parsed, dict) and 'today_interests' in parsed:
                    logger.info(f"Day {target_date}: Call 3a succeeded — "
                                f"{len(parsed.get('today_interests', []))} active, "
                                f"{len(parsed.get('absent_interests', []))} absent")
                    return parsed
                logger.warning(f"Day {target_date}: Call 3a attempt {attempt+1} unparsable")
            else:
                logger.warning(f"Day {target_date}: Call 3a attempt {attempt+1} empty")
            wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
            time.sleep(wait)

        logger.error(f"Day {target_date}: Call 3a failed")
        return None

    # ── Call 3b: Insight ──

    def _run_call3b_insight(self, target_date: str,
                             registry_section: str,
                             today_section: str,
                             display_section: str,
                             ctx_section: str) -> dict | None:
        """Call 3b: generate the most interesting insight for today."""
        prompt = (
            f"You are the insight engine for a personal interest tracking system.\n"
            f"Today is {target_date}.\n\n"
            "=== FULL INTEREST REGISTRY ===\n\n"
            f"{registry_section}\n\n"
            "=== TODAY'S INTERESTS ===\n\n"
            f"{today_section}\n\n"
            "=== RECENT DISPLAYS (last 7 days — do NOT repeat same type/angle) ===\n\n"
            f"{display_section}\n\n"
            "=== USER CONTEXT ===\n\n"
            f"{ctx_section}\n\n"
            "=== TASK ===\n\n"
            "Pick the single most INTERESTING, PERSONAL insight for today.\n"
            "Types: new_interest / pattern / milestone / evolution / cultural / "
            "cluster / velocity / contrast\n\n"
            "Make it specific. Not 'You watched 12 videos today.'\n"
            "Instead: 'Your Iran tracking shifted from Malayalam debate channels to English "
            "military analysis — you're going deeper into the tactical angle.'\n\n"
            "Return ONLY JSON:\n"
            "{\n"
            '  "insight": {\n'
            '    "type": "<type>",\n'
            '    "headline": "<one compelling sentence>",\n'
            '    "detail": "<2-3 sentences>",\n'
            '    "related_interest_ids": [<ids>]\n'
            '  }\n'
            "}\n"
        )

        for attempt in range(MAX_RETRIES):
            raw = self._chatgpt_request(prompt, timeout=CALL2_TIMEOUT)
            if raw:
                parsed = self._parse_chatgpt_json(raw)
                if parsed and isinstance(parsed, dict) and 'insight' in parsed:
                    logger.info(f"Day {target_date}: Call 3b succeeded — {parsed['insight'].get('type', '?')}")
                    return parsed
                logger.warning(f"Day {target_date}: Call 3b attempt {attempt+1} unparsable")
            else:
                logger.warning(f"Day {target_date}: Call 3b attempt {attempt+1} empty")
            wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
            time.sleep(wait)

        logger.error(f"Day {target_date}: Call 3b failed")
        return None

    # ── Call 3c: Questions & Inferences ──

    def _run_call3c_questions(self, target_date: str,
                               registry_section: str,
                               ctx_section: str,
                               pending_section: str) -> dict | None:
        """Call 3c: generate question + infer user context."""
        prompt = (
            f"You are analyzing a user's interest registry to generate a clarification "
            f"question and infer context about who this person is.\n"
            f"Today is {target_date}.\n\n"
            "=== FULL INTEREST REGISTRY ===\n\n"
            f"{registry_section}\n\n"
            "=== KNOWN USER CONTEXT ===\n\n"
            f"{ctx_section}\n\n"
            "=== PENDING UNANSWERED QUESTIONS ===\n\n"
            f"{pending_section}\n\n"
            "=== TASKS ===\n\n"
            "1. QUESTION (optional): Generate ONE question if you see:\n"
            "   - An ambiguous interest (kids content or nostalgia?)\n"
            "   - An emerging cluster needing context (Africa = business or travel?)\n"
            "   - A strong interest with no user context after 5+ days\n"
            "   Do NOT ask if a similar question is already pending.\n"
            "   If nothing warrants a question, set to null.\n\n"
            "2. INFERENCES: What can you infer about this person from their interest patterns?\n"
            "   Only include things with medium or high confidence.\n"
            "   Examples: location, profession, family situation, consumption habits.\n\n"
            "Return ONLY JSON:\n"
            "{\n"
            '  "question": {\n'
            '    "text": "<question>",\n'
            '    "context": "<why asking — shown to user>",\n'
            '    "related_interest_ids": [<ids>]\n'
            '  } or null,\n'
            '  "inferences": [\n'
            '    {"key": "<context key>", "value": "<value>", "confidence": "high|medium|low"}\n'
            '  ]\n'
            "}\n"
        )

        for attempt in range(MAX_RETRIES):
            raw = self._chatgpt_request(prompt, timeout=CALL2_TIMEOUT)
            if raw:
                parsed = self._parse_chatgpt_json(raw)
                if parsed and isinstance(parsed, dict):
                    logger.info(f"Day {target_date}: Call 3c succeeded")
                    return parsed
                logger.warning(f"Day {target_date}: Call 3c attempt {attempt+1} unparsable")
            else:
                logger.warning(f"Day {target_date}: Call 3c attempt {attempt+1} empty")
            wait = min(RETRY_BACKOFF_BASE * (2 ** attempt), RETRY_BACKOFF_MAX)
            time.sleep(wait)

        logger.error(f"Day {target_date}: Call 3c failed")
        return None

    # ── Call 3d: Real-World Grounding (Perplexity) ──

    PERPLEXITY_MODEL = "perplexity/sonar-pro-search"

    def _run_call3d_grounding(self, conn, target_date: str,
                               today_interests: list[dict]) -> dict | None:
        """Call 3d: ground today's interests against real-world events via web search.

        Uses Perplexity Sonar (web search built-in) to find what's actually happening
        in the world related to the user's interests TODAY.
        """
        # Only ground strong/moderate interests (not weak single searches)
        groundable = [i for i in today_interests
                      if i.get('strength') in ('strong', 'moderate')]

        if not groundable:
            logger.info(f"Day {target_date}: Call 3d skipped — no strong/moderate interests")
            return None

        interest_names = [i['interest'] for i in groundable]
        interests_text = '\n'.join(f"- {name}" for name in interest_names)

        prompt = (
            f"Today is {target_date}.\n\n"
            "A user is tracking these interests based on their YouTube/search activity:\n\n"
            f"{interests_text}\n\n"
            "For each interest, search the web and find:\n"
            "1. What is actually happening in the real world RIGHT NOW related to this topic?\n"
            "2. Any breaking news, developments, or events from today or this week?\n"
            "3. Context that would help the user understand WHY this topic is trending.\n\n"
            "Be specific — include dates, names, numbers. Not generic summaries.\n"
            "If nothing notable is happening for an interest, say so.\n\n"
            "Return JSON:\n"
            "{\n"
            '  "grounding": [\n'
            '    {\n'
            '      "interest": "<interest name>",\n'
            '      "real_world_context": "<what is happening now — specific events, dates, names>",\n'
            '      "trending_reason": "<why this topic is active right now>",\n'
            '      "sources": ["<source 1>", "<source 2>"]\n'
            '    }\n'
            '  ],\n'
            '  "cross_connections": "<any connections between the user\'s interests and current events>"\n'
            "}\n"
        )

        # Use Perplexity model for web search
        # Need to call API directly since inference service uses the default model
        api_key = os.environ.get('OPENROUTER_API_KEY', '')
        if not api_key:
            # Try to get from core DB settings
            try:
                core_db = self._get_core_db_path()
                core_conn = _sqlite3.connect(core_db)
                row = core_conn.execute(
                    "SELECT value FROM skill_settings WHERE skill_name = 'chatgpt_inference' AND key = 'openrouter_api_key'"
                ).fetchone()
                if row:
                    api_key = row[0]
                core_conn.close()
            except Exception:
                pass

        # Use _chatgpt_request with web_search=True
        # It routes to API (Perplexity) or browser (ChatGPT with web search) automatically
        logger.info(f"Day {target_date}: Call 3d — grounding {len(groundable)} interests (web_search=True)")

        for attempt in range(3):
            raw = self._chatgpt_request(prompt, timeout=CALL2_TIMEOUT, web_search=True)
            if raw:
                parsed = self._parse_chatgpt_json(raw)
                if parsed and isinstance(parsed, dict) and 'grounding' in parsed:
                    logger.info(f"Day {target_date}: Call 3d succeeded — {len(parsed.get('grounding', []))} grounded")
                    return parsed
                logger.warning(f"Day {target_date}: Call 3d attempt {attempt+1} unparsable")
            else:
                logger.warning(f"Day {target_date}: Call 3d attempt {attempt+1} empty/failed")

            time.sleep(10)

        logger.error(f"Day {target_date}: Call 3d failed after 3 attempts")
        return None

    # ── Orchestrate Call 3 ──

    def _run_daily_intelligence(self, conn, target_date: str,
                                today_interests: list[dict]) -> dict | None:
        """Run Call 3 (split into 3a/3b/3c/3d). All 4 run in PARALLEL."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        registry_section = self._build_registry_section(conn)
        today_section = self._build_today_section(today_interests)

        # Pre-fetch data that Call 3b and 3c need from DB (avoid cross-thread SQLite)
        display_rows = conn.execute("""
            SELECT date, display_type, content FROM display_log
            WHERE date > date(?, '-7 days') ORDER BY date DESC
        """, (target_date,)).fetchall()

        display_lines = []
        for d in display_rows:
            try:
                c = json.loads(d[2]) if d[2] else {}
                headline = c.get('insight', {}).get('headline', c.get('headline', ''))
            except (json.JSONDecodeError, TypeError):
                headline = ''
            display_lines.append(f"{d[0]}: [{d[1]}] {headline[:80]}")
        display_section = '\n'.join(display_lines) if display_lines else 'No previous displays.'

        ctx_rows = conn.execute("SELECT key, value FROM user_context").fetchall()
        ctx_section = '\n'.join(f"{c[0]}: {c[1]}" for c in ctx_rows) if ctx_rows else 'No user context yet.'

        pending = conn.execute("SELECT question, asked_on FROM questions WHERE status = 'pending'").fetchall()
        pending_section = '\n'.join(f'"{p[0]}" (asked: {p[1]})' for p in pending) if pending else 'None.'

        combined = {}

        # All 4 calls are independent — run in parallel
        logger.info(f"Day {target_date}: launching Call 3a/3b/3c/3d in parallel")

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    self._run_call3a_interest_status,
                    None, target_date, registry_section, today_section
                ): '3a',
                executor.submit(
                    self._run_call3b_insight,
                    target_date, registry_section, today_section,
                    display_section, ctx_section
                ): '3b',
                executor.submit(
                    self._run_call3c_questions,
                    target_date, registry_section,
                    ctx_section, pending_section
                ): '3c',
                executor.submit(
                    self._run_call3d_grounding,
                    None, target_date, today_interests
                ): '3d',
            }

            for future in as_completed(futures):
                call_name = futures[future]
                try:
                    result = future.result()
                    if call_name == '3a' and result:
                        combined['today_interests'] = result.get('today_interests', [])
                        combined['absent_interests'] = result.get('absent_interests', [])
                    elif call_name == '3b' and result:
                        combined['insight'] = result.get('insight', {})
                    elif call_name == '3c' and result:
                        combined['question'] = result.get('question')
                        combined['inferences'] = result.get('inferences', [])
                    elif call_name == '3d' and result:
                        combined['grounding'] = result.get('grounding', [])
                        combined['cross_connections'] = result.get('cross_connections', '')

                    logger.info(f"Day {target_date}: Call {call_name} completed")
                except Exception as e:
                    logger.error(f"Day {target_date}: Call {call_name} failed: {e}")

        if not combined:
            return None

        # Store display log
        insight = combined.get('insight', {})
        conn.execute("""
            INSERT INTO display_log (date, display_type, content)
            VALUES (?, ?, ?)
        """, (
            target_date,
            insight.get('type', 'unknown'),
            json.dumps(combined, ensure_ascii=False),
        ))

        # Store question
        question = combined.get('question')
        if question and isinstance(question, dict) and question.get('text'):
            conn.execute("""
                INSERT INTO questions (question, context, status, related_interest_ids, asked_on)
                VALUES (?, ?, 'pending', ?, ?)
            """, (
                question['text'],
                question.get('context', ''),
                json.dumps(question.get('related_interest_ids', []), ensure_ascii=False),
                target_date,
            ))

        # Store inferences
        for inf in combined.get('inferences', []):
            if inf.get('key') and inf.get('confidence') in ('high', 'medium'):
                conn.execute("""
                    INSERT OR REPLACE INTO user_context (key, value, source, answered_at)
                    VALUES (?, ?, 'inferred', datetime('now'))
                """, (inf['key'], inf.get('value', '')))

        conn.commit()
        return combined

    # ── Dashboard ─────────────────────────────────────────────

    def get_widgets(self) -> list[WidgetDefinition]:
        return [
            WidgetDefinition(
                name="interest_overview",
                title="Interest Timeline",
                display_type="stat_cards",
                refresh_seconds=3600,
                size="wide",
            )
        ]

    def get_page_sections(self) -> list[PageSection]:
        return [
            PageSection(
                name="interests_active",
                title="Active Interests",
                display_type="table",
            )
        ]

    def get_notification_rules(self) -> list[NotificationRule]:
        return []

    def get_stats(self, conn) -> list[dict]:
        """Return stat cards for dashboard."""
        total = conn.execute("SELECT COUNT(*) FROM interest_registry").fetchone()[0]
        active = conn.execute(
            "SELECT COUNT(*) FROM interest_registry WHERE lifecycle_status IN ('emerging','active','core')"
        ).fetchone()[0]
        dormant = conn.execute(
            "SELECT COUNT(*) FROM interest_registry WHERE lifecycle_status = 'dormant'"
        ).fetchone()[0]
        families = conn.execute("SELECT COUNT(*) FROM interest_families").fetchone()[0]

        return [
            {"label": "Total Interests", "value": total},
            {"label": "Active", "value": active},
            {"label": "Dormant", "value": dormant},
            {"label": "Families", "value": families},
        ]

    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        """Search interests by name."""
        rows = conn.execute(
            "SELECT canonical_name, category, lifecycle_status, last_seen "
            "FROM interest_registry WHERE canonical_name LIKE ? "
            "ORDER BY last_seen DESC LIMIT ?",
            (f"%{query}%", limit)
        ).fetchall()
        return [
            {
                "title": r[0],
                "subtitle": f"{r[2]} | {r[1] or 'uncategorized'}",
                "timestamp": r[3],
            }
            for r in rows
        ]
