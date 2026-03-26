"""
Google MyActivity Skill — collects ALL activity from myactivity.google.com.

Covers: YouTube watched, Chrome visited, Google searches, app usage, viewed content,
notifications, Discover feed, and unknown/future types.

CHANGELOG:
  v0.3.0 (2026-03-21): All 12 gap fixes — fresh tab, dedup, batches, timezone,
                        retry limits, graceful failure, progress logging, backfill settings
  v0.2.0 (2026-03-21): Full collector — date filter, scroll, extract, day-by-day backfill
  v0.1.0 (2026-03-21): DB schema only

Architecture:
  - Single `activities` table for ALL types (Watched/Visited/Searched/Used/Viewed/Unknown)
  - raw_data JSON blob captures everything from DOM (future-proof)
  - Structured columns for known fields (fast queries)
  - collection_days table for day-level tracking and gap detection
  - Repeat viewings stored individually (same video at different times = different rows)

Collection:
  - Each day = fresh tab → filter → scroll → extract → save → close tab
  - Day-by-day backfill, newest first, random cooldown (5s to 4 min)
  - No time limit (max_minutes_per_run=720)
  - Failed days retry up to 3 times, then abandoned
  - Graceful failure: connection errors → long wait → next day

__version__ = "0.4.1"
"""
__version__ = "0.4.1"

import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs, unquote

logger = logging.getLogger("memory_tap.skill.google_activity")

from src.skills.base import (
    BaseSkill, SkillManifest, CollectResult, StopStrategy, RunLimits, SkillSetting,
)
from src.db.sync_tracker import SyncTracker
from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule
from src.cdp_client import CDPTab, CDPClient
from src.human import scroll_slowly, wait_human, move_mouse

MAX_RETRY_PER_DAY = 3


class GoogleActivitySkill(BaseSkill):
    """Collects ALL activity from Google MyActivity — YouTube, Chrome, Search, Apps, etc."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="google_activity",
            version=__version__,
            target_url="https://myactivity.google.com",
            description="Collects ALL Google activity — YouTube, Chrome, Search, Apps, notifications",
            auth_provider="google",
            schedule_hours=1,
            login_url="https://accounts.google.com/ServiceLogin",
            max_items_first_run=0,
            max_items_per_run=0,
            max_minutes_per_run=720,  # 12 hours — no practical limit
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.TIME_LIMIT

    def _main_table_name(self) -> str:
        return "activities"

    # ── Schema ────────────────────────────────────────────────────

    def create_schema(self, conn) -> None:
        """Create tables for Google MyActivity data."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS activities (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_type      TEXT NOT NULL,
                source_app      TEXT,
                timestamp       TEXT NOT NULL,
                date            TEXT NOT NULL,
                raw_data        TEXT NOT NULL,

                -- Watched (YouTube)
                video_id        TEXT,
                video_title     TEXT,
                channel         TEXT,
                channel_url     TEXT,
                duration        TEXT,
                duration_secs   INTEGER,
                watch_pct       INTEGER,
                thumbnail_url   TEXT,

                -- Visited (Chrome)
                page_title      TEXT,
                url             TEXT,
                domain          TEXT,

                -- Searched (Google)
                query_text      TEXT,
                search_url      TEXT,
                search_type     TEXT,

                -- Used (Apps)
                app_name        TEXT,
                play_store_url  TEXT,

                -- Viewed
                content_text    TEXT,
                content_url     TEXT,
                source_type     TEXT,

                -- Maps / Location
                place_name      TEXT,
                place_address   TEXT,
                latitude        REAL,
                longitude       REAL,
                maps_url        TEXT,

                -- Unknown / Notifications
                notification_topics TEXT,

                -- Garbage detection
                is_garbage      INTEGER DEFAULT 0,
                garbage_reason  TEXT,

                -- Entry tagging & classification
                tag             TEXT,
                channel_summary TEXT,
                multi_session   INTEGER DEFAULT 0,
                actual_minutes_watched REAL,
                session_id      INTEGER,
                time_context    TEXT,

                -- Metadata
                synced_at       TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
            CREATE INDEX IF NOT EXISTS idx_activities_type ON activities(entry_type);
            CREATE INDEX IF NOT EXISTS idx_activities_type_date ON activities(entry_type, date);
            CREATE INDEX IF NOT EXISTS idx_activities_timestamp ON activities(timestamp);
            CREATE INDEX IF NOT EXISTS idx_activities_video_id ON activities(video_id);
            CREATE INDEX IF NOT EXISTS idx_activities_channel ON activities(channel);
            CREATE INDEX IF NOT EXISTS idx_activities_domain ON activities(domain);
            CREATE INDEX IF NOT EXISTS idx_activities_query ON activities(query_text);

            CREATE TABLE IF NOT EXISTS collection_days (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                date            TEXT NOT NULL UNIQUE,
                watched_count   INTEGER DEFAULT 0,
                visited_count   INTEGER DEFAULT 0,
                searched_count  INTEGER DEFAULT 0,
                used_count      INTEGER DEFAULT 0,
                viewed_count    INTEGER DEFAULT 0,
                unknown_count   INTEGER DEFAULT 0,
                total_count     INTEGER DEFAULT 0,
                status          TEXT DEFAULT 'complete',
                retry_count     INTEGER DEFAULT 0,
                collected_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_collection_days_date ON collection_days(date);
            CREATE INDEX IF NOT EXISTS idx_collection_days_status ON collection_days(status);

            CREATE TABLE IF NOT EXISTS channels (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_url     TEXT UNIQUE NOT NULL,
                channel_name    TEXT,
                subscriber_count TEXT,
                video_count     TEXT,
                is_subscribed   INTEGER,
                about_text      TEXT,
                sections_data   TEXT,
                analysis_full   TEXT,
                analysis_summary TEXT,
                verdict         TEXT,
                screenshot_path TEXT,
                evaluated_at    TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_channels_verdict ON channels(verdict);

            CREATE TABLE IF NOT EXISTS garbage_channels (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_url     TEXT UNIQUE NOT NULL,
                channel_name    TEXT,
                reason          TEXT,
                blocked_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS potential_garbage (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_url     TEXT UNIQUE NOT NULL,
                channel_name    TEXT,
                trigger_count   INTEGER DEFAULT 0,
                flagged_video_ids TEXT,
                first_seen      TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen       TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_potential_garbage_channel ON potential_garbage(channel_url);

            CREATE TABLE IF NOT EXISTS settings (
                key             TEXT PRIMARY KEY,
                value           TEXT NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );

            INSERT OR IGNORE INTO settings (key, value) VALUES
                ('backfill_depth_days', '180'),
                ('backfill_started', 'false'),
                ('backfill_completed', 'false'),
                ('oldest_collected_date', ''),
                ('newest_collected_date', ''),
                ('timezone', '');
        """)
        conn.commit()

    def migrate_schema(self, conn, old_version: str, new_version: str) -> None:
        """Handle schema changes between versions."""
        # v0.3.0: add retry_count, Maps columns
        for col, table, typedef in [
            ("retry_count", "collection_days", "INTEGER DEFAULT 0"),
            ("place_name", "activities", "TEXT"),
            ("place_address", "activities", "TEXT"),
            ("latitude", "activities", "REAL"),
            ("longitude", "activities", "REAL"),
            ("maps_url", "activities", "TEXT"),
            # v0.4.0: garbage detection columns
            ("is_garbage", "activities", "INTEGER DEFAULT 0"),
            ("garbage_reason", "activities", "TEXT"),
            # v0.4.1: entry tagging columns
            ("tag", "activities", "TEXT"),
            ("channel_summary", "activities", "TEXT"),
            ("multi_session", "activities", "INTEGER DEFAULT 0"),
            ("actual_minutes_watched", "activities", "REAL"),
            ("session_id", "activities", "INTEGER"),
            ("time_context", "activities", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # column already exists

        # v0.4.0: potential_garbage table
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS potential_garbage (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_url     TEXT UNIQUE NOT NULL,
                channel_name    TEXT,
                trigger_count   INTEGER DEFAULT 0,
                flagged_video_ids TEXT,
                first_seen      TEXT NOT NULL DEFAULT (datetime('now')),
                last_seen       TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_potential_garbage_channel ON potential_garbage(channel_url);
        """)
        conn.commit()

    # ── Collection ────────────────────────────────────────────────

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into Google (SID cookie on .google.com)."""
        result = tab._send("Network.getCookies")
        if isinstance(result, dict) and "_error" not in result:
            cookies = result.get("cookies", [])
            for c in cookies:
                if c.get("name") == "SID" and ".google.com" in c.get("domain", ""):
                    logger.info("Google: logged in (SID cookie found)")
                    return True
        logger.warning("Google: not logged in")
        return False

    def collect(self, tab: CDPTab, tracker: SyncTracker, limits: RunLimits) -> CollectResult:
        """Collect activity from myactivity.google.com day-by-day."""
        conn = tracker.conn
        total_found = 0
        total_new = 0

        # Store timezone on first run
        tz_row = conn.execute("SELECT value FROM settings WHERE key = 'timezone'").fetchone()
        if not tz_row or not tz_row[0]:
            local_tz = datetime.now().astimezone().tzname()
            conn.execute(
                "UPDATE settings SET value = ?, updated_at = datetime('now') WHERE key = 'timezone'",
                (local_tz,)
            )
            conn.commit()

        # Mark backfill started
        conn.execute(
            "UPDATE settings SET value = 'true', updated_at = datetime('now') "
            "WHERE key = 'backfill_started'"
        )
        conn.commit()

        # Determine which days need collection
        days_to_collect = self._find_days_to_collect(conn)
        if not days_to_collect:
            logger.info("All days up to date — nothing to collect")
            # Mark backfill complete
            conn.execute(
                "UPDATE settings SET value = 'true', updated_at = datetime('now') "
                "WHERE key = 'backfill_completed'"
            )
            conn.commit()
            return CollectResult(items_found=0, items_new=0, items_updated=0)

        logger.info(f"Days to collect: {len(days_to_collect)} "
                     f"(oldest: {days_to_collect[-1]}, newest: {days_to_collect[0]})")

        for i, target_date in enumerate(days_to_collect):
            logger.info(f"=== Day {i+1}/{len(days_to_collect)}: {target_date} ===")

            try:
                found, new = self._collect_one_day(tab, conn, target_date)
                total_found += found
                total_new += new
                logger.info(f"Day {target_date}: {found} found, {new} new")

                # Run garbage detection on freshly collected day
                garbage_count = self._run_garbage_detection(conn, target_date)
                if garbage_count:
                    logger.info(f"Day {target_date}: {garbage_count} garbage videos flagged")

                # Tag entries for this day
                tagged = self._tag_entries(conn, target_date)
                logger.info(f"Day {target_date}: {tagged} entries tagged")

                # Detect sessions for this day
                session_count = self._detect_sessions(conn, target_date)
                logger.info(f"Day {target_date}: {session_count} sessions detected")

                # Update running skip rate baseline
                self._update_skip_rate(conn, target_date)

            except Exception as e:
                logger.error(f"Failed to collect {target_date}: {e}")
                # Increment retry count
                existing = conn.execute(
                    "SELECT retry_count FROM collection_days WHERE date = ?",
                    (target_date,)
                ).fetchone()
                retry = (existing[0] if existing else 0) + 1

                status = "failed"
                if retry >= MAX_RETRY_PER_DAY:
                    logger.warning(f"Day {target_date} failed {retry} times — will retry next run")

                conn.execute(
                    "INSERT OR REPLACE INTO collection_days "
                    "(date, status, retry_count, collected_at, updated_at) "
                    "VALUES (?, ?, ?, datetime('now'), datetime('now'))",
                    (target_date, status, retry)
                )
                conn.commit()

                # On error, wait longer (10s to 30s) before next day
                if i < len(days_to_collect) - 1:
                    error_wait = random.uniform(10, 30)
                    logger.info(f"Error cooldown: {error_wait:.0f}s")
                    time.sleep(error_wait)
                continue

            # Random cooldown between days (3s to 30s)
            if i < len(days_to_collect) - 1:
                cooldown = random.uniform(3, 30)
                logger.info(f"Cooldown: {cooldown:.0f}s before next day")
                time.sleep(cooldown)

        # Check if all days are now complete
        remaining = self._find_days_to_collect(conn)
        if not remaining:
            conn.execute(
                "UPDATE settings SET value = 'true', updated_at = datetime('now') "
                "WHERE key = 'backfill_completed'"
            )
            conn.commit()
            logger.info("Backfill complete — all days collected")

        # Run channel analysis for channels that now qualify
        self._run_channel_analysis(conn)

        # Backfill channel summaries onto entries from analyzed channels
        summary_count = self._backfill_channel_summaries(conn)
        if summary_count:
            logger.info(f"Backfilled channel summaries onto {summary_count} entries")

        # Detect multi-session viewing (same video on multiple days)
        multi_count = self._detect_multi_session(conn)
        if multi_count:
            logger.info(f"Detected {multi_count} multi-session viewing entries")

        return CollectResult(items_found=total_found, items_new=total_new, items_updated=0)

    # ── Day planning ─────────────────────────────────────────────

    def _find_days_to_collect(self, conn) -> list[str]:
        """Find which days need collection. Returns dates newest-first."""
        row = conn.execute(
            "SELECT value FROM settings WHERE key = 'backfill_depth_days'"
        ).fetchone()
        depth_days = int(row[0]) if row else 30

        today = datetime.now().date()
        oldest_target = today - timedelta(days=depth_days)

        # Get completed and abandoned days
        skip = set()
        for row in conn.execute(
            "SELECT date FROM collection_days WHERE status = 'complete'"
        ).fetchall():
            skip.add(row[0])

        # Find missing days (newest first)
        missing = []
        d = today
        while d >= oldest_target:
            date_str = d.isoformat()
            if date_str not in skip:
                missing.append(date_str)
            d -= timedelta(days=1)

        return missing

    # ── Single day collection ────────────────────────────────────

    def _collect_one_day(self, tab: CDPTab, conn, target_date: str) -> tuple[int, int]:
        """Collect all entries for a single day using a fresh tab."""
        dt = datetime.fromisoformat(target_date)
        next_day = (dt + timedelta(days=1)).strftime("%m/%d/%Y")
        target_fmt = dt.strftime("%m/%d/%Y")

        # Step 1: Open a truly fresh tab (clean scroll state)
        logger.info(f"Opening fresh tab for {target_date}")
        from src.cdp_client import CDPClient
        cdp = CDPClient(port=self.CDP_PORT)
        day_tab = cdp.new_tab("https://myactivity.google.com")
        try:
            for _ in range(20):
                if (day_tab.js("return document.readyState") or "") == "complete":
                    break
                time.sleep(2)
            wait_human(3, 5)

            # Check login
            if not self.check_login(day_tab):
                raise RuntimeError("Not logged into Google")

            return self._collect_one_day_inner(day_tab, conn, target_date, target_fmt, next_day)
        finally:
            try:
                cdp.close_tab(day_tab)
            except Exception:
                pass

    def _collect_one_day_inner(self, tab: CDPTab, conn, target_date: str,
                                target_fmt: str, next_day: str) -> tuple[int, int]:
        """Inner collection logic after tab is ready and logged in."""

        # Step 2: Open filter panel
        wait_human(0.5, 1.5)
        if not self._open_filter_panel(tab):
            raise RuntimeError("Could not open filter panel")
        wait_human(0.5, 1)

        # Step 3: Set After date
        self._set_date_field(tab, field_index=1, date_str=target_fmt)
        wait_human(0.3, 0.8)

        # Step 4: Set Before date
        self._set_date_field(tab, field_index=2, date_str=next_day)
        wait_human(0.3, 0.8)

        # Step 5: Click Apply
        if not self._click_apply(tab):
            raise RuntimeError("Could not click Apply")
        wait_human(2, 4)

        # Step 6: Check for empty / no results
        initial_count = int(tab.js('return document.querySelectorAll(".GqCJpe").length') or 0)
        if initial_count == 0:
            # Wait a bit more — might still be loading
            wait_human(3, 5)
            initial_count = int(tab.js('return document.querySelectorAll(".GqCJpe").length') or 0)

        if initial_count == 0:
            logger.info(f"No entries found for {target_date}")
            conn.execute(
                "INSERT OR REPLACE INTO collection_days "
                "(date, total_count, status, collected_at, updated_at) "
                "VALUES (?, 0, 'complete', datetime('now'), datetime('now'))",
                (target_date,)
            )
            conn.commit()
            return (0, 0)

        # Step 7: Scroll to load all entries
        entry_count = self._scroll_to_bottom(tab)
        logger.info(f"Fully loaded: {entry_count} entries for {target_date}")

        # Step 8: Extract entries in batches
        entries = self._extract_all_entries_batched(tab, target_date)
        logger.info(f"Extracted {len(entries)} entries for {target_date}")

        # Step 9: Store in DB
        new_count = self._store_entries(conn, entries, target_date)

        return (len(entries), new_count)

    # ── Filter panel interaction ─────────────────────────────────

    def _mouse_click(self, tab: CDPTab, x: int, y: int) -> None:
        """Send a full mouse click sequence at coordinates (move → press → release)."""
        # Move mouse to position first
        tab._send('Input.dispatchMouseEvent', {
            'type': 'mouseMoved', 'x': x, 'y': y
        })
        time.sleep(0.05)
        # Press
        tab._send('Input.dispatchMouseEvent', {
            'type': 'mousePressed', 'x': x, 'y': y,
            'button': 'left', 'clickCount': 1
        })
        time.sleep(0.05)
        # Release
        tab._send('Input.dispatchMouseEvent', {
            'type': 'mouseReleased', 'x': x, 'y': y,
            'button': 'left', 'clickCount': 1
        })

    def _open_filter_panel(self, tab: CDPTab) -> bool:
        """Open the filter panel. Returns True if opened."""
        # Scroll down a bit then back to top — forces page to re-render filter button
        tab.js('window.scrollTo(0, 300)')
        wait_human(0.5, 1)
        tab.js('window.scrollTo(0, 0)')
        wait_human(1, 2)

        for attempt in range(5):
            pos = tab.js('''
                var btn = document.querySelector("button[aria-label*='Filter by date']");
                if (btn) {
                    var rect = btn.getBoundingClientRect();
                    if (rect.width > 50) return JSON.stringify({x: Math.round(rect.x + rect.width/2), y: Math.round(rect.y + rect.height/2)});
                }
                var all = document.querySelectorAll("button");
                for (var el of all) {
                    if (el.textContent.trim().startsWith("Filter by date")) {
                        var rect = el.getBoundingClientRect();
                        if (rect.width > 50)
                            return JSON.stringify({x: Math.round(rect.x + rect.width/2), y: Math.round(rect.y + rect.height/2)});
                    }
                }
                return null;
            ''')
            if not pos:
                logger.warning(f"Filter button not found (attempt {attempt+1})")
                wait_human(2, 4)
                continue

            p = json.loads(pos)
            self._mouse_click(tab, p['x'], p['y'])
            wait_human(2, 3)

            opened = tab.js('''
                var b = document.querySelectorAll("button");
                for (var x of b) { if (x.textContent.trim()==="Apply") return "OPEN"; }
                return "CLOSED";
            ''')
            if opened == "OPEN":
                return True

            logger.warning(f"Filter panel click attempt {attempt+1} failed")
            # Scroll down then back up to re-render
            tab.js('window.scrollTo(0, 500)')
            wait_human(0.5, 1)
            tab.js('window.scrollTo(0, 0)')
            wait_human(1, 2)

        return False

    def _set_date_field(self, tab: CDPTab, field_index: int, date_str: str) -> None:
        """Set a date field (1=After, 2=Before)."""
        pos = tab.js(f'''
            var inputs = document.querySelectorAll("input[type='text']");
            var count = 0;
            for (var inp of inputs) {{
                if (inp.placeholder === "Search your activity") continue;
                count++;
                if (count === {field_index}) {{
                    var rect = inp.getBoundingClientRect();
                    return JSON.stringify({{x: Math.round(rect.x + rect.width/2), y: Math.round(rect.y + rect.height/2)}});
                }}
            }}
            return null;
        ''')
        if not pos:
            raise RuntimeError(f"Date field {field_index} not found")

        p = json.loads(pos)
        self._mouse_click(tab, p['x'], p['y'])
        time.sleep(0.5)
        tab._send("Input.insertText", {"text": date_str})
        wait_human(1, 2)

    def _click_apply(self, tab: CDPTab) -> bool:
        """Click the Apply button."""
        for attempt in range(3):
            pos = tab.js('''
                var b = document.querySelectorAll("button");
                for (var x of b) {
                    if (x.textContent.trim() === "Apply") {
                        var rect = x.getBoundingClientRect();
                        return JSON.stringify({x: Math.round(rect.x + rect.width/2), y: Math.round(rect.y + rect.height/2)});
                    }
                }
                return null;
            ''')
            if not pos:
                wait_human(1, 2)
                continue

            p = json.loads(pos)
            self._mouse_click(tab, p['x'], p['y'])
            wait_human(3, 5)

            closed = tab.js('''
                var b = document.querySelectorAll("button");
                for (var x of b) { if (x.textContent.trim()==="Apply") return "OPEN"; }
                return "CLOSED";
            ''')
            if closed == "CLOSED":
                return True

            logger.warning(f"Apply click attempt {attempt+1} failed")
            wait_human(1, 2)

        return False

    # ── Scrolling ────────────────────────────────────────────────

    def _scroll_to_bottom(self, tab: CDPTab) -> int:
        """Scroll until all entries loaded. Returns total count."""
        prev_count = 0
        stable_checks = 0

        for scroll_num in range(60):
            tab.js('window.scrollTo(0, document.body.scrollHeight)')
            wait_human(3, 5)

            count = int(tab.js('return document.querySelectorAll(".GqCJpe").length') or 0)

            if count == prev_count and count > 0:
                stable_checks += 1
                if stable_checks >= 3:
                    return count
            else:
                stable_checks = 0
                if scroll_num % 5 == 0:
                    logger.info(f"  Scroll {scroll_num+1}: {count} entries")

            prev_count = count

        logger.warning(f"Hit scroll limit (60), returning {prev_count} entries")
        return prev_count

    # ── Entry extraction (batched) ───────────────────────────────

    _EXTRACT_JS = r'''
        var entries = document.querySelectorAll(".GqCJpe");
        var results = [];
        var start = %d;
        var end = Math.min(start + %d, entries.length);

        for (var i = start; i < end; i++) {
            var e = entries[i];
            var text = e.textContent || "";

            var type = "Unknown";
            if (text.includes("Watched")) type = "Watched";
            else if (text.includes("Visited")) type = "Visited";
            else if (text.includes("Searched for")) type = "Searched";
            else if (text.includes("Used")) type = "Used";
            else if (text.includes("Viewed")) type = "Viewed";

            var logoImg = e.querySelector("img");
            var sourceApp = logoImg ? (logoImg.alt || "").replace("Logo for ", "") : "";

            var links = [];
            var anchors = e.querySelectorAll("a");
            for (var a of anchors) {
                if (a.textContent.trim() === "Details") continue;
                links.push({text: a.textContent.trim().substring(0, 200), href: a.href});
            }

            var texts = [];
            var spans = e.querySelectorAll("span, div, p, a");
            for (var s of spans) {
                if (s.children.length === 0 && s.textContent.trim().length > 0) {
                    var t = s.textContent.trim().substring(0, 500);
                    if (t !== "Details" && !texts.includes(t)) texts.push(t);
                }
            }

            var imgs = [];
            var imgEls = e.querySelectorAll("img");
            for (var img of imgEls) {
                imgs.push({src: img.src, alt: (img.alt || "")});
            }

            var watchPct = null;
            var progressBar = e.querySelector(".HmLFgd");
            if (progressBar) {
                var style = progressBar.getAttribute("style") || "";
                var match = style.match(/width:(\d+)%%/);
                if (match) watchPct = parseInt(match[1]);
            } else if (type === "Watched") {
                watchPct = 100;
            }

            // Extract time from specific DOM element (not regex on full text)
            var timeStr = "";
            var timeEl = e.querySelector(".H3Q9vf, .wlgrwd");
            if (timeEl) {
                var timeMatch = timeEl.textContent.match(/(\d{1,2}:\d{2}\s*(?:AM|PM))/i);
                if (timeMatch) timeStr = timeMatch[1];
            }
            // Fallback: search text nodes for "H:MM AM/PM •" pattern
            if (!timeStr) {
                for (var i = 0; i < textNodes.length; i++) {
                    var nodeMatch = textNodes[i].match(/^(\d{1,2}:\d{2}\s*(?:AM|PM))\s*[•·]/i);
                    if (nodeMatch) { timeStr = nodeMatch[1]; break; }
                }
            }

            results.push({
                type: type,
                sourceApp: sourceApp,
                texts: texts,
                links: links,
                imgs: imgs,
                watchPct: watchPct,
                time: timeStr,
                rawText: text.trim().substring(0, 1000).replace(/\n/g, " | ")
            });
        }
        return JSON.stringify(results);
    '''

    def _extract_all_entries_batched(self, tab: CDPTab, target_date: str) -> list[dict]:
        """Extract all entries in batches of 100."""
        total = int(tab.js('return document.querySelectorAll(".GqCJpe").length') or 0)
        all_parsed = []
        batch_size = 100

        for offset in range(0, total, batch_size):
            js_code = self._EXTRACT_JS % (offset, batch_size)
            raw = tab.js(js_code) or '[]'
            batch = json.loads(raw)
            logger.info(f"  Batch {offset//batch_size + 1}: extracted {len(batch)} entries "
                        f"({offset+len(batch)}/{total})")

            for entry in batch:
                parsed = self._parse_entry(entry, target_date)
                if parsed:
                    all_parsed.append(parsed)

            wait_human(0.5, 1.5)

        return all_parsed

    # ── Entry parsing ────────────────────────────────────────────

    def _parse_entry(self, entry: dict, target_date: str) -> dict | None:
        """Parse a raw DOM entry into a structured dict."""
        entry_type = entry.get("type", "Unknown")
        time_str = entry.get("time", "")

        # Build timestamp: target_date + time (local timezone)
        timestamp = target_date
        if time_str:
            for fmt in ("%I:%M %p", "%I:%M%p", "%H:%M"):
                try:
                    t = datetime.strptime(time_str.strip(), fmt)
                    timestamp = f"{target_date}T{t.strftime('%H:%M:%S')}"
                    break
                except ValueError:
                    continue

        result = {
            "entry_type": entry_type,
            "source_app": entry.get("sourceApp", ""),
            "timestamp": timestamp,
            "date": target_date,
            "raw_data": json.dumps(entry, ensure_ascii=False),
        }

        links = entry.get("links", [])
        texts = entry.get("texts", [])

        if entry_type == "Watched":
            for link in links:
                href = link.get("href", "")
                if "youtube.com/watch" in href:
                    result["video_title"] = link.get("text", "")
                    parsed_url = urlparse(href)
                    qs = parse_qs(parsed_url.query)
                    result["video_id"] = qs.get("v", [None])[0]
                    break

            for link in links:
                href = link.get("href", "")
                if "youtube.com/channel/" in href or "youtube.com/@" in href:
                    result["channel"] = link.get("text", "")
                    result["channel_url"] = href
                    break

            for link in reversed(links):
                href = link.get("href", "")
                text = link.get("text", "")
                if "youtube.com/watch" in href and re.match(r'^\d+:\d+', text):
                    result["duration"] = text
                    result["duration_secs"] = self._parse_duration(text)
                    break

            result["watch_pct"] = entry.get("watchPct")

            for img in entry.get("imgs", []):
                if "ytimg.com" in img.get("src", ""):
                    result["thumbnail_url"] = img["src"]
                    break

        elif entry_type == "Visited":
            for link in links:
                href = link.get("href", "")
                if href:
                    actual_url = href
                    if "google.com/url" in href:
                        parsed_url = urlparse(href)
                        qs = parse_qs(parsed_url.query)
                        actual_url = unquote(qs.get("q", [href])[0])
                    result["url"] = actual_url
                    result["page_title"] = link.get("text", "")
                    try:
                        result["domain"] = urlparse(actual_url).netloc
                    except Exception:
                        result["domain"] = ""
                    break

        elif entry_type == "Searched":
            for link in links:
                href = link.get("href", "")
                if any(x in href for x in (
                    "google.com/search", "google.com/?",
                    "play.google.com/store/search",
                    "youtube.com/results",
                    "/maps/search/", "/maps/place/",
                )):
                    result["query_text"] = link.get("text", "")
                    result["search_url"] = href
                    break
            # Fallback: if no link matched, use first non-meta text
            if not result.get("query_text"):
                for t in texts:
                    if t not in ("Details", entry.get("sourceApp", ""), "Searched with an image"):
                        result["query_text"] = t
                        result["search_url"] = links[0]["href"] if links else ""
                        break
            result["search_type"] = entry.get("sourceApp", "Search")

            # Extract Maps location data if present
            search_url = result.get("search_url", "")
            if "/maps/" in search_url:
                result["maps_url"] = search_url
                # Extract coordinates from URL: /@lat,lng,zoom
                coord_match = re.search(r'@(-?\d+\.?\d*),(-?\d+\.?\d*)', search_url)
                if coord_match:
                    result["latitude"] = float(coord_match.group(1))
                    result["longitude"] = float(coord_match.group(2))
                # Extract place name from /maps/place/Name/
                place_match = re.search(r'/maps/place/([^/@]+)', search_url)
                if place_match:
                    result["place_name"] = unquote(place_match.group(1)).replace('+', ' ')
                    # Full address from link text if available
                    result["place_address"] = result.get("query_text", "")

        elif entry_type == "Used":
            for link in links:
                href = link.get("href", "")
                if "play.google.com" in href:
                    result["app_name"] = link.get("text", "")
                    result["play_store_url"] = href
                    break
            if not result.get("app_name"):
                for t in texts:
                    if t.startswith("Used "):
                        result["app_name"] = t[5:]
                        break
                    elif t not in ("Details", entry.get("sourceApp", "")):
                        result["app_name"] = t
                        break

        elif entry_type == "Viewed":
            for link in links:
                href = link.get("href", "")
                if "youtube.com/post/" in href:
                    result["content_text"] = link.get("text", "")
                    result["content_url"] = href
                    result["source_type"] = "youtube_post"
                    break
                elif href:
                    result["content_url"] = href
                    result["source_type"] = "image_search"
                    for t in texts:
                        if t not in ("Details", entry.get("sourceApp", "")):
                            result["content_text"] = t
                            break
                    break

            for link in links:
                href = link.get("href", "")
                if "youtube.com/channel/" in href or "youtube.com/@" in href:
                    result["channel"] = link.get("text", "")
                    result["channel_url"] = href
                    break

        elif entry_type == "Unknown":
            topics = []
            for t in texts:
                if t in ("Details", "Including topics:", entry.get("sourceApp", "")):
                    continue
                if re.search(r'\d+ notifications', t):
                    continue
                if re.search(r'\d+ cards in your feed', t):
                    continue

                # Parse notification topics
                if " - dismissed" in t:
                    topic = t.replace(" - dismissed", "").strip()
                    topics.append({"topic": topic, "status": "dismissed"})
                elif " - clicked" in t:
                    topic = t.replace(" - clicked", "").strip()
                    topics.append({"topic": topic, "status": "clicked"})
                else:
                    topics.append({"topic": t, "status": "shown"})

            if topics:
                result["notification_topics"] = json.dumps(topics, ensure_ascii=False)

        return result

    @staticmethod
    def _parse_duration(duration_str: str) -> int | None:
        """Parse duration like '8:04', '1:29:49' to seconds."""
        parts = duration_str.split(":")
        try:
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                return int(parts[0]) * 60 + int(parts[1])
        except ValueError:
            pass
        return None

    # ── Storage ──────────────────────────────────────────────────

    def _store_entries(self, conn, entries: list[dict], target_date: str) -> int:
        """Store entries in DB. Dedup by timestamp + entry_type + identifying key."""
        new_count = 0
        type_counts = {}

        for entry in entries:
            entry_type = entry.get("entry_type", "Unknown")
            ts = entry.get("timestamp", "")

            # Dedup: same timestamp + type + specific identifier = same entry
            # But same video at different times = different entries (repeat viewing)
            dedup_check = False
            if entry_type == "Watched" and entry.get("video_id"):
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND video_id=?",
                    (ts, entry_type, entry["video_id"])
                ).fetchone()
            elif entry_type == "Visited" and entry.get("url"):
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND url=?",
                    (ts, entry_type, entry["url"])
                ).fetchone()
            elif entry_type == "Searched" and entry.get("query_text"):
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND query_text=?",
                    (ts, entry_type, entry["query_text"])
                ).fetchone()
            elif entry_type == "Used" and entry.get("app_name"):
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND app_name=?",
                    (ts, entry_type, entry["app_name"])
                ).fetchone()
            elif entry_type == "Viewed" and entry.get("content_url"):
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND content_url=?",
                    (ts, entry_type, entry["content_url"])
                ).fetchone()
            else:
                # Unknown or no identifying field — use raw_data hash
                raw_short = (entry.get("raw_data", ""))[:200]
                dedup_check = conn.execute(
                    "SELECT 1 FROM activities WHERE timestamp=? AND entry_type=? AND substr(raw_data,1,200)=?",
                    (ts, entry_type, raw_short)
                ).fetchone()

            if dedup_check:
                continue

            conn.execute(
                """INSERT INTO activities (
                    entry_type, source_app, timestamp, date, raw_data,
                    video_id, video_title, channel, channel_url,
                    duration, duration_secs, watch_pct, thumbnail_url,
                    page_title, url, domain,
                    query_text, search_url, search_type,
                    app_name, play_store_url,
                    content_text, content_url, source_type,
                    place_name, place_address, latitude, longitude, maps_url,
                    notification_topics
                ) VALUES (
                    :entry_type, :source_app, :timestamp, :date, :raw_data,
                    :video_id, :video_title, :channel, :channel_url,
                    :duration, :duration_secs, :watch_pct, :thumbnail_url,
                    :page_title, :url, :domain,
                    :query_text, :search_url, :search_type,
                    :app_name, :play_store_url,
                    :content_text, :content_url, :source_type,
                    :place_name, :place_address, :latitude, :longitude, :maps_url,
                    :notification_topics
                )""",
                {k: entry.get(k) for k in [
                    "entry_type", "source_app", "timestamp", "date", "raw_data",
                    "video_id", "video_title", "channel", "channel_url",
                    "duration", "duration_secs", "watch_pct", "thumbnail_url",
                    "page_title", "url", "domain",
                    "query_text", "search_url", "search_type",
                    "app_name", "play_store_url",
                    "content_text", "content_url", "source_type",
                    "place_name", "place_address", "latitude", "longitude", "maps_url",
                    "notification_topics",
                ]}
            )
            new_count += 1
            type_counts[entry_type] = type_counts.get(entry_type, 0) + 1

        conn.commit()

        # Update collection_days
        conn.execute(
            """INSERT OR REPLACE INTO collection_days
               (date, watched_count, visited_count, searched_count,
                used_count, viewed_count, unknown_count, total_count,
                status, retry_count, collected_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'complete', 0, datetime('now'), datetime('now'))""",
            (
                target_date,
                type_counts.get("Watched", 0),
                type_counts.get("Visited", 0),
                type_counts.get("Searched", 0),
                type_counts.get("Used", 0),
                type_counts.get("Viewed", 0),
                type_counts.get("Unknown", 0),
                new_count,
            )
        )

        # Update settings
        conn.execute(
            "UPDATE settings SET value = ?, updated_at = datetime('now') "
            "WHERE key = 'newest_collected_date' AND (value = '' OR value < ?)",
            (target_date, target_date)
        )
        conn.execute(
            "UPDATE settings SET value = ?, updated_at = datetime('now') "
            "WHERE key = 'oldest_collected_date' AND (value = '' OR value > ?)",
            (target_date, target_date)
        )
        conn.commit()

        logger.info(f"  Stored {new_count} new entries: {type_counts}")
        return new_count

    # ── Service Request Helper ────────────────────────────────

    CORE_DB_PATH = None  # set by framework or test harness

    def _get_core_db_path(self):
        """Get path to core.db for service requests."""
        if self.CORE_DB_PATH:
            return self.CORE_DB_PATH
        import os
        return os.path.join(os.environ.get('LOCALAPPDATA', ''), 'MemoryTap', 'core.db')

    def _chatgpt_request(self, prompt: str, image_paths: list[str] | None = None,
                         web_search: bool = False, timeout: int = 300) -> str:
        """Submit prompt to ChatGPT inference via service request. Returns reply text.

        This is the ONLY way google_activity talks to ChatGPT.
        All ChatGPT interaction is handled by the chatgpt_inference skill.
        """
        import sqlite3 as _sqlite3

        # Clean surrogates from prompt (can appear in scraped page content)
        clean_prompt = prompt.encode('utf-8', errors='surrogatepass').decode('utf-8', errors='replace')
        payload = {
            "prompt": clean_prompt,
            "web_search": web_search,
            "model": self.LLM_MODEL,
        }
        service_name = "execute_prompt"
        if image_paths:
            payload["image_paths"] = image_paths
            service_name = "execute_prompt_with_image"

        core_db = self._get_core_db_path()
        conn = _sqlite3.connect(core_db)
        conn.row_factory = _sqlite3.Row
        conn.execute(
            "INSERT INTO service_requests (from_skill, to_skill, service_name, payload) "
            "VALUES (?, ?, ?, ?)",
            ("google_activity", "chatgpt_inference", service_name,
             json.dumps(payload, ensure_ascii=False))
        )
        conn.commit()
        req_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()

        logger.info(f"    Service request {req_id}: {service_name} ({len(prompt)} chars, "
                     f"web_search={web_search})")

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
                logger.info(f"    Service request {req_id}: COMPLETED ({len(reply)} chars)")
                return reply
            elif row["state"] in ("FAILED", "TIMEOUT"):
                error = row["error"] or "Unknown error"
                logger.error(f"    Service request {req_id}: {row['state']} — {error}")
                return ""

            time.sleep(1)

        logger.error(f"    Service request {req_id}: timed out after {timeout}s")
        return ""

    # ── Channel Analysis ──────────────────────────────────────

    CHANNEL_THRESHOLD_VIDEOS = 5
    CHANNEL_THRESHOLD_WATCH_PCT = 50

    def _run_channel_analysis(self, conn) -> int:
        """Find channels that qualify for analysis and analyze them.

        Trigger: ≥5 videos watched at ≥50%, channel not in channels or garbage_channels.
        """
        # Find qualifying channels not yet analyzed
        qualifying = conn.execute("""
            SELECT a.channel_url, a.channel, COUNT(*) as vid_count
            FROM activities a
            WHERE a.entry_type = 'Watched'
            AND a.watch_pct >= ?
            AND a.is_garbage = 0
            AND a.duration_secs > 120
            AND a.channel_url IS NOT NULL AND a.channel_url != ''
            AND a.channel_url NOT IN (SELECT channel_url FROM channels)
            AND a.channel_url NOT IN (SELECT channel_url FROM garbage_channels)
            GROUP BY a.channel_url
            HAVING vid_count >= ?
        """, (self.CHANNEL_THRESHOLD_WATCH_PCT, self.CHANNEL_THRESHOLD_VIDEOS)).fetchall()

        if not qualifying:
            return 0

        logger.info(f"Channel analysis: {len(qualifying)} new channels qualify")
        analyzed = 0

        for ch_url, ch_name, vid_count in qualifying:
            logger.info(f"  Analyzing channel: {ch_name} ({vid_count} videos at ≥{self.CHANNEL_THRESHOLD_WATCH_PCT}%)")

            try:
                result = self._analyze_channel(ch_url, ch_name)
                if result:
                    conn.execute("""
                        INSERT OR REPLACE INTO channels
                        (channel_url, channel_name, subscriber_count, video_count,
                         is_subscribed, about_text, sections_data,
                         analysis_full, analysis_summary, verdict,
                         evaluated_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ANALYZED',
                                datetime('now'), datetime('now'))
                    """, (
                        ch_url, result.get('name', ch_name),
                        result.get('subs', ''), result.get('video_count', ''),
                        result.get('is_subscribed', 0), result.get('about_text', ''),
                        json.dumps(result.get('sections', []), ensure_ascii=False),
                        result.get('analysis_full', ''),
                        result.get('analysis_summary', ''),
                    ))
                    conn.commit()
                    analyzed += 1
                    logger.info(f"    Stored analysis for {ch_name}")
                else:
                    logger.warning(f"    Failed to analyze {ch_name}")

            except Exception as e:
                logger.error(f"    Channel analysis error for {ch_name}: {e}")

            # Cooldown between channel analyses
            wait_human(5, 15)

        return analyzed

    def _analyze_channel(self, channel_url: str, channel_name: str) -> dict | None:
        """Visit channel page, extract data, send to ChatGPT (via inference service) with web search.

        Channel page visit uses CDP directly. ChatGPT goes through inference service.
        Returns dict with all channel data + ChatGPT analysis, or None on failure.
        """
        try:
            cdp = CDPClient(port=self.CDP_PORT)
        except Exception:
            logger.warning("Cannot connect to CDP for channel page visit")
            return None

        channel_tab = None
        try:
            channel_tab = cdp.new_tab(channel_url)
            for _ in range(20):
                if (channel_tab.js("return document.readyState") or "") == "complete":
                    break
                time.sleep(2)
            time.sleep(8)

            # Extract channel header — universal selector works on all channel layouts
            channel_header = channel_tab.js('''
                var h = document.querySelector("yt-page-header-view-model");
                return h ? h.textContent.trim().substring(0, 1000) : "";
            ''') or ""

            if not channel_header:
                logger.warning(f"    No channel header found for {channel_name}")

            # Scroll to load sections
            channel_tab.js("window.scrollTo(0, 500)")
            time.sleep(3)

            # Extract sections with video titles
            sections = json.loads(channel_tab.js('''
                var shelves = document.querySelectorAll("ytd-item-section-renderer, ytd-shelf-renderer, ytd-reel-shelf-renderer");
                var results = [];
                for (var shelf of shelves) {
                    var title = shelf.querySelector("#title, .ytd-shelf-renderer #title-text, h2");
                    var videos = shelf.querySelectorAll("ytd-grid-video-renderer #video-title, ytd-rich-item-renderer #video-title, a#video-title-link, ytd-reel-item-renderer #title");
                    var vids = [];
                    for (var v of videos) vids.push(v.textContent.trim().substring(0, 100));
                    if (vids.length > 0)
                        results.push({section: title ? title.textContent.trim() : "untitled", videos: vids.slice(0, 10)});
                }
                return JSON.stringify(results);
            ''') or '[]')

            # Get tabs
            tabs_list = json.loads(channel_tab.js('''
                var chips = document.querySelectorAll("yt-tab-shape, .yt-tab-shape-wiz__tab");
                var r = []; for (var c of chips) r.push(c.textContent.trim()); return JSON.stringify(r);
            ''') or '[]')

        except Exception as e:
            logger.error(f"    Channel page extraction failed: {e}")
            return None

        finally:
            if channel_tab:
                try:
                    cdp.close_tab(channel_tab)
                except Exception:
                    pass

        # Build prompt and send via inference service (with web search)
        section_text = ""
        for s in sections[:8]:
            section_text += f"\n  Section: {s['section']}\n"
            for v in s['videos'][:8]:
                section_text += f"    - {v}\n"

        prompt = f"""Analyze this YouTube channel in depth. Be EXTREMELY SPECIFIC and DETAILED.

CHANNEL HEADER (raw from YouTube page):
{channel_header}

CHANNEL TABS: {', '.join(tabs_list)}

CONTENT SECTIONS:{section_text if section_text else ' (none visible)'}

ANALYZE THE FOLLOWING (be specific, not generic):

1. CONTENT RANGE: What is the FULL spectrum of topics this channel covers?
2. PERSPECTIVE: What lens/angle does this channel bring?
3. TONE: Does it educate, provoke, inform, entertain, challenge?
4. FORMAT: What content formats — news analysis, vlogs, tutorials, reactions, documentaries?
5. CREATOR: Who runs this? Individual or organization? What background?
6. AUDIENCE: Who would watch this channel? What need does it fulfill?
7. DISTINCTIVENESS: What makes this channel different from alternatives?

USE WEB SEARCH to find additional information about this channel.

Return your analysis as JSON:
{{
  "channel_name": "name",
  "content_range": "detailed description of all topics covered",
  "perspective": "what lens/angle",
  "tone": "educate/provoke/inform/entertain",
  "format": "content formats used",
  "creator": "who runs it, background",
  "audience": "who watches and why",
  "distinctiveness": "what makes it unique",
  "summary": "2-3 sentence summary that captures the essence of this channel"
}}"""

        response = self._chatgpt_request(prompt, web_search=True)

        if not response:
            logger.warning(f"    Channel analysis: empty ChatGPT response for {channel_name}")
            return None

        logger.info(f"    ChatGPT analysis: {len(response)} chars for {channel_name}")

        # Build result
        result = {
            'name': channel_name,
            'subs': '',
            'video_count': '',
            'is_subscribed': 0,
            'about_text': channel_header,
            'sections': sections,
            'analysis_full': response,
        }

        # Extract summary from JSON response
        try:
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                parsed = json.loads(response[json_start:json_end])
                result['analysis_summary'] = parsed.get('summary', response[:500])
            else:
                result['analysis_summary'] = response[:500]
        except (json.JSONDecodeError, ValueError):
            result['analysis_summary'] = response[:500]

        return result

    # ── Garbage Detection ──────────────────────────────────────

    GARBAGE_DURATION_THRESHOLD = 7200  # 2 hours in seconds
    CDP_PORT = 9494  # production Chrome; override to 9777 for probe testing
    DEFAULT_LLM_MODEL = "google/gemini-3.1-flash-lite-preview"  # fast model for garbage/channel detection

    @property
    def LLM_MODEL(self):
        """Get LLM model. Priority: core.db skill_settings > class default."""
        try:
            import sqlite3 as _sq
            core_db = self._get_core_db_path()
            conn = _sq.connect(core_db)
            row = conn.execute(
                "SELECT value FROM skill_settings WHERE skill_name = 'google_activity' AND key = 'llm_model'"
            ).fetchone()
            conn.close()
            if row and row[0]:
                return row[0]
        except Exception:
            pass
        return self.DEFAULT_LLM_MODEL

    def _run_garbage_detection(self, conn, target_date: str) -> int:
        """Flag garbage videos from a collected day. Returns count of newly flagged videos.

        Flow:
        1. Find Watched videos >2hr, 100%, not already flagged
        2. Skip if channel is known (≥5 videos at ≥50%)
        3. Auto-discard if channel is already banned
        4. Skip if video has previous partial watch entries (multi-session viewing)
        5. Check activity gap (other activity during video duration)
        6. Flag as garbage if activity overlap detected
        """
        flagged = 0

        # Get known channels (≥5 videos at ≥50%)
        known_channels = set()
        rows = conn.execute("""
            SELECT channel_url FROM channels WHERE verdict != 'GARBAGE'
        """).fetchall()
        for r in rows:
            known_channels.add(r[0])

        # Get banned channels
        banned_channels = set()
        rows = conn.execute("SELECT channel_url FROM garbage_channels").fetchall()
        for r in rows:
            banned_channels.add(r[0])

        # Find candidate videos: >2hr, ≥50% watched, not already flagged
        candidates = conn.execute("""
            SELECT id, timestamp, video_id, video_title, channel, channel_url,
                   duration, duration_secs, watch_pct, date
            FROM activities
            WHERE date = ?
            AND entry_type = 'Watched'
            AND is_garbage = 0
            AND duration_secs IS NOT NULL
            AND duration_secs > ?
            AND watch_pct >= 50
        """, (target_date, self.GARBAGE_DURATION_THRESHOLD)).fetchall()

        # Also find videos with livestream keywords in title (any duration)
        # These are suspicious regardless of length
        livestream_keywords = conn.execute("""
            SELECT id, timestamp, video_id, video_title, channel, channel_url,
                   duration, duration_secs, watch_pct, date
            FROM activities
            WHERE date = ?
            AND entry_type = 'Watched'
            AND is_garbage = 0
            AND duration_secs IS NOT NULL
            AND duration_secs > 1800
            AND watch_pct >= 50
            AND (LOWER(video_title) LIKE '%#live%'
                 OR LOWER(video_title) LIKE '%#livestream%'
                 OR LOWER(video_title) LIKE '%#shortslivestream%'
                 OR LOWER(video_title) LIKE '%live stream%'
                 OR LOWER(video_title) LIKE '%livestream%'
                 OR LOWER(video_title) LIKE '%is live%'
                 OR LOWER(video_title) LIKE 'live %'
                 OR LOWER(video_title) LIKE '% live %'
                 OR LOWER(video_title) LIKE '% live|%'
                 OR LOWER(video_title) LIKE '%|live %'
                 OR LOWER(video_title) LIKE '% live')
        """, (target_date,)).fetchall()

        # Merge candidates, dedup by id
        seen_ids = {c[0] for c in candidates}
        for lk in livestream_keywords:
            if lk[0] not in seen_ids:
                candidates.append(lk)
                seen_ids.add(lk[0])

        if not candidates:
            return 0

        logger.info(f"  Garbage check: {len(candidates)} candidates ({len(livestream_keywords)} via livestream keywords)")

        for c in candidates:
            vid_id = c[2]
            vid_title = c[3] or "untitled"
            channel = c[4] or "unknown"
            channel_url = c[5] or ""
            timestamp = c[1]
            duration_secs = c[7]
            entry_id = c[0]

            # Skip if channel is known/trusted
            if channel_url and channel_url in known_channels:
                logger.info(f"    SKIP (known channel): {vid_title[:50]}")
                continue

            # Auto-discard if channel is banned
            if channel_url and channel_url in banned_channels:
                conn.execute(
                    "UPDATE activities SET is_garbage = 1, garbage_reason = ? WHERE id = ?",
                    ("Channel banned", entry_id)
                )
                flagged += 1
                logger.info(f"    AUTO-DISCARD (banned channel): {vid_title[:50]}")
                continue

            # Auto-discard if same video was already flagged as garbage on another day
            if vid_id:
                prev_garbage = conn.execute(
                    "SELECT garbage_reason FROM activities WHERE video_id = ? AND is_garbage = 1 AND id != ? LIMIT 1",
                    (vid_id, entry_id)
                ).fetchone()
                if prev_garbage:
                    conn.execute(
                        "UPDATE activities SET is_garbage = 1, garbage_reason = ? WHERE id = ?",
                        (f"Same video flagged before: {prev_garbage[0][:100]}", entry_id)
                    )
                    flagged += 1
                    logger.info(f"    AUTO-DISCARD (previously flagged video): {vid_title[:50]}")
                    continue

            # Check for previous partial watch entries (multi-session viewing)
            if vid_id:
                partial = conn.execute("""
                    SELECT COUNT(*) FROM activities
                    WHERE video_id = ? AND watch_pct > 10 AND watch_pct < 100
                    AND id != ?
                """, (vid_id, entry_id)).fetchone()[0]
                if partial > 0:
                    logger.info(f"    SKIP (multi-session viewing, {partial} partial entries): {vid_title[:50]}")
                    continue

            # Activity gap check: is there other activity during this video's playback?
            has_overlap = False
            if 'T' in timestamp:
                try:
                    video_start = datetime.fromisoformat(timestamp)
                    video_end = video_start + timedelta(seconds=duration_secs)

                    overlap_count = conn.execute("""
                        SELECT COUNT(*) FROM activities
                        WHERE date = ?
                        AND timestamp > ?
                        AND timestamp < ?
                        AND id != ?
                    """, (target_date, timestamp, video_end.isoformat(), entry_id)).fetchone()[0]

                    has_overlap = overlap_count > 0
                except (ValueError, TypeError) as e:
                    logger.warning(f"    Cannot check gap for {vid_title[:50]}: {e}")
                    has_overlap = True  # assume suspicious if we can't check
            else:
                has_overlap = True  # no timestamp = suspicious

            if not has_overlap:
                logger.info(f"    KEEP (no overlapping activity): {vid_title[:50]}")
                continue

            # Suspicious video — visit page and ask ChatGPT
            video_url = ""
            if vid_id:
                video_url = f"https://www.youtube.com/watch?v={vid_id}"

            if video_url:
                verdict, reason = self._evaluate_video_chatgpt(video_url, vid_title, channel, duration_secs)
            else:
                verdict = "GARBAGE"
                reason = "No video URL + activity overlap"

            if verdict == "GARBAGE":
                full_reason = f"{reason} (activity overlap during {duration_secs // 3600}h video)"
                conn.execute(
                    "UPDATE activities SET is_garbage = 1, garbage_reason = ? WHERE id = ?",
                    (full_reason, entry_id)
                )
                flagged += 1

                if channel_url:
                    self._increment_potential_garbage(conn, channel_url, channel, vid_id)

                logger.info(f"    GARBAGE: {vid_title[:50]} ({full_reason[:80]})")
            else:
                logger.info(f"    KEEP (ChatGPT: {reason[:60]}): {vid_title[:50]}")

        conn.commit()

        if flagged:
            logger.info(f"  Garbage detection: {flagged} videos flagged on {target_date}")

        return flagged

    def _evaluate_video_chatgpt(self, video_url: str, title: str, channel: str,
                                duration_secs: int) -> tuple[str, str]:
        """Visit video page, extract data, ask ChatGPT (via inference service) if it's garbage.

        Video page visit uses CDP directly (google_activity's own concern).
        ChatGPT interaction goes through chatgpt_inference service (modular).

        Returns: (verdict, reason) where verdict is 'GARBAGE' or 'LEGITIMATE'.
        """
        try:
            cdp = CDPClient(port=self.CDP_PORT)
        except Exception:
            logger.warning("Cannot connect to CDP for video page visit")
            return "GARBAGE", "CDP unavailable — flagged by activity gap"

        video_tab = None
        try:
            # Open video page
            video_tab = cdp.new_tab(video_url)
            for _ in range(20):
                if (video_tab.js("return document.readyState") or "") == "complete":
                    break
                time.sleep(2)
            time.sleep(8)

            # Scroll to load comments
            video_tab.js("window.scrollTo(0, 800)")
            time.sleep(3)
            video_tab.js("window.scrollTo(0, 2000)")
            time.sleep(5)

            # Extract data
            data = json.loads(video_tab.js('''
                var r = {};
                var t = document.querySelector("h1.ytd-watch-metadata yt-formatted-string");
                r.title = t ? t.textContent.trim() : "";
                var ch = document.querySelector("#channel-name a");
                r.channel = ch ? ch.textContent.trim() : "";
                var s = document.querySelector("#owner-sub-count");
                r.subs = s ? s.textContent.trim() : "";
                var v = document.querySelector("#info-strings yt-formatted-string");
                r.views = v ? v.textContent.trim() : "";
                var d = document.querySelector(".ytp-time-duration");
                r.duration = d ? d.textContent.trim() : "";
                var chat = document.querySelector("ytd-live-chat-frame");
                r.hasChatReplay = !!chat;
                var desc = document.querySelector("#description-inline-expander, ytd-expandable-video-description-body-renderer");
                r.description = desc ? desc.textContent.trim().substring(0, 1500) : "";
                var cmts = document.querySelectorAll("ytd-comment-thread-renderer #content-text");
                r.commentCount = cmts.length;
                var cc = document.querySelector("#comments #count");
                r.commentCountText = cc ? cc.textContent.trim() : "0";
                var l = document.querySelector("[aria-label*='like this video']");
                r.likes = l ? l.getAttribute("aria-label") : "";
                return JSON.stringify(r);
            ''') or '{}')

        except Exception as e:
            logger.error(f"    Video page extraction failed: {e}")
            return "GARBAGE", f"Page extraction error: {str(e)[:100]}"

        finally:
            if video_tab:
                try:
                    cdp.close_tab(video_tab)
                except Exception:
                    pass

        # Build prompt and send via inference service
        prompt = f"""Evaluate this YouTube video — is it LEGITIMATE content or GARBAGE (live stream, looping, engagement bait)?

Title: "{data.get('title', title)}"
Channel: {data.get('channel', channel)} ({data.get('subs', '?')})
Duration: {data.get('duration', f'{duration_secs//3600}h')}
Views: {data.get('views', '?')}
Chat Replay: {"YES (was a live stream)" if data.get('hasChatReplay') else "NO"}
Description: {"YES (" + str(len(data.get('description', ''))) + " chars)" if data.get('description') else "NONE"}
Comments: {data.get('commentCountText', '0')}
Likes: {data.get('likes', '?')[:40]}

Respond with EXACTLY one line:
VERDICT: GARBAGE — reason
or
VERDICT: LEGITIMATE — reason"""

        response = self._chatgpt_request(prompt)

        if not response:
            return "GARBAGE", "ChatGPT returned empty response — defaulting to garbage"

        # Parse verdict — handles both plain text and JSON formats
        response_upper = response.upper()

        # Try JSON first (some models return {"verdict": "GARBAGE", "reason": "..."})
        try:
            parsed = json.loads(response)
            verdict = (parsed.get("verdict") or parsed.get("VERDICT") or "").upper().strip()
            reason = (parsed.get("reason") or parsed.get("REASON") or "")[:200]
            if verdict in ("GARBAGE", "LEGITIMATE"):
                return verdict, reason or f"ChatGPT verdict: {verdict.lower()}"
        except (json.JSONDecodeError, AttributeError):
            pass

        # Plain text: VERDICT: GARBAGE — reason
        if "VERDICT: GARBAGE" in response_upper or "VERDICT:GARBAGE" in response_upper:
            idx = response_upper.find("GARBAGE")
            reason = response[idx + 7:].strip().lstrip("—-– ").strip()[:200]
            return "GARBAGE", reason or "ChatGPT verdict: garbage"
        elif "VERDICT: LEGITIMATE" in response_upper or "VERDICT:LEGITIMATE" in response_upper:
            idx = response_upper.find("LEGITIMATE")
            reason = response[idx + 10:].strip().lstrip("—-– ").strip()[:200]
            return "LEGITIMATE", reason or "ChatGPT verdict: legitimate"

        # Check for keyword match anywhere
        if "GARBAGE" in response_upper:
            return "GARBAGE", response[:200]
        elif "LEGITIMATE" in response_upper:
            return "LEGITIMATE", response[:200]

        logger.warning(f"    ChatGPT response unparseable: {response[:100]}")
        return "GARBAGE", "Unparseable ChatGPT response — defaulting to garbage"

    def _increment_potential_garbage(self, conn, channel_url: str, channel_name: str, video_id: str):
        """Increment garbage trigger count for a channel. At 3, evaluate channel via ChatGPT."""
        existing = conn.execute(
            "SELECT trigger_count, flagged_video_ids FROM potential_garbage WHERE channel_url = ?",
            (channel_url,)
        ).fetchone()

        if existing:
            count = existing[0] + 1
            vid_ids = existing[1] or ""
            if video_id and video_id not in vid_ids:
                vid_ids = f"{vid_ids},{video_id}" if vid_ids else video_id
            conn.execute(
                "UPDATE potential_garbage SET trigger_count = ?, flagged_video_ids = ?, "
                "last_seen = datetime('now') WHERE channel_url = ?",
                (count, vid_ids, channel_url)
            )
        else:
            count = 1
            conn.execute(
                "INSERT INTO potential_garbage (channel_url, channel_name, trigger_count, "
                "flagged_video_ids) VALUES (?, ?, 1, ?)",
                (channel_url, channel_name, video_id or "")
            )

        conn.commit()

        if count == 3:
            logger.info(f"    Channel '{channel_name}' hit 3 garbage triggers — "
                        f"evaluating channel via ChatGPT...")
            verdict = self._evaluate_channel_chatgpt(channel_url, channel_name)
            if verdict == "GARBAGE":
                conn.execute(
                    "INSERT OR IGNORE INTO garbage_channels (channel_url, channel_name, reason) "
                    "VALUES (?, ?, ?)",
                    (channel_url, channel_name, "3+ garbage videos, ChatGPT confirmed")
                )
                conn.execute(
                    "DELETE FROM potential_garbage WHERE channel_url = ?",
                    (channel_url,)
                )
                conn.commit()
                logger.info(f"    BANNED channel: {channel_name}")
            else:
                # Reset trigger count — channel is legitimate
                conn.execute(
                    "UPDATE potential_garbage SET trigger_count = 0 WHERE channel_url = ?",
                    (channel_url,)
                )
                conn.commit()
                logger.info(f"    Channel '{channel_name}' KEPT — trigger count reset")

    def _evaluate_channel_chatgpt(self, channel_url: str, channel_name: str) -> str:
        """Visit channel page, extract data, ask ChatGPT (via inference service) for verdict.

        Channel page visit uses CDP directly. ChatGPT goes through inference service.
        Returns 'GARBAGE' or 'KEEP'.
        """
        try:
            cdp = CDPClient(port=self.CDP_PORT)
        except Exception:
            logger.warning("Cannot connect to CDP for channel page visit")
            return "GARBAGE"

        channel_tab = None
        try:
            channel_tab = cdp.new_tab(channel_url)
            for _ in range(20):
                if (channel_tab.js("return document.readyState") or "") == "complete":
                    break
                time.sleep(2)
            time.sleep(8)

            # Universal channel header
            channel_header = channel_tab.js('''
                var h = document.querySelector("yt-page-header-view-model");
                return h ? h.textContent.trim().substring(0, 1000) : "";
            ''') or ""

            sections = json.loads(channel_tab.js('''
                var shelves = document.querySelectorAll("ytd-item-section-renderer, ytd-shelf-renderer");
                var results = [];
                for (var shelf of shelves) {
                    var title = shelf.querySelector("#title, h2");
                    var videos = shelf.querySelectorAll("ytd-grid-video-renderer #video-title, a#video-title-link");
                    var vids = [];
                    for (var v of videos) vids.push(v.textContent.trim().substring(0, 80));
                    if (vids.length > 0)
                        results.push({section: title ? title.textContent.trim() : "untitled", videos: vids.slice(0, 8)});
                }
                return JSON.stringify(results);
            ''') or '[]')

            tabs_list = json.loads(channel_tab.js('''
                var chips = document.querySelectorAll("yt-tab-shape");
                var r = []; for (var c of chips) r.push(c.textContent.trim()); return JSON.stringify(r);
            ''') or '[]')

        except Exception as e:
            logger.error(f"    Channel page extraction failed: {e}")
            return "GARBAGE"

        finally:
            if channel_tab:
                try:
                    cdp.close_tab(channel_tab)
                except Exception:
                    pass

        # Build prompt and send via inference service
        section_text = ""
        for s in sections[:5]:
            section_text += f"\n  Section: {s['section']}\n"
            for v in s['videos'][:5]:
                section_text += f"    - {v}\n"

        prompt = f"""Evaluate this YouTube channel — is it LEGITIMATE or GARBAGE (live stream factory, looping content, engagement bait)?

CHANNEL HEADER (raw from YouTube page):
{channel_header}

CHANNEL TABS: {', '.join(tabs_list)}

Content sections:{section_text if section_text else ' (none visible)'}

Respond with EXACTLY one line:
VERDICT: GARBAGE — reason
or
VERDICT: KEEP — reason"""

        response = self._chatgpt_request(prompt)
        if not response:
            return "GARBAGE"

        resp_upper = response.upper()

        # Try JSON first
        try:
            parsed = json.loads(response)
            verdict = (parsed.get("verdict") or parsed.get("VERDICT") or "").upper().strip()
            if verdict == "GARBAGE":
                return "GARBAGE"
            if verdict in ("KEEP", "LEGITIMATE"):
                return "KEEP"
        except (json.JSONDecodeError, AttributeError):
            pass

        if "GARBAGE" in resp_upper:
            return "GARBAGE"
        if "KEEP" in resp_upper or "LEGITIMATE" in resp_upper:
            return "KEEP"
        return "GARBAGE"  # conservative default

    # ── Dashboard / Abstract methods ────────────────────────────

    def should_stop_collecting(self, item: dict, tracker=None) -> bool:
        """TIME_LIMIT stop strategy — handled by base class."""
        return False

    def get_configurable_settings(self) -> list:
        return [
            SkillSetting(
                key="backfill_depth_days",
                label="Backfill Depth",
                setting_type="select",
                default="30",
                options=[
                    {"value": "7", "label": "1 week"},
                    {"value": "30", "label": "1 month"},
                    {"value": "90", "label": "3 months"},
                    {"value": "180", "label": "6 months"},
                    {"value": "365", "label": "1 year"},
                    {"value": "730", "label": "2 years"},
                    {"value": "1095", "label": "3 years"},
                ],
                description="How far back to collect activity history",
            ),
        ]

    def get_stats(self, conn) -> list[dict]:
        """Stats for the dashboard home widget."""
        days = conn.execute(
            "SELECT COUNT(*) as c FROM collection_days WHERE status='complete'"
        ).fetchone()["c"]
        total = conn.execute("SELECT COUNT(*) as c FROM activities").fetchone()["c"]
        watched = conn.execute(
            "SELECT COUNT(*) as c FROM activities WHERE entry_type='Watched'"
        ).fetchone()["c"]
        visited = conn.execute(
            "SELECT COUNT(*) as c FROM activities WHERE entry_type='Visited'"
        ).fetchone()["c"]
        searched = conn.execute(
            "SELECT COUNT(*) as c FROM activities WHERE entry_type='Searched'"
        ).fetchone()["c"]
        used = conn.execute(
            "SELECT COUNT(*) as c FROM activities WHERE entry_type='Used'"
        ).fetchone()["c"]
        # Backfill progress
        depth = conn.execute(
            "SELECT value FROM settings WHERE key='backfill_depth_days'"
        ).fetchone()
        depth_days = int(depth["value"]) if depth else 30
        backfill_done = conn.execute(
            "SELECT value FROM settings WHERE key='backfill_completed'"
        ).fetchone()
        is_done = backfill_done and backfill_done["value"] == "true"

        return [
            {"label": "Days Collected", "value": days},
            {"label": "Total Entries", "value": total},
            {"label": "YouTube", "value": watched},
            {"label": "Chrome", "value": visited},
            {"label": "Searches", "value": searched},
            {"label": "Apps", "value": used},
            {"label": "Backfill Depth", "value": f"{depth_days}d"},
            {"label": "Backfill", "value": "Complete" if is_done else "In Progress"},
        ]

    def get_widgets(self) -> list:
        return [
            WidgetDefinition(
                name="stats",
                title="Google Activity",
                display_type="stat_cards",
                data_query="",
                refresh_seconds=300,
                size="medium",
                click_action="skill_page",
            ),
            WidgetDefinition(
                name="recent_activity",
                title="Recent Activity",
                display_type="timeline",
                data_query=(
                    "SELECT entry_type, timestamp, "
                    "COALESCE(video_title, page_title, query_text, app_name, content_text, 'Unknown') as title, "
                    "source_app, watch_pct "
                    "FROM activities ORDER BY timestamp DESC LIMIT 10"
                ),
                refresh_seconds=300,
                size="medium",
                click_action="skill_page#activity",
            ),
        ]

    def get_page_sections(self) -> list:
        return [
            PageSection(
                name="stats",
                title="Overview",
                display_type="stat_cards",
                data_query="",
                position=0,
            ),
            PageSection(
                name="collection_progress",
                title="Collection Progress",
                display_type="table",
                data_query=(
                    "SELECT date, total_count, watched_count, visited_count, "
                    "searched_count, used_count, viewed_count, unknown_count, status "
                    "FROM collection_days ORDER BY date DESC"
                ),
                position=1,
                paginated=True,
                page_size=30,
            ),
            PageSection(
                name="activity",
                title="All Activity",
                display_type="timeline",
                data_query=(
                    "SELECT entry_type, timestamp, "
                    "COALESCE(video_title, page_title, query_text, app_name, content_text, 'Unknown') as title, "
                    "source_app, watch_pct, channel, domain, search_type "
                    "FROM activities ORDER BY timestamp DESC"
                ),
                position=2,
                paginated=True,
                page_size=50,
            ),
            PageSection(
                name="settings",
                title="Settings",
                display_type="key_value",
                data_query=(
                    "SELECT key, value, updated_at FROM settings ORDER BY key"
                ),
                position=3,
            ),
        ]

    def get_notification_rules(self) -> list:
        return [
            NotificationRule(
                event="after_collection",
                condition="items_new > 0",
                title_template="{items_new} new Google activities collected",
                message_template="Collected from {date}",
            ),
        ]

    # ── Entry Tagging (3D) ──────────────────────────────────────

    def _tag_entries(self, conn, target_date: str) -> int:
        """Tag all entries for a given date based on type and engagement.

        Tags (for YouTube Watched):
          GARBAGE     — is_garbage=1 (overrides everything)
          SHORT-WATCHED — duration ≤60s and watch_pct ≥ 90
          SHORT-SKIPPED — duration ≤60s and watch_pct < 90
          STRONG      — duration >60s and watch_pct = 100
          MODERATE    — duration >60s and 25 ≤ watch_pct < 100
          SKIP        — duration >60s and watch_pct < 25

        Tags (for other types):
          SEARCH      — entry_type='Searched'
          VISIT       — entry_type='Visited'
          APP-USE     — entry_type='Used'
          VIEW        — entry_type='Viewed'
          OTHER       — anything else (Unknown, notifications, maps)
        """
        # First: tag garbage entries (overrides all)
        conn.execute(
            "UPDATE activities SET tag = 'GARBAGE' "
            "WHERE date = ? AND is_garbage = 1 AND (tag IS NULL OR tag != 'GARBAGE')",
            (target_date,)
        )

        # YouTube Watched — shorts (≤60s)
        conn.execute(
            "UPDATE activities SET tag = 'SHORT-WATCHED' "
            "WHERE date = ? AND entry_type = 'Watched' AND is_garbage = 0 "
            "AND duration_secs IS NOT NULL AND duration_secs <= 60 AND watch_pct >= 90",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'SHORT-SKIPPED' "
            "WHERE date = ? AND entry_type = 'Watched' AND is_garbage = 0 "
            "AND duration_secs IS NOT NULL AND duration_secs <= 60 AND watch_pct < 90",
            (target_date,)
        )

        # YouTube Watched — regular videos (>60s)
        conn.execute(
            "UPDATE activities SET tag = 'STRONG' "
            "WHERE date = ? AND entry_type = 'Watched' AND is_garbage = 0 "
            "AND (duration_secs IS NULL OR duration_secs > 60) AND watch_pct = 100",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'MODERATE' "
            "WHERE date = ? AND entry_type = 'Watched' AND is_garbage = 0 "
            "AND (duration_secs IS NULL OR duration_secs > 60) "
            "AND watch_pct >= 25 AND watch_pct < 100",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'SKIP' "
            "WHERE date = ? AND entry_type = 'Watched' AND is_garbage = 0 "
            "AND (duration_secs IS NULL OR duration_secs > 60) AND watch_pct < 25",
            (target_date,)
        )

        # Non-YouTube types
        conn.execute(
            "UPDATE activities SET tag = 'SEARCH' "
            "WHERE date = ? AND entry_type = 'Searched' AND tag IS NULL",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'VISIT' "
            "WHERE date = ? AND entry_type = 'Visited' AND tag IS NULL",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'APP-USE' "
            "WHERE date = ? AND entry_type = 'Used' AND tag IS NULL",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'VIEW' "
            "WHERE date = ? AND entry_type = 'Viewed' AND tag IS NULL",
            (target_date,)
        )
        conn.execute(
            "UPDATE activities SET tag = 'OTHER' "
            "WHERE date = ? AND tag IS NULL",
            (target_date,)
        )

        # Compute actual minutes watched for YouTube videos
        conn.execute(
            "UPDATE activities SET actual_minutes_watched = "
            "ROUND(duration_secs * watch_pct / 100.0 / 60.0, 1) "
            "WHERE date = ? AND entry_type = 'Watched' "
            "AND duration_secs IS NOT NULL AND watch_pct IS NOT NULL "
            "AND actual_minutes_watched IS NULL",
            (target_date,)
        )

        # Tag time context based on hour
        for hour_range, label in [
            ("('00','01','02','03','04','05')", 'late-night'),
            ("('06','07','08')", 'early-morning'),
            ("('09','10','11')", 'morning'),
            ("('12','13')", 'lunch'),
            ("('14','15','16','17')", 'afternoon'),
            ("('18','19','20')", 'evening'),
            ("('21','22','23')", 'night'),
        ]:
            conn.execute(
                f"UPDATE activities SET time_context = '{label}' "
                f"WHERE date = ? AND time_context IS NULL "
                f"AND SUBSTR(timestamp, 12, 2) IN {hour_range}",
                (target_date,)
            )

        conn.commit()

        # Return count of tagged entries
        row = conn.execute(
            "SELECT COUNT(*) FROM activities WHERE date = ? AND tag IS NOT NULL",
            (target_date,)
        ).fetchone()
        return row[0] if row else 0

    SESSION_GAP_MINUTES = 15
    SESSION_MIN_ENTRIES = 3

    def _detect_sessions(self, conn, target_date: str) -> int:
        """Cluster entries into sessions based on time gaps.

        Rules:
          - Gap of ≥15 min between consecutive entries = new session.
          - Sessions with < 3 entries are merged into the nearest real session.
          - session_id is per-day, starting from 1.

        Returns number of sessions detected.
        """
        rows = conn.execute(
            "SELECT id, timestamp FROM activities WHERE date = ? ORDER BY timestamp",
            (target_date,)
        ).fetchall()

        if not rows:
            return 0

        # Build raw sessions by time gap
        raw_sessions = []  # list of lists of (id, datetime)
        current = []
        prev_ts = None

        for row_id, ts_str in rows:
            try:
                ts = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue

            if prev_ts and (ts - prev_ts).total_seconds() / 60 >= self.SESSION_GAP_MINUTES:
                raw_sessions.append(current)
                current = []

            current.append((row_id, ts))
            prev_ts = ts

        if current:
            raw_sessions.append(current)

        # Separate into real sessions (≥3 entries) and orphans (<3)
        real = []
        orphans = []
        for s in raw_sessions:
            if len(s) >= self.SESSION_MIN_ENTRIES:
                real.append(list(s))
            else:
                orphans.append(s)

        # Merge orphans into nearest real session
        for orph in orphans:
            if not real:
                # No real sessions at all — make orphan a session
                real.append(list(orph))
                continue

            orph_time = orph[0][1]
            best_dist = float('inf')
            best_idx = 0
            for j, m in enumerate(real):
                dist_start = abs((orph_time - m[0][1]).total_seconds())
                dist_end = abs((orph_time - m[-1][1]).total_seconds())
                dist = min(dist_start, dist_end)
                if dist < best_dist:
                    best_dist = dist
                    best_idx = j

            real[best_idx].extend(orph)
            real[best_idx].sort(key=lambda x: x[1])

        # Assign session_ids (1-based, ordered by time)
        real.sort(key=lambda s: s[0][1])

        for session_idx, session in enumerate(real, start=1):
            ids = [entry[0] for entry in session]
            placeholders = ",".join(["?"] * len(ids))
            conn.execute(
                f"UPDATE activities SET session_id = ? WHERE id IN ({placeholders})",
                [session_idx] + ids
            )

        conn.commit()
        return len(real)

    def _update_skip_rate(self, conn, target_date: str) -> None:
        """Update running skip rate baseline with today's shorts data.

        Stores cumulative counts in settings so we never need to scan all data.
        avg_skip_rate = skip_rate_skipped / skip_rate_total * 100
        """
        # Count today's shorts
        today_total = conn.execute(
            "SELECT COUNT(*) FROM activities "
            "WHERE date = ? AND tag IN ('SHORT-WATCHED', 'SHORT-SKIPPED')",
            (target_date,)
        ).fetchone()[0]
        today_skipped = conn.execute(
            "SELECT COUNT(*) FROM activities "
            "WHERE date = ? AND tag = 'SHORT-SKIPPED'",
            (target_date,)
        ).fetchone()[0]

        if today_total == 0:
            return

        # Get current running totals
        row_total = conn.execute(
            "SELECT value FROM settings WHERE key = 'skip_rate_total'"
        ).fetchone()
        row_skipped = conn.execute(
            "SELECT value FROM settings WHERE key = 'skip_rate_skipped'"
        ).fetchone()

        running_total = int(row_total[0]) if row_total else 0
        running_skipped = int(row_skipped[0]) if row_skipped else 0

        # Add today's counts
        running_total += today_total
        running_skipped += today_skipped
        avg_rate = round(running_skipped / running_total * 100, 1) if running_total else 0

        # Store all three
        for key, val in [
            ('skip_rate_total', str(running_total)),
            ('skip_rate_skipped', str(running_skipped)),
            ('avg_skip_rate', str(avg_rate)),
        ]:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value, updated_at) "
                "VALUES (?, ?, datetime('now'))",
                (key, val)
            )
        conn.commit()

    def _backfill_channel_summaries(self, conn) -> int:
        """Copy channel analysis_summary onto activity entries from analyzed channels.

        Only updates entries where channel_summary is NULL and channel has been analyzed.
        """
        cursor = conn.execute("""
            UPDATE activities
            SET channel_summary = (
                SELECT c.analysis_summary
                FROM channels c
                WHERE c.channel_url = activities.channel_url
                AND c.analysis_summary IS NOT NULL
            )
            WHERE entry_type = 'Watched'
            AND channel_url IS NOT NULL
            AND channel_summary IS NULL
            AND channel_url IN (SELECT channel_url FROM channels WHERE analysis_summary IS NOT NULL)
        """)
        conn.commit()
        return cursor.rowcount

    def _detect_multi_session(self, conn) -> int:
        """Detect videos 5+ min watched across multiple days.

        Only videos ≥300s (5 min) qualify — shorts and short clips repeat
        via the algorithm, not by user choice. A 5+ min video appearing on
        multiple days means the user genuinely invested time across sessions.
        """
        # Find video_ids that appear on multiple dates (5+ min videos only)
        multi_videos = conn.execute("""
            SELECT video_id
            FROM activities
            WHERE entry_type = 'Watched'
            AND video_id IS NOT NULL
            AND watch_pct > 10
            AND is_garbage = 0
            AND duration_secs IS NOT NULL AND duration_secs >= 300
            GROUP BY video_id
            HAVING COUNT(DISTINCT date) >= 2
        """).fetchall()

        if not multi_videos:
            return 0

        video_ids = [r[0] for r in multi_videos]

        # Mark all entries for these video_ids
        placeholders = ",".join(["?"] * len(video_ids))
        cursor = conn.execute(
            f"UPDATE activities SET multi_session = 1 "
            f"WHERE video_id IN ({placeholders}) AND multi_session = 0",
            video_ids
        )
        conn.commit()
        return cursor.rowcount

    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        """Search across all activity types."""
        results = []
        rows = conn.execute(
            """SELECT entry_type, timestamp, video_title, page_title, query_text,
                      app_name, content_text, source_app
               FROM activities
               WHERE video_title LIKE ? OR page_title LIKE ? OR query_text LIKE ?
                  OR app_name LIKE ? OR content_text LIKE ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            tuple([f"%{query}%"] * 5 + [limit])
        ).fetchall()
        for r in rows:
            title = r[2] or r[3] or r[4] or r[5] or r[6] or "Unknown"
            results.append({
                "title": title,
                "subtitle": f"{r[0]} via {r[7] or 'unknown'}",
                "timestamp": r[1],
            })
        return results
