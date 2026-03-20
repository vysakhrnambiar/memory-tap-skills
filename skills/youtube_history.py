"""
YouTube History Skill — collects watch history, descriptions, top comments, shorts.

CHANGELOG:
  v0.4.5 (2026-03-19): Fix description extraction — poll #expanded selector + snippet fallback
  v0.4.0 (2026-03-19):
    - Full Phase 1+2 rewrite with verified CDP selectors
    - Phase 1A: clicks "Videos" tab, extracts from yt-lockup-view-model
    - Phase 1B: clicks "Shorts" tab, extracts from ytm-shorts-lockup-view-model carousel
    - Phase 2: new tab per video with &t= parameter (video loads paused)
    - Page load retry: poll for target element, retry navigation, check internet
    - _ensure_chips_visible(): scroll to top, verify Y positions before clicking
    - Smart scroll backoff: 2s -> 3s -> 5s, 3 misses = stop
    - Top 5 channels widget (50%+ watch completion)
    - Shorts now have titles + views extracted from carousel
    - "Collected at" timestamp in widget data
    - Extraction JS split into smaller calls (was timing out with 300+ containers)
  v0.3.2 (2026-03-17): YouTube Phase 2 date stop fix + title load retry
  v0.3.1 (2026-03-16): Incremental save, sidebar restoration, crash retry
  v0.2.0 (2026-03-14): Initial version

Verified selectors via CDP probe (2026-03-18):
- History page chips: .ytChipBarViewModelChipWrapper
- Videos tab: yt-lockup-view-model (content-id on inner div)
- Shorts tab: ytm-shorts-lockup-view-model (note ytm prefix)
- Video page: h1 yt-formatted-string, #expand + description, comments
- &t= parameter verified working on CDP Chrome (video loads paused)

__version__ = "0.4.6"
"""
__version__ = "0.4.6"

import json
import logging
import random
import re
import time
from datetime import datetime, timedelta

logger = logging.getLogger("memory_tap.skill.youtube")

from src.skills.base import (
    BaseSkill, SkillManifest, CollectResult, StopStrategy, RunLimits,
)
from src.skills.ui_manifest import WidgetDefinition, PageSection, NotificationRule
from src.cdp_client import CDPTab, CDPClient
from src.db.sync_tracker import SyncTracker
from src.human import scroll_slowly, wait_human, move_mouse, click_at


# ── Date parsing ──────────────────────────────────────────────────────

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_ABBREVS = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def _parse_date_group(date_group: str) -> str | None:
    """Convert YouTube date group header to ISO date (YYYY-MM-DD).

    Patterns (verified via CDP probe 2026-03-15):
      "Today"       -> today
      "Yesterday"   -> yesterday
      "Friday"      -> last Friday
      "Mar 8"       -> 2026-03-08 (current year)
    """
    if not date_group:
        return None

    today = datetime.now().date()

    if date_group.strip().lower() == "today":
        return today.isoformat()
    if date_group.strip().lower() == "yesterday":
        return (today - timedelta(days=1)).isoformat()

    # Day name: Monday, Tuesday, ...
    for i, day_name in enumerate(DAY_NAMES):
        if date_group.strip() == day_name:
            # Calculate how many days back
            current_weekday = today.weekday()  # 0=Monday
            target_weekday = i
            days_back = (current_weekday - target_weekday) % 7
            if days_back == 0:
                days_back = 7  # "Monday" means LAST Monday, not today
            return (today - timedelta(days=days_back)).isoformat()

    # "Mon DD" format: Mar 8, Feb 22
    match = re.match(r'(\w{3})\s+(\d{1,2})', date_group.strip())
    if match:
        month_str, day_str = match.group(1), match.group(2)
        month = MONTH_ABBREVS.get(month_str)
        if month:
            try:
                d = datetime(today.year, month, int(day_str)).date()
                # If the date is in the future, it's from last year
                if d > today:
                    d = datetime(today.year - 1, month, int(day_str)).date()
                return d.isoformat()
            except ValueError:
                pass

    logger.warning("Could not parse date group: '%s'", date_group)
    return None


def _extract_video_id(href: str) -> str | None:
    """Extract video ID from YouTube URL."""
    if not href:
        return None
    match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})', href)
    return match.group(1) if match else None


def _extract_short_id(href: str) -> str | None:
    """Extract short ID from YouTube shorts URL."""
    if not href:
        return None
    match = re.search(r'/shorts/([a-zA-Z0-9_-]{11})', href)
    return match.group(1) if match else None


def _extract_resume_seconds(href: str) -> int:
    """Extract resume time from &t=200s parameter."""
    if not href:
        return 0
    match = re.search(r'[?&]t=(\d+)s?', href)
    return int(match.group(1)) if match else 0


def _duration_to_seconds(duration_str: str) -> int:
    """Convert "29:03" or "1:02:15" to seconds."""
    if not duration_str:
        return 0
    parts = duration_str.strip().split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        pass
    return 0


# ── Skill class ───────────────────────────────────────────────────────

class YouTubeHistorySkill(BaseSkill):
    """Collects YouTube watch history — videos, shorts, descriptions, comments."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="youtube_history",
            version=__version__,
            target_url="https://www.youtube.com/feed/history",
            description="Collects YouTube watch history — titles, descriptions, top comments, watch progress",
            auth_provider="google",
            schedule_hours=3,
            login_url="https://accounts.google.com/ServiceLogin?service=youtube",
            max_items_first_run=100,
            max_items_per_run=0,  # unlimited for subsequent
            max_minutes_per_run=30,
        )

    @property
    def stop_strategy(self) -> StopStrategy:
        return StopStrategy.DATE_GROUP

    def _main_table_name(self) -> str:
        return "videos"

    # ── Schema ────────────────────────────────────────────────────

    def create_schema(self, conn) -> None:
        """Create YouTube-specific tables in skill's own DB."""
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS videos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT UNIQUE NOT NULL,
                title TEXT NOT NULL DEFAULT '',
                channel TEXT DEFAULT '',
                channel_url TEXT DEFAULT '',
                url TEXT NOT NULL,
                description TEXT DEFAULT '',
                duration TEXT DEFAULT '',
                duration_seconds INTEGER DEFAULT 0,
                top_comment TEXT DEFAULT '',
                top_comment_author TEXT DEFAULT '',
                views_text TEXT DEFAULT '',
                publish_date TEXT DEFAULT '',
                watch_percent INTEGER DEFAULT 0,
                resume_time_seconds INTEGER DEFAULT 0,
                date_group TEXT DEFAULT '',
                watched_date TEXT DEFAULT '',
                content_type TEXT NOT NULL DEFAULT 'video',
                dismissed_unfinished INTEGER NOT NULL DEFAULT 0,
                synced_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS shorts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                short_id TEXT UNIQUE NOT NULL,
                title TEXT DEFAULT '',
                url TEXT NOT NULL,
                views_text TEXT DEFAULT '',
                date_group TEXT DEFAULT '',
                watched_date TEXT DEFAULT '',
                synced_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS collection_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS videos_fts USING fts5(
                title, description, top_comment, channel,
                content='videos', content_rowid='id',
                tokenize='porter unicode61'
            );

            -- Triggers to keep FTS in sync
            CREATE TRIGGER IF NOT EXISTS videos_ai AFTER INSERT ON videos BEGIN
                INSERT INTO videos_fts(rowid, title, description, top_comment, channel)
                VALUES (new.id, new.title, new.description, new.top_comment, new.channel);
            END;

            CREATE TRIGGER IF NOT EXISTS videos_ad AFTER DELETE ON videos BEGIN
                INSERT INTO videos_fts(videos_fts, rowid, title, description, top_comment, channel)
                VALUES ('delete', old.id, old.title, old.description, old.top_comment, old.channel);
            END;

            CREATE TRIGGER IF NOT EXISTS videos_au AFTER UPDATE ON videos BEGIN
                INSERT INTO videos_fts(videos_fts, rowid, title, description, top_comment, channel)
                VALUES ('delete', old.id, old.title, old.description, old.top_comment, old.channel);
                INSERT INTO videos_fts(rowid, title, description, top_comment, channel)
                VALUES (new.id, new.title, new.description, new.top_comment, new.channel);
            END;

            CREATE INDEX IF NOT EXISTS idx_videos_watched_date ON videos(watched_date);
            CREATE INDEX IF NOT EXISTS idx_videos_watch_percent ON videos(watch_percent);
            CREATE INDEX IF NOT EXISTS idx_shorts_watched_date ON shorts(watched_date);
        """)
        # Seed last_date_group to today so first run only collects today's videos
        from datetime import datetime as _dt
        today = _dt.now().date().isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO collection_state (key, value) VALUES ('last_date_group', ?)",
            (today,),
        )
        conn.commit()

    def migrate_schema(self, conn, old_version: str, new_version: str) -> None:
        """Handle schema changes between versions.

        Add ALTER TABLE / CREATE INDEX statements here when the schema changes.
        For code-only version bumps (no schema change), this is a no-op.

        Example for future use:
            if old_version < "0.5.0":
                conn.execute("ALTER TABLE videos ADD COLUMN category TEXT DEFAULT ''")
                conn.commit()
        """
        pass

    # ── Login detection ───────────────────────────────────────────

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into YouTube.

        Verified detection (2026-03-14 CDP probe):
        - Logged in:  button#avatar-btn exists, title contains watch count,
                      no "Sign in" text visible
        - Not logged in: no avatar, title is just "YouTube", "Sign in" text
        """
        tab.navigate("https://www.youtube.com/feed/history")
        wait_human(3, 5)

        # Primary: avatar button
        avatar = tab.query_selector("button#avatar-btn")
        if avatar and avatar.get("visible"):
            logger.info("YouTube: logged in (avatar button found)")
            return True

        # Secondary: page title contains "Watch history" (loads only when authenticated)
        title = tab.get_title() or ""
        if "Watch history" in title:
            logger.info("YouTube: logged in (page title contains 'Watch history')")
            return True

        # Tertiary: subscriptions visible in sidebar (only when logged in)
        subs = tab.query_selector("[aria-label='Subscriptions']")
        if subs:
            logger.info("YouTube: logged in (Subscriptions sidebar found)")
            return True

        # Negative: "Sign in" text visible
        if tab.has_text("Sign in"):
            logger.info("YouTube: not logged in (Sign in text visible)")
            return False

        # Still unclear — wait longer for slow-loading avatar and retry
        logger.info("YouTube: login unclear, waiting extra time for avatar...")
        wait_human(3, 5)
        avatar = tab.query_selector("button#avatar-btn")
        if avatar and avatar.get("visible"):
            logger.info("YouTube: logged in (avatar found after extra wait)")
            return True

        logger.info("YouTube: login unclear, treating as not logged in")
        return False

    # ── Stop signal ───────────────────────────────────────────────

    def should_stop_collecting(self, item: dict, tracker: SyncTracker) -> bool:
        """DATE_GROUP stop: stop when date_group <= last collected AND item is known.

        We stop when we see 3 consecutive known items within a date_group
        that we've already fully collected in a previous run.
        """
        # Get the state from skill DB (via tracker)
        conn = tracker._skill_conn
        row = conn.execute(
            "SELECT value FROM collection_state WHERE key = 'last_date_group'"
        ).fetchone()
        if not row:
            return False  # First run — never stop via this mechanism

        last_date = row["value"]
        item_date = item.get("watched_date", "")

        if not item_date or not last_date:
            return False

        # If this item's date is older than our last collected date, stop
        if item_date < last_date:
            logger.info("Stop: item date %s < last collected %s", item_date, last_date)
            return True

        return False

    # ── Page load helpers ──────────────────────────────────────

    def _navigate_and_verify(self, tab: CDPTab, url: str,
                              target_selector: str, page_name: str,
                              max_attempts: int = 2) -> bool:
        """Navigate to URL and verify target element exists.

        1. Navigate, wait for readyState=complete
        2. Poll every 2s for target element (up to 30s)
        3. Not found → retry navigation
        4. Still not found → check internet
        5. Internet down → log, return False
        6. Internet up but element missing → screenshot, return False
        """
        for attempt in range(max_attempts):
            logger.info("Navigating to %s (attempt %d/%d)", page_name, attempt + 1, max_attempts)
            tab.navigate(url)

            # Wait for readyState=complete
            for _ in range(20):
                ready = tab.js("return document.readyState") or ""
                if ready == "complete":
                    break
                time.sleep(1)

            # Poll for target element every 2s, up to 30s
            for poll in range(15):
                count = tab.js(
                    f'return document.querySelectorAll("{target_selector}").length'
                ) or 0
                try:
                    count = int(count)
                except (ValueError, TypeError):
                    count = 0
                if count > 0:
                    logger.info("%s loaded: %d '%s' elements found",
                                page_name, count, target_selector)
                    return True
                time.sleep(2)

            logger.warning("%s: '%s' not found after 30s (attempt %d)",
                           page_name, target_selector, attempt + 1)

        # All attempts failed — check why
        try:
            import requests as _req
            _req.get("https://www.google.com/generate_204", timeout=5)
            logger.error("%s: internet OK but '%s' not found — possible site change",
                         page_name, target_selector)
        except Exception:
            logger.error("%s: no internet connection — skipping", page_name)

        return False

    # ── Chip bar helpers ──────────────────────────────────────────

    def _ensure_chips_visible(self, tab: CDPTab) -> list[dict]:
        """Scroll to top and read chip bar positions.

        Returns list of {text, x, y} for each chip.
        Retries once if chips are not in expected Y range (0-500).
        """
        tab.js("window.scrollTo(0, 0)")
        wait_human(1, 1.5)

        for attempt in range(2):
            chips_raw = tab.js("""
                return (function() {
                    var wrappers = document.querySelectorAll('.ytChipBarViewModelChipWrapper');
                    var out = [];
                    for (var i = 0; i < wrappers.length; i++) {
                        var el = wrappers[i];
                        var r = el.getBoundingClientRect();
                        var text = el.textContent.trim();
                        if (text && r.width > 0) {
                            out.push({
                                text: text,
                                x: Math.round(r.x + r.width / 2),
                                y: Math.round(r.y + r.height / 2)
                            });
                        }
                    }
                    return JSON.stringify(out);
                })();
            """)

            chips = []
            if chips_raw:
                try:
                    chips = json.loads(chips_raw)
                except (json.JSONDecodeError, TypeError):
                    pass

            if not chips:
                logger.warning("_ensure_chips_visible: no chips found (attempt %d)", attempt + 1)
                if attempt == 0:
                    tab.js("window.scrollTo(0, 0)")
                    wait_human(1.5, 2)
                continue

            # Verify all Y coordinates are in visible range
            all_ok = all(0 < c["y"] < 500 for c in chips)
            if all_ok:
                logger.info("Chips visible: %s", [c["text"] for c in chips])
                return chips

            # Chips out of range — scroll to top and retry
            logger.warning("Chips out of Y range, scrolling to top (attempt %d)", attempt + 1)
            tab.js("window.scrollTo(0, 0)")
            wait_human(1.5, 2)

        logger.warning("_ensure_chips_visible: returning chips despite position issues")
        return chips

    def _click_chip(self, tab: CDPTab, chips: list[dict], chip_name: str) -> bool:
        """Find and click a chip by name using CDP mouse events.

        Returns True if the chip was found and clicked.
        """
        target = None
        for c in chips:
            if c["text"].strip().lower() == chip_name.strip().lower():
                target = c
                break

        if not target:
            logger.warning("Chip '%s' not found in: %s", chip_name, [c["text"] for c in chips])
            return False

        click_at(tab, target["x"], target["y"])
        return True

    def _verify_chip_selected(self, tab: CDPTab, expected_text: str) -> bool:
        """Verify that the expected chip tab is selected."""
        selected = tab.js("""
            return (function() {
                var chips = document.querySelectorAll('.ytChipBarViewModelChipWrapper');
                for (var i = 0; i < chips.length; i++) {
                    var selected = chips[i].querySelector('[aria-selected="true"]');
                    if (selected) return chips[i].textContent.trim();
                }
                return '';
            })();
        """) or ""
        return selected.strip().lower() == expected_text.strip().lower()

    # ── Collection ────────────────────────────────────────────────

    def collect(self, tab: CDPTab, tracker: SyncTracker,
                limits: RunLimits) -> CollectResult:
        """Phase 1A: Videos, Phase 1B: Shorts, Phase 1C: Restore,
        Phase 2: Visit each new video page for details."""
        result = CollectResult()
        conn = tracker._skill_conn

        # Get CDPClient for Phase 2 (new tab creation)
        cdp_port = int(tab._cdp_base_url.split(':')[-1])
        _cdp = CDPClient(port=cdp_port)

        # Get last collected date for stop signal
        row = conn.execute(
            "SELECT value FROM collection_state WHERE key = 'last_date_group'"
        ).fetchone()
        last_collected_date = row["value"] if row else None

        # Phase 1A: Collect videos from "Videos" tab
        if not self._navigate_and_verify(tab, "https://www.youtube.com/feed/history",
                                          ".ytChipBarViewModelChipWrapper", "history page"):
            logger.error("Failed to load YouTube history page after retries")
            return result

        videos, latest_date_group = self._collect_history_list(
            tab, conn, limits, last_collected_date
        )

        # Phase 1B: Collect shorts from "Shorts" tab
        shorts_new_count = self._collect_shorts(tab, conn)

        # Phase 1C: Restore — click "All" chip
        self._restore_all_chip(tab)

        result.items_found = len(videos) + shorts_new_count

        # Phase 2: Visit video pages for details (new tab per video)
        # Process BOTH new videos from Phase 1A AND existing stubs from DB
        new_count = 0
        updated_count = 0

        # First: process new videos from Phase 1A
        for video in videos:
            if limits.time_exceeded:
                logger.info("Stopping video visits: time limit reached")
                break

            existing = conn.execute(
                "SELECT id, title, description FROM videos WHERE video_id = ?",
                (video["video_id"],)
            ).fetchone()

            if existing and existing["title"] and existing["description"]:
                if video.get("watch_percent") is not None:
                    conn.execute(
                        "UPDATE videos SET watch_percent = ?, resume_time_seconds = ?, "
                        "updated_at = datetime('now') WHERE video_id = ?",
                        (video["watch_percent"], video["resume_time_seconds"],
                         video["video_id"]),
                    )
                    conn.commit()
                    updated_count += 1
                continue

            try:
                details = self._visit_video_page(_cdp, video)
                self._save_video(conn, video, details)
                new_count += 1
                limits.item_done()
                logger.info("Collected video %d: %s",
                            new_count, details.get("title", "")[:60])
            except Exception as e:
                logger.warning("Failed video %s: %s", video["video_id"], e)
            wait_human(2, 4)

        # Second: fill in stubs from DB that don't have full details yet
        if not limits.time_exceeded:
            stubs = conn.execute(
                "SELECT video_id, url, watch_percent, resume_time_seconds, "
                "duration_seconds FROM videos "
                "WHERE (title = '' OR title IS NULL OR description = '' OR description IS NULL) "
                "ORDER BY rowid LIMIT 200"
            ).fetchall()
            if stubs:
                logger.info("Phase 2 backfill: %d stubs need details", len(stubs))
            for stub in stubs:
                if limits.time_exceeded:
                    logger.info("Stopping backfill: time limit reached")
                    break
                video = {
                    "video_id": stub["video_id"],
                    "url": stub["url"],
                    "watch_percent": stub["watch_percent"] or 0,
                    "resume_time_seconds": stub["resume_time_seconds"] or 0,
                    "duration_seconds": stub["duration_seconds"] or 0,
                }
                try:
                    details = self._visit_video_page(_cdp, video)
                    self._save_video(conn, video, details)
                    new_count += 1
                    logger.info("Backfill video %d: %s",
                                new_count, details.get("title", "")[:60])
                except Exception as e:
                    logger.warning("Failed backfill %s: %s", video["video_id"], e)
                wait_human(2, 4)

        new_count += shorts_new_count

        # Update collection state
        if latest_date_group:
            watched_date = _parse_date_group(latest_date_group)
            if watched_date:
                conn.execute(
                    "INSERT OR REPLACE INTO collection_state (key, value, updated_at) "
                    "VALUES ('last_date_group', ?, datetime('now'))",
                    (watched_date,),
                )
                conn.commit()

        result.items_new = new_count
        result.items_updated = updated_count
        return result

    # ── Phase 1A: Videos collection ───────────────────────────────

    def _collect_history_list(self, tab: CDPTab, conn, limits: RunLimits,
                              last_collected_date: str | None) -> tuple[list[dict], str | None]:
        """Navigate to Videos chip, scroll and extract video stubs.

        Returns (videos_list, latest_date_group).
        """
        # Step 3: Ensure chips visible
        chips = self._ensure_chips_visible(tab)
        if not chips:
            logger.error("No chips found on history page — cannot filter to Videos")
            return [], None

        # Step 4: Click "Videos" chip
        clicked = self._click_chip(tab, chips, "Videos")
        if not clicked:
            logger.error("Could not find 'Videos' chip")
            return [], None

        wait_human(3, 4)

        # Step 5: Verify "Videos" is selected
        if not self._verify_chip_selected(tab, "Videos"):
            logger.warning("Videos chip not selected after click — retrying")
            # Re-read positions and retry once
            chips = self._ensure_chips_visible(tab)
            clicked = self._click_chip(tab, chips, "Videos")
            if clicked:
                wait_human(3, 4)
            if not self._verify_chip_selected(tab, "Videos"):
                logger.error("Videos chip still not selected after retry — taking screenshot and stopping")
                tab.screenshot("_scripts/e2e_screenshots/videos_chip_fail.png")
                return [], None

        # Pre-load known video IDs from DB (for consecutive_known stop)
        known_ids = set()
        rows = conn.execute("SELECT video_id FROM videos").fetchall()
        for r in rows:
            known_ids.add(r["video_id"])
        logger.info("Pre-loaded %d known video IDs from DB", len(known_ids))

        # Step 7-10: Scroll and extract videos
        videos = []
        seen_video_ids = set()
        latest_date_group = None
        consecutive_misses = 0
        consecutive_known = 0
        prev_item_count = 0

        while True:
            if limits.time_exceeded:
                break

            # Extract videos (longer timeout — page can have 300+ containers)
            page_data = tab.js("""
                return (function() {
                    var containers = document.querySelectorAll('yt-lockup-view-model');
                    var vids = [];
                    for (var i = 0; i < containers.length; i++) {
                        var c = containers[i];
                        var vid_id = '';
                        var inner = c.querySelector('div[class*="content-id-"]');
                        if (inner) {
                            var m = (inner.getAttribute('class') || '').match(/content-id-([a-zA-Z0-9_-]+)/);
                            if (m) vid_id = m[1];
                        }
                        if (!vid_id) {
                            var link = c.querySelector('a[href*="/watch"]');
                            if (link) {
                                var hm = (link.getAttribute('href') || '').match(/[?&]v=([a-zA-Z0-9_-]+)/);
                                if (hm) vid_id = hm[1];
                            }
                        }
                        if (!vid_id) continue;
                        var titleEl = c.querySelector('h3 a span');
                        var metaEl = c.querySelector('yt-content-metadata-view-model');
                        var metaText = metaEl ? metaEl.textContent.trim() : '';
                        var mp = metaText.split(' \\u2022 ');
                        var durEl = c.querySelector('.yt-badge-shape__text');
                        var progEl = c.querySelector('.ytThumbnailOverlayProgressBarHostWatchedProgressBarSegment');
                        var wp = 0;
                        if (progEl) { var pm = (progEl.getAttribute('style') || '').match(/width:\\s*(\\d+)%/); if (pm) wp = parseInt(pm[1]); }
                        var aEl = c.querySelector('a[href*="/watch"]');
                        var href = aEl ? aEl.getAttribute('href') || '' : '';
                        var tm = href.match(/[?&]t=(\\d+)s?/);
                        vids.push({
                            video_id: vid_id,
                            title: titleEl ? titleEl.textContent.trim() : '',
                            channel: mp.length > 0 ? mp[0].trim() : '',
                            views_text: mp.length > 1 ? mp[1].trim() : '',
                            duration: durEl ? durEl.textContent.trim() : '',
                            watchPercent: wp,
                            href: href,
                            resumeTime: tm ? parseInt(tm[1]) : 0
                        });
                    }
                    return JSON.stringify(vids);
                })();
            """, timeout=60)

            # Separate call for date headers (lightweight — targeted selector)
            date_raw = tab.js("""
                return (function() {
                    var out = [];
                    var seen = {};
                    var els = document.querySelectorAll('h2, h3, [class*="header"], [class*="title"]');
                    var days = {Today:1, Yesterday:1, Monday:1, Tuesday:1, Wednesday:1, Thursday:1, Friday:1, Saturday:1, Sunday:1};
                    for (var i = 0; i < els.length; i++) {
                        var t = els[i].textContent.trim();
                        if (t in days && !seen[t]) { seen[t] = 1; out.push(t); }
                    }
                    return JSON.stringify(out);
                })();
            """)
            date_headers_list = []
            if date_raw:
                try:
                    date_headers_list = json.loads(date_raw)
                except (json.JSONDecodeError, TypeError):
                    pass

            if page_data:
                try:
                    vids_list = json.loads(page_data)
                    if isinstance(vids_list, dict):
                        vids_list = vids_list.get("videos", [])

                    for v in vids_list:
                        vid = v.get("video_id", "")
                        if not vid or vid in seen_video_ids:
                            continue
                        seen_video_ids.add(vid)

                        # Consecutive known stop: if 10 in a row are already in DB, stop
                        if vid in known_ids:
                            consecutive_known += 1
                            if consecutive_known >= 10:
                                logger.info("Consecutive known stop: 10 videos already in DB — reached old territory")
                                return videos, latest_date_group
                        else:
                            consecutive_known = 0

                        # Use first date header as the group (top of visible area)
                        date_group = date_headers_list[0] if date_headers_list else ""
                        if not latest_date_group and date_headers_list:
                            latest_date_group = date_headers_list[0]

                        watched_date = _parse_date_group(date_group)

                        # Date stop: stop when items older than boundary
                        # First run: limit to Today + Yesterday (no last_collected_date yet)
                        effective_date_limit = last_collected_date
                        if not effective_date_limit:
                            effective_date_limit = (datetime.now().date() - timedelta(days=1)).isoformat()
                        if effective_date_limit and watched_date and watched_date < effective_date_limit:
                            logger.info("Date stop: item date %s < limit %s",
                                        watched_date, effective_date_limit)
                            return videos, latest_date_group

                        item = {
                            "video_id": vid,
                            "url": f"https://www.youtube.com/watch?v={vid}",
                            "href": v.get("href", ""),
                            "title": v.get("title", ""),
                            "channel": v.get("channel", ""),
                            "views_text": v.get("views_text", ""),
                            "duration": v.get("duration", ""),
                            "duration_seconds": _duration_to_seconds(v.get("duration", "")),
                            "watch_percent": v.get("watchPercent", 0),
                            "resume_time_seconds": v.get("resumeTime", 0),
                            "date_group": date_group,
                            "watched_date": watched_date or "",
                        }
                        videos.append(item)

                        # Save video stub immediately (INSERT OR IGNORE)
                        conn.execute(
                            "INSERT OR IGNORE INTO videos "
                            "(video_id, title, channel, url, watch_percent, resume_time_seconds, "
                            "duration, duration_seconds, views_text, "
                            "date_group, watched_date, content_type) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'video')",
                            (vid, item["title"], item["channel"], item["url"],
                             item["watch_percent"], item["resume_time_seconds"],
                             item["duration"], item["duration_seconds"],
                             item["views_text"],
                             item["date_group"], item["watched_date"]),
                        )
                        conn.commit()

                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning("Failed to parse video data: %s", e)

            # Smart scroll with backoff
            current_count = len(seen_video_ids)
            if current_count > prev_item_count:
                # Got new items — reset miss counter
                consecutive_misses = 0
                prev_item_count = current_count
            else:
                # No new items — start backoff sequence
                # Wait 2s, count again
                tab._send("Input.dispatchMouseEvent", {
                    "type": "mouseWheel", "x": 640, "y": 400,
                    "deltaX": 0, "deltaY": 800,
                })
                wait_human(2, 2.5)

                # Re-extract and check
                recheck_count = tab.js("""
                    return document.querySelectorAll('yt-lockup-view-model').length;
                """) or 0

                if int(recheck_count) <= current_count:
                    # Wait 3s more
                    wait_human(3, 3.5)
                    recheck2 = tab.js("""
                        return document.querySelectorAll('yt-lockup-view-model').length;
                    """) or 0

                    if int(recheck2) <= current_count:
                        # Wait 5s more
                        wait_human(5, 5.5)
                        consecutive_misses += 1
                        logger.info("Scroll miss %d (total ~10s wait, %d items so far)",
                                    consecutive_misses, current_count)

                        if consecutive_misses >= 3:
                            logger.info("3 consecutive scroll misses — stopping")
                            break
                        continue  # skip the normal scroll below

            # Normal scroll
            tab._send("Input.dispatchMouseEvent", {
                "type": "mouseWheel", "x": 640, "y": 400,
                "deltaX": 0, "deltaY": 800,
            })
            wait_human(2, 3)

        logger.info("Phase 1A complete: %d videos collected", len(videos))
        return videos, latest_date_group

    # ── Phase 1B: Shorts collection ───────────────────────────────

    def _collect_shorts(self, tab: CDPTab, conn) -> int:
        """Click Shorts chip, extract today's shorts carousel.

        Returns count of new shorts saved.
        """
        # Step 12: Ensure chips visible (page scrolled during Phase 1A)
        chips = self._ensure_chips_visible(tab)
        if not chips:
            logger.warning("No chips found — skipping shorts collection")
            return 0

        # Step 13: Click "Shorts" chip
        clicked = self._click_chip(tab, chips, "Shorts")
        if not clicked:
            logger.warning("Could not find 'Shorts' chip — skipping")
            return 0

        wait_human(3, 4)

        # Step 14: Verify "Shorts" is selected
        if not self._verify_chip_selected(tab, "Shorts"):
            logger.warning("Shorts chip not selected — skipping")
            return 0

        # Step 15: Find "Next" button and click to load all Today's shorts
        next_btn = tab.js("""
            return (function() {
                var btns = document.querySelectorAll('[aria-label="Next"]');
                for (var i = 0; i < btns.length; i++) {
                    var r = btns[i].getBoundingClientRect();
                    if (r.y > 150 && r.y < 600 && r.width > 0) {
                        return JSON.stringify({x: Math.round(r.x + r.width/2), y: Math.round(r.y + r.height/2)});
                    }
                }
                return null;
            })();
        """)

        if next_btn:
            try:
                pos = json.loads(next_btn)
                click_at(tab, pos["x"], pos["y"])
                logger.info("Clicked 'Next' button for shorts carousel")
            except (json.JSONDecodeError, TypeError):
                pass

        wait_human(2, 3)

        # Step 17: Extract from ytm-shorts-lockup-view-model containers
        shorts_raw = tab.js("""
            return (function() {
                var containers = document.querySelectorAll('ytm-shorts-lockup-view-model');
                var out = [];
                var seenIds = {};
                for (var i = 0; i < containers.length; i++) {
                    var c = containers[i];
                    var aEl = c.querySelector('a[href*="/shorts/"]');
                    if (!aEl) continue;
                    var href = aEl.getAttribute('href') || '';
                    var m = href.match(/\\/shorts\\/([a-zA-Z0-9_-]{11})/);
                    if (!m || seenIds[m[1]]) continue;
                    seenIds[m[1]] = true;

                    var text = c.textContent.trim();
                    // Split title from views: "Some title here123K views"
                    var titleViews = text.match(/(.+?)(\\d+\\.?\\d*[KMB]?\\s*views?)\\s*$/i);
                    var title = '';
                    var views = '';
                    if (titleViews) {
                        title = titleViews[1].trim();
                        views = titleViews[2].trim();
                    } else {
                        title = text;
                    }

                    out.push({
                        short_id: m[1],
                        title: title.substring(0, 300),
                        views: views,
                        href: href
                    });
                }
                return JSON.stringify(out);
            })();
        """)

        shorts_new_count = 0
        if shorts_raw:
            try:
                shorts = json.loads(shorts_raw)
                for s in shorts:
                    sid = s.get("short_id", "")
                    if not sid:
                        continue

                    # Step 18: Save each short to DB immediately
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO shorts "
                        "(short_id, title, url, views_text, date_group, watched_date) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (sid, s.get("title", ""),
                         f"https://www.youtube.com/shorts/{sid}",
                         s.get("views", ""),
                         "Today",
                         _parse_date_group("Today") or ""),
                    )
                    if cur.rowcount > 0:
                        shorts_new_count += 1
                    conn.commit()

                logger.info("Phase 1B complete: %d shorts found, %d new",
                            len(shorts), shorts_new_count)
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse shorts data: %s", e)

        return shorts_new_count

    # ── Phase 1C: Restore ─────────────────────────────────────────

    def _restore_all_chip(self, tab: CDPTab):
        """Click 'All' chip to restore default history state."""
        chips = self._ensure_chips_visible(tab)
        if chips:
            self._click_chip(tab, chips, "All")
            wait_human(1, 2)
            logger.info("Phase 1C: restored 'All' chip")

    # ── Phase 2: Video page visits ────────────────────────────────

    def _visit_video_page(self, cdp: CDPClient, video: dict) -> dict:
        """Open video in NEW tab, extract details, close tab.

        Uses CDPClient.new_tab() for proper tab isolation.
        """
        vid_id = video.get("video_id", "")
        resume = video.get("resume_time_seconds", 0) or 0
        duration_secs = video.get("duration_seconds", 0) or 0
        watch_pct = video.get("watch_percent", 0) or 0

        # Step 23: Build URL with &t=
        url = f"https://www.youtube.com/watch?v={vid_id}"
        if watch_pct >= 100 and duration_secs > 20:
            url += f"&t={duration_secs - 10}s"
        elif resume > 0:
            url += f"&t={resume}s"

        # Step 24-26: Open in NEW tab and navigate
        video_tab = cdp.new_tab(url)
        try:
            # Wait for page to fully load
            for _w in range(15):
                ready = video_tab.js("return document.readyState") or ""
                if ready == "complete":
                    break
                time.sleep(1)
            # Extra wait for dynamic content (description, comments)
            time.sleep(10)

            # Step 29: Extract details

            # Title — retry up to 5 times, 2s between
            title = ""
            for _attempt in range(5):
                title = video_tab.js("""
                    var t = document.querySelector('h1.ytd-watch-metadata yt-formatted-string, h1 yt-formatted-string');
                    return t ? t.textContent.trim() : '';
                """) or ""
                if len(title) > 3:
                    break
                time.sleep(2)

            # Channel
            channel = video_tab.js("""
                return (document.querySelector('ytd-channel-name a, #channel-name a') || {}).textContent || '';
            """) or ""
            channel = channel.strip()

            # Channel URL
            channel_url = video_tab.js("""
                var c = document.querySelector('ytd-channel-name a[href*="/@"], #channel-name a[href*="/@"]');
                return c ? c.getAttribute('href') : '';
            """) or ""

            # Description — click #expand, poll for full content, fall back to snippet
            video_tab.js("""
                var btn = document.querySelector('#expand, #description-inline-expander #expand');
                if (btn) btn.click();
            """)
            description = ""
            for _desc_attempt in range(5):
                time.sleep(2)
                description = video_tab.js("""
                    var d = document.querySelector('#expanded yt-attributed-string');
                    return d ? d.textContent.trim().substring(0, 10000) : '';
                """) or ""
                if description:
                    break
            # Fall back to snippet preview if expand failed
            if not description:
                description = video_tab.js("""
                    var d = document.querySelector('#snippet-text yt-attributed-string');
                    return d ? d.textContent.trim().substring(0, 10000) : '';
                """) or ""

            # Duration
            duration = video_tab.js("""
                var d = document.querySelector('.ytp-time-duration');
                return d ? d.textContent : '';
            """) or ""

            # Views text
            views_text = video_tab.js("""
                var info = document.querySelector('#info-container yt-formatted-string, ytd-watch-info-text');
                return info ? info.textContent.trim() : '';
            """) or ""

            # Top comment — scroll down first
            scroll_slowly(video_tab, random.randint(500, 800))
            wait_human(2, 4)

            top_comment = ""
            top_comment_author = ""
            comment_data = video_tab.js("""
                var c = document.querySelector('ytd-comment-thread-renderer #content-text');
                var a = document.querySelector('ytd-comment-thread-renderer #author-text');
                if (c) {
                    return JSON.stringify({
                        text: c.textContent.trim().substring(0, 2000),
                        author: a ? a.textContent.trim() : ''
                    });
                }
                return null;
            """)

            if comment_data:
                try:
                    cd = json.loads(comment_data)
                    top_comment = cd.get("text", "")
                    top_comment_author = cd.get("author", "")
                except (json.JSONDecodeError, TypeError):
                    pass

            # Parse views + date from views_text
            parsed_views = ""
            publish_date = ""
            if views_text:
                v_match = re.search(r'([\d,]+\s*views)', views_text)
                if v_match:
                    parsed_views = v_match.group(1)
                d_match = re.search(r'([A-Z][a-z]{2}\s+\d{1,2},?\s*\d{4})', views_text)
                if d_match:
                    publish_date = d_match.group(1)

            return {
                "title": title,
                "channel": channel,
                "channel_url": channel_url,
                "duration": duration,
                "duration_seconds": _duration_to_seconds(duration),
                "views_text": parsed_views or views_text,
                "publish_date": publish_date,
                "description": description,
                "top_comment": top_comment,
                "top_comment_author": top_comment_author,
            }

        finally:
            # Step 31-32: Close tab, cleanup orphans
            try:
                cdp.close_tab(video_tab)
            except Exception:
                pass

    def _save_video(self, conn, video: dict, details: dict) -> None:
        """Insert or update a video in the skill DB."""
        conn.execute("""
            INSERT INTO videos (
                video_id, title, channel, channel_url, url, description,
                duration, duration_seconds, top_comment, top_comment_author,
                views_text, publish_date, watch_percent, resume_time_seconds,
                date_group, watched_date, content_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'video')
            ON CONFLICT(video_id) DO UPDATE SET
                title = excluded.title,
                channel = excluded.channel,
                channel_url = excluded.channel_url,
                description = excluded.description,
                duration = excluded.duration,
                duration_seconds = excluded.duration_seconds,
                top_comment = excluded.top_comment,
                top_comment_author = excluded.top_comment_author,
                views_text = excluded.views_text,
                publish_date = excluded.publish_date,
                watch_percent = excluded.watch_percent,
                resume_time_seconds = excluded.resume_time_seconds,
                updated_at = datetime('now')
        """, (
            video["video_id"],
            details.get("title", ""),
            details.get("channel", ""),
            details.get("channel_url", ""),
            video["url"],
            details.get("description", ""),
            details.get("duration", ""),
            details.get("duration_seconds", 0),
            details.get("top_comment", ""),
            details.get("top_comment_author", ""),
            details.get("views_text", ""),
            details.get("publish_date", ""),
            video.get("watch_percent", 0),
            video.get("resume_time_seconds", 0),
            video.get("date_group", ""),
            video.get("watched_date", ""),
        ))
        conn.commit()

    # ── UI Manifest ───────────────────────────────────────────────

    def get_widgets(self) -> list[WidgetDefinition]:
        return [
            WidgetDefinition(
                name="stats",
                title="YouTube",
                display_type="stat_cards",
                data_query="",
                refresh_seconds=600,
                size="small",
                click_action="skill_page",
            ),
            WidgetDefinition(
                name="unfinished",
                title="Didn't Finish Watching",
                display_type="progress_list",
                data_query=(
                    "SELECT title, channel, url, watch_percent, duration "
                    "FROM videos WHERE watch_percent >= 30 AND watch_percent < 100 AND dismissed_unfinished = 0 AND title != '' "
                    "ORDER BY updated_at DESC LIMIT 5"
                ),
                refresh_seconds=600,
                size="medium",
                click_action="skill_page#unfinished",
            ),
            WidgetDefinition(
                name="recent",
                title="Recent Videos",
                display_type="timeline",
                data_query=(
                    "SELECT title, channel, duration, watched_date, url, synced_at "
                    "FROM videos ORDER BY watched_date DESC LIMIT 8"
                ),
                refresh_seconds=300,
                size="medium",
                click_action="skill_page#history",
            ),
            WidgetDefinition(
                name="top_channels",
                title="Top Channels",
                display_type="list",
                data_query=(
                    "SELECT channel, COUNT(*) as watch_count FROM videos "
                    "WHERE watch_percent >= 50 AND channel != '' "
                    "GROUP BY channel ORDER BY watch_count DESC LIMIT 5"
                ),
                size="small",
            ),
        ]

    def get_page_sections(self) -> list[PageSection]:
        return [
            PageSection(
                name="stats",
                title="Overview",
                display_type="stat_cards",
                data_query="",
                position=0,
            ),
            PageSection(
                name="unfinished",
                title="Videos You Didn't Finish",
                display_type="progress_list",
                data_query=(
                    "SELECT title, channel, url, watch_percent, duration, watched_date "
                    "FROM videos WHERE watch_percent >= 30 AND watch_percent < 100 AND dismissed_unfinished = 0 AND title != '' "
                    "ORDER BY updated_at DESC"
                ),
                position=1,
                collapsible=True,
            ),
            PageSection(
                name="history",
                title="Watch History",
                display_type="timeline",
                data_query=(
                    "SELECT title, channel, duration, watched_date, url, watch_percent "
                    "FROM videos ORDER BY watched_date DESC"
                ),
                position=2,
                paginated=True,
                page_size=20,
            ),
            PageSection(
                name="shorts",
                title="Shorts",
                display_type="grid",
                data_query="SELECT title, url, watched_date FROM shorts ORDER BY watched_date DESC",
                position=3,
                paginated=True,
                page_size=30,
            ),
            PageSection(
                name="search",
                title="Search",
                display_type="search",
                data_query="",
                position=4,
            ),
        ]

    def get_notification_rules(self) -> list[NotificationRule]:
        return [
            NotificationRule(
                event="after_collection",
                condition="items_new > 0",
                title_template="{items_new} new YouTube videos",
                message_template="Collected {items_new} new videos from {items_found} found",
                level="info",
                link_to="/skill/youtube_history",
            ),
            NotificationRule(
                event="after_collection",
                condition="items_found == 0 and previous_count > 0",
                title_template="YouTube: No data found",
                message_template="Previously had {previous_count} videos. Site may have changed.",
                level="warning",
                link_to="/skill/youtube_history",
            ),
            NotificationRule(
                event="on_login_fail",
                condition="True",
                title_template="YouTube: Sign-in required",
                message_template="Please sign in to YouTube to continue collecting",
                level="action_required",
                link_to="/settings",
            ),
        ]

    def get_stats(self, conn) -> list[dict]:
        total_v = conn.execute("SELECT COUNT(*) as c FROM videos").fetchone()["c"]
        total_s = conn.execute("SELECT COUNT(*) as c FROM shorts").fetchone()["c"]
        unfinished = conn.execute(
            "SELECT COUNT(*) as c FROM videos "
            "WHERE watch_percent >= 30 AND watch_percent < 100 AND dismissed_unfinished = 0 AND title != ''"
        ).fetchone()["c"]
        completed = conn.execute(
            "SELECT COUNT(*) as c FROM videos WHERE watch_percent = 100"
        ).fetchone()["c"]
        # Top channel
        top_channel_row = conn.execute(
            "SELECT channel, COUNT(*) as cnt FROM videos "
            "WHERE watch_percent >= 50 AND channel != '' "
            "GROUP BY channel ORDER BY cnt DESC LIMIT 1"
        ).fetchone()
        top_channel = top_channel_row["channel"] if top_channel_row else "—"
        return [
            {"label": "Videos", "value": total_v},
            {"label": "Shorts", "value": total_s},
            {"label": "Unfinished", "value": unfinished},
            {"label": "Completed", "value": completed},
            {"label": "Top Channel", "value": top_channel},
        ]

    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        rows = conn.execute(
            "SELECT v.title, v.channel, v.url, v.description, v.watched_date "
            "FROM videos_fts f JOIN videos v ON v.id = f.rowid "
            "WHERE videos_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ).fetchall()
        return [
            {
                "type": "video",
                "title": r["title"],
                "snippet": (r["description"] or "")[:200],
                "url": r["url"],
                "date": r["watched_date"],
                "source": "youtube_history",
            }
            for r in rows
        ]
