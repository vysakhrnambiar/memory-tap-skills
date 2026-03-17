"""
YouTube History Skill — collects watch history, descriptions, top comments, shorts.

Verified selectors via CDP probe (2026-03-15):
- History page: yt-lockup-view-model, a[href*="/watch?v="], a[href*="/shorts/"]
- Date headers: div#title with font-size >= 18px
- Watch progress: yt-thumbnail-overlay-progress-bar-view-model inner div style width %
- Video page: h1 yt-formatted-string (title), #expand + #description-inline-expander (desc),
  ytd-comment-thread-renderer #content-text (comment), .ytp-time-duration (duration)
- Mute+Pause: video.muted=true; video.pause() — verified working

Stop strategy: DATE_GROUP
- History groups by: Today, Yesterday, day names, then "Mon DD"
- Stop when: current date_group <= last_collected_date_group AND all videos in it are known

__version__ = "0.2.8"
"""
__version__ = "0.2.8"

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
      "Today"       → today
      "Yesterday"   → yesterday
      "Friday"      → last Friday
      "Mar 8"       → 2026-03-08 (current year)
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

    # ── Collection ────────────────────────────────────────────────

    def collect(self, tab: CDPTab, tracker: SyncTracker,
                limits: RunLimits) -> CollectResult:
        """Scroll through history, collect video URLs + progress,
        then visit each new video for details."""
        result = CollectResult()
        conn = tracker._skill_conn

        # Phase 1: Collect video URLs + watch progress from history list
        history_items = self._collect_history_list(tab, conn, limits)
        result.items_found = len(history_items["videos"]) + len(history_items["shorts"])

        # Phase 2: Visit each new video page for details (mute+pause)
        new_count = 0
        updated_count = 0

        for video in history_items["videos"]:
            stop, reason = self.should_stop(video, tracker, limits)
            if stop:
                logger.info("Stopping video visits: %s", reason)
                break

            existing = conn.execute(
                "SELECT id, title FROM videos WHERE video_id = ?",
                (video["video_id"],)
            ).fetchone()

            if existing and existing["title"]:
                # Already have full details — just update watch_percent
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

            # New video — visit page for full details
            try:
                details = self._get_video_details(tab, video)
                self._save_video(conn, video, details)
                new_count += 1
                limits.item_done()
                logger.info("Collected video %d: %s",
                            new_count, details.get("title", "")[:60])
            except Exception as e:
                logger.warning("Failed video %s: %s", video["video_id"], e)

            wait_human(2, 4)

        # Phase 3: Shorts already saved incrementally in Phase 1
        new_count += history_items.get("shorts_new_count", 0)

        # Phase 4: Update collection state
        if history_items["latest_date_group"]:
            watched_date = _parse_date_group(history_items["latest_date_group"])
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

    def _collect_history_list(self, tab: CDPTab, conn, limits: RunLimits) -> dict:
        """Scroll through history page collecting URLs, progress, date groups.

        Returns dict with 'videos', 'shorts', 'latest_date_group'.
        Does NOT visit individual pages — just reads the list.

        Uses its OWN scroll budget (max 15 scrolls for first run, 8 for subsequent)
        to avoid consuming all RunLimits time before video visits.
        """
        tab.navigate("https://www.youtube.com/feed/history")
        wait_human(3, 5)

        videos = []
        shorts = []
        seen_video_ids = set()
        seen_short_ids = set()
        current_date_group = "Today"
        latest_date_group = None
        consecutive_known = 0
        shorts_new_count = 0

        # List phase has its own scroll budget — don't use RunLimits scroll sessions
        is_first_run = limits.max_items > 0
        max_list_scrolls = 15 if is_first_run else 8

        for scroll_num in range(max_list_scrolls):
            if limits.time_exceeded:
                break

            # Extract all items visible now
            page_data = tab.js("""
                return (function() {
                    // Get date headers
                    var headers = document.querySelectorAll("div#title");
                    var dateGroups = [];
                    for (var h = 0; h < headers.length; h++) {
                        var el = headers[h];
                        var style = window.getComputedStyle(el);
                        var fontSize = parseFloat(style.fontSize);
                        var text = el.textContent.trim();
                        if (fontSize >= 18 && text.length < 50 && text !== "Shorts") {
                            var r = el.getBoundingClientRect();
                            dateGroups.push({text: text, y: r.y});
                        }
                    }

                    // Get video links with progress
                    var links = document.querySelectorAll("a[href*='/watch?v=']");
                    var vids = [];
                    var seenHrefs = new Set();
                    for (var i = 0; i < links.length; i++) {
                        var a = links[i];
                        var href = a.getAttribute("href") || "";
                        if (seenHrefs.has(href)) continue;
                        seenHrefs.add(href);

                        var r = a.getBoundingClientRect();
                        if (r.width === 0) continue;

                        // Find watch progress bar
                        var container = a.closest("yt-lockup-view-model") || a.parentElement;
                        var progressBar = container ?
                            container.querySelector("yt-thumbnail-overlay-progress-bar-view-model div[style*='width']") :
                            null;
                        var watchPercent = 0;
                        if (progressBar) {
                            var style = progressBar.getAttribute("style") || "";
                            var m = style.match(/width:\\s*(\\d+)%/);
                            if (m) watchPercent = parseInt(m[1]);
                        }

                        // Find which date group this video belongs to
                        var dateGroup = "";
                        for (var d = dateGroups.length - 1; d >= 0; d--) {
                            if (dateGroups[d].y < r.y) {
                                dateGroup = dateGroups[d].text;
                                break;
                            }
                        }

                        vids.push({
                            href: href,
                            y: r.y,
                            watchPercent: watchPercent,
                            dateGroup: dateGroup,
                        });
                    }

                    // Get shorts links
                    var shortLinks = document.querySelectorAll("a[href*='/shorts/']");
                    var shortsOut = [];
                    var seenShorts = new Set();
                    for (var s = 0; s < shortLinks.length; s++) {
                        var sa = shortLinks[s];
                        var shref = sa.getAttribute("href") || "";
                        if (seenShorts.has(shref)) continue;
                        seenShorts.add(shref);
                        var sr = sa.getBoundingClientRect();
                        if (sr.width === 0) continue;

                        var stitle = sa.getAttribute("aria-label") || "";
                        var sdateGroup = "";
                        for (var sd = dateGroups.length - 1; sd >= 0; sd--) {
                            if (dateGroups[sd].y < sr.y) {
                                sdateGroup = dateGroups[sd].text;
                                break;
                            }
                        }
                        shortsOut.push({href: shref, title: stitle, dateGroup: sdateGroup});
                    }

                    return JSON.stringify({
                        dateGroups: dateGroups.map(function(d) { return d.text; }),
                        videos: vids,
                        shorts: shortsOut,
                    });
                })();
            """)

            if page_data:
                try:
                    data = json.loads(page_data)

                    # Track date groups
                    for dg in data.get("dateGroups", []):
                        if not latest_date_group:
                            latest_date_group = dg
                        current_date_group = dg

                    # Process videos
                    for v in data.get("videos", []):
                        vid = _extract_video_id(v["href"])
                        if not vid or vid in seen_video_ids:
                            continue
                        seen_video_ids.add(vid)

                        watched_date = _parse_date_group(v.get("dateGroup", ""))
                        resume_secs = _extract_resume_seconds(v["href"])

                        item = {
                            "video_id": vid,
                            "url": f"https://www.youtube.com/watch?v={vid}",
                            "href": v["href"],
                            "watch_percent": v.get("watchPercent", 0),
                            "resume_time_seconds": resume_secs,
                            "date_group": v.get("dateGroup", ""),
                            "watched_date": watched_date or "",
                        }

                        # Check stop signal
                        if self.should_stop_collecting(item, tracker=type('T', (), {'_skill_conn': conn})()):
                            consecutive_known += 1
                            if consecutive_known >= 5:
                                logger.info("Stop: 5 consecutive known items in old territory")
                                return {"videos": videos, "shorts": shorts,
                                        "latest_date_group": latest_date_group,
                                        "shorts_new_count": shorts_new_count}
                        else:
                            consecutive_known = 0

                        videos.append(item)

                        # Save video stub immediately (INSERT OR IGNORE)
                        # Phase 2 will UPDATE with full details later
                        conn.execute(
                            "INSERT OR IGNORE INTO videos "
                            "(video_id, url, watch_percent, resume_time_seconds, "
                            "date_group, watched_date, content_type) "
                            "VALUES (?, ?, ?, ?, ?, ?, 'video')",
                            (vid, item["url"], item["watch_percent"],
                             item["resume_time_seconds"],
                             item["date_group"], item["watched_date"]),
                        )
                        conn.commit()

                    # Process shorts — save immediately to DB
                    for s in data.get("shorts", []):
                        sid = _extract_short_id(s["href"])
                        if not sid or sid in seen_short_ids:
                            continue
                        seen_short_ids.add(sid)
                        short_item = {
                            "short_id": sid,
                            "url": f"https://www.youtube.com/shorts/{sid}",
                            "title": s.get("title", ""),
                            "date_group": s.get("dateGroup", ""),
                            "watched_date": _parse_date_group(s.get("dateGroup", "")) or "",
                        }
                        shorts.append(short_item)

                        # Save short immediately (INSERT OR IGNORE)
                        cur = conn.execute(
                            "INSERT OR IGNORE INTO shorts "
                            "(short_id, title, url, date_group, watched_date) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (sid, short_item["title"], short_item["url"],
                             short_item["date_group"], short_item["watched_date"]),
                        )
                        if cur.rowcount > 0:
                            shorts_new_count += 1
                        conn.commit()

                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning("Failed to parse history page data: %s", e)

            # Scroll down humanly — fixed pace for list collection
            scroll_slowly(tab, random.randint(400, 700))
            wait_human(1.5, 3.0)

        return {"videos": videos, "shorts": shorts, "latest_date_group": latest_date_group,
                "shorts_new_count": shorts_new_count}

    def _get_video_details(self, tab: CDPTab, video: dict) -> dict:
        """Navigate to video page and extract title, channel, description, comment.

        CRITICAL: First JS call MUST be mute + pause to prevent audio.
        """
        url = video.get("url", "") or f"https://www.youtube.com/watch?v={video['video_id']}"
        tab.navigate(url)
        wait_human(2, 3)

        # FIRST THING: mute and pause video
        tab.js("""
            var videos = document.querySelectorAll("video");
            for (var i = 0; i < videos.length; i++) {
                videos[i].muted = true;
                videos[i].pause();
            }
        """)

        wait_human(1, 2)

        # Title
        title = tab.js("""
            var t = document.querySelector('h1.ytd-watch-metadata yt-formatted-string, h1 yt-formatted-string');
            return t ? t.textContent.trim() : '';
        """) or ""

        # Channel — ytd-channel-name a has the text, #owner a is just the avatar (empty text)
        channel = tab.js("""
            return (document.querySelector('ytd-channel-name a, #channel-name a') || {}).textContent || '';
        """) or ""
        channel = channel.strip()

        channel_url = tab.js("""
            var c = document.querySelector('ytd-channel-name a[href*="/@"], #channel-name a[href*="/@"]');
            return c ? c.getAttribute('href') : '';
        """) or ""

        # Duration
        duration = tab.js("""
            var d = document.querySelector('.ytp-time-duration');
            return d ? d.textContent : '';
        """) or ""

        # Views + publish date (in description area before expanding)
        views_info = tab.js("""
            var info = document.querySelector('#info-container yt-formatted-string, ytd-watch-info-text');
            return info ? info.textContent.trim() : '';
        """) or ""

        # Expand description
        tab.js("""
            var btn = document.querySelector('#expand, #description-inline-expander #expand');
            if (btn) btn.click();
        """)
        wait_human(0.5, 1)

        # Description
        description = tab.js("""
            var d = document.querySelector('#description-inline-expander yt-attributed-string, #description yt-attributed-string');
            return d ? d.textContent.trim().substring(0, 10000) : '';
        """) or ""

        # Parse views + date from description area or views_info
        views_text = ""
        publish_date = ""
        if views_info:
            # Pattern: "420,524 views  Feb 12, 2026"
            v_match = re.search(r'([\d,]+\s*views)', views_info)
            if v_match:
                views_text = v_match.group(1)
            d_match = re.search(r'([A-Z][a-z]{2}\s+\d{1,2},?\s*\d{4})', views_info)
            if d_match:
                publish_date = d_match.group(1)

        # Scroll to comments
        scroll_slowly(tab, random.randint(500, 800))
        wait_human(2, 4)

        # Top comment (may need more waiting for lazy load)
        top_comment = ""
        top_comment_author = ""
        comment_data = tab.js("""
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

        # Navigate back to history for next video
        tab.navigate("https://www.youtube.com/feed/history")
        wait_human(2, 3)

        return {
            "title": title,
            "channel": channel,
            "channel_url": channel_url,
            "duration": duration,
            "duration_seconds": _duration_to_seconds(duration),
            "views_text": views_text,
            "publish_date": publish_date,
            "description": description,
            "top_comment": top_comment,
            "top_comment_author": top_comment_author,
        }

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
                    "SELECT title, channel, duration, watched_date, url "
                    "FROM videos ORDER BY watched_date DESC LIMIT 8"
                ),
                refresh_seconds=300,
                size="medium",
                click_action="skill_page#history",
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
        return [
            {"label": "Videos", "value": total_v},
            {"label": "Shorts", "value": total_s},
            {"label": "Unfinished", "value": unfinished},
            {"label": "Completed", "value": completed},
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
