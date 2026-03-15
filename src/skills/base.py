"""
Skill base interface — all collection skills implement this.

Skills are self-contained Python files that navigate a specific website
using CDP, collect data, and store it in SQLite via SyncTracker.

Skills are published to: https://github.com/vysakhrnambiar/memory-tap-skills
The system auto-pulls new versions from that repo.
"""
import logging
import os
import time as _time
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

from ..cdp_client import CDPTab, CDPClient
from ..db.models import add_alert, get_connection
from ..db.sync_tracker import SyncTracker

logger = logging.getLogger("memory_tap.skill")


class StopStrategy(Enum):
    """How a skill decides when to stop collecting.

    Every skill MUST declare one. Determined during CDP probe.
    """
    TIMESTAMP = "timestamp"        # Site shows time per item → stop at last collected time
    DATE_GROUP = "date_group"      # Site groups by date (Today, Yesterday) → stop at last date
    CONSECUTIVE_KNOWN = "consecutive_known"  # Stop after N consecutive already-known items
    ITEM_COUNT = "item_count"      # Simple count limit (fallback, not recommended)


@dataclass
class RunLimits:
    """Tracks collection limits during a skill run.

    Skills call should_stop() after each item. Framework enforces:
    - Item count (first run only)
    - Time cap
    - Scroll behavior (humanness)

    Stop signal (when to stop because we've reached old data) is
    handled by the skill's should_stop_collecting() method, NOT here.
    """
    max_items: int = 0              # 0 = unlimited (subsequent runs)
    max_minutes: float = 30.0
    max_scrolls_before_pause: int = 10
    pause_seconds_min: float = 30.0
    pause_seconds_max: float = 90.0
    max_scroll_sessions: int = 20

    # Runtime state
    items_collected: int = 0
    _start_time: float = field(default_factory=_time.time)
    _scroll_count: int = 0
    _session_count: int = 0

    def item_done(self):
        """Call after collecting one item."""
        self.items_collected += 1

    def scroll_done(self):
        """Call after each scroll. Returns pause duration if pause needed, else 0."""
        self._scroll_count += 1
        if self._scroll_count >= self.max_scrolls_before_pause:
            self._scroll_count = 0
            self._session_count += 1
            import random
            return random.uniform(self.pause_seconds_min, self.pause_seconds_max)
        return 0

    @property
    def elapsed_minutes(self) -> float:
        return (_time.time() - self._start_time) / 60.0

    @property
    def time_exceeded(self) -> bool:
        return self.elapsed_minutes >= self.max_minutes

    @property
    def items_exceeded(self) -> bool:
        return self.max_items > 0 and self.items_collected >= self.max_items

    @property
    def sessions_exceeded(self) -> bool:
        return self._session_count >= self.max_scroll_sessions

    def should_stop(self) -> bool:
        """Check if any limit is hit (time, items, sessions).

        NOTE: This does NOT check the skill's stop signal (old data detection).
        Skills must also call their own should_stop_collecting() separately.
        """
        if self.time_exceeded:
            logger.info("RunLimits: time cap reached (%.1f min)", self.elapsed_minutes)
            return True
        if self.items_exceeded:
            logger.info("RunLimits: item cap reached (%d)", self.items_collected)
            return True
        if self.sessions_exceeded:
            logger.info("RunLimits: scroll session cap reached (%d)", self._session_count)
            return True
        return False

    @property
    def stop_reason(self) -> str:
        if self.time_exceeded:
            return f"time_limit ({self.elapsed_minutes:.0f} min)"
        if self.items_exceeded:
            return f"item_limit ({self.items_collected})"
        if self.sessions_exceeded:
            return f"scroll_sessions ({self._session_count})"
        return ""


@dataclass
class SkillManifest:
    """Metadata for a skill — matches manifest.json on GitHub."""
    name: str                    # e.g. "youtube_history"
    version: str                 # semver e.g. "1.0.0"
    target_url: str              # e.g. "https://youtube.com"
    description: str             # human-readable
    auth_provider: str = ""      # "google", "openai", etc — groups sign-in on dashboard
    tabs_needed: int = 1         # how many persistent tabs this skill needs
    schedule_hours: int = 3      # how often to run
    login_url: str = ""          # URL to show user for manual login
    checksum: str = ""           # sha256 of the skill file
    # Collection limits
    max_items_first_run: int = 100   # item cap on first run (0 = unlimited)
    max_items_per_run: int = 0       # 0 = unlimited for subsequent runs
    max_minutes_per_run: int = 30    # time cap per run


@dataclass
class CollectResult:
    """Result of a skill's collect() run."""
    items_found: int = 0
    items_new: int = 0
    items_updated: int = 0
    error: str | None = None
    details: dict = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.error is None


class BaseSkill(ABC):
    """Base class for all collection skills.

    Subclasses MUST implement:
    - manifest: SkillManifest property
    - check_login(tab): verify user is logged in
    - collect(tab, tracker, limits): navigate + collect data
    - stop_strategy: which StopStrategy this skill uses
    - should_stop_collecting(item, tracker): detect when we've reached old data

    The skill engine handles:
    - Chrome/CDP lifecycle
    - Tab creation and cleanup
    - Sync logging
    - Error handling
    """

    @property
    @abstractmethod
    def manifest(self) -> SkillManifest:
        """Return this skill's manifest."""
        ...

    @abstractmethod
    def check_login(self, tab: CDPTab) -> bool:
        """Check if user is logged into this service.

        Navigate to target_url and check for login indicators.
        Return True if logged in, False if not.
        """
        ...

    @abstractmethod
    def collect(self, tab: CDPTab, tracker: SyncTracker,
                limits: RunLimits) -> CollectResult:
        """Navigate the site and collect data.

        Use human-like interactions from src.human module.
        Store data via the tracker.
        Check limits.should_stop() AND self.should_stop_collecting() after each item.
        Return what was found.
        """
        ...

    @property
    @abstractmethod
    def stop_strategy(self) -> StopStrategy:
        """Declare which stop strategy this skill uses.

        Determined during CDP probe for each website.
        MUST be implemented — no default.
        """
        ...

    @abstractmethod
    def should_stop_collecting(self, item: dict, tracker: SyncTracker) -> bool:
        """Check if we've reached previously collected territory.

        Called after each item. The skill decides based on its stop_strategy:
        - TIMESTAMP: item's timestamp <= last collected timestamp
        - DATE_GROUP: item's date group is before last collected date
        - CONSECUTIVE_KNOWN: 3+ consecutive already-known items
        - ITEM_COUNT: simple count (fallback)

        This is ONE layer of the stop mechanism. RunLimits provides additional
        layers (time cap, scroll sessions). Both must be checked.

        MUST be implemented — no default. Determined during CDP probe.
        """
        ...

    def should_stop(self, item: dict, tracker: SyncTracker,
                    limits: RunLimits) -> tuple[bool, str]:
        """Multi-layer stop check. Fool-proof — checks ALL stop mechanisms.

        Returns (should_stop, reason).
        Skills call this after each item instead of checking individually.

        Layers:
        1. RunLimits: time cap exceeded?
        2. RunLimits: item cap exceeded? (first run only)
        3. RunLimits: scroll sessions exceeded?
        4. Skill's own stop signal: reached old data?

        All layers are checked every time. If ANY says stop → stop.
        """
        # Layer 1-3: Framework limits
        if limits.should_stop():
            return True, f"limit: {limits.stop_reason}"

        # Layer 4: Skill's own stop signal
        if self.should_stop_collecting(item, tracker):
            return True, "reached_previously_collected"

        return False, ""

    def create_run_limits(self, is_first_run: bool) -> RunLimits:
        """Create RunLimits for this run based on manifest and first/subsequent."""
        m = self.manifest
        if is_first_run:
            return RunLimits(
                max_items=m.max_items_first_run,
                max_minutes=m.max_minutes_per_run,
            )
        else:
            return RunLimits(
                max_items=m.max_items_per_run,  # 0 = unlimited
                max_minutes=m.max_minutes_per_run,
            )

    def get_login_url(self) -> str:
        """URL to open for user to log in. Defaults to target_url."""
        return self.manifest.login_url or self.manifest.target_url

    @staticmethod
    def _get_previous_item_count(skill_name: str, db_path: str | None = None) -> int:
        """Check how many items this skill collected in previous runs."""
        conn = get_connection(db_path)
        # Check conversations
        row = conn.execute(
            "SELECT COUNT(*) as c FROM conversations WHERE source = ?", (skill_name,)
        ).fetchone()
        conv_count = row["c"] if row else 0
        # Check videos (for youtube)
        vid_count = 0
        if "youtube" in skill_name:
            row = conn.execute("SELECT COUNT(*) as c FROM youtube_videos").fetchone()
            vid_count = row["c"] if row else 0
        conn.close()
        return conv_count + vid_count

    @staticmethod
    def _save_error_screenshot(tab: CDPTab, skill_name: str) -> str | None:
        """Take a screenshot on error/warning for debugging."""
        try:
            screenshots_dir = os.path.join(
                os.environ.get("LOCALAPPDATA", ""), "MemoryTap", "logs", "screenshots"
            )
            os.makedirs(screenshots_dir, exist_ok=True)
            import time as _time
            path = os.path.join(
                screenshots_dir,
                f"{skill_name}_{int(_time.time())}.png",
            )
            tab.screenshot(path)
            logger.info("Error screenshot saved: %s", path)
            return path
        except Exception as e:
            logger.warning("Failed to save screenshot: %s", e)
            return None

    @staticmethod
    def _build_github_issue_url(manifest: 'SkillManifest', prev_count: int,
                                 screenshot_path: str | None) -> str:
        """Build a pre-filled GitHub issue URL. No personal data included."""
        title = f"[{manifest.name}] Zero items found — possible site change"
        body = (
            f"## Skill Info\n"
            f"- **Skill**: {manifest.name}\n"
            f"- **Version**: {manifest.version}\n"
            f"- **Target URL**: {manifest.target_url}\n\n"
            f"## Problem\n"
            f"Previously collected {prev_count} items, but latest run found 0.\n"
            f"The website may have changed its layout or CSS selectors.\n\n"
            f"## Screenshot\n"
            f"{'Please attach the screenshot from: ' + screenshot_path if screenshot_path else 'No screenshot available'}\n\n"
            f"## Expected\n"
            f"Skill should find and collect items from the conversation/history list.\n"
        )
        params = urllib.parse.urlencode({"title": title, "body": body})
        return f"https://github.com/vysakhrnambiar/memory-tap-skills/issues/new?{params}"

    def run(self, client: CDPClient, db_path: str | None = None) -> CollectResult:
        """Full skill execution: get/create persistent tab → check login → collect.

        Uses persistent tabs that survive between runs. Tabs are kept open
        in Chrome — only the WebSocket connection is released after each run.
        """
        m = self.manifest
        tracker = SyncTracker(m.name, db_path)
        log_id = tracker.start_sync()

        tab = None
        try:
            # Get or create persistent tab for this skill
            tab = client.get_or_create_tab(m.name, m.target_url, db_path=db_path)
            tab.set_working()

            # Navigate to target URL (tab may be on a different page from last run)
            current_url = tab.get_url()
            if m.target_url not in current_url:
                tab.navigate(m.target_url)

            # Check login
            if not self.check_login(tab):
                screenshot_path = self._save_error_screenshot(tab, f"{m.name}_login_fail")
                tracker.set_login_status("not_logged_in")
                client.save_tab_state(m.name, tab, login_verified=False, db_path=db_path)
                error_msg = (
                    f"Not logged in to {m.name}. "
                    f"Please open the dashboard and sign into {m.target_url}"
                )
                logger.warning("Skill %s: %s", m.name, error_msg)
                result = CollectResult(error=error_msg)
                result.details["screenshot"] = screenshot_path
                tracker.finish_sync(0, 0, 0, error=error_msg)
                return result

            tracker.set_login_status("logged_in")
            client.save_tab_state(m.name, tab, login_verified=True, db_path=db_path)

            # Create run limits
            is_first_run = tracker.get_login_status() != "logged_in" or \
                not self._get_previous_item_count(m.name, db_path)
            limits = self.create_run_limits(is_first_run)
            logger.info("Skill %s: %s run, limits: max_items=%d, max_min=%.0f",
                        m.name, "first" if is_first_run else "subsequent",
                        limits.max_items, limits.max_minutes)

            # Collect
            result = self.collect(tab, tracker, limits)

            # Record limit info
            result.details["elapsed_minutes"] = round(limits.elapsed_minutes, 1)
            result.details["items_collected"] = limits.items_collected
            if limits.stop_reason:
                result.details["limit_reached"] = limits.stop_reason

            tracker.finish_sync(
                result.items_found, result.items_new, result.items_updated,
                error=result.error,
            )
            logger.info(
                "Skill %s: found=%d new=%d updated=%d (%.1f min, %s)",
                m.name, result.items_found, result.items_new, result.items_updated,
                limits.elapsed_minutes,
                limits.stop_reason or "completed",
            )

            # Zero-items warning: if logged in but found nothing, and we had data before
            if result.items_found == 0 and result.success:
                prev_count = self._get_previous_item_count(m.name, db_path)
                if prev_count > 0:
                    # Possible site change — take screenshot and alert
                    screenshot_path = self._save_error_screenshot(tab, m.name)
                    github_url = self._build_github_issue_url(m, prev_count, screenshot_path)
                    add_alert(
                        f"{m.name}: No data found",
                        f"Previously had {prev_count} items, now found 0. "
                        f"The website may have changed its layout. "
                        f"Please report this issue.",
                        level="warning",
                        source=m.name,
                        db_path=db_path,
                    )
                    result.details["zero_items_warning"] = True
                    result.details["previous_count"] = prev_count
                    result.details["github_issue_url"] = github_url
                    result.details["screenshot"] = screenshot_path
                    logger.warning(
                        "Skill %s: ZERO items found but previously had %d — possible site change",
                        m.name, prev_count,
                    )

            return result

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.error("Skill %s failed: %s", m.name, error_msg)
            screenshot_path = None
            if tab:
                screenshot_path = self._save_error_screenshot(tab, f"{m.name}_error")
            tracker.finish_sync(0, 0, 0, error=error_msg)
            result = CollectResult(error=error_msg)
            result.details["screenshot"] = screenshot_path
            return result

        finally:
            if tab:
                try:
                    tab.set_idle()
                    # Release WS connection but keep tab open in Chrome
                    client.release_tab(m.name, tab, db_path=db_path)
                except Exception:
                    pass
