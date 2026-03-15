"""
Skill base interface — all collection skills implement this.

Skills are self-contained Python files that navigate a specific website
using CDP, collect data, and store it in SQLite via SyncTracker.

Skills are published to: https://github.com/vysakhrnambiar/memory-tap-skills
The system auto-pulls new versions from that repo.
"""
import logging
import os
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..cdp_client import CDPTab, CDPClient
from ..db.models import add_alert, get_connection
from ..db.sync_tracker import SyncTracker

logger = logging.getLogger("memory_tap.skill")


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

    Subclasses must implement:
    - manifest: SkillManifest property
    - check_login(tab): verify user is logged in
    - collect(tab, tracker): navigate + collect data

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
    def collect(self, tab: CDPTab, tracker: SyncTracker) -> CollectResult:
        """Navigate the site and collect data.

        Use human-like interactions from src.human module.
        Store data via the tracker.
        Return what was found.
        """
        ...

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

            # Collect
            result = self.collect(tab, tracker)
            tracker.finish_sync(
                result.items_found, result.items_new, result.items_updated,
                error=result.error,
            )
            logger.info(
                "Skill %s: found=%d new=%d updated=%d",
                m.name, result.items_found, result.items_new, result.items_updated,
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
