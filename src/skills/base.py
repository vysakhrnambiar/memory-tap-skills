"""
Skill base interface — all collection skills implement this.

Skills are self-contained Python files that navigate a specific website
using CDP, collect data, and store it in SQLite via SyncTracker.

Skills are published to: https://github.com/vysakhrnambiar/memory-tap-skills
The system auto-pulls new versions from that repo.
"""
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from ..cdp_client import CDPTab, CDPClient
from ..db.sync_tracker import SyncTracker

logger = logging.getLogger("memory_tap.skill")


@dataclass
class SkillManifest:
    """Metadata for a skill — matches manifest.json on GitHub."""
    name: str                    # e.g. "youtube_history"
    version: str                 # semver e.g. "1.0.0"
    target_url: str              # e.g. "https://youtube.com"
    description: str             # human-readable
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

    def run(self, client: CDPClient, db_path: str | None = None) -> CollectResult:
        """Full skill execution: check login → collect → report.

        Called by the scheduler. Handles tab lifecycle.
        """
        m = self.manifest
        tracker = SyncTracker(m.name, db_path)
        log_id = tracker.start_sync()

        tab = None
        try:
            tab = client.new_tab(m.target_url)

            # Check login
            if not self.check_login(tab):
                tracker.set_login_status("not_logged_in")
                result = CollectResult(error=f"Not logged in to {m.target_url}")
                tracker.finish_sync(0, 0, 0, error=result.error)
                return result

            tracker.set_login_status("logged_in")

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
            return result

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}"
            logger.error("Skill %s failed: %s", m.name, error_msg)
            tracker.finish_sync(0, 0, 0, error=error_msg)
            return CollectResult(error=error_msg)

        finally:
            if tab:
                try:
                    client.close_tab(tab)
                except Exception:
                    pass
