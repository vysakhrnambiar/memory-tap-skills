"""
Skill base interface — all collection skills implement this.

Skills are self-contained Python files that navigate a specific website
using CDP, collect data, and store it in SQLite via SyncTracker.

Skills are published to: https://github.com/vysakhrnambiar/memory-tap-skills
The system auto-pulls new versions from that repo.
"""
import json
import logging
import os
import time as _time
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

import requests as _requests

from ..cdp_client import CDPTab, CDPClient
from ..db.core_db import add_alert, add_notification, get_core_connection, CORE_DB_PATH
from ..db.skill_db import SkillDBManager
from ..db.sync_tracker import SyncTracker
from .ui_manifest import WidgetDefinition, PageSection, NotificationRule

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
    needs_browser: bool = True   # False for skills that read from DB only (no CDP tab needed)
    # Collection limits
    max_items_first_run: int = 100   # item cap on first run (0 = unlimited)
    max_items_per_run: int = 0       # 0 = unlimited for subsequent runs
    max_minutes_per_run: int = 30    # time cap per run


@dataclass
class ServiceDefinition:
    """Definition of a service that a skill provides to other skills."""
    name: str
    description: str = ""
    input_schema: dict = field(default_factory=dict)
    output_schema: dict = field(default_factory=dict)
    max_duration_seconds: int = 60


@dataclass
class SkillSetting:
    """A user-configurable setting for a skill.

    Skills declare these in get_configurable_settings(). The actual values
    live in the skill's own DB (settings table), surviving updates/hot-reloads.
    The dashboard renders UI based on setting_type.
    """
    key: str                            # DB key (e.g., "backfill_depth_days")
    label: str                          # Display label (e.g., "Backfill Depth")
    setting_type: str = "text"          # text, number, select, toggle
    default: str = ""                   # Default value (string — stored as text in DB)
    options: list[dict] = field(default_factory=list)  # For select: [{value, label}]
    description: str = ""               # Help text shown below the field
    min_value: float | None = None      # For number type
    max_value: float | None = None      # For number type


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
    - create_schema(conn): create tables in skill's own DB
    - get_search_results(conn, query, limit): return search results
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
    def create_schema(self, conn) -> None:
        """Create tables and indexes in this skill's own DB.

        Called once when the skill is first installed.
        conn is a sqlite3.Connection to skill_data/{name}.db.
        """
        ...

    def migrate_schema(self, conn, old_version: str, new_version: str) -> None:
        """Handle DB schema changes between skill versions.

        Called automatically when the skill version changes. Every skill MUST
        implement this method — even if it's just `pass` for code-only bumps.

        IMPORTANT — NEW SKILL CHECKLIST:
        - Every new skill must implement migrate_schema().
        - If the version bump only changes code (not tables), body is `pass`.
        - If tables change, add ALTER TABLE / CREATE INDEX here with version guards:

            if old_version < "0.5.0":
                conn.execute("ALTER TABLE items ADD COLUMN category TEXT DEFAULT ''")
                conn.commit()

        Without this method, version bumps trigger a "recreating schema" warning
        and re-run create_schema() — which works but is noisy and fragile.
        """
        pass

    @abstractmethod
    def get_search_results(self, conn, query: str, limit: int = 20) -> list[dict]:
        """Search this skill's data. Called by dashboard for cross-skill search.

        Each result must have: type, title, snippet, url (optional), date (optional).
        conn is a sqlite3.Connection to this skill's DB.
        """
        ...

    @abstractmethod
    def get_widgets(self) -> list[WidgetDefinition]:
        """Define widgets for the dashboard home screen.

        Return a list of WidgetDefinition objects.
        """
        ...

    @abstractmethod
    def get_page_sections(self) -> list[PageSection]:
        """Define sections for this skill's full page.

        Return a list of PageSection objects.
        """
        ...

    @abstractmethod
    def get_notification_rules(self) -> list[NotificationRule]:
        """Define when to push notifications.

        Return a list of NotificationRule objects.
        """
        ...

    def get_stats(self, conn) -> list[dict]:
        """Return stat cards data. Override if using stat_cards display type.

        Default: returns empty list. Skills with stat_cards widgets must override.
        Each item: {"label": "Videos", "value": 142}
        """
        return []

    def get_configurable_settings(self) -> list[SkillSetting]:
        """Declare user-editable settings for this skill.

        Default: empty (no configurable settings). Override to expose settings
        on the dashboard. Values are stored in the skill's own DB (settings table)
        and survive skill updates/hot-reloads.

        The dashboard renders UI based on setting_type:
          - "text": text input
          - "number": number input with optional min/max
          - "select": dropdown with options
          - "toggle": on/off switch (value "true"/"false")
        """
        return []

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

    # --- Service provider/consumer methods ---

    def get_services(self) -> list[ServiceDefinition]:
        """Return list of services this skill provides to other skills.

        Override in inference skills (e.g. ChatGPTInferenceSkill).
        Collection-only skills return empty list (default).
        """
        return []

    def handle_request(self, service_name: str, payload: dict, cdp: CDPClient) -> dict:
        """Handle an incoming service request from another skill.

        Override in inference skills. The skill opens/closes its own tabs.
        Args:
            service_name: operation name (e.g. "summarize", "execute_prompt")
            payload: request data matching the service's input_schema
            cdp: CDPClient instance for creating tabs
        Returns:
            Result dict matching the service's output_schema
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement handle_request()"
        )

    def request_service(self, full_service_name: str, payload: dict,
                        timeout: int = 60) -> dict:
        """Request a service from another skill via the DB queue.

        Args:
            full_service_name: "skill_name.operation" (e.g. "chatgpt_inference.summarize")
            payload: request data matching the service's input_schema
            timeout: max seconds to wait for result
        Returns:
            Result dict on success, or {"error": "..."} on failure/timeout
        """
        # Parse skill_name and service_name
        parts = full_service_name.split(".", 1)
        if len(parts) != 2:
            return {"error": f"Invalid service name format: '{full_service_name}' (expected 'skill.operation')"}
        to_skill, service_name = parts

        core_path = self._core_db_path if hasattr(self, '_core_db_path') else CORE_DB_PATH

        # Check service registry
        conn = get_core_connection(core_path)
        reg = conn.execute(
            "SELECT status FROM service_registry WHERE skill_name = ? AND service_name = ?",
            (to_skill, service_name),
        ).fetchone()
        if not reg:
            conn.close()
            return {"error": f"No provider for '{full_service_name}'"}
        if reg["status"] != "ready":
            conn.close()
            return {"error": f"Service '{full_service_name}' is not ready (status: {reg['status']})"}

        # Create PENDING request
        from_skill = self.manifest.name
        cur = conn.execute(
            "INSERT INTO service_requests (from_skill, to_skill, service_name, payload) "
            "VALUES (?, ?, ?, ?)",
            (from_skill, to_skill, service_name, json.dumps(payload, ensure_ascii=False)),
        )
        req_id = cur.lastrowid
        conn.commit()
        conn.close()
        logger.info("Service request %d: %s -> %s.%s", req_id, from_skill, to_skill, service_name)

        # Poll for result
        deadline = _time.time() + timeout
        while _time.time() < deadline:
            _time.sleep(2)
            conn = get_core_connection(core_path)
            row = conn.execute(
                "SELECT state, result, error FROM service_requests WHERE id = ?",
                (req_id,),
            ).fetchone()
            conn.close()

            if not row:
                return {"error": f"Service request {req_id} disappeared"}

            if row["state"] == "COMPLETED":
                try:
                    return json.loads(row["result"]) if row["result"] else {}
                except (json.JSONDecodeError, TypeError):
                    return {"error": f"Invalid result JSON from service request {req_id}"}

            if row["state"] in ("FAILED", "TIMEOUT"):
                return {"error": row["error"] or f"Service request {req_id} {row['state']}"}

        # Timeout — mark request as timed out by consumer
        logger.warning("Service request %d timed out after %ds", req_id, timeout)
        return {"error": f"Timeout waiting for '{full_service_name}' after {timeout}s"}

    @staticmethod
    def check_internet() -> bool:
        """Check internet connectivity using Google's generate_204 endpoint.

        Tries once, waits 3s, retries once on failure. Returns True if connected.
        """
        for attempt in range(2):
            try:
                resp = _requests.get("https://www.google.com/generate_204", timeout=5)
                if resp.status_code == 204:
                    return True
            except Exception:
                pass
            if attempt == 0:
                _time.sleep(3)
        return False

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

    def run(self, client: CDPClient,
            skill_db_mgr: SkillDBManager,
            core_db_path: str | None = None) -> CollectResult:
        """Full skill execution: ensure DB → get tab → check login → collect.

        Architecture:
        - Skill's own DB (via skill_db_mgr) for skill data
        - core.db for sync_log, login_status, notifications, tabs
        - Persistent tabs survive between runs
        """
        m = self.manifest
        core_path = core_db_path or CORE_DB_PATH

        # Service-only skills (no collection) skip the full run cycle
        if self.get_services() and m.schedule_hours == 0:
            logger.info("Skill %s is service-only — skipping collection run", m.name)
            return CollectResult()

        # Check internet before anything else
        if not self.check_internet():
            logger.warning("No internet — skipping skill %s", m.name)
            return CollectResult(error=f"No internet connection — skipping skill {m.name}")

        # Ensure skill DB schema exists
        skill_db_mgr.ensure_schema(self)

        # Get skill DB connection
        skill_conn = skill_db_mgr.get_connection(m.name)

        # Create tracker with split connections
        tracker = SyncTracker(m.name, skill_conn, core_path)
        log_id = tracker.start_sync()

        # Skills that don't need a browser (DB readers) — skip tab/login
        if not m.needs_browser:
            logger.info("Skill %s: no browser needed — running data curation", m.name)
            # Show a status tab so user knows what's happening
            status_tab = None
            try:
                status_tab = client.get_or_create_tab(m.name, "about:blank", db_path=core_path)
                status_tab.js("""
                    document.body.style.cssText = 'background:#1a1a2e;color:#e0e0e0;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;margin:0';
                    document.body.innerHTML = '<div style="text-align:center"><h2 style="color:#7c3aed">Interest Timeline</h2><p>Curating your interests in the background...</p><p style="color:#888;font-size:14px">This tab will update when complete</p></div>';
                """)
            except Exception:
                pass  # status tab is nice-to-have, not critical

            try:
                limits = RunLimits(m.max_items_first_run, m.max_items_per_run, m.max_minutes_per_run)
                result = self.collect(status_tab, tracker, limits)
                tracker.finish_sync(result.added, result.updated, result.skipped,
                                    error=result.error)
                # Update status tab on completion
                if status_tab:
                    try:
                        msg = f"Done! {result.added} new, {result.updated} updated" if not result.error else f"Error: {result.error[:100]}"
                        status_tab.js(f"""
                            document.body.innerHTML = '<div style="text-align:center"><h2 style="color:#7c3aed">Interest Timeline</h2><p style="color:#22c55e">{msg}</p></div>';
                        """)
                    except Exception:
                        pass
                return result
            except Exception as e:
                logger.error("Skill %s collect failed: %s", m.name, e)
                tracker.finish_sync(0, 0, 0, error=str(e))
                return CollectResult(error=str(e))

        tab = None
        try:
            # Get or create persistent tab
            tab = client.get_or_create_tab(m.name, m.target_url, db_path=core_path)
            tab.set_working()

            # Navigate to target URL if needed
            current_url = tab.get_url()
            if m.target_url not in current_url:
                tab.navigate(m.target_url)

            # Check login
            if not self.check_login(tab):
                screenshot_path = self._save_error_screenshot(tab, f"{m.name}_login_fail")
                tracker.set_login_status("not_logged_in")
                client.save_tab_state(m.name, tab, login_verified=False, db_path=core_path)
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
            client.save_tab_state(m.name, tab, login_verified=True, db_path=core_path)

            # Create run limits
            is_first_run = tracker.item_count(self._main_table_name()) == 0
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
                elapsed_minutes=limits.elapsed_minutes,
                stop_reason=limits.stop_reason or "completed",
                error=result.error,
            )
            logger.info(
                "Skill %s: found=%d new=%d updated=%d (%.1f min, %s)",
                m.name, result.items_found, result.items_new, result.items_updated,
                limits.elapsed_minutes,
                limits.stop_reason or "completed",
            )

            # Zero-items warning
            if result.items_found == 0 and result.success:
                prev_count = tracker.item_count(self._main_table_name())
                if prev_count > 0:
                    screenshot_path = self._save_error_screenshot(tab, m.name)
                    github_url = self._build_github_issue_url(m, prev_count, screenshot_path)
                    add_alert(
                        f"{m.name}: No data found",
                        f"Previously had {prev_count} items, now found 0. "
                        f"The website may have changed its layout.",
                        level="warning", source=m.name, db_path=core_path,
                    )
                    result.details["zero_items_warning"] = True
                    result.details["previous_count"] = prev_count
                    result.details["github_issue_url"] = github_url
                    result.details["screenshot"] = screenshot_path

            # Evaluate notification rules
            self._evaluate_notifications("after_collection", result, tracker, limits)

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
                    client.release_tab(m.name, tab, db_path=core_path)
                except Exception:
                    pass
            # Close skill DB connection
            try:
                skill_conn.close()
            except Exception:
                pass

    def _main_table_name(self) -> str:
        """Return the main data table name for item counting.

        Override in subclass if different. Default: 'items'.
        """
        return "items"

    def _evaluate_notifications(self, event: str, result: CollectResult,
                                tracker: SyncTracker, limits: RunLimits):
        """Evaluate notification rules after a collection run.

        Checks all rules for the given event, evaluates conditions,
        pushes matching notifications via tracker.notify().
        """
        rules = self.get_notification_rules()
        context = {
            "skill_name": self.manifest.name,
            "items_found": result.items_found,
            "items_new": result.items_new,
            "items_updated": result.items_updated,
            "elapsed_minutes": round(limits.elapsed_minutes, 1),
            "stop_reason": limits.stop_reason or "",
            "previous_count": result.details.get("previous_count", 0),
            "error": result.error or "",
        }

        for rule in rules:
            if rule.event != event:
                continue
            evaluated = rule.evaluate(context)
            if evaluated:
                title, message = evaluated
                link = rule.link_to.format(**context) if rule.link_to else ""
                tracker.notify(title, message, rule.level, link)
