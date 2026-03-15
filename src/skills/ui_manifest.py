"""
Skill UI Manifest — dataclasses that skills use to define their dashboard presence.

Skills return these from get_widgets(), get_page_sections(), get_notification_rules().
The dashboard reads them and renders the appropriate UI.

See spec/skill_ui_manifest.md for full specification.
"""
from dataclasses import dataclass, field


# --- Display Types (what the dashboard knows how to render) ---

DISPLAY_TYPES = {
    "stat_cards",       # Row of number cards
    "timeline",         # Vertical list grouped by date
    "list",             # Simple list of items
    "progress_list",    # List with progress bars
    "grid",             # Thumbnail grid
    "table",            # Sortable/filterable table
    "search",           # Search box + results
    "conversation",     # Chat-style messages
}

WIDGET_SIZES = {
    "small",    # 1x1
    "medium",   # 2x1
    "large",    # 2x2
    "wide",     # 3x1
}

NOTIFICATION_LEVELS = {
    "info",
    "warning",
    "error",
    "action_required",
}

NOTIFICATION_EVENTS = {
    "after_collection",
    "on_login_fail",
    "on_error",
    "on_first_run",
    "on_skill_update",
}


# --- Dataclasses ---

@dataclass
class WidgetDefinition:
    """A widget card for the dashboard home screen.

    Skills return a list of these from get_widgets().
    """
    name: str                           # unique within skill (e.g., "recent_videos")
    title: str                          # display title (e.g., "Recent Videos")
    display_type: str                   # one of DISPLAY_TYPES
    data_query: str | None = None       # SQL to run on skill's DB (None = uses get_stats)
    refresh_seconds: int = 300          # auto-refresh interval (0 = no auto-refresh)
    size: str = "medium"                # one of WIDGET_SIZES
    click_action: str = "skill_page"    # "skill_page", "skill_page#section", or URL

    def __post_init__(self):
        if self.display_type not in DISPLAY_TYPES:
            raise ValueError(f"Unknown display_type '{self.display_type}'. "
                             f"Must be one of: {DISPLAY_TYPES}")
        if self.size not in WIDGET_SIZES:
            raise ValueError(f"Unknown size '{self.size}'. "
                             f"Must be one of: {WIDGET_SIZES}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "title": self.title,
            "display_type": self.display_type,
            "data_query": self.data_query,
            "refresh_seconds": self.refresh_seconds,
            "size": self.size,
            "click_action": self.click_action,
        }


@dataclass
class PageSection:
    """A section on the skill's full page.

    Skills return a list of these from get_page_sections().
    """
    name: str                           # unique within skill
    title: str                          # section header
    display_type: str                   # one of DISPLAY_TYPES
    data_query: str | None = None       # SQL on skill's DB
    position: int = 0                   # order on page (lower = higher)
    collapsible: bool = False           # can user collapse this section
    paginated: bool = False             # show pagination
    page_size: int = 20                 # items per page (if paginated)

    def __post_init__(self):
        if self.display_type not in DISPLAY_TYPES:
            raise ValueError(f"Unknown display_type '{self.display_type}'. "
                             f"Must be one of: {DISPLAY_TYPES}")

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "title": self.title,
            "display_type": self.display_type,
            "data_query": self.data_query,
            "position": self.position,
            "collapsible": self.collapsible,
            "paginated": self.paginated,
            "page_size": self.page_size,
        }


@dataclass
class NotificationRule:
    """A rule for when to push a notification.

    Skills return a list of these from get_notification_rules().
    The framework evaluates them after each collection run.
    """
    event: str                          # one of NOTIFICATION_EVENTS
    condition: str                      # Python expression (e.g., "items_new > 0")
    title_template: str                 # f-string template (e.g., "{items_new} new videos")
    message_template: str               # f-string template
    level: str = "info"                 # one of NOTIFICATION_LEVELS
    link_to: str = ""                   # URL when clicked (supports {skill_name} template)

    def __post_init__(self):
        if self.event not in NOTIFICATION_EVENTS:
            raise ValueError(f"Unknown event '{self.event}'. "
                             f"Must be one of: {NOTIFICATION_EVENTS}")
        if self.level not in NOTIFICATION_LEVELS:
            raise ValueError(f"Unknown level '{self.level}'. "
                             f"Must be one of: {NOTIFICATION_LEVELS}")

    def evaluate(self, context: dict) -> tuple[bool, str, str] | None:
        """Evaluate this rule against a context.

        Args:
            context: dict with items_new, items_found, skill_name, etc.

        Returns:
            (title, message) if condition is met, None otherwise.
        """
        try:
            if eval(self.condition, {"__builtins__": {}}, context):
                title = self.title_template.format(**context)
                message = self.message_template.format(**context)
                return title, message
        except Exception:
            pass
        return None

    def to_dict(self) -> dict:
        return {
            "event": self.event,
            "condition": self.condition,
            "title_template": self.title_template,
            "message_template": self.message_template,
            "level": self.level,
            "link_to": self.link_to,
        }
