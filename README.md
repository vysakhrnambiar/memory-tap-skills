# Memory Tap Skills

Official skill repository for [Memory Tap](https://github.com/vysakhrnambiar/memory-tap) — the personal knowledge collector.

## What Are Skills?

Skills are Python scripts that navigate websites using Chrome DevTools Protocol (CDP) and collect your data into a local SQLite database. They run in the background without needing any API keys or LLM — pure navigation scripts that behave like a human browsing.

## Available Skills

| Skill | Version | Site | What It Collects |
|-------|---------|------|-----------------|
| `youtube_history` | 0.1.0 | youtube.com | Watch history, video descriptions, top comments |
| `chatgpt_history` | 0.1.0 | chatgpt.com | All conversations, messages, thinking blocks, artifact downloads |
| `gemini_history` | 0.1.0 | gemini.google.com | All conversations, messages, thinking blocks |

## How Skills Work

1. Memory Tap launches an isolated Chrome instance with your Google account
2. Skills navigate to their target site using CDP
3. They scroll slowly, click naturally, and read page content — mimicking human behavior
4. Collected data is stored in your local SQLite database
5. Skills run on a schedule (default: every 3 hours)

## Auto-Updates

Memory Tap automatically checks this repository for skill updates every 6 hours. When a skill is updated (e.g., because a website changed its layout), your local copy is automatically replaced.

## Skill Interface

Every skill implements this interface:

```python
from memory_tap.skills.base import BaseSkill, SkillManifest, CollectResult

class MySkill(BaseSkill):
    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="my_skill",
            version="1.0.0",
            target_url="https://example.com",
            description="What this skill collects",
        )

    def check_login(self, tab) -> bool:
        """Return True if user is logged in."""
        ...

    def collect(self, tab, tracker) -> CollectResult:
        """Navigate and collect data."""
        ...
```

## manifest.json

The `manifest.json` file in this repo is the source of truth. Memory Tap reads it to discover available skills, check versions, and verify checksums.

## Security

- Skills only run from THIS repository (hardcoded in Memory Tap)
- Each skill file has a SHA-256 checksum in the manifest
- Skills execute in the same Chrome instance as the user's session — they can only access what the user can access
- No data leaves your machine — everything stays in local SQLite

## Contributing

This is a managed repository. If you'd like a new skill added, open an issue describing the website and what data to collect.
