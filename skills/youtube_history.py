"""
YouTube History Skill — collects watch history with descriptions and top comments.

Flow:
1. Navigate to youtube.com/feed/history
2. Check if logged in (avatar present)
3. Scroll through history, collecting video titles + URLs + timestamps
4. For each new video: navigate to video page, wait, grab description + top comment
5. Store in youtube_videos table

__version__ = "0.1.0"
"""
__version__ = "0.1.0"

import json
import logging
import random
import re
import time

logger = logging.getLogger("memory_tap.skill.youtube")

# These imports work when loaded by Memory Tap's skill engine
# The skill engine injects the base classes at load time
# These imports resolve because the scheduler injects the project root into sys.path
from src.skills.base import BaseSkill, SkillManifest, CollectResult
from src.cdp_client import CDPTab
from src.db.sync_tracker import SyncTracker
from src.human import (
    scroll_slowly, scroll_to_bottom, click_at, click_element,
    wait_human, watch_page, move_mouse,
)


class YouTubeHistorySkill(BaseSkill):
    """Collects YouTube watch history."""

    @property
    def manifest(self) -> SkillManifest:
        return SkillManifest(
            name="youtube_history",
            version=__version__,
            target_url="https://www.youtube.com/feed/history",
            description="Collects YouTube watch history — titles, descriptions, top comments",
            auth_provider="google",
            schedule_hours=3,
            login_url="https://accounts.google.com/ServiceLogin?service=youtube",
        )

    def check_login(self, tab: CDPTab) -> bool:
        """Check if logged into YouTube.

        Verified detection (2026-03-14 CDP probe):
        - Logged in:  button#avatar-btn exists, title contains watch count like "(1113)",
                      no "Sign in" text visible
        - Not logged in: no avatar button, title is just "YouTube", "Sign in" text visible
        """
        tab.navigate("https://www.youtube.com/feed/history")
        wait_human(2, 4)

        # Primary: check for avatar button (most reliable indicator)
        avatar = tab.query_selector("button#avatar-btn")
        if avatar and avatar.get("visible"):
            logger.info("YouTube: logged in (avatar button found)")
            return True

        # Secondary: check for "Sign in" text
        if tab.has_text("Sign in"):
            logger.info("YouTube: not logged in (Sign in text visible)")
            return False

        # If unclear — NOT logged in (never assume)
        logger.info("YouTube: login unclear, treating as not logged in")
        return False

    def collect(self, tab: CDPTab, tracker: SyncTracker) -> CollectResult:
        """Scroll through history and collect videos."""
        result = CollectResult()

        # Navigate to history page
        tab.navigate("https://www.youtube.com/feed/history")
        wait_human(2, 4)

        # Collect video entries from the history page
        videos_to_process = []
        seen_ids = set()
        max_scrolls = 15  # Don't scroll forever

        for scroll_num in range(max_scrolls):
            # Extract video entries currently visible
            entries = tab.js("""
                var items = document.querySelectorAll('ytd-video-renderer, ytd-reel-shelf-renderer a');
                var out = [];
                for (var i = 0; i < items.length; i++) {
                    var item = items[i];
                    var titleEl = item.querySelector('#video-title, h3 a');
                    if (!titleEl) continue;
                    var href = titleEl.getAttribute('href') || titleEl.closest('a')?.getAttribute('href') || '';
                    var title = titleEl.textContent.trim();
                    var channelEl = item.querySelector('#channel-name a, .ytd-channel-name a');
                    var channel = channelEl ? channelEl.textContent.trim() : '';
                    if (href && title) {
                        out.push({href: href, title: title, channel: channel});
                    }
                }
                return JSON.stringify(out);
            """)

            if entries:
                try:
                    for entry in json.loads(entries):
                        video_id = _extract_video_id(entry["href"])
                        if video_id and video_id not in seen_ids:
                            seen_ids.add(video_id)
                            if not tracker.video_exists(video_id):
                                videos_to_process.append({
                                    "video_id": video_id,
                                    "title": entry["title"],
                                    "channel": entry["channel"],
                                    "url": f"https://www.youtube.com/watch?v={video_id}",
                                })
                except (json.JSONDecodeError, TypeError):
                    pass

            # Scroll down to load more
            scroll_slowly(tab, random.randint(400, 700))
            wait_human(1.5, 3.0)

            # If no new videos found in last scroll, stop
            if scroll_num > 3 and len(videos_to_process) == 0:
                break

        result.items_found = len(seen_ids)
        logger.info("Found %d videos in history, %d new to process", len(seen_ids), len(videos_to_process))

        # Visit each new video page for description + top comment
        for i, video in enumerate(videos_to_process):
            try:
                desc, top_comment, top_comment_author, duration = self._get_video_details(
                    tab, video["url"]
                )
                tracker.add_video(
                    video_id=video["video_id"],
                    title=video["title"],
                    channel=video["channel"],
                    url=video["url"],
                    description=desc,
                    top_comment=top_comment,
                    top_comment_author=top_comment_author,
                    duration=duration,
                )
                result.items_new += 1
                logger.info("Collected video %d/%d: %s", i + 1, len(videos_to_process), video["title"][:60])
            except Exception as e:
                logger.warning("Failed to collect video %s: %s", video["video_id"], e)

            # Be gentle — wait between video pages
            wait_human(2, 5)

        return result

    def _get_video_details(self, tab: CDPTab, url: str) -> tuple:
        """Navigate to video page and extract description + top comment.

        Returns (description, top_comment, top_comment_author, duration).
        """
        tab.navigate(url)
        wait_human(3, 5)  # Let video page load

        # Simulate watching for a few seconds
        watch_page(tab, random.uniform(3, 6))

        # Get duration
        duration = tab.js("""
            var dur = document.querySelector('.ytp-time-duration');
            return dur ? dur.textContent : null;
        """)

        # Expand description (click "...more")
        click_element(tab, "tp-yt-paper-button#expand, #description-inline-expander #expand")
        wait_human(1, 2)

        # Get description text
        description = tab.js("""
            var desc = document.querySelector('#description-inner, #description yt-attributed-string');
            return desc ? desc.textContent.trim().substring(0, 5000) : null;
        """) or ""

        # Scroll down to comments
        scroll_slowly(tab, random.randint(600, 1000))
        wait_human(2, 4)

        # Get top comment
        top_comment = None
        top_comment_author = None
        comment_data = tab.js("""
            var comment = document.querySelector('ytd-comment-thread-renderer #content-text');
            var author = document.querySelector('ytd-comment-thread-renderer #author-text');
            if (comment) {
                return JSON.stringify({
                    text: comment.textContent.trim().substring(0, 2000),
                    author: author ? author.textContent.trim() : ''
                });
            }
            return null;
        """)
        if comment_data:
            try:
                data = json.loads(comment_data)
                top_comment = data["text"]
                top_comment_author = data["author"]
            except (json.JSONDecodeError, TypeError):
                pass

        return description, top_comment, top_comment_author, duration


def _extract_video_id(href: str) -> str | None:
    """Extract video ID from YouTube URL."""
    if not href:
        return None
    # /watch?v=XXXXX
    match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})', href)
    if match:
        return match.group(1)
    # /shorts/XXXXX
    match = re.search(r'/shorts/([a-zA-Z0-9_-]{11})', href)
    if match:
        return match.group(1)
    return None
