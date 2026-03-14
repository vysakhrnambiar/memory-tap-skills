"""
Human-like interaction module — makes CDP automation indistinguishable from a real user.

Features:
- Bezier curve mouse movements (not straight lines)
- Randomized scroll speeds with momentum
- Natural typing with variable inter-key delays
- Realistic pauses (reading time, thinking time)
- Mouse jitter and drift
- Click with micro-delays between press/release
"""
import json
import math
import random
import time

from .cdp_client import CDPTab


# --- Mouse Movement (Bezier curves) ---

def _bezier_point(t: float, p0: tuple, p1: tuple, p2: tuple, p3: tuple) -> tuple:
    """Cubic Bezier interpolation."""
    u = 1 - t
    return (
        u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0],
        u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1],
    )


def _generate_mouse_path(
    start: tuple[float, float],
    end: tuple[float, float],
    steps: int = 0,
) -> list[tuple[float, float]]:
    """Generate a natural-looking mouse path using cubic Bezier curves.

    Control points are offset randomly to create a slightly curved path,
    like a real hand movement.
    """
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    dist = math.sqrt(dx**2 + dy**2)

    if steps == 0:
        steps = max(10, int(dist / 15))  # ~15px per step

    # Random control point offsets (perpendicular to the line)
    offset1 = random.uniform(-dist * 0.15, dist * 0.15)
    offset2 = random.uniform(-dist * 0.15, dist * 0.15)

    # Perpendicular direction
    if dist > 0:
        perp_x = -dy / dist
        perp_y = dx / dist
    else:
        perp_x, perp_y = 0, 0

    # Control points: roughly 1/3 and 2/3 along the path, with perpendicular offset
    cp1 = (
        start[0] + dx * 0.3 + perp_x * offset1,
        start[1] + dy * 0.3 + perp_y * offset1,
    )
    cp2 = (
        start[0] + dx * 0.7 + perp_x * offset2,
        start[1] + dy * 0.7 + perp_y * offset2,
    )

    path = []
    for i in range(steps + 1):
        t = i / steps
        # Ease in-out (start/end slower, middle faster)
        t = t * t * (3 - 2 * t)
        point = _bezier_point(t, start, cp1, cp2, end)
        # Add micro-jitter (1-2px random noise)
        jitter_x = random.uniform(-1.5, 1.5)
        jitter_y = random.uniform(-1.5, 1.5)
        path.append((point[0] + jitter_x, point[1] + jitter_y))

    # Final point is exact (no jitter)
    path[-1] = end
    return path


def move_mouse(tab: CDPTab, x: float, y: float, from_pos: tuple[float, float] | None = None):
    """Move mouse along a natural Bezier curve to (x, y).

    If from_pos not given, starts from a random edge position.
    """
    if from_pos is None:
        from_pos = (random.uniform(100, 900), random.uniform(100, 600))

    path = _generate_mouse_path(from_pos, (x, y))
    for px, py in path:
        tab._send("Input.dispatchMouseEvent", {
            "type": "mouseMoved",
            "x": int(px), "y": int(py),
        })
        # Variable speed: faster in middle, slower at start/end
        time.sleep(random.uniform(0.005, 0.02))


def click_at(tab: CDPTab, x: float, y: float, from_pos: tuple[float, float] | None = None):
    """Move mouse naturally to position, then click with human-like timing."""
    move_mouse(tab, x, y, from_pos)
    time.sleep(random.uniform(0.05, 0.15))  # hesitation before click

    tab._send("Input.dispatchMouseEvent", {
        "type": "mousePressed", "x": int(x), "y": int(y),
        "button": "left", "clickCount": 1,
    })
    time.sleep(random.uniform(0.05, 0.12))  # hold duration
    tab._send("Input.dispatchMouseEvent", {
        "type": "mouseReleased", "x": int(x), "y": int(y),
        "button": "left", "clickCount": 1,
    })


def click_element(tab: CDPTab, selector: str, timeout: float = 10) -> bool:
    """Find element by CSS, move mouse naturally to it, click."""
    el = tab.wait_for_selector(selector, timeout=timeout)
    if not el:
        return False
    # Add slight randomness within element bounds
    x = el["x"] + random.uniform(-el["w"] * 0.2, el["w"] * 0.2)
    y = el["y"] + random.uniform(-el["h"] * 0.2, el["h"] * 0.2)
    click_at(tab, x, y)
    return True


def click_text(tab: CDPTab, text: str, tag: str | None = None) -> bool:
    """Find element by text content, click it naturally."""
    tag_filter = f"el.tagName === '{tag.upper()}'" if tag else "true"
    result = tab.js(f"""
        var els = document.querySelectorAll('*');
        for (var i = 0; i < els.length; i++) {{
            var el = els[i];
            var r = el.getBoundingClientRect();
            if (r.width === 0 || r.height === 0) continue;
            if (el.children.length > 3) continue;
            if (el.textContent.trim() === {json.dumps(text)} && {tag_filter}) {{
                return JSON.stringify({{
                    x: r.x + r.width/2, y: r.y + r.height/2,
                    w: r.width, h: r.height
                }});
            }}
        }}
        return null;
    """)
    if result:
        try:
            pos = json.loads(result)
            x = pos["x"] + random.uniform(-pos["w"] * 0.15, pos["w"] * 0.15)
            y = pos["y"] + random.uniform(-pos["h"] * 0.15, pos["h"] * 0.15)
            click_at(tab, x, y)
            return True
        except Exception:
            pass
    return False


# --- Scrolling ---

def scroll_slowly(tab: CDPTab, pixels: int = 300, x: int = 640, y: int = 400):
    """Scroll with momentum — starts slow, speeds up, slows down. Like a real scroll wheel."""
    remaining = abs(pixels)
    direction = 1 if pixels > 0 else -1

    while remaining > 0:
        # Variable chunk size (momentum effect)
        chunk = min(remaining, random.randint(40, 120))
        tab._send("Input.dispatchMouseEvent", {
            "type": "mouseWheel",
            "x": x + random.randint(-5, 5),  # slight x jitter
            "y": y + random.randint(-5, 5),
            "deltaX": 0,
            "deltaY": chunk * direction,
        })
        remaining -= chunk
        # Variable delay between scroll events
        time.sleep(random.uniform(0.03, 0.08))

    # Settle pause after scrolling
    time.sleep(random.uniform(0.2, 0.5))


def scroll_to_bottom(tab: CDPTab, max_scrolls: int = 50, pause_range: tuple = (1.0, 3.0)):
    """Scroll to bottom of page like a human reading — pause between scrolls.

    Returns number of scrolls performed.
    """
    scrolls = 0
    last_height = 0

    for _ in range(max_scrolls):
        height = tab.get_scroll_height()
        pos = tab.get_scroll_position()
        viewport = tab.js("return window.innerHeight") or 800

        if pos + viewport >= height - 50:
            # At bottom — check if more content loaded
            time.sleep(1.5)
            new_height = tab.get_scroll_height()
            if new_height <= height:
                break  # truly at bottom
            height = new_height

        # Scroll a "page" worth with human-like variation
        scroll_amount = random.randint(int(viewport * 0.5), int(viewport * 0.85))
        scroll_slowly(tab, scroll_amount)
        scrolls += 1

        # Pause like reading
        time.sleep(random.uniform(*pause_range))

    return scrolls


def scroll_up(tab: CDPTab, pixels: int = 300, x: int = 640, y: int = 400):
    """Scroll up with momentum."""
    scroll_slowly(tab, -abs(pixels), x, y)


# --- Typing ---

def type_text(tab: CDPTab, text: str, wpm: int = 0):
    """Type text with human-like inter-key delays.

    If wpm is 0, uses a natural random range (~40-80 WPM).
    """
    for ch in text:
        tab._send("Input.dispatchKeyEvent", {
            "type": "keyDown", "text": ch, "key": ch, "unmodifiedText": ch,
        })
        tab._send("Input.dispatchKeyEvent", {
            "type": "keyUp", "key": ch,
        })
        if wpm > 0:
            delay = 60.0 / (wpm * 5)  # 5 chars per word avg
        else:
            delay = random.uniform(0.03, 0.12)
            # Occasional longer pause (thinking)
            if random.random() < 0.05:
                delay += random.uniform(0.2, 0.5)
        time.sleep(delay)


def press_enter(tab: CDPTab):
    """Press Enter key."""
    tab._send("Input.dispatchKeyEvent", {
        "type": "rawKeyDown", "key": "Enter", "code": "Enter",
        "windowsVirtualKeyCode": 13,
    })
    tab._send("Input.dispatchKeyEvent", {
        "type": "keyUp", "key": "Enter", "code": "Enter",
        "windowsVirtualKeyCode": 13,
    })


# --- Waiting ---

def wait_human(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """Pause like a human (reading, thinking, deciding)."""
    time.sleep(random.uniform(min_seconds, max_seconds))


def wait_page_settle(tab: CDPTab, timeout: float = 5.0):
    """Wait for page to stop changing (no new network requests, DOM stable)."""
    # Simple approach: check if page text stays stable for 1 second
    last_text = ""
    stable_since = None
    deadline = time.time() + timeout

    while time.time() < deadline:
        text = tab.get_page_text()
        if text == last_text:
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since > 1.0:
                return True
        else:
            last_text = text
            stable_since = None
        time.sleep(0.3)
    return False


def watch_page(tab: CDPTab, seconds: float = 3.0):
    """Simulate watching/reading a page — small mouse drifts, no clicks."""
    end_time = time.time() + seconds
    x = random.uniform(300, 800)
    y = random.uniform(200, 500)

    while time.time() < end_time:
        # Small drift
        x += random.uniform(-30, 30)
        y += random.uniform(-20, 20)
        x = max(50, min(1200, x))
        y = max(50, min(700, y))
        tab._send("Input.dispatchMouseEvent", {
            "type": "mouseMoved", "x": int(x), "y": int(y),
        })
        time.sleep(random.uniform(0.3, 0.8))
