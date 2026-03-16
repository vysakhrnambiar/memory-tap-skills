"""
RAG Search — full-text search across all per-skill databases.

Uses SQLite FTS5 for fast text search. Each skill has its own DB with
its own FTS5 indexes. This module queries all of them and merges results.

Provides context assembly for LLM queries.
"""
import logging
import os
import sqlite3

logger = logging.getLogger("memory_tap.rag.search")

# Skill DB directory (same as SkillDBManager uses)
LOCALAPPDATA = os.environ.get("LOCALAPPDATA", "")
SKILL_DATA_DIR = os.path.join(LOCALAPPDATA, "MemoryTap", "skill_data")


def _get_skill_conn(skill_name: str, data_dir: str | None = None) -> sqlite3.Connection | None:
    """Get a read-only connection to a skill's DB. Returns None if DB doesn't exist."""
    base = data_dir or SKILL_DATA_DIR
    path = os.path.join(base, f"{skill_name}.db")
    if not os.path.isfile(path):
        return None
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def search_youtube(query: str, limit: int = 10,
                   data_dir: str | None = None) -> list[dict]:
    """Search YouTube watch history via videos_fts."""
    conn = _get_skill_conn("youtube_history", data_dir)
    if not conn:
        return []
    try:
        rows = conn.execute(
            "SELECT v.title, v.channel, v.url, v.description, "
            "v.top_comment, v.watched_date, rank "
            "FROM videos_fts f JOIN videos v ON v.id = f.rowid "
            "WHERE videos_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ).fetchall()
        return [
            {
                "type": "video",
                "source": "youtube_history",
                "title": r["title"],
                "snippet": (r["description"] or "")[:300],
                "url": r["url"],
                "date": r["watched_date"],
                "channel": r["channel"],
                "top_comment": (r["top_comment"] or "")[:200],
                "score": r["rank"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("YouTube search failed: %s", e)
        return []
    finally:
        conn.close()


def search_chatgpt(query: str, limit: int = 10,
                   data_dir: str | None = None) -> list[dict]:
    """Search ChatGPT conversations via messages_fts."""
    conn = _get_skill_conn("chatgpt_history", data_dir)
    if not conn:
        return []
    try:
        rows = conn.execute(
            "SELECT m.content, m.role, m.sources, c.title, c.url, "
            "c.last_updated, rank "
            "FROM messages_fts f "
            "JOIN messages m ON m.id = f.rowid "
            "JOIN conversations c ON c.id = m.conversation_id "
            "WHERE messages_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ).fetchall()
        return [
            {
                "type": "message",
                "source": "chatgpt_history",
                "title": r["title"],
                "snippet": (r["content"] or "")[:300],
                "url": r["url"],
                "date": r["last_updated"],
                "role": r["role"],
                "sources": r["sources"] or "",
                "score": r["rank"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("ChatGPT search failed: %s", e)
        return []
    finally:
        conn.close()


def search_gemini(query: str, limit: int = 10,
                  data_dir: str | None = None) -> list[dict]:
    """Search Gemini conversations via messages_fts."""
    conn = _get_skill_conn("gemini_history", data_dir)
    if not conn:
        return []
    try:
        rows = conn.execute(
            "SELECT m.content, m.role, m.thinking_block, c.title, c.url, "
            "c.last_updated, rank "
            "FROM messages_fts f "
            "JOIN messages m ON m.id = f.rowid "
            "JOIN conversations c ON c.id = m.conversation_id "
            "WHERE messages_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit),
        ).fetchall()
        return [
            {
                "type": "message",
                "source": "gemini_history",
                "title": r["title"],
                "snippet": (r["content"] or "")[:300],
                "url": r["url"],
                "date": r["last_updated"],
                "role": r["role"],
                "has_thinking": bool(r["thinking_block"]),
                "score": r["rank"],
            }
            for r in rows
        ]
    except Exception as e:
        logger.warning("Gemini search failed: %s", e)
        return []
    finally:
        conn.close()


def search_all(query: str, limit: int = 20,
               data_dir: str | None = None) -> list[dict]:
    """Search across all 3 skill DBs via FTS5.

    Returns unified results sorted by FTS5 rank across all sources.
    Each result has: source, title, snippet, date, score, type, url.
    """
    per_source = max(5, limit)  # Get extras, then trim after merge

    results = []
    results.extend(search_youtube(query, limit=per_source, data_dir=data_dir))
    results.extend(search_chatgpt(query, limit=per_source, data_dir=data_dir))
    results.extend(search_gemini(query, limit=per_source, data_dir=data_dir))

    # Sort by FTS5 rank (lower = more relevant, rank is negative)
    results.sort(key=lambda r: r.get("score", 0))

    return results[:limit]


def build_rag_context(query: str, max_tokens: int = 4000,
                      data_dir: str | None = None) -> str:
    """Build context string for LLM from search results.

    Assembles the most relevant search hits into a structured context
    that fits within the token budget.
    """
    results = search_all(query, limit=20, data_dir=data_dir)

    if not results:
        return "No relevant information found in your collected data."

    parts = ["## Relevant information from your collected data:\n"]
    char_budget = max_tokens * 4  # rough chars-to-tokens ratio

    for r in results:
        if r["type"] == "video":
            entry = (
                f"### [YouTube] {r['title']}\n"
                f"**Channel**: {r.get('channel', 'Unknown')}\n"
                f"{r['snippet']}\n"
            )
            if r.get("top_comment"):
                entry += f"**Top comment**: {r['top_comment']}\n"
        else:
            # ChatGPT or Gemini message
            source_label = "ChatGPT" if r["source"] == "chatgpt_history" else "Gemini"
            entry = (
                f"### [{source_label}] {r['title']}\n"
                f"**{r.get('role', 'unknown')}**: {r['snippet']}\n"
            )
            if r.get("date"):
                entry += f"*Date*: {r['date']}\n"

        if len("\n".join(parts)) + len(entry) > char_budget:
            break
        parts.append(entry)

    return "\n".join(parts)
