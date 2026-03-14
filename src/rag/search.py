"""
RAG Search — full-text search across all collected data.

Uses SQLite FTS5 for fast text search. No embeddings needed.
Provides context assembly for LLM queries.
"""
import logging
from ..db.models import get_connection

logger = logging.getLogger("memory_tap.rag")


def search_messages(query: str, limit: int = 10, db_path: str | None = None) -> list[dict]:
    """Search across conversation messages using FTS5."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT m.content, m.role, m.thinking_block, m.message_order,
                  c.title, c.source, c.url, c.external_id
           FROM messages_fts f
           JOIN messages m ON m.id = f.rowid
           JOIN conversations c ON c.id = m.conversation_id
           WHERE messages_fts MATCH ?
           ORDER BY rank
           LIMIT ?""",
        (query, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_videos(query: str, limit: int = 10, db_path: str | None = None) -> list[dict]:
    """Search across YouTube videos using FTS5."""
    conn = get_connection(db_path)
    rows = conn.execute(
        """SELECT v.video_id, v.title, v.channel, v.url, v.description,
                  v.top_comment, v.watched_at
           FROM youtube_fts f
           JOIN youtube_videos v ON v.id = f.rowid
           WHERE youtube_fts MATCH ?
           ORDER BY rank
           LIMIT ?""",
        (query, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def search_all(query: str, limit: int = 20, db_path: str | None = None) -> list[dict]:
    """Search across all data sources. Returns unified results."""
    results = []

    for msg in search_messages(query, limit=limit // 2, db_path=db_path):
        results.append({
            "type": "message",
            "source": msg["source"],
            "title": msg["title"],
            "role": msg["role"],
            "content": msg["content"][:500],
            "thinking": msg.get("thinking_block", ""),
            "url": msg.get("url"),
        })

    for vid in search_videos(query, limit=limit // 2, db_path=db_path):
        results.append({
            "type": "youtube",
            "source": "youtube",
            "title": vid["title"],
            "channel": vid["channel"],
            "content": vid.get("description", "")[:500],
            "url": vid["url"],
        })

    return results


def build_rag_context(query: str, max_tokens: int = 4000, db_path: str | None = None) -> str:
    """Build context string for LLM from search results.

    Assembles the most relevant search hits into a structured context
    that fits within the token budget.
    """
    results = search_all(query, limit=15, db_path=db_path)

    if not results:
        return "No relevant information found in your collected data."

    parts = ["## Relevant information from your collected data:\n"]
    char_budget = max_tokens * 4  # rough chars-to-tokens ratio

    for r in results:
        if r["type"] == "message":
            entry = (
                f"### [{r['source']}] {r['title']}\n"
                f"**{r['role']}**: {r['content']}\n"
            )
            if r.get("thinking"):
                entry += f"*Thinking*: {r['thinking'][:200]}\n"
        else:
            entry = (
                f"### [YouTube] {r['title']}\n"
                f"**Channel**: {r.get('channel', 'Unknown')}\n"
                f"{r['content']}\n"
            )

        if len("\n".join(parts)) + len(entry) > char_budget:
            break
        parts.append(entry)

    return "\n".join(parts)
