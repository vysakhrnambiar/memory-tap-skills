"""
RAG Chat — chat with your collected data using LLM via OpenRouter.

Uses FTS5 search to find relevant context from all skill DBs,
then sends to LLM for natural language answers.
"""
import json
import logging

import requests

from ..db.core_db import get_setting
from .search import search_all, build_rag_context

logger = logging.getLogger("memory_tap.rag.chat")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """You are Memory Tap, a personal knowledge assistant. You help the user explore and understand their collected data from YouTube watch history, ChatGPT conversations, and Gemini conversations.

You have access to the user's locally collected data (provided as context below).
Answer questions based on this data. Be specific — cite conversation titles, video names, channels, dates when available.

If the data doesn't contain an answer, say so clearly.
Do not make up information. Only use what's in the provided context.

When referencing sources, mention whether it came from YouTube, ChatGPT, or Gemini."""


def chat(
    user_message: str,
    conversation_history: list[dict] | None = None,
    core_db_path: str | None = None,
    data_dir: str | None = None,
) -> dict:
    """Send a message to the LLM with RAG context from collected data.

    Args:
        user_message: The user's question
        conversation_history: Previous messages [{role, content}, ...]
        core_db_path: Path to core.db (for settings)
        data_dir: Path to skill_data/ directory (for search)

    Returns:
        dict with keys: response (str), sources (list[dict]), error (str|None)
    """
    api_key = get_setting("api_key", db_path=core_db_path)
    if not api_key:
        return {
            "response": "Please set your OpenRouter API key in Settings first. "
                        "Go to Settings and enter your key (get one at openrouter.ai).",
            "sources": [],
            "error": "no_api_key",
        }

    model = get_setting(
        "llm_provider",
        default="google/gemini-2.5-flash",
        db_path=core_db_path,
    )

    # Search across all skill DBs for context
    search_results = search_all(user_message, limit=20, data_dir=data_dir)

    # Build RAG context
    context = build_rag_context(user_message, max_tokens=4000, data_dir=data_dir)

    # Assemble messages
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if conversation_history:
        # Keep last 10 turns to stay within token budget
        messages.extend(conversation_history[-10:])

    # Augment user message with context
    augmented_message = f"{user_message}\n\n---\n{context}"
    messages.append({"role": "user", "content": augmented_message})

    # Call OpenRouter
    try:
        resp = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/vysakhrnambiar/memory-tap",
                "X-Title": "Memory Tap",
            },
            json={
                "model": model,
                "messages": messages,
                "max_tokens": 2000,
                "temperature": 0.3,
            },
            timeout=60,
        )

        if resp.status_code != 200:
            error = resp.text[:200]
            logger.error("OpenRouter error %d: %s", resp.status_code, error)
            return {
                "response": f"LLM API error ({resp.status_code}): {error}",
                "sources": [],
                "error": f"api_error_{resp.status_code}",
            }

        data = resp.json()
        answer = data["choices"][0]["message"]["content"]

        # Build source summary for the UI
        sources = []
        seen = set()
        for r in search_results[:10]:
            key = (r["source"], r["title"])
            if key not in seen:
                seen.add(key)
                sources.append({
                    "source": r["source"],
                    "title": r["title"],
                    "url": r.get("url", ""),
                    "type": r["type"],
                })

        return {
            "response": answer,
            "sources": sources,
            "error": None,
        }

    except requests.Timeout:
        return {
            "response": "LLM request timed out. Please try again.",
            "sources": [],
            "error": "timeout",
        }
    except Exception as e:
        logger.error("Chat error: %s", e)
        return {
            "response": f"Error: {e}",
            "sources": [],
            "error": str(e),
        }
