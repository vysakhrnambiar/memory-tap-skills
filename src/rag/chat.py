"""
RAG Chat — chat with your collected data using LLM via OpenRouter.

Uses FTS5 search to find relevant context, then sends to LLM for natural
language answers.
"""
import json
import logging

import requests

from ..db.models import get_setting
from .search import build_rag_context

logger = logging.getLogger("memory_tap.rag.chat")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

SYSTEM_PROMPT = """You are Memory Tap Assistant. You help the user explore and understand
their personal browsing data — conversations from ChatGPT, Gemini, and YouTube watch history.

You have access to the user's locally collected data (provided as context below).
Answer questions based on this data. Be specific — cite conversation titles, video names,
dates when available. If the data doesn't contain an answer, say so clearly.

Do not make up information. Only use what's in the provided context."""


def chat(
    user_message: str,
    conversation_history: list[dict] | None = None,
    db_path: str | None = None,
) -> str:
    """Send a message to the LLM with RAG context from collected data.

    Args:
        user_message: The user's question
        conversation_history: Previous messages in this chat session
        db_path: SQLite database path

    Returns:
        The LLM's response text
    """
    api_key = get_setting("api_key", db_path=db_path)
    if not api_key:
        return "Please set your OpenRouter API key in Settings first."

    model = get_setting("llm_provider", default="google/gemini-2.5-flash", db_path=db_path)

    # Build RAG context from search
    context = build_rag_context(user_message, max_tokens=4000, db_path=db_path)

    # Assemble messages
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    if conversation_history:
        messages.extend(conversation_history[-10:])  # Keep last 10 turns

    # Add context + user message
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
            return f"LLM API error ({resp.status_code}): {error}"

        data = resp.json()
        return data["choices"][0]["message"]["content"]

    except requests.Timeout:
        return "LLM request timed out. Please try again."
    except Exception as e:
        logger.error("Chat error: %s", e)
        return f"Error: {e}"
