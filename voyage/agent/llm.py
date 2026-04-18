"""Anthropic + instructor client wrapper.

Every LLM call in the agent goes through ``chat()``.  The response is
always a typed Pydantic model — no free-form string parsing anywhere.

Usage::

    model, tok_in, tok_out = chat(
        MyModel,
        [{"role": "user", "content": "..."}],
        system="You are ...",
    )
"""

from __future__ import annotations

from typing import Any

import anthropic
import instructor
from pydantic import BaseModel

from voyage import config

# Module-level singletons — created once per process.
_anthropic_client: anthropic.Anthropic | None = None
_instructor_client: Any = None


def _get_client() -> Any:
    """Lazily initialise and return the instructor-wrapped Anthropic client."""
    global _anthropic_client, _instructor_client
    if _instructor_client is None:
        _anthropic_client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        _instructor_client = instructor.from_anthropic(_anthropic_client)
    return _instructor_client


def chat[T: BaseModel](
    response_model: type[T],
    messages: list[dict[str, str]],
    *,
    system: str = "",
    model: str = "",
    max_retries: int = 2,
) -> tuple[T, int, int]:
    """Call the LLM and return a typed model plus token counts.

    Args:
        response_model: Pydantic model class the LLM must populate.
        messages:       Chat history in ``[{"role": ..., "content": ...}]`` format.
        system:         Optional system prompt.
        model:          Model ID override; defaults to ``config.MODEL_PRIMARY``.
        max_retries:    instructor validation retries (not network retries).

    Returns:
        ``(parsed_model, tokens_in, tokens_out)``
    """
    client = _get_client()
    effective_model = model or config.MODEL_PRIMARY
    kwargs: dict[str, Any] = {
        "model": effective_model,
        "max_tokens": 2048,
        "messages": messages,
        "response_model": response_model,
        "max_retries": max_retries,
    }
    if system:
        kwargs["system"] = system

    result: T
    result, completion = client.messages.create_with_completion(**kwargs)
    usage = getattr(completion, "usage", None)
    tokens_in = int(getattr(usage, "input_tokens", 0))
    tokens_out = int(getattr(usage, "output_tokens", 0))
    return result, tokens_in, tokens_out
