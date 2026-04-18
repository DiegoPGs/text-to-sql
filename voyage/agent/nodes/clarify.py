"""clarify node — pause execution to ask the user for clarification.

Uses LangGraph's :func:`interrupt` primitive.  The node calls ``interrupt(...)``
with a payload describing what is unclear; the graph pauses and the runtime
returns control to the caller.  The caller (the CLI) prompts the user, then
resumes the graph with ``Command(resume=user_text)``.  On resume the
``interrupt(...)`` call returns the user's text, which is written to
``state.clarification`` and execution continues to ``retrieve_context``.
"""

from __future__ import annotations

import time
from typing import Any

from langgraph.types import interrupt

from voyage.agent.state import AgentState, Span


def clarify(state: AgentState) -> dict[str, Any]:
    """Pause for HITL clarification, then write the user's reply to state."""
    t0 = time.monotonic()

    intent = state["intent"]
    rationale = intent.rationale if intent is not None else "Question is ambiguous."

    # Pauses the graph; on resume returns the value passed via Command(resume=...).
    user_response = interrupt(
        {
            "question": state["question"],
            "reason": rationale,
        }
    )

    duration = round((time.monotonic() - t0) * 1000, 2)
    return {
        "clarification": str(user_response),
        "errors": [],
        "trace": [Span(node="clarify", duration_ms=duration)],
    }
