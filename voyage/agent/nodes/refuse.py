"""refuse node — terminal Answer for out-of-scope questions.

Routed to from ``classify_intent`` when the intent is ``out_of_scope``.
Produces a brief Answer that explains why the agent will not answer.
"""

from __future__ import annotations

import time
from typing import Any

from voyage.agent.state import AgentState, Answer, Span


def refuse(state: AgentState) -> dict[str, Any]:
    """Produce a short refusal Answer and end the graph."""
    t0 = time.monotonic()

    intent = state["intent"]
    rationale = (
        intent.rationale
        if intent is not None
        else "The question is outside the scope of the warehouse copilot."
    )

    answer = Answer(
        summary=(
            "I can't help with that question. "
            "This assistant only answers analytics questions over the vacation-rental "
            f"warehouse, and never returns PII or runs write operations. {rationale}"
        ),
        highlights=[],
        chart_spec=None,
    )

    duration = round((time.monotonic() - t0) * 1000, 2)
    return {
        "answer": answer,
        "errors": [],
        "trace": [Span(node="refuse", duration_ms=duration)],
    }
