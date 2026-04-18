"""terminate_failure node — terminal Answer after retries are exhausted.

Routed to from ``validate_sql`` or ``execute_sql`` when ``retry_count`` has
reached the cap.  Surfaces the last few errors so the user (and the trace)
can see why the agent gave up.
"""

from __future__ import annotations

import time
from typing import Any

from voyage.agent.state import AgentState, Answer, Span


def terminate_failure(state: AgentState) -> dict[str, Any]:
    """Produce a failure Answer summarising why the agent gave up."""
    t0 = time.monotonic()

    last_errors = [
        f"{e.node}: {e.error}"
        for e in state["errors"]
        if e.node in ("validate_sql", "execute_sql", "draft_sql")
    ][-3:]

    answer = Answer(
        summary=(
            "I wasn't able to produce a working query for that question after "
            f"{state['retry_count']} attempts. Try rephrasing or narrowing the question."
        ),
        highlights=last_errors,
        chart_spec=None,
    )

    duration = round((time.monotonic() - t0) * 1000, 2)
    return {
        "answer": answer,
        "errors": [],
        "trace": [Span(node="terminate_failure", duration_ms=duration)],
    }
