"""execute_sql node — run the validated query and store the result.

Uses the cleaned SQL from ValidationResult (which has LIMIT injected) so
the query that actually runs is always the validator-approved version.

On database errors the node records the error in state.  Phase 5 wires
the retry edge back to draft_sql; in the Phase 4 happy path the graph
simply terminates if execution fails.
"""

from __future__ import annotations

import time
from typing import Any

from langchain_core.runnables import RunnableConfig

from voyage.agent.client import WarehouseClient
from voyage.agent.state import AgentState, NodeError, Span


async def execute_sql(state: AgentState, config: RunnableConfig) -> dict[str, Any]:
    """Execute the validated SQL and populate query_result."""
    t0 = time.monotonic()

    val = state["validation_result"]
    if val is None or not val.ok:
        duration = round((time.monotonic() - t0) * 1000, 2)
        err = "Cannot execute: validation did not pass"
        return {
            "query_result": None,
            "errors": [NodeError(node="execute_sql", error=err)],
            "trace": [Span(node="execute_sql", duration_ms=duration, error=err)],
        }

    client: WarehouseClient = config["configurable"]["client"]
    try:
        result = await client.run_query(val.sql)
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "query_result": result,
            "errors": [],
            "trace": [Span(node="execute_sql", duration_ms=duration)],
        }
    except Exception as exc:  # noqa: BLE001
        duration = round((time.monotonic() - t0) * 1000, 2)
        next_retry = state["retry_count"] + 1
        return {
            "query_result": None,
            "retry_count": next_retry,
            "errors": [NodeError(node="execute_sql", error=str(exc))],
            "trace": [
                Span(
                    node="execute_sql",
                    duration_ms=duration,
                    retry_count=next_retry,
                    error=str(exc),
                )
            ],
        }
