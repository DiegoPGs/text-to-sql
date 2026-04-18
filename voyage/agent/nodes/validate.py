"""validate_sql node — check safety, then check cost.

Pipeline:
  1. Run validate_and_sanitize (pure Python — SELECT-only, no forbidden
     functions, LIMIT injection).
  2. If validation passes, call explain_query to get the estimated cost.
  3. If estimated cost exceeds MAX_COST, reject the query.

Returns a ValidationResult in state.  On failure, errors are appended so
Phase 5 can route back to draft_sql for self-correction.
"""

from __future__ import annotations

import time
from typing import Any

from langchain_core.runnables import RunnableConfig

from server._models import ValidationResult
from server._validator import validate_and_sanitize
from voyage import config as _cfg
from voyage.agent.client import WarehouseClient
from voyage.agent.state import AgentState, NodeError, Span


async def validate_sql(state: AgentState, config: RunnableConfig) -> dict[str, Any]:
    """Validate and cost-check the SQL draft."""
    t0 = time.monotonic()

    next_retry = state["retry_count"] + 1

    draft = state["sql_draft"]
    if draft is None:
        duration = round((time.monotonic() - t0) * 1000, 2)
        err = "No SQL draft to validate"
        return {
            "validation_result": ValidationResult(ok=False, errors=[err]),
            "retry_count": next_retry,
            "errors": [NodeError(node="validate_sql", error=err)],
            "trace": [Span(node="validate_sql", duration_ms=duration, error=err)],
        }

    # Step 1 — pure-Python safety check
    val = validate_and_sanitize(draft.sql, row_limit=_cfg.ROW_LIMIT)
    if not val.ok:
        duration = round((time.monotonic() - t0) * 1000, 2)
        error_str = "; ".join(val.errors)
        return {
            "validation_result": val,
            "retry_count": next_retry,
            "errors": [NodeError(node="validate_sql", error=error_str)],
            "trace": [
                Span(
                    node="validate_sql",
                    duration_ms=duration,
                    retry_count=next_retry,
                    error=error_str,
                )
            ],
        }

    # Step 2 — cost check via EXPLAIN
    client: WarehouseClient = config["configurable"]["client"]
    try:
        plan = await client.explain_query(val.sql)
        if plan.estimated_cost > _cfg.MAX_COST:
            err = f"Query cost {plan.estimated_cost:.0f} exceeds MAX_COST {_cfg.MAX_COST}"
            duration = round((time.monotonic() - t0) * 1000, 2)
            return {
                "validation_result": ValidationResult(ok=False, errors=[err]),
                "retry_count": next_retry,
                "errors": [NodeError(node="validate_sql", error=err)],
                "trace": [
                    Span(
                        node="validate_sql",
                        duration_ms=duration,
                        retry_count=next_retry,
                        error=err,
                    )
                ],
            }
    except Exception:  # noqa: BLE001
        # EXPLAIN failed (e.g. no DB); proceed with the static validation result.
        # The execute node will surface the real error if the query is broken.
        pass

    duration = round((time.monotonic() - t0) * 1000, 2)
    return {
        "validation_result": val,
        "errors": [],
        "trace": [Span(node="validate_sql", duration_ms=duration)],
    }
