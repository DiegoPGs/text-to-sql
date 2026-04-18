"""interpret_result node — turn raw query results into a plain-English answer.

Feeds the question, SQL, and result set to the LLM to produce:
  - A one- or two-sentence summary for the operations manager.
  - Up to three bullet-point highlights (empty list if nothing notable).
  - An optional ChartSpec when a visualisation would add clear value.
"""

from __future__ import annotations

import json
import time
from typing import Any

from voyage.agent import llm
from voyage.agent.state import AgentState, Answer, NodeError, Span

_SYSTEM = """\
You are a data analyst presenting results to a vacation-rental operations
manager.  Given the question, the SQL that was run, and the result rows,
produce a concise plain-English answer.

Guidelines:
- Summary: one or two sentences.  Be specific — include numbers.
- Highlights: up to three bullet points on notable observations.
  Leave the list empty if there is nothing interesting to call out.
- ChartSpec: only include when a bar/line/pie chart would add clear value
  (e.g. comparing several markets).  For a single scalar, set to null.
- Never mention SQL, table names, or technical details in the summary.
"""


def _fmt_result(query_result: Any) -> str:
    """Render the query result as a compact JSON string for the prompt."""
    if query_result is None:
        return "(no results)"
    rows = [dict(zip(query_result.columns, row, strict=False)) for row in query_result.rows[:20]]
    truncated = query_result.truncated
    text = json.dumps(rows, default=str, indent=2)
    if truncated:
        text += f"\n... (truncated, {query_result.row_count} rows shown)"
    return text


def interpret_result(state: AgentState) -> dict[str, Any]:
    """Produce the final Answer from the question + query result."""
    t0 = time.monotonic()

    sql_text = state["sql_draft"].sql if state["sql_draft"] else "(unknown)"
    result_text = _fmt_result(state["query_result"])

    user_msg = f"Question: {state['question']}\n\nSQL:\n{sql_text}\n\nResult:\n{result_text}"

    try:
        answer, tok_in, tok_out = llm.chat(
            Answer,
            [{"role": "user", "content": user_msg}],
            system=_SYSTEM,
        )
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "answer": answer,
            "errors": [],
            "trace": [
                Span(
                    node="interpret_result",
                    duration_ms=duration,
                    tokens_in=tok_in,
                    tokens_out=tok_out,
                )
            ],
        }
    except Exception as exc:  # noqa: BLE001
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "answer": None,
            "errors": [NodeError(node="interpret_result", error=str(exc))],
            "trace": [Span(node="interpret_result", duration_ms=duration, error=str(exc))],
        }
