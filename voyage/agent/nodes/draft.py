"""draft_sql node — generate a SELECT query from retrieved context.

Builds a structured prompt with:
  - Table schemas (columns, types, sample rows)
  - Named metrics catalog
  - Few-shot examples
  - The user question (+ optional clarification)

Uses instructor at temperature 0 to return a typed SqlDraft model.
On retry (Phase 5), the previous validation errors are included so the
LLM can self-correct.
"""

from __future__ import annotations

import time
from typing import Any

# Import here to avoid circular import; llm module is self-contained.
from voyage.agent import llm
from voyage.agent.state import AgentState, NodeError, Span, SqlDraft

_SYSTEM = """\
You are a PostgreSQL expert generating SELECT queries for a vacation-rental
data warehouse.  The warehouse schema lives in the 'warehouse' schema.

Rules:
1. Only write SELECT statements.  Never INSERT, UPDATE, DELETE, or DDL.
2. Always qualify table names with the 'warehouse' schema prefix.
3. Always include a LIMIT clause (max 1000).
4. For revenue and occupancy, always filter WHERE status = 'confirmed'.
5. Use ROUND(..., 2) for monetary values.
6. Prefer named metrics from the catalog when the question matches.
7. Use NULL-safe aggregations (NULLIF, COALESCE) where appropriate.

Today's date for relative time references: {today}
"""


def _fmt_table(schema: Any) -> str:
    """Format a TableSchema as a compact prompt block."""
    cols = "\n".join(
        f"    {c.name} {c.type}{'  -- FK → ' + c.fk_to if c.fk_to else ''}"
        f"{'  [nullable]' if c.nullable else ''}"
        f"{('  -- ' + c.description) if c.description else ''}"
        for c in schema.columns
    )
    return f"TABLE warehouse.{schema.name}  (~{schema.row_count:,} rows)\n{cols}"


def _fmt_metric(m: Any) -> str:
    """Format a Metric as a prompt block."""
    return f"METRIC {m.name}: {m.description}\n  SQL: {m.sql_template}"


def _fmt_example(ex: Any) -> str:
    return f"Q: {ex.question}\nSQL:\n{ex.sql}"


def _build_user_message(state: AgentState) -> str:
    """Assemble the full context + question into a user message."""

    parts: list[str] = []

    if state["retrieved_tables"]:
        parts.append("## Schema\n" + "\n\n".join(_fmt_table(t) for t in state["retrieved_tables"]))

    if state["metrics_catalog"]:
        parts.append(
            "## Named metrics\n" + "\n\n".join(_fmt_metric(m) for m in state["metrics_catalog"])
        )

    if state["retrieved_examples"]:
        parts.append(
            "## Examples\n" + "\n\n".join(_fmt_example(e) for e in state["retrieved_examples"])
        )

    # On retry, include previous errors so the LLM can self-correct.
    node_errors = [e for e in state["errors"] if e.node in ("validate_sql", "execute_sql")]
    if node_errors:
        error_text = "\n".join(f"- {e.error}" for e in node_errors)
        parts.append(f"## Previous attempt failed\n{error_text}\nPlease fix these issues.")

    question = state["question"]
    if state["clarification"]:
        question = f"{question}\nAdditional context: {state['clarification']}"
    parts.append(f"## Question\n{question}")

    return "\n\n".join(parts)


def draft_sql(state: AgentState) -> dict[str, Any]:
    """Generate SQL for the question using retrieved context."""
    import datetime

    t0 = time.monotonic()
    today = datetime.date.today().isoformat()

    try:
        sql_draft, tok_in, tok_out = llm.chat(
            SqlDraft,
            [{"role": "user", "content": _build_user_message(state)}],
            system=_SYSTEM.format(today=today),
        )
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "sql_draft": sql_draft,
            "errors": [],
            "trace": [
                Span(
                    node="draft_sql",
                    duration_ms=duration,
                    tokens_in=tok_in,
                    tokens_out=tok_out,
                )
            ],
        }
    except Exception as exc:  # noqa: BLE001
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "sql_draft": None,
            "errors": [NodeError(node="draft_sql", error=str(exc))],
            "trace": [Span(node="draft_sql", duration_ms=duration, error=str(exc))],
        }
