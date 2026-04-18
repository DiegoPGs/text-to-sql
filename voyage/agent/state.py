"""Agent state and all nested Pydantic models.

One concept per module: this file owns the state schema and every model
that hangs off it.  LangGraph state is a TypedDict; all complex values
are typed Pydantic models.
"""

from __future__ import annotations

import operator
from enum import StrEnum
from typing import Annotated, TypedDict

from pydantic import BaseModel, Field

from server._models import Metric, QueryResult, TableSchema, ValidationResult

# ---------------------------------------------------------------------------
# Nested Pydantic models
# ---------------------------------------------------------------------------


class IntentEnum(StrEnum):
    """Classification of the user's question."""

    DATA = "data"
    AMBIGUOUS = "ambiguous"
    OUT_OF_SCOPE = "out_of_scope"


class Intent(BaseModel):
    """Classified intent returned by classify_intent."""

    value: IntentEnum = Field(
        description=(
            "Classification: 'data' if the question is answerable from the warehouse, "
            "'ambiguous' if key information is missing (e.g. no date range), "
            "'out_of_scope' if the question cannot or should not be answered."
        )
    )
    rationale: str = Field(
        description="One sentence explaining why this classification was chosen."
    )


class FewShot(BaseModel):
    """A question→SQL example retrieved from the index."""

    question: str
    sql: str


class SqlDraft(BaseModel):
    """LLM-generated SQL query with rationale."""

    sql: str = Field(
        description=(
            "A valid PostgreSQL SELECT statement that answers the question. "
            "Always qualify table names with the 'warehouse' schema prefix. "
            "Include a LIMIT clause. Use confirmed-only filter for revenue/occupancy."
        )
    )
    rationale: str = Field(
        description="One or two sentences: which tables were used, why, and what the SQL computes."
    )
    confidence: float = Field(
        ge=0.0,
        le=1.0,
        description=(
            "Confidence 0–1 that the SQL correctly answers the question given the schema. "
            "Use <0.6 when the schema fit is uncertain."
        ),
    )


class ChartSpec(BaseModel):
    """Minimal chart specification for the CLI renderer."""

    chart_type: str = Field(description="One of: bar, line, pie.")
    x: str = Field(description="Column name for the x-axis or category.")
    y: str = Field(description="Column name for the y-axis or numeric value.")
    title: str = Field(default="", description="Optional chart title.")


class Answer(BaseModel):
    """Final answer delivered to the user."""

    summary: str = Field(
        description=(
            "One or two sentences summarising the result in plain English, "
            "as if explaining to an operations manager."
        )
    )
    highlights: list[str] = Field(
        default_factory=list,
        description=(
            "Up to three bullet-point insights or notable observations from the data. "
            "Empty list if there is nothing notable to call out."
        ),
    )
    chart_spec: ChartSpec | None = Field(
        default=None,
        description=(
            "Set only when a bar/line/pie chart would clearly add value "
            "(e.g. comparing values across several categories). "
            "Null for scalar results or when a chart would add no insight."
        ),
    )


class NodeError(BaseModel):
    """Structured error emitted by a node into state."""

    node: str
    error: str


class Span(BaseModel):
    """Observability trace span emitted by every node."""

    node: str
    duration_ms: float
    tokens_in: int = 0
    tokens_out: int = 0
    model: str = ""
    retry_count: int = 0
    error: str = ""


# ---------------------------------------------------------------------------
# LangGraph state
# ---------------------------------------------------------------------------


class AgentState(TypedDict):
    """Mutable state threaded through every node in the agent graph.

    Fields annotated with ``operator.add`` are *appended to* on each node
    update rather than replaced.  All other fields are replaced wholesale.
    """

    question: str
    intent: Intent | None
    clarification: str | None

    retrieved_tables: list[TableSchema]
    retrieved_examples: list[FewShot]
    metrics_catalog: list[Metric]

    sql_draft: SqlDraft | None
    validation_result: ValidationResult | None
    query_result: QueryResult | None

    retry_count: int
    answer: Answer | None

    # Reducer: each node appends its own errors / spans.
    errors: Annotated[list[NodeError], operator.add]
    trace: Annotated[list[Span], operator.add]


def initial_state(question: str) -> AgentState:
    """Return a freshly initialised AgentState for *question*."""
    return AgentState(
        question=question,
        intent=None,
        clarification=None,
        retrieved_tables=[],
        retrieved_examples=[],
        metrics_catalog=[],
        sql_draft=None,
        validation_result=None,
        query_result=None,
        retry_count=0,
        answer=None,
        errors=[],
        trace=[],
    )
