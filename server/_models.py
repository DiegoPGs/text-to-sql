"""Pydantic models for all MCP tool inputs and outputs.

Every LLM call and tool response is typed through these models — no
free-form string parsing anywhere in the server layer.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ValidationResult(BaseModel):
    """Internal result returned by the SQL validator."""

    ok: bool
    errors: list[str] = Field(default_factory=list)
    sql: str = ""  # cleaned / LIMIT-injected SQL when ok=True


class ColumnInfo(BaseModel):
    """Schema information for a single table column."""

    name: str
    type: str
    description: str = ""
    nullable: bool = True
    fk_to: str | None = None  # "schema.table.column" or None


class TableSummary(BaseModel):
    """Lightweight table descriptor returned by list_tables."""

    name: str
    description: str
    row_count_estimate: int


class TableSchema(BaseModel):
    """Full table schema returned by describe_table."""

    name: str
    columns: list[ColumnInfo]
    sample_rows: list[dict[str, object]]
    row_count: int


class MetricInput(BaseModel):
    """A parameterised input slot in a named metric SQL template."""

    name: str
    type: str


class Metric(BaseModel):
    """Named business metric with a reusable SQL template."""

    name: str
    description: str
    sql_template: str
    inputs: list[MetricInput] = Field(default_factory=list)


class ExplainPlan(BaseModel):
    """Output of EXPLAIN for a SQL query."""

    sql: str
    plan: str
    estimated_cost: float


class QueryResult(BaseModel):
    """Result set returned by run_query."""

    columns: list[str]
    rows: list[list[object]]
    row_count: int
    truncated: bool
    execution_ms: float
