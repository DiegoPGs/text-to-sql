"""Voyage BI Copilot — MCP warehouse server.

Exposes five read-only tools over the vacation rental data warehouse:

    list_tables          — discover tables with row-count estimates
    describe_table       — full column schema + sample rows
    get_metrics_catalog  — named business metrics with SQL templates
    explain_query        — EXPLAIN a SELECT, return plan + estimated cost
    run_query            — execute a SELECT, return typed result set

All queries run under the ``bi_copilot_ro`` Postgres role (SELECT-only).
Every SQL string is validated by :mod:`server._validator` before execution.

Run via:
    make mcp
    uv run python -m server.warehouse_mcp
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
import yaml
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

from ._models import (
    ColumnInfo,
    ExplainPlan,
    Metric,
    MetricInput,
    QueryResult,
    TableSchema,
    TableSummary,
    ValidationResult,
)
from ._validator import validate_and_sanitize

load_dotenv()

_METRICS_PATH = Path(__file__).parent.parent / "data" / "metrics.yaml"
_DEFAULT_ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "1000"))
_STMT_TIMEOUT_MS = int(os.environ.get("STATEMENT_TIMEOUT_MS", "10000"))


# ---------------------------------------------------------------------------
# Connection pool (lifespan-managed)
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: Any) -> AsyncGenerator[dict[str, Any], None]:
    ro_url = os.environ.get("RO_DATABASE_URL")
    if not ro_url:
        raise RuntimeError("RO_DATABASE_URL is not set — cannot start MCP server")
    pool: Any = await asyncpg.create_pool(ro_url, min_size=1, max_size=5)
    try:
        yield {"pool": pool}
    finally:
        await pool.close()


mcp = FastMCP(
    "voyage-warehouse",
    instructions=(
        "You have access to a vacation rental data warehouse. "
        "Use list_tables to discover the schema, describe_table for column details, "
        "get_metrics_catalog for named business metrics, explain_query to preview "
        "query cost before running, and run_query to execute SELECT statements. "
        "Only SELECT statements are permitted — all others are rejected."
    ),
    lifespan=_lifespan,
)


def _get_pool(ctx: Any) -> Any:
    """Extract the connection pool from the FastMCP request context."""
    return ctx.request_context.lifespan_context["pool"]


# ---------------------------------------------------------------------------
# Tool 1 — list_tables
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_tables(ctx: Any) -> list[TableSummary]:
    """List all warehouse tables with descriptions and approximate row counts.

    Returns one entry per table in the ``warehouse`` schema, ordered
    alphabetically by table name.
    """
    pool = _get_pool(ctx)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                t.table_name,
                COALESCE(d.description, '') AS description,
                COALESCE(c.reltuples::bigint, 0) AS row_count_estimate
            FROM information_schema.tables t
            LEFT JOIN pg_catalog.pg_class c
                ON  c.relname = t.table_name
            LEFT JOIN pg_catalog.pg_namespace n
                ON  n.oid     = c.relnamespace
                AND n.nspname = t.table_schema
            LEFT JOIN pg_catalog.pg_description d
                ON  d.objoid   = c.oid
                AND d.objsubid = 0
            WHERE t.table_schema = 'warehouse'
              AND t.table_type   = 'BASE TABLE'
            ORDER BY t.table_name
            """
        )
    return [
        TableSummary(
            name=r["table_name"],
            description=r["description"],
            row_count_estimate=int(r["row_count_estimate"]),
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Tool 2 — describe_table
# ---------------------------------------------------------------------------


@mcp.tool()
async def describe_table(name: str, ctx: Any) -> TableSchema:
    """Return full schema for a warehouse table.

    Includes column names, types, nullability, foreign-key targets,
    an approximate row count, and five sample rows.

    Args:
        name: Table name without schema prefix (e.g. ``"reservations"``).
    """
    pool = _get_pool(ctx)
    async with pool.acquire() as conn:
        col_rows = await conn.fetch(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                COALESCE(d.description, '') AS description,
                (
                    SELECT ccu.table_schema || '.' || ccu.table_name
                           || '.' || ccu.column_name
                    FROM information_schema.key_column_usage       kcu
                    JOIN information_schema.referential_constraints rc
                        ON  rc.constraint_name   = kcu.constraint_name
                        AND rc.constraint_schema = kcu.constraint_schema
                    JOIN information_schema.constraint_column_usage ccu
                        ON  ccu.constraint_name = rc.unique_constraint_name
                    WHERE kcu.table_schema = 'warehouse'
                      AND kcu.table_name   = c.table_name
                      AND kcu.column_name  = c.column_name
                    LIMIT 1
                ) AS fk_to
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_user_tables t
                ON  t.schemaname = c.table_schema
                AND t.relname    = c.table_name
            LEFT JOIN pg_catalog.pg_description d
                ON  d.objoid   = t.relid
                AND d.objsubid = c.ordinal_position
            WHERE c.table_schema = 'warehouse'
              AND c.table_name   = $1
            ORDER BY c.ordinal_position
            """,
            name,
        )

        if not col_rows:
            raise ValueError(f"Table 'warehouse.{name}' not found")

        row_count: int = int(
            await conn.fetchval(
                """
                SELECT COALESCE(c.reltuples::bigint, 0)
                FROM pg_catalog.pg_class     c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'warehouse' AND c.relname = $1
                """,
                name,
            )
            or 0
        )

        # Sample rows — safe: no user SQL, table name is validated above
        sample_records = await conn.fetch(
            f'SELECT * FROM warehouse."{name}" LIMIT 5'  # noqa: S608
        )
        sample_rows: list[dict[str, object]] = [dict(r) for r in sample_records]

    columns = [
        ColumnInfo(
            name=r["column_name"],
            type=r["udt_name"] if r["data_type"] == "USER-DEFINED" else r["data_type"],
            description=r["description"],
            nullable=r["is_nullable"] == "YES",
            fk_to=r["fk_to"],
        )
        for r in col_rows
    ]

    return TableSchema(name=name, columns=columns, sample_rows=sample_rows, row_count=row_count)


# ---------------------------------------------------------------------------
# Tool 3 — get_metrics_catalog
# ---------------------------------------------------------------------------


@mcp.tool()
async def get_metrics_catalog() -> list[Metric]:
    """Return all named business metrics with their SQL templates.

    Metrics are defined in ``data/metrics.yaml``.  The agent should prefer
    these over ad-hoc SQL when the user's intent matches a named metric
    (ADR, occupancy rate, RevPAR, total revenue, cancellation rate,
    average review rating).
    """
    raw: dict[str, Any] = yaml.safe_load(_METRICS_PATH.read_text())
    return [
        Metric(
            name=m["name"],
            description=m["description"].strip(),
            sql_template=m["sql_template"].strip(),
            inputs=[MetricInput(name=inp["name"], type=inp["type"]) for inp in m.get("inputs", [])],
        )
        for m in raw.get("metrics", [])
    ]


# ---------------------------------------------------------------------------
# Tool 4 — explain_query
# ---------------------------------------------------------------------------


@mcp.tool()
async def explain_query(sql: str, ctx: Any) -> ExplainPlan:
    """Run EXPLAIN on a SELECT query and return the plan with estimated cost.

    Validates the query is SELECT-only before explaining.  Use this to check
    query cost before calling run_query on large or complex statements.

    Args:
        sql: A SELECT statement to explain.
    """
    result: ValidationResult = validate_and_sanitize(sql, row_limit=_DEFAULT_ROW_LIMIT)
    if not result.ok:
        raise ValueError(f"SQL validation failed: {'; '.join(result.errors)}")

    pool = _get_pool(ctx)
    async with pool.acquire() as conn:
        await conn.execute(f"SET statement_timeout = {_STMT_TIMEOUT_MS}")
        plan_rows = await conn.fetch(f"EXPLAIN {result.sql}")

    plan_lines: list[str] = [r[0] for r in plan_rows]
    plan_text = "\n".join(plan_lines)

    estimated_cost = 0.0
    for line in plan_lines:
        if "cost=" in line:
            try:
                cost_part = line.split("cost=")[1].split(" ")[0]
                estimated_cost = float(cost_part.split("..")[1])
            except (IndexError, ValueError):
                pass
            break

    return ExplainPlan(sql=result.sql, plan=plan_text, estimated_cost=estimated_cost)


# ---------------------------------------------------------------------------
# Tool 5 — run_query
# ---------------------------------------------------------------------------


@mcp.tool()
async def run_query(
    sql: str,
    ctx: Any,
    row_limit: int = _DEFAULT_ROW_LIMIT,
    timeout_s: int = 10,
) -> QueryResult:
    """Execute a SELECT query and return the result set.

    Enforces: SELECT-only, row cap (max 1 000), statement timeout (max 30 s).
    Always runs under the ``bi_copilot_ro`` read-only database role.

    Args:
        sql:       A SELECT statement.  Non-SELECT statements are rejected.
        row_limit: Maximum rows to return (server cap: 1 000).
        timeout_s: Statement timeout in seconds (server cap: 30).
    """
    row_limit = min(row_limit, _DEFAULT_ROW_LIMIT)
    timeout_s = min(timeout_s, 30)
    timeout_ms = timeout_s * 1000

    result: ValidationResult = validate_and_sanitize(sql, row_limit=row_limit)
    if not result.ok:
        raise ValueError(f"SQL validation failed: {'; '.join(result.errors)}")

    pool = _get_pool(ctx)
    async with pool.acquire() as conn:
        await conn.execute(f"SET statement_timeout = {timeout_ms}")
        t0 = time.monotonic()
        records = await conn.fetch(result.sql)
        elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    if not records:
        return QueryResult(
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            execution_ms=elapsed_ms,
        )

    columns: list[str] = list(records[0].keys())
    rows: list[list[object]] = [list(r.values()) for r in records]

    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=len(rows) >= row_limit,
        execution_ms=elapsed_ms,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
