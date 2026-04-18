"""Async warehouse client — the agent's interface to the read-only database.

Mirrors the five MCP tool methods so agent nodes stay consistent with the
MCP server contract without requiring a live MCP transport.  Accepts any
asyncpg pool as ``Any`` because asyncpg ships no type stubs.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import yaml

from server._models import (
    ColumnInfo,
    ExplainPlan,
    Metric,
    MetricInput,
    QueryResult,
    TableSchema,
    TableSummary,
)
from server._validator import validate_and_sanitize, validate_identifier
from voyage import config

_METRICS_PATH = Path(__file__).parent.parent.parent / "data" / "metrics.yaml"


class WarehouseClient:
    """Thin asyncpg wrapper providing the same five methods as the MCP server.

    Args:
        pool: An asyncpg connection pool (typed as ``Any`` — no stubs).
    """

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # list_tables
    # ------------------------------------------------------------------

    async def list_tables(self) -> list[TableSummary]:
        """Return all warehouse tables with descriptions and row-count estimates."""
        async with self._pool.acquire() as conn:
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

    # ------------------------------------------------------------------
    # describe_table
    # ------------------------------------------------------------------

    async def describe_table(self, name: str) -> TableSchema:
        """Return full schema for *name* including sample rows."""
        # Validate identifier before any interpolation (Bandit B608 defence).
        validate_identifier(name)
        async with self._pool.acquire() as conn:
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

            row_count = int(
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

            # name validated by validate_identifier() above.
            sample_records = await conn.fetch(
                f'SELECT * FROM warehouse."{name}" LIMIT 5'  # nosec B608  # noqa: S608
            )
            sample_rows: list[dict[str, object]] = [dict(r) for r in sample_records]

        columns = [
            ColumnInfo(
                name=r["column_name"],
                type=(r["udt_name"] if r["data_type"] == "USER-DEFINED" else r["data_type"]),
                description=r["description"],
                nullable=r["is_nullable"] == "YES",
                fk_to=r["fk_to"],
            )
            for r in col_rows
        ]
        return TableSchema(
            name=name,
            columns=columns,
            sample_rows=sample_rows,
            row_count=row_count,
        )

    # ------------------------------------------------------------------
    # get_metrics_catalog
    # ------------------------------------------------------------------

    def get_metrics_catalog(self) -> list[Metric]:
        """Return all named business metrics from the YAML catalog."""
        raw: dict[str, Any] = yaml.safe_load(_METRICS_PATH.read_text())
        return [
            Metric(
                name=m["name"],
                description=m["description"].strip(),
                sql_template=m["sql_template"].strip(),
                inputs=[
                    MetricInput(name=inp["name"], type=inp["type"]) for inp in m.get("inputs", [])
                ],
            )
            for m in raw.get("metrics", [])
        ]

    # ------------------------------------------------------------------
    # explain_query
    # ------------------------------------------------------------------

    async def explain_query(self, sql: str) -> ExplainPlan:
        """Run EXPLAIN on *sql* and return the plan with estimated cost."""
        result = validate_and_sanitize(sql, row_limit=config.ROW_LIMIT)
        if not result.ok:
            raise ValueError(f"SQL validation failed: {'; '.join(result.errors)}")

        async with self._pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {config.STATEMENT_TIMEOUT_MS}")
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

    # ------------------------------------------------------------------
    # run_query
    # ------------------------------------------------------------------

    async def run_query(
        self,
        sql: str,
        row_limit: int = 1000,
        timeout_s: int = 10,
    ) -> QueryResult:
        """Execute *sql* and return the result set."""
        row_limit = min(row_limit, config.ROW_LIMIT)
        timeout_s = min(timeout_s, 30)
        result = validate_and_sanitize(sql, row_limit=row_limit)
        if not result.ok:
            raise ValueError(f"SQL validation failed: {'; '.join(result.errors)}")

        async with self._pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {timeout_s * 1000}")
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
