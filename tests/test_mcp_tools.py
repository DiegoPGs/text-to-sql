"""Unit tests for MCP warehouse tools.

DB-bound tests mock the asyncpg pool so they run without a live database.
Integration tests (requiring a real DB) are marked @pytest.mark.integration.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from server._models import ExplainPlan, Metric, QueryResult, TableSchema, TableSummary
from server.warehouse_mcp import (
    describe_table,
    explain_query,
    get_metrics_catalog,
    list_tables,
    run_query,
)

# ---------------------------------------------------------------------------
# Helpers — lightweight DB stubs
# ---------------------------------------------------------------------------


def _make_conn(**overrides: Any) -> Any:
    """Return a mock asyncpg connection with sensible async defaults."""
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=0)
    for attr, val in overrides.items():
        setattr(conn, attr, val)
    return conn


def _make_pool(conn: Any) -> Any:
    """Return a mock asyncpg pool whose acquire() yields *conn*."""
    pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=conn)
    cm.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = cm
    return pool


def _ctx(pool: Any) -> Any:
    """Return a minimal FastMCP context stub."""
    ctx = MagicMock()
    ctx.request_context.lifespan_context = {"pool": pool}
    return ctx


# ---------------------------------------------------------------------------
# Tool: get_metrics_catalog  (pure YAML — no DB)
# ---------------------------------------------------------------------------


class TestGetMetricsCatalog:
    async def test_returns_list_of_metrics(self) -> None:
        metrics = await get_metrics_catalog()
        assert isinstance(metrics, list)
        assert len(metrics) > 0
        assert all(isinstance(m, Metric) for m in metrics)

    async def test_known_metric_names_present(self) -> None:
        names = {m.name for m in await get_metrics_catalog()}
        assert {"adr", "occupancy_rate", "revpar"}.issubset(names)

    async def test_every_metric_has_sql_template(self) -> None:
        for metric in await get_metrics_catalog():
            assert metric.sql_template.strip(), f"Metric '{metric.name}' has an empty sql_template"

    async def test_metric_inputs_are_typed(self) -> None:
        for metric in await get_metrics_catalog():
            for inp in metric.inputs:
                assert inp.name
                assert inp.type


# ---------------------------------------------------------------------------
# Tool: run_query — validation failures (no DB needed)
# ---------------------------------------------------------------------------


class TestRunQueryValidation:
    async def test_drop_table_rejected(self) -> None:
        ctx = _ctx(_make_pool(_make_conn()))
        with pytest.raises(ValueError, match="validation failed"):
            await run_query("DROP TABLE warehouse.properties", ctx)

    async def test_insert_rejected(self) -> None:
        ctx = _ctx(_make_pool(_make_conn()))
        with pytest.raises(ValueError, match="validation failed"):
            await run_query("INSERT INTO warehouse.markets VALUES (1,'x','y','z','w')", ctx)

    async def test_update_rejected(self) -> None:
        ctx = _ctx(_make_pool(_make_conn()))
        with pytest.raises(ValueError, match="validation failed"):
            await run_query("UPDATE warehouse.markets SET name='pwned'", ctx)

    async def test_pg_function_rejected(self) -> None:
        ctx = _ctx(_make_pool(_make_conn()))
        with pytest.raises(ValueError, match="validation failed"):
            await run_query("SELECT pg_read_file('/etc/passwd')", ctx)

    async def test_row_limit_capped_at_server_default(self) -> None:
        """row_limit values above the server cap are silently clamped."""
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[{"id": 1}])
        ctx = _ctx(_make_pool(conn))
        # Passing a huge limit must not raise; the server cap takes precedence.
        result = await run_query("SELECT 1 AS id", ctx, row_limit=999_999)
        assert isinstance(result, QueryResult)

    async def test_timeout_capped_at_30s(self) -> None:
        """Statement timeout above 30 s is clamped to 30 s."""
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        ctx = _ctx(_make_pool(conn))
        await run_query("SELECT 1", ctx, timeout_s=9999)
        # Execution succeeded and the mock recorded an execute() call with ≤30000 ms
        execute_call_arg: str = conn.execute.call_args[0][0]
        timeout_val = int(execute_call_arg.split("=")[1])
        assert timeout_val == 30_000


# ---------------------------------------------------------------------------
# Tool: run_query — happy path (mocked DB)
# ---------------------------------------------------------------------------


class TestRunQueryHappyPath:
    async def test_empty_result_set(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        ctx = _ctx(_make_pool(conn))
        result = await run_query("SELECT * FROM warehouse.markets", ctx)
        assert result.row_count == 0
        assert result.rows == []
        assert result.columns == []
        assert not result.truncated

    async def test_returns_typed_columns_and_rows(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[{"market_id": 1, "name": "Joshua Tree"}])
        ctx = _ctx(_make_pool(conn))
        result = await run_query("SELECT market_id, name FROM warehouse.markets LIMIT 1", ctx)
        assert result.columns == ["market_id", "name"]
        assert result.rows == [[1, "Joshua Tree"]]
        assert result.row_count == 1
        assert not result.truncated

    async def test_limit_injected_into_executed_sql(self) -> None:
        """The validator injects LIMIT; the final SQL sent to the DB must include it."""
        conn = _make_conn()
        fetch_mock = AsyncMock(return_value=[])
        conn.fetch = fetch_mock
        ctx = _ctx(_make_pool(conn))
        await run_query("SELECT * FROM warehouse.markets", ctx, row_limit=42)
        executed_sql: str = fetch_mock.call_args[0][0]
        assert "LIMIT" in executed_sql.upper()
        assert "42" in executed_sql

    async def test_truncated_flag_set_at_limit(self) -> None:
        row_limit = 3
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[{"id": i} for i in range(row_limit)])
        ctx = _ctx(_make_pool(conn))
        result = await run_query("SELECT id FROM warehouse.markets", ctx, row_limit=row_limit)
        assert result.truncated

    async def test_truncated_flag_clear_below_limit(self) -> None:
        row_limit = 10
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[{"id": i} for i in range(3)])
        ctx = _ctx(_make_pool(conn))
        result = await run_query("SELECT id FROM warehouse.markets", ctx, row_limit=row_limit)
        assert not result.truncated


# ---------------------------------------------------------------------------
# Tool: explain_query
# ---------------------------------------------------------------------------

_PLAN_LINE = "Seq Scan on markets  (cost=0.00..1.10 rows=10 width=100)"


class TestExplainQuery:
    async def test_non_select_rejected(self) -> None:
        ctx = _ctx(_make_pool(_make_conn()))
        with pytest.raises(ValueError, match="validation failed"):
            await explain_query("DELETE FROM warehouse.reservations", ctx)

    async def test_returns_explain_plan(self) -> None:
        plan_row = MagicMock()
        plan_row.__getitem__ = MagicMock(return_value=_PLAN_LINE)
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[plan_row])
        ctx = _ctx(_make_pool(conn))
        result = await explain_query("SELECT * FROM warehouse.markets", ctx)
        assert isinstance(result, ExplainPlan)
        assert "Seq Scan" in result.plan

    async def test_estimated_cost_parsed(self) -> None:
        plan_row = MagicMock()
        plan_row.__getitem__ = MagicMock(return_value=_PLAN_LINE)
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[plan_row])
        ctx = _ctx(_make_pool(conn))
        result = await explain_query("SELECT * FROM warehouse.markets", ctx)
        assert result.estimated_cost == pytest.approx(1.10)

    async def test_sql_in_result_has_limit(self) -> None:
        """The SQL stored in ExplainPlan is the post-validation (LIMIT-injected) SQL."""
        plan_row = MagicMock()
        plan_row.__getitem__ = MagicMock(return_value=_PLAN_LINE)
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[plan_row])
        ctx = _ctx(_make_pool(conn))
        result = await explain_query("SELECT * FROM warehouse.markets", ctx)
        assert "LIMIT" in result.sql.upper()


# ---------------------------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------------------------


class TestListTables:
    async def test_returns_table_summaries(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(
            return_value=[
                {
                    "table_name": "markets",
                    "description": "Geographic markets",
                    "row_count_estimate": 10,
                },
                {
                    "table_name": "reservations",
                    "description": "Booking records",
                    "row_count_estimate": 10_000,
                },
            ]
        )
        ctx = _ctx(_make_pool(conn))
        result = await list_tables(ctx)
        assert len(result) == 2
        assert all(isinstance(t, TableSummary) for t in result)
        assert result[0].name == "markets"
        assert result[1].row_count_estimate == 10_000

    async def test_empty_warehouse_returns_empty_list(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        ctx = _ctx(_make_pool(conn))
        assert await list_tables(ctx) == []


# ---------------------------------------------------------------------------
# Tool: describe_table
# ---------------------------------------------------------------------------

_COL_ROWS = [
    {
        "column_name": "market_id",
        "data_type": "integer",
        "udt_name": "int4",
        "is_nullable": "NO",
        "description": "Primary key",
        "fk_to": None,
    },
    {
        "column_name": "name",
        "data_type": "text",
        "udt_name": "text",
        "is_nullable": "NO",
        "description": "Market name",
        "fk_to": None,
    },
]


class TestDescribeTable:
    async def test_malicious_table_name_rejected_before_db(self) -> None:
        """validate_identifier must fire before any DB call (B608 guard)."""
        conn = _make_conn()
        ctx = _ctx(_make_pool(conn))
        with pytest.raises(ValueError, match="Invalid identifier"):
            await describe_table('evil"; DROP TABLE warehouse.markets --', ctx)
        # The DB must never have been called.
        conn.fetch.assert_not_called()

    async def test_unknown_table_raises_value_error(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])  # empty → table not found
        ctx = _ctx(_make_pool(conn))
        with pytest.raises(ValueError, match="not found"):
            await describe_table("nonexistent", ctx)

    async def test_returns_table_schema(self) -> None:
        conn = _make_conn()
        # First fetch → col_rows; second fetch → sample_rows
        conn.fetch = AsyncMock(
            side_effect=[
                _COL_ROWS,
                [{"market_id": 1, "name": "Joshua Tree"}],
            ]
        )
        conn.fetchval = AsyncMock(return_value=10)
        ctx = _ctx(_make_pool(conn))
        result = await describe_table("markets", ctx)
        assert isinstance(result, TableSchema)
        assert result.name == "markets"
        assert len(result.columns) == 2
        assert result.columns[0].name == "market_id"
        assert not result.columns[0].nullable
        assert result.row_count == 10

    async def test_sample_rows_included(self) -> None:
        conn = _make_conn()
        conn.fetch = AsyncMock(
            side_effect=[
                _COL_ROWS,
                [{"market_id": 1, "name": "Joshua Tree"}],
            ]
        )
        conn.fetchval = AsyncMock(return_value=10)
        ctx = _ctx(_make_pool(conn))
        result = await describe_table("markets", ctx)
        assert len(result.sample_rows) == 1
        assert result.sample_rows[0]["name"] == "Joshua Tree"

    async def test_user_defined_type_uses_udt_name(self) -> None:
        """Columns with data_type='USER-DEFINED' should expose their udt_name."""
        col_rows = [
            {
                "column_name": "coords",
                "data_type": "USER-DEFINED",
                "udt_name": "geometry",
                "is_nullable": "YES",
                "description": "",
                "fk_to": None,
            }
        ]
        conn = _make_conn()
        conn.fetch = AsyncMock(side_effect=[col_rows, []])
        conn.fetchval = AsyncMock(return_value=0)
        ctx = _ctx(_make_pool(conn))
        result = await describe_table("geo_table", ctx)
        assert result.columns[0].type == "geometry"
