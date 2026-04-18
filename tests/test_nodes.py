"""Unit tests for individual agent nodes.

All LLM calls are mocked with unittest.mock so these tests run without an
API key or database.  Each test verifies:
  - the node returns the expected state keys
  - the node appends a Span to 'trace'
  - the node handles errors gracefully (returns NodeError, not raises)

Integration tests that require a live DB + LLM are marked with
@pytest.mark.integration and @pytest.mark.llm — deselected by default.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from server._models import (
    ExplainPlan,
    Metric,
    QueryResult,
    TableSchema,
    TableSummary,
    ValidationResult,
)
from voyage.agent.state import (
    AgentState,
    Answer,
    Intent,
    IntentEnum,
    NodeError,
    SqlDraft,
    initial_state,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state(**overrides: Any) -> AgentState:
    s = initial_state("What is total revenue last month?")
    for k, v in overrides.items():
        s[k] = v  # type: ignore[literal-required]
    return s


def _make_config(client: Any = None) -> dict[str, Any]:
    return {"configurable": {"client": client or MagicMock()}}


def _summary(name: str) -> TableSummary:
    return TableSummary(name=name, description=f"{name} table", row_count_estimate=100)


def _schema(name: str) -> TableSchema:
    return TableSchema(name=name, columns=[], sample_rows=[], row_count=100)


# ---------------------------------------------------------------------------
# classify_intent
# ---------------------------------------------------------------------------


class TestClassifyIntent:
    @patch("voyage.agent.nodes.classify.chat")
    def test_returns_intent_and_span(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.classify import classify_intent

        mock_chat.return_value = (
            Intent(value=IntentEnum.DATA, rationale="It's a data question"),
            10,
            20,
        )
        result = classify_intent(_make_state())
        assert isinstance(result["intent"], Intent)
        assert result["intent"].value == IntentEnum.DATA
        assert len(result["trace"]) == 1
        assert result["trace"][0].node == "classify_intent"
        assert result["trace"][0].tokens_in == 10
        assert result["errors"] == []

    @patch("voyage.agent.nodes.classify.chat")
    def test_llm_error_falls_back_to_data(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.classify import classify_intent

        mock_chat.side_effect = RuntimeError("API down")
        result = classify_intent(_make_state())
        assert result["intent"].value == IntentEnum.DATA
        assert len(result["errors"]) == 1
        assert result["errors"][0].node == "classify_intent"

    @patch("voyage.agent.nodes.classify.chat")
    def test_out_of_scope_intent(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.classify import classify_intent

        mock_chat.return_value = (
            Intent(value=IntentEnum.OUT_OF_SCOPE, rationale="Asks for PII"),
            5,
            5,
        )
        result = classify_intent(_make_state(question="Show me guest credit cards"))
        assert result["intent"].value == IntentEnum.OUT_OF_SCOPE


# ---------------------------------------------------------------------------
# retrieve_context
# ---------------------------------------------------------------------------


class TestRetrieveContext:
    @pytest.mark.asyncio
    async def test_returns_schemas_and_metrics(self) -> None:
        from voyage.agent.nodes.retrieve import retrieve_context

        client = MagicMock()
        client.list_tables = AsyncMock(return_value=[_summary("reservations"), _summary("markets")])
        client.describe_table = AsyncMock(return_value=_schema("reservations"))
        client.get_metrics_catalog = MagicMock(
            return_value=[Metric(name="adr", description="ADR", sql_template="SELECT 1", inputs=[])]
        )

        result = await retrieve_context(_make_state(), _make_config(client))
        assert len(result["retrieved_tables"]) > 0
        assert len(result["metrics_catalog"]) == 1
        assert result["errors"] == []
        assert result["trace"][0].node == "retrieve_context"

    @pytest.mark.asyncio
    async def test_client_error_returns_empty_and_node_error(self) -> None:
        from voyage.agent.nodes.retrieve import retrieve_context

        client = MagicMock()
        client.list_tables = AsyncMock(side_effect=ConnectionError("DB down"))

        result = await retrieve_context(_make_state(), _make_config(client))
        assert result["retrieved_tables"] == []
        assert len(result["errors"]) == 1
        assert result["errors"][0].node == "retrieve_context"


# ---------------------------------------------------------------------------
# draft_sql
# ---------------------------------------------------------------------------


class TestDraftSql:
    @patch("voyage.agent.nodes.draft.llm.chat")
    def test_returns_sql_draft_and_span(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.draft import draft_sql

        mock_chat.return_value = (
            SqlDraft(
                sql="SELECT SUM(net_revenue) FROM warehouse.reservations WHERE status='confirmed' LIMIT 1000",
                rationale="Simple revenue aggregation",
                confidence=0.9,
            ),
            50,
            30,
        )
        state = _make_state(
            retrieved_tables=[_schema("reservations")],
            metrics_catalog=[],
            retrieved_examples=[],
        )
        result = draft_sql(state)
        assert result["sql_draft"] is not None
        assert "SELECT" in result["sql_draft"].sql
        assert result["trace"][0].tokens_in == 50
        assert result["errors"] == []

    @patch("voyage.agent.nodes.draft.llm.chat")
    def test_llm_error_returns_none_draft(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.draft import draft_sql

        mock_chat.side_effect = RuntimeError("Rate limited")
        result = draft_sql(_make_state())
        assert result["sql_draft"] is None
        assert result["errors"][0].node == "draft_sql"

    @patch("voyage.agent.nodes.draft.llm.chat")
    def test_includes_retry_errors_in_prompt(self, mock_chat: Any) -> None:
        """When state has prior validate/execute errors, they appear in context."""
        from voyage.agent.nodes.draft import _build_user_message

        state = _make_state(errors=[NodeError(node="validate_sql", error="LIMIT missing")])
        msg = _build_user_message(state)
        assert "Previous attempt failed" in msg
        assert "LIMIT missing" in msg


# ---------------------------------------------------------------------------
# validate_sql
# ---------------------------------------------------------------------------


class TestValidateSql:
    @pytest.mark.asyncio
    async def test_valid_select_passes(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        client = MagicMock()
        client.explain_query = AsyncMock(
            return_value=ExplainPlan(sql="SELECT 1 LIMIT 1000", plan="Seq Scan", estimated_cost=1.0)
        )
        draft = SqlDraft(
            sql="SELECT SUM(net_revenue) FROM warehouse.reservations WHERE status='confirmed'",
            rationale="ok",
            confidence=0.9,
        )
        result = await validate_sql(_make_state(sql_draft=draft), _make_config(client))
        assert result["validation_result"].ok
        assert result["errors"] == []

    @pytest.mark.asyncio
    async def test_non_select_fails(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        draft = SqlDraft(
            sql="DROP TABLE warehouse.reservations",
            rationale="evil",
            confidence=0.1,
        )
        result = await validate_sql(_make_state(sql_draft=draft), _make_config(MagicMock()))
        assert not result["validation_result"].ok
        assert result["errors"][0].node == "validate_sql"

    @pytest.mark.asyncio
    async def test_none_draft_returns_error(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        result = await validate_sql(_make_state(sql_draft=None), _make_config())
        assert not result["validation_result"].ok
        assert result["errors"][0].node == "validate_sql"

    @pytest.mark.asyncio
    async def test_cost_too_high_fails(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        client = MagicMock()
        client.explain_query = AsyncMock(
            return_value=ExplainPlan(
                sql="SELECT 1 LIMIT 1000", plan="Seq Scan", estimated_cost=999_999.0
            )
        )
        draft = SqlDraft(
            sql="SELECT * FROM warehouse.reservations",
            rationale="expensive",
            confidence=0.5,
        )
        result = await validate_sql(_make_state(sql_draft=draft), _make_config(client))
        assert not result["validation_result"].ok
        assert "MAX_COST" in result["errors"][0].error

    @pytest.mark.asyncio
    async def test_explain_failure_does_not_block_validation(self) -> None:
        """If EXPLAIN raises (e.g. no DB), the static validation result still passes."""
        from voyage.agent.nodes.validate import validate_sql

        client = MagicMock()
        client.explain_query = AsyncMock(side_effect=ConnectionError("no DB"))
        draft = SqlDraft(
            sql="SELECT COUNT(*) FROM warehouse.markets",
            rationale="ok",
            confidence=0.8,
        )
        result = await validate_sql(_make_state(sql_draft=draft), _make_config(client))
        # Static validation passed; explain failure is swallowed.
        assert result["validation_result"].ok
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# execute_sql
# ---------------------------------------------------------------------------


class TestExecuteSql:
    @pytest.mark.asyncio
    async def test_runs_query_and_returns_result(self) -> None:
        from voyage.agent.nodes.execute import execute_sql

        client = MagicMock()
        client.run_query = AsyncMock(
            return_value=QueryResult(
                columns=["total_revenue"],
                rows=[[42000.0]],
                row_count=1,
                truncated=False,
                execution_ms=12.3,
            )
        )
        val = ValidationResult(
            ok=True, sql="SELECT SUM(net_revenue) FROM warehouse.reservations LIMIT 1000"
        )
        result = await execute_sql(_make_state(validation_result=val), _make_config(client))
        assert result["query_result"] is not None
        assert result["query_result"].row_count == 1
        assert result["errors"] == []

    @pytest.mark.asyncio
    async def test_no_validation_result_returns_error(self) -> None:
        from voyage.agent.nodes.execute import execute_sql

        result = await execute_sql(_make_state(validation_result=None), _make_config())
        assert result["query_result"] is None
        assert result["errors"][0].node == "execute_sql"

    @pytest.mark.asyncio
    async def test_failed_validation_skips_db(self) -> None:
        from voyage.agent.nodes.execute import execute_sql

        val = ValidationResult(ok=False, errors=["Not SELECT"])
        result = await execute_sql(_make_state(validation_result=val), _make_config(MagicMock()))
        assert result["query_result"] is None
        assert result["errors"][0].node == "execute_sql"

    @pytest.mark.asyncio
    async def test_db_error_captured_in_state(self) -> None:
        from voyage.agent.nodes.execute import execute_sql

        client = MagicMock()
        client.run_query = AsyncMock(side_effect=Exception("DB timeout"))
        val = ValidationResult(ok=True, sql="SELECT 1 LIMIT 1")
        result = await execute_sql(_make_state(validation_result=val), _make_config(client))
        assert result["query_result"] is None
        assert "DB timeout" in result["errors"][0].error


# ---------------------------------------------------------------------------
# interpret_result
# ---------------------------------------------------------------------------


class TestInterpretResult:
    @patch("voyage.agent.nodes.interpret.llm.chat")
    def test_returns_answer_and_span(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.interpret import interpret_result

        mock_chat.return_value = (
            Answer(
                summary="Total revenue last month was $42,000.",
                highlights=["Up 5% vs prior month"],
                chart_spec=None,
            ),
            40,
            25,
        )
        state = _make_state(
            sql_draft=SqlDraft(
                sql="SELECT SUM(net_revenue) FROM warehouse.reservations LIMIT 1",
                rationale="x",
                confidence=0.9,
            ),
            query_result=QueryResult(
                columns=["total_revenue"],
                rows=[[42000.0]],
                row_count=1,
                truncated=False,
                execution_ms=5.0,
            ),
        )
        result = interpret_result(state)
        assert result["answer"] is not None
        assert "42,000" in result["answer"].summary
        assert result["errors"] == []
        assert result["trace"][0].tokens_out == 25

    @patch("voyage.agent.nodes.interpret.llm.chat")
    def test_llm_error_returns_none_answer(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.interpret import interpret_result

        mock_chat.side_effect = RuntimeError("Timeout")
        result = interpret_result(_make_state())
        assert result["answer"] is None
        assert result["errors"][0].node == "interpret_result"

    @patch("voyage.agent.nodes.interpret.llm.chat")
    def test_none_query_result_handled(self, mock_chat: Any) -> None:
        from voyage.agent.nodes.interpret import interpret_result

        mock_chat.return_value = (
            Answer(summary="No data found.", highlights=[], chart_spec=None),
            10,
            10,
        )
        result = interpret_result(_make_state(query_result=None))
        assert result["answer"] is not None


# ---------------------------------------------------------------------------
# initial_state helper
# ---------------------------------------------------------------------------


class TestInitialState:
    def test_all_required_fields_present(self) -> None:
        s = initial_state("Test question")
        assert s["question"] == "Test question"
        assert s["intent"] is None
        assert s["retry_count"] == 0
        assert s["errors"] == []
        assert s["trace"] == []

    def test_question_stored(self) -> None:
        s = initial_state("hello")
        assert s["question"] == "hello"
