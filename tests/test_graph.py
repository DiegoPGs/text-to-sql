"""Phase 5 — graph routing, HITL, and self-correction tests.

Covers:
  - Conditional routers (route_after_classify / _validate / _execute)
  - clarify, refuse, terminate_failure node behaviour
  - retry_count is incremented on validate / execute failure
  - End-to-end graph runs that exercise: refuse path, retry-then-recover,
    retries-exhausted, and HITL clarification via interrupt + Command.

Note on patching:
  ``draft`` and ``interpret`` both reach the LLM via ``voyage.agent.llm.chat``,
  so we cannot patch ``draft.llm.chat`` and ``interpret.llm.chat`` independently
  — they resolve to the same attribute.  The integration tests use a single
  dispatcher patched on ``voyage.agent.llm.chat`` that returns the right
  response keyed on the requested response_model.  ``classify_intent`` uses a
  bound import (``from ... import chat``) so it is patched separately.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.types import Command

from server._models import (
    ExplainPlan,
    Metric,
    QueryResult,
    TableSchema,
    TableSummary,
    ValidationResult,
)
from voyage.agent.graph import (
    MAX_RETRIES,
    build_graph,
    route_after_classify,
    route_after_execute,
    route_after_validate,
)
from voyage.agent.state import (
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


def _state(**overrides: Any) -> dict[str, Any]:
    s: dict[str, Any] = dict(initial_state("Top markets by revenue"))
    s.update(overrides)
    return s


def _summary(name: str) -> TableSummary:
    return TableSummary(name=name, description=f"{name} table", row_count_estimate=100)


def _schema(name: str) -> TableSchema:
    return TableSchema(name=name, columns=[], sample_rows=[], row_count=100)


def _qr() -> QueryResult:
    return QueryResult(columns=["x"], rows=[[1]], row_count=1, truncated=False, execution_ms=1.0)


def _dispatch_chat(by_model: dict[type, list[tuple[Any, int, int]]]) -> Any:
    """Build a chat() side_effect that returns by response_model class."""

    def _impl(
        response_model: type,
        messages: list[dict[str, str]],
        *,
        system: str = "",
        model: str = "",
        max_retries: int = 2,
    ) -> tuple[Any, int, int]:
        queue = by_model.get(response_model)
        if not queue:
            raise AssertionError(f"No mocked response queued for {response_model.__name__}")
        return queue.pop(0)

    return _impl


def _client(*, run_query_side_effect: Any = None) -> MagicMock:
    """Build a mock WarehouseClient for graph integration tests."""
    c = MagicMock()
    c.list_tables = AsyncMock(return_value=[_summary("reservations")])
    c.describe_table = AsyncMock(return_value=_schema("reservations"))
    c.get_metrics_catalog = MagicMock(
        return_value=[Metric(name="adr", description="ADR", sql_template="SELECT 1", inputs=[])]
    )
    c.explain_query = AsyncMock(
        return_value=ExplainPlan(sql="SELECT 1 LIMIT 1000", plan="x", estimated_cost=1.0)
    )
    if run_query_side_effect is not None:
        c.run_query = AsyncMock(side_effect=run_query_side_effect)
    else:
        c.run_query = AsyncMock(return_value=_qr())
    return c


# ---------------------------------------------------------------------------
# Conditional routers
# ---------------------------------------------------------------------------


class TestRouteAfterClassify:
    def test_data_intent_routes_to_retrieve(self) -> None:
        s = _state(intent=Intent(value=IntentEnum.DATA, rationale="ok"))
        assert route_after_classify(s) == "retrieve_context"  # type: ignore[arg-type]

    def test_ambiguous_intent_routes_to_clarify(self) -> None:
        s = _state(intent=Intent(value=IntentEnum.AMBIGUOUS, rationale="needs date"))
        assert route_after_classify(s) == "clarify"  # type: ignore[arg-type]

    def test_out_of_scope_routes_to_refuse(self) -> None:
        s = _state(intent=Intent(value=IntentEnum.OUT_OF_SCOPE, rationale="PII"))
        assert route_after_classify(s) == "refuse"  # type: ignore[arg-type]

    def test_none_intent_falls_back_to_data_path(self) -> None:
        s = _state(intent=None)
        assert route_after_classify(s) == "retrieve_context"  # type: ignore[arg-type]


class TestRouteAfterValidate:
    def test_passing_validation_routes_to_execute(self) -> None:
        s = _state(validation_result=ValidationResult(ok=True, sql="SELECT 1"))
        assert route_after_validate(s) == "execute_sql"  # type: ignore[arg-type]

    def test_failing_validation_under_cap_retries_draft(self) -> None:
        s = _state(
            validation_result=ValidationResult(ok=False, errors=["bad"]),
            retry_count=1,
        )
        assert route_after_validate(s) == "draft_sql"  # type: ignore[arg-type]

    def test_failing_validation_at_cap_terminates(self) -> None:
        s = _state(
            validation_result=ValidationResult(ok=False, errors=["bad"]),
            retry_count=MAX_RETRIES,
        )
        assert route_after_validate(s) == "terminate_failure"  # type: ignore[arg-type]


class TestRouteAfterExecute:
    def test_query_result_routes_to_interpret(self) -> None:
        s = _state(query_result=_qr())
        assert route_after_execute(s) == "interpret_result"  # type: ignore[arg-type]

    def test_no_result_under_cap_retries_draft(self) -> None:
        s = _state(query_result=None, retry_count=1)
        assert route_after_execute(s) == "draft_sql"  # type: ignore[arg-type]

    def test_no_result_at_cap_terminates(self) -> None:
        s = _state(query_result=None, retry_count=MAX_RETRIES)
        assert route_after_execute(s) == "terminate_failure"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Node-level: clarify / refuse / terminate_failure
# ---------------------------------------------------------------------------


class TestClarifyNode:
    """clarify() requires a graph runtime context to call interrupt().

    The full pause-and-resume behaviour is exercised in
    ``test_clarify_pauses_then_resumes`` below.
    """

    def test_clarify_calls_interrupt_primitive(self) -> None:
        from langgraph.errors import GraphInterrupt

        from voyage.agent.nodes.clarify import clarify

        # Outside a graph runtime, interrupt() raises GraphInterrupt or a
        # RuntimeError (depending on whether a previous run left runnable
        # context set).  Either proves the node delegated to the primitive.
        with pytest.raises((GraphInterrupt, RuntimeError)):
            clarify(_state(intent=Intent(value=IntentEnum.AMBIGUOUS, rationale="?")))  # type: ignore[arg-type]


class TestRefuseNode:
    def test_returns_terminal_answer(self) -> None:
        from voyage.agent.nodes.refuse import refuse

        s = _state(intent=Intent(value=IntentEnum.OUT_OF_SCOPE, rationale="asks for PII"))
        result = refuse(s)  # type: ignore[arg-type]
        assert isinstance(result["answer"], Answer)
        assert "asks for PII" in result["answer"].summary
        assert result["trace"][0].node == "refuse"


class TestTerminateFailureNode:
    def test_returns_failure_answer_with_recent_errors(self) -> None:
        from voyage.agent.nodes.failure import terminate_failure

        s = _state(
            retry_count=3,
            errors=[
                NodeError(node="validate_sql", error="LIMIT missing"),
                NodeError(node="execute_sql", error="syntax error"),
            ],
        )
        result = terminate_failure(s)  # type: ignore[arg-type]
        assert isinstance(result["answer"], Answer)
        assert "3 attempts" in result["answer"].summary
        assert any("LIMIT missing" in h for h in result["answer"].highlights)


# ---------------------------------------------------------------------------
# Retry-counter increments on validate / execute failure
# ---------------------------------------------------------------------------


class TestRetryCounterIncrement:
    @pytest.mark.asyncio
    async def test_validate_failure_increments_retry_count(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        draft = SqlDraft(sql="DROP TABLE x", rationale="evil", confidence=0.1)
        cfg = {"configurable": {"client": MagicMock()}}
        result = await validate_sql(_state(sql_draft=draft, retry_count=1), cfg)  # type: ignore[arg-type]
        assert result["retry_count"] == 2

    @pytest.mark.asyncio
    async def test_validate_success_does_not_increment(self) -> None:
        from voyage.agent.nodes.validate import validate_sql

        client = MagicMock()
        client.explain_query = AsyncMock(
            return_value=ExplainPlan(sql="SELECT 1 LIMIT 1000", plan="x", estimated_cost=1.0)
        )
        draft = SqlDraft(
            sql="SELECT COUNT(*) FROM warehouse.markets", rationale="ok", confidence=0.9
        )
        result = await validate_sql(
            _state(sql_draft=draft, retry_count=1),  # type: ignore[arg-type]
            {"configurable": {"client": client}},
        )
        assert "retry_count" not in result  # untouched, reducer keeps prior value

    @pytest.mark.asyncio
    async def test_execute_failure_increments_retry_count(self) -> None:
        from voyage.agent.nodes.execute import execute_sql

        client = MagicMock()
        client.run_query = AsyncMock(side_effect=Exception("bad column"))
        val = ValidationResult(ok=True, sql="SELECT 1 LIMIT 1")
        result = await execute_sql(
            _state(validation_result=val, retry_count=1),  # type: ignore[arg-type]
            {"configurable": {"client": client}},
        )
        assert result["retry_count"] == 2


# ---------------------------------------------------------------------------
# Graph integration tests — happy + safety branches
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_examples_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """Avoid filesystem dependency on evals/examples.yaml in unit tests."""
    monkeypatch.setattr("voyage.agent.nodes.retrieve._load_examples", lambda _question, _top_k: [])


class TestGraphIntegration:
    @pytest.mark.asyncio
    @patch("voyage.agent.nodes.classify.chat")
    async def test_refuse_path_terminates_without_db_calls(self, mock_classify: Any) -> None:
        mock_classify.return_value = (
            Intent(value=IntentEnum.OUT_OF_SCOPE, rationale="asks for PII"),
            5,
            5,
        )
        graph = build_graph()
        client = _client()
        cfg = {"configurable": {"client": client, "thread_id": "refuse-1"}}

        state = await graph.ainvoke(initial_state("show all guest emails"), config=cfg)
        assert state["answer"] is not None
        assert "PII" in state["answer"].summary
        assert client.list_tables.call_count == 0
        assert client.run_query.call_count == 0

    @pytest.mark.asyncio
    @patch("voyage.agent.llm.chat")
    @patch("voyage.agent.nodes.classify.chat")
    async def test_retry_then_recover(
        self,
        mock_classify: Any,
        mock_llm_chat: Any,
    ) -> None:
        """A first draft references a missing column; the second succeeds."""
        mock_classify.return_value = (
            Intent(value=IntentEnum.DATA, rationale="data"),
            5,
            5,
        )
        mock_llm_chat.side_effect = _dispatch_chat(
            {
                SqlDraft: [
                    (
                        SqlDraft(
                            sql="SELECT bogus FROM warehouse.reservations LIMIT 10",
                            rationale="first try",
                            confidence=0.6,
                        ),
                        30,
                        20,
                    ),
                    (
                        SqlDraft(
                            sql="SELECT id FROM warehouse.reservations LIMIT 10",
                            rationale="recovered",
                            confidence=0.9,
                        ),
                        30,
                        20,
                    ),
                ],
                Answer: [
                    (Answer(summary="ok", highlights=[], chart_spec=None), 10, 10),
                ],
            }
        )
        # First run_query raises; second succeeds.
        client = _client()
        client.run_query = AsyncMock(
            side_effect=[Exception('column "bogus" does not exist'), _qr()]
        )

        graph = build_graph()
        cfg = {"configurable": {"client": client, "thread_id": "retry-1"}}
        state = await graph.ainvoke(initial_state("how many reservations?"), config=cfg)

        assert state["answer"] is not None
        assert state["answer"].summary == "ok"
        assert client.run_query.call_count == 2
        assert state["retry_count"] == 1

    @pytest.mark.asyncio
    @patch("voyage.agent.llm.chat")
    @patch("voyage.agent.nodes.classify.chat")
    async def test_retries_exhausted_terminates_with_failure(
        self, mock_classify: Any, mock_llm_chat: Any
    ) -> None:
        mock_classify.return_value = (
            Intent(value=IntentEnum.DATA, rationale="data"),
            5,
            5,
        )
        # Every draft is a non-SELECT — validate always fails, never reaches execute.
        bad_draft = (
            SqlDraft(sql="DROP TABLE x", rationale="bad", confidence=0.1),
            30,
            20,
        )
        mock_llm_chat.side_effect = _dispatch_chat({SqlDraft: [bad_draft] * 10})

        client = _client()
        graph = build_graph()
        cfg = {"configurable": {"client": client, "thread_id": "exhaust-1"}}
        state = await graph.ainvoke(initial_state("anything"), config=cfg)

        assert state["answer"] is not None
        assert "wasn't able" in state["answer"].summary
        assert state["retry_count"] >= MAX_RETRIES
        assert client.run_query.call_count == 0  # never reached execute

    @pytest.mark.asyncio
    @patch("voyage.agent.llm.chat")
    @patch("voyage.agent.nodes.classify.chat")
    async def test_clarify_pauses_then_resumes(
        self,
        mock_classify: Any,
        mock_llm_chat: Any,
    ) -> None:
        """An ambiguous question pauses on interrupt; resume continues to answer."""
        mock_classify.return_value = (
            Intent(value=IntentEnum.AMBIGUOUS, rationale="which market?"),
            5,
            5,
        )
        mock_llm_chat.side_effect = _dispatch_chat(
            {
                SqlDraft: [
                    (
                        SqlDraft(
                            sql="SELECT 1 FROM warehouse.markets LIMIT 1",
                            rationale="ok",
                            confidence=0.9,
                        ),
                        10,
                        10,
                    )
                ],
                Answer: [(Answer(summary="done", highlights=[], chart_spec=None), 10, 10)],
            }
        )
        client = _client()
        graph = build_graph()
        cfg = {"configurable": {"client": client, "thread_id": "clarify-1"}}

        # First invocation hits the interrupt in clarify().
        state = await graph.ainvoke(initial_state("ADR for next weekend"), config=cfg)
        assert state.get("__interrupt__"), "graph did not pause for clarification"

        # Resume with the user's clarification.
        state = await graph.ainvoke(Command(resume="Joshua Tree"), config=cfg)
        assert state["answer"] is not None
        assert state["answer"].summary == "done"
        assert state["clarification"] == "Joshua Tree"
