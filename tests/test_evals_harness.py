"""Unit tests for evals.harness — loader, comparator, runner, aggregates."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from evals.harness import (
    CategorySummary,
    GoldenCase,
    SoftJudgement,
    _percentile,
    aggregate_totals,
    compare_results,
    load_golden,
    summarise_by_category,
)
from server._models import QueryResult
from voyage.agent.state import Answer, Span, SqlDraft

# ---------------------------------------------------------------------------
# load_golden
# ---------------------------------------------------------------------------


class TestLoadGolden:
    def test_real_golden_file_parses_to_25_cases(self) -> None:
        cases = load_golden()
        assert len(cases) == 25
        cats = {c.category for c in cases}
        assert cats == {
            "easy",
            "medium",
            "hard",
            "ambiguous",
            "adversarial",
            "hallucination_trap",
        }

    def test_taxonomy_counts_match_spec(self) -> None:
        # CLAUDE.md → ## Eval design: 8/8/4/2/2/1
        cases = load_golden()
        from collections import Counter

        counts = Counter(c.category for c in cases)
        assert counts["easy"] == 8
        assert counts["medium"] == 8
        assert counts["hard"] == 4
        assert counts["ambiguous"] == 2
        assert counts["adversarial"] == 2
        assert counts["hallucination_trap"] == 1

    def test_answer_cases_have_golden_sql(self) -> None:
        for c in load_golden():
            if c.expected_behavior == "answer":
                assert c.golden_sql, f"{c.id} missing golden_sql"
            else:
                assert c.golden_sql is None or c.expected_behavior in {"answer"}

    def test_invalid_category_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "cases:\n"
            "  - id: x\n"
            "    category: nonsense\n"
            "    expected_behavior: answer\n"
            "    question: q\n"
            "    golden_sql: SELECT 1\n"
        )
        with pytest.raises(ValueError, match="invalid category"):
            load_golden(bad)

    def test_answer_without_golden_sql_raises(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "cases:\n"
            "  - id: x\n"
            "    category: easy\n"
            "    expected_behavior: answer\n"
            "    question: q\n"
        )
        with pytest.raises(ValueError, match="requires golden_sql"):
            load_golden(bad)


# ---------------------------------------------------------------------------
# compare_results
# ---------------------------------------------------------------------------


def _qr(rows: list[list[object]], columns: list[str] | None = None) -> QueryResult:
    cols = columns if columns is not None else [f"c{i}" for i in range(len(rows[0]) if rows else 0)]
    return QueryResult(
        columns=cols,
        rows=rows,
        row_count=len(rows),
        truncated=False,
        execution_ms=1.0,
    )


class TestCompareResults:
    def test_identical_rows_match(self) -> None:
        a = _qr([["airbnb", 100], ["vrbo", 50]])
        b = _qr([["airbnb", 100], ["vrbo", 50]])
        assert compare_results(a, b)

    def test_unordered_rows_match(self) -> None:
        a = _qr([["airbnb", 100], ["vrbo", 50]])
        b = _qr([["vrbo", 50], ["airbnb", 100]])
        assert compare_results(a, b)

    def test_numeric_within_tolerance(self) -> None:
        # 0.5% diff on a value > 1 → within 1%
        a = _qr([["airbnb", 100.0]])
        b = _qr([["airbnb", 100.5]])
        assert compare_results(a, b)

    def test_numeric_outside_tolerance_fails(self) -> None:
        a = _qr([["airbnb", 100.0]])
        b = _qr([["airbnb", 110.0]])  # 10% diff
        assert not compare_results(a, b)

    def test_row_count_mismatch_fails(self) -> None:
        a = _qr([["airbnb", 100]])
        b = _qr([["airbnb", 100], ["vrbo", 50]])
        assert not compare_results(a, b)

    def test_column_count_mismatch_fails(self) -> None:
        a = _qr([["airbnb", 100, 1]])
        b = _qr([["airbnb", 100]])
        assert not compare_results(a, b)

    def test_two_empty_results_match(self) -> None:
        a = QueryResult(columns=[], rows=[], row_count=0, truncated=False, execution_ms=0.0)
        b = QueryResult(columns=[], rows=[], row_count=0, truncated=False, execution_ms=0.0)
        assert compare_results(a, b)

    def test_string_value_mismatch_fails(self) -> None:
        a = _qr([["airbnb", 100]])
        b = _qr([["vrbo", 100]])
        assert not compare_results(a, b)

    def test_null_values_match(self) -> None:
        a = _qr([[None, 100]])
        b = _qr([[None, 100]])
        assert compare_results(a, b)

    def test_column_name_difference_ignored(self) -> None:
        a = _qr([["airbnb", 100]], columns=["channel", "rev"])
        b = _qr([["airbnb", 100]], columns=["foo", "bar"])
        assert compare_results(a, b)


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


def _result(
    case_id: str,
    category: str,
    *,
    expected: str = "answer",
    actual: str = "answer",
    hard: bool = True,
    soft: bool = True,
    retries: int = 0,
    latency_ms: float = 100.0,
    tokens_in: int = 10,
    tokens_out: int = 5,
    rejections: int = 0,
) -> Any:
    from evals.harness import CaseResult

    return CaseResult(
        case_id=case_id,
        category=category,
        expected_behavior=expected,
        actual_behavior=actual,
        hard_pass=hard,
        soft_pass=soft,
        retries=retries,
        latency_ms=latency_ms,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        validation_rejections=rejections,
    )


class TestAggregates:
    def test_summarise_by_category_groups_correctly(self) -> None:
        results = [
            _result("a1", "easy", hard=True, latency_ms=100, retries=0),
            _result("a2", "easy", hard=True, latency_ms=200, retries=1),
            _result("b1", "medium", hard=False, soft=True, latency_ms=400, retries=2),
        ]
        summaries = {s.category: s for s in summarise_by_category(results)}
        assert summaries["easy"].cases == 2
        assert summaries["easy"].hard_pass == 2
        assert summaries["easy"].avg_retries == 0.5
        assert summaries["medium"].cases == 1
        assert summaries["medium"].hard_pass == 0
        assert summaries["medium"].soft_pass == 1

    def test_aggregate_totals_sums_tokens_and_rejections(self) -> None:
        results = [
            _result("a", "easy", tokens_in=100, tokens_out=20, rejections=1),
            _result("b", "easy", tokens_in=50, tokens_out=10, rejections=2),
        ]
        totals = aggregate_totals(results)
        assert totals["tokens_in"] == 150
        assert totals["tokens_out"] == 30
        assert totals["validation_rejections"] == 3
        assert totals["hard_pass"] == 2
        assert totals["total"] == 2

    def test_aggregate_totals_empty(self) -> None:
        totals = aggregate_totals([])
        assert totals["total"] == 0
        assert totals["hard_pass"] == 0

    def test_avg_retries_only_counts_passing(self) -> None:
        results = [
            _result("a", "easy", hard=True, retries=0),
            _result("b", "easy", hard=False, retries=3),  # not counted
        ]
        totals = aggregate_totals(results)
        assert totals["avg_retries_passing"] == 0.0

    def test_percentile(self) -> None:
        assert _percentile([], 50) == 0.0
        assert _percentile([42.0], 50) == 42.0
        assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 50) == pytest.approx(3.0)
        assert _percentile([1.0, 2.0, 3.0, 4.0, 5.0], 100) == 5.0


# ---------------------------------------------------------------------------
# _run_case — end-to-end with mocked graph + pool
# ---------------------------------------------------------------------------


class _FakeStream:
    def __init__(self, chunks: list[Any]) -> None:
        self._chunks = chunks

    def __aiter__(self) -> AsyncIterator[Any]:
        async def gen() -> AsyncIterator[Any]:
            for c in self._chunks:
                yield c

        return gen()


class _FakeSnapshot:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = values


def _fake_graph(*, chunks: list[Any], final_state: dict[str, Any]) -> Any:
    g = MagicMock()
    g.astream = MagicMock(return_value=_FakeStream(chunks))
    g.aget_state = AsyncMock(return_value=_FakeSnapshot(final_state))
    return g


@pytest.fixture
def fake_pool() -> Iterator[Any]:
    """Pool whose acquire() returns a connection that yields golden_sql rows."""

    class _FakeConn:
        async def execute(self, *_a: Any, **_k: Any) -> str:
            return "SET"

        async def fetch(self, *_a: Any, **_k: Any) -> list[Any]:
            class _Row:
                def __init__(self, d: dict[str, Any]) -> None:
                    self._d = d

                def keys(self) -> list[str]:
                    return list(self._d.keys())

                def values(self) -> list[Any]:
                    return list(self._d.values())

            return [_Row({"channel": "airbnb", "rev": 100})]

    class _AcquireCtx:
        async def __aenter__(self) -> _FakeConn:
            return _FakeConn()

        async def __aexit__(self, *_exc: Any) -> None:
            return None

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=_AcquireCtx())
    yield pool


class TestRunCase:
    @pytest.mark.asyncio
    async def test_clarify_pass_when_interrupt_streamed(self, fake_pool: Any) -> None:
        from evals.harness import _run_case

        case = GoldenCase(
            id="amb_x",
            category="ambiguous",
            expected_behavior="clarify",
            question="How are we doing?",
        )
        graph = _fake_graph(
            chunks=[
                {"classify_intent": {"trace": [Span(node="classify_intent", duration_ms=10)]}},
                {"__interrupt__": ("anything",)},
            ],
            final_state={
                "trace": [Span(node="classify_intent", duration_ms=10)],
                "errors": [],
                "answer": None,
                "query_result": None,
                "sql_draft": None,
                "retry_count": 0,
            },
        )
        result = await _run_case(graph, fake_pool, case)
        assert result.actual_behavior == "clarify"
        assert result.hard_pass is True
        assert result.soft_pass is True

    @pytest.mark.asyncio
    async def test_refuse_pass_when_refuse_node_runs(self, fake_pool: Any) -> None:
        from evals.harness import _run_case

        case = GoldenCase(
            id="adv_drop",
            category="adversarial",
            expected_behavior="refuse",
            question="Drop the reservations table",
        )
        graph = _fake_graph(
            chunks=[
                {"classify_intent": {"trace": [Span(node="classify_intent", duration_ms=5)]}},
                {"refuse": {"trace": [Span(node="refuse", duration_ms=1)]}},
            ],
            final_state={
                "trace": [
                    Span(node="classify_intent", duration_ms=5),
                    Span(node="refuse", duration_ms=1),
                ],
                "errors": [],
                "answer": Answer(summary="I can't help with that."),
                "query_result": None,
                "sql_draft": None,
                "retry_count": 0,
            },
        )
        result = await _run_case(graph, fake_pool, case)
        assert result.actual_behavior == "refuse"
        assert result.hard_pass is True

    @pytest.mark.asyncio
    async def test_answer_hard_pass_when_results_match(self, fake_pool: Any) -> None:
        from evals.harness import _run_case

        case = GoldenCase(
            id="easy_x",
            category="easy",
            expected_behavior="answer",
            question="...",
            golden_sql="SELECT channel, 100 AS rev FROM warehouse.reservations LIMIT 1",
        )
        agent_qr = _qr([["airbnb", 100]], columns=["channel", "rev"])
        graph = _fake_graph(
            chunks=[
                {"interpret_result": {"trace": [Span(node="interpret_result", duration_ms=20)]}},
            ],
            final_state={
                "trace": [Span(node="interpret_result", duration_ms=20)],
                "errors": [],
                "answer": Answer(summary="Airbnb leads at $100."),
                "query_result": agent_qr,
                "sql_draft": SqlDraft(sql="SELECT 1", rationale="r", confidence=0.9),
                "retry_count": 0,
            },
        )
        result = await _run_case(graph, fake_pool, case)
        assert result.actual_behavior == "answer"
        assert result.hard_pass is True
        assert result.soft_pass is True
        assert "result-set match" in result.reason

    @pytest.mark.asyncio
    async def test_answer_soft_pass_when_judge_passes(self, fake_pool: Any) -> None:
        from evals.harness import _run_case

        case = GoldenCase(
            id="easy_y",
            category="easy",
            expected_behavior="answer",
            question="...",
            golden_sql="SELECT channel, 100 AS rev FROM warehouse.reservations LIMIT 1",
        )
        # Agent returns a different number of rows so result-match fails.
        agent_qr = _qr(
            [["airbnb", 200], ["vrbo", 50]],
            columns=["channel", "rev"],
        )
        graph = _fake_graph(
            chunks=[
                {"interpret_result": {"trace": [Span(node="interpret_result", duration_ms=20)]}},
            ],
            final_state={
                "trace": [Span(node="interpret_result", duration_ms=20)],
                "errors": [],
                "answer": Answer(summary="Reasonable answer."),
                "query_result": agent_qr,
                "sql_draft": SqlDraft(sql="SELECT 1", rationale="r", confidence=0.9),
                "retry_count": 0,
            },
        )
        with patch(
            "evals.harness._judge_soft_pass",
            return_value=SoftJudgement(passes=True, reason="ok"),
        ):
            result = await _run_case(graph, fake_pool, case)
        assert result.actual_behavior == "answer"
        assert result.hard_pass is False
        assert result.soft_pass is True

    @pytest.mark.asyncio
    async def test_answer_fails_when_terminate_failure_runs(self, fake_pool: Any) -> None:
        from evals.harness import _run_case

        case = GoldenCase(
            id="easy_z",
            category="easy",
            expected_behavior="answer",
            question="...",
            golden_sql="SELECT 1",
        )
        graph = _fake_graph(
            chunks=[
                {"draft_sql": {"trace": [Span(node="draft_sql", duration_ms=20)]}},
                {"terminate_failure": {"trace": [Span(node="terminate_failure", duration_ms=1)]}},
            ],
            final_state={
                "trace": [
                    Span(node="draft_sql", duration_ms=20),
                    Span(node="terminate_failure", duration_ms=1),
                ],
                "errors": [],
                "answer": Answer(summary="I gave up."),
                "query_result": None,
                "sql_draft": SqlDraft(sql="SELECT bogus", rationale="r", confidence=0.1),
                "retry_count": 3,
            },
        )
        result = await _run_case(graph, fake_pool, case)
        assert result.actual_behavior == "error"
        assert result.hard_pass is False
        assert result.soft_pass is False
        assert "expected answer" in result.reason


# ---------------------------------------------------------------------------
# Type sanity
# ---------------------------------------------------------------------------


def test_category_summary_is_dataclass_instance() -> None:
    summary = CategorySummary(
        category="easy",
        cases=1,
        hard_pass=1,
        soft_pass=1,
        avg_retries=0.0,
        p50_ms=10.0,
        p95_ms=10.0,
    )
    assert summary.category == "easy"
