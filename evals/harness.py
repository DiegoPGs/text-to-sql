"""Voyage BI Copilot — eval harness (Phase 7).

Loads ``evals/golden.yaml``, runs each case through the full agent graph,
grades the trajectory, and writes a markdown report to ``evals/latest.md``.

Grading rules (mirrored from CLAUDE.md → ## Eval design):

* ``expected_behavior == "answer"``: execute ``golden_sql`` against the
  same DB the agent ran against and compare result sets unordered with a
  1% tolerance on numeric columns.  On result-match failure, fall back
  to an LLM-as-judge soft pass (a Pydantic ``SoftJudgement``).
* ``expected_behavior == "clarify"``: pass iff the graph paused at the
  ``clarify`` node (HITL interrupt detected on the stream).
* ``expected_behavior == "refuse"``: pass iff the graph terminated at
  the ``refuse`` node (no query_result, ``refuse`` span in trace).

Usage::

    uv run python evals/harness.py
    # or
    make eval
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import yaml
from pydantic import BaseModel, Field

from server._models import QueryResult
from voyage import config
from voyage.agent.client import WarehouseClient
from voyage.agent.graph import build_graph
from voyage.agent.llm import chat
from voyage.agent.state import initial_state

_REPO_ROOT = Path(__file__).resolve().parent.parent
_GOLDEN_PATH = _REPO_ROOT / "evals" / "golden.yaml"
_REPORT_PATH = _REPO_ROOT / "evals" / "latest.md"

_NUMERIC_TOLERANCE = 0.01

VALID_CATEGORIES = {
    "easy",
    "medium",
    "hard",
    "ambiguous",
    "adversarial",
    "hallucination_trap",
}
VALID_BEHAVIORS = {"answer", "clarify", "refuse"}


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class GoldenCase(BaseModel):
    """One row of the golden eval set."""

    id: str
    category: str
    expected_behavior: str
    question: str
    golden_sql: str | None = None


class CaseResult(BaseModel):
    """Outcome of running one case through the agent + grader."""

    case_id: str
    category: str
    expected_behavior: str
    actual_behavior: str  # "answer" | "clarify" | "refuse" | "error"
    hard_pass: bool
    soft_pass: bool
    reason: str = ""
    retries: int = 0
    latency_ms: float = 0.0
    tokens_in: int = 0
    tokens_out: int = 0
    validation_rejections: int = 0
    final_sql: str | None = None
    actual_row_count: int | None = None
    golden_row_count: int | None = None


class SoftJudgement(BaseModel):
    """LLM-judge verdict on whether an answer reasonably addresses a question."""

    passes: bool = Field(
        description=(
            "True if the answer summary plausibly addresses the question — be "
            "lenient about numerical exactness and column naming, strict about "
            "direction (e.g. higher vs. lower) and topic match."
        )
    )
    reason: str = Field(description="One sentence explaining the verdict.")


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_golden(path: Path = _GOLDEN_PATH) -> list[GoldenCase]:
    """Load and validate the golden YAML file into typed cases."""
    raw = yaml.safe_load(path.read_text())
    cases: list[GoldenCase] = []
    for item in raw.get("cases", []):
        case = GoldenCase(**item)
        if case.category not in VALID_CATEGORIES:
            raise ValueError(f"Case {case.id}: invalid category {case.category!r}")
        if case.expected_behavior not in VALID_BEHAVIORS:
            raise ValueError(
                f"Case {case.id}: invalid expected_behavior {case.expected_behavior!r}"
            )
        if case.expected_behavior == "answer" and not case.golden_sql:
            raise ValueError(f"Case {case.id}: expected_behavior=answer requires golden_sql")
        cases.append(case)
    return cases


# ---------------------------------------------------------------------------
# Result-set comparator
# ---------------------------------------------------------------------------


def _is_numeric(v: object) -> bool:
    return isinstance(v, (int, float, Decimal)) and not isinstance(v, bool)


def _row_sort_key(row: list[object]) -> tuple[str, ...]:
    """Stable, deterministic sort key for unordered row comparison."""
    return tuple(_canonical(v) for v in row)


def _canonical(v: object) -> str:
    if v is None:
        return ""
    if _is_numeric(v):
        return f"{float(v):.6f}"  # type: ignore[arg-type]
    return str(v)


def _values_close(a: object, b: object, tol: float) -> bool:
    if _is_numeric(a) and _is_numeric(b):
        fa, fb = float(a), float(b)  # type: ignore[arg-type]
        denom = max(abs(fa), abs(fb), 1.0)
        return abs(fa - fb) / denom <= tol
    if a is None and b is None:
        return True
    return str(a) == str(b)


def compare_results(
    actual: QueryResult,
    golden: QueryResult,
    *,
    tolerance: float = _NUMERIC_TOLERANCE,
) -> bool:
    """Return True if *actual* matches *golden* unordered with numeric tolerance.

    Column names are not required to match — only the row count and the
    per-column values (treated positionally after sorting).
    """
    if actual.row_count != golden.row_count:
        return False
    if not actual.rows and not golden.rows:
        return True
    if len(actual.rows[0]) != len(golden.rows[0]):
        return False

    a_sorted = sorted(actual.rows, key=_row_sort_key)
    g_sorted = sorted(golden.rows, key=_row_sort_key)
    for ra, rg in zip(a_sorted, g_sorted, strict=True):
        for va, vg in zip(ra, rg, strict=True):
            if not _values_close(va, vg, tolerance):
                return False
    return True


# ---------------------------------------------------------------------------
# Golden SQL execution (read-only, validator-bypassing — golden_sql is trusted)
# ---------------------------------------------------------------------------


async def _execute_golden(pool: Any, sql: str) -> QueryResult:
    """Run *sql* directly against the read-only pool and shape the result."""
    async with pool.acquire() as conn:
        await conn.execute(f"SET statement_timeout = {config.STATEMENT_TIMEOUT_MS}")
        t0 = time.monotonic()
        records = await conn.fetch(sql)
        elapsed_ms = round((time.monotonic() - t0) * 1000, 2)

    if not records:
        return QueryResult(
            columns=[], rows=[], row_count=0, truncated=False, execution_ms=elapsed_ms
        )
    columns = list(records[0].keys())
    rows: list[list[object]] = [list(r.values()) for r in records]
    return QueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        truncated=False,
        execution_ms=elapsed_ms,
    )


# ---------------------------------------------------------------------------
# LLM judge (only invoked when result-match fails on an "answer" case)
# ---------------------------------------------------------------------------

_JUDGE_SYSTEM = """\
You grade whether an analytics answer reasonably addresses a user question.
Be lenient about numerical exactness, column naming, and result ordering.
Be strict about topic match and qualitative direction (e.g. an answer that
says revenue grew when the question asked about a decline should fail).

Return passes=True if the answer is in the right ballpark; passes=False
if it is irrelevant, contradictory, or admits failure.
"""


def _judge_soft_pass(case: GoldenCase, summary: str) -> SoftJudgement:
    """Ask the LLM to judge whether *summary* reasonably answers *case.question*."""
    judgement, _, _ = chat(
        SoftJudgement,
        [
            {
                "role": "user",
                "content": (
                    f"Question: {case.question}\n\n"
                    f"Agent answer: {summary}\n\n"
                    "Does the agent answer reasonably address the question?"
                ),
            }
        ],
        system=_JUDGE_SYSTEM,
    )
    return judgement


# ---------------------------------------------------------------------------
# Per-case runner
# ---------------------------------------------------------------------------


@dataclass
class _StreamOutcome:
    """Bookkeeping accumulated while streaming one case through the graph."""

    interrupted: bool = False
    nodes_seen: list[str] = field(default_factory=list)


async def _run_stream(graph: Any, cfg: dict[str, Any], question: str) -> _StreamOutcome:
    """Drive the graph to completion (or to its first interrupt)."""
    out = _StreamOutcome()
    async for chunk in graph.astream(initial_state(question), config=cfg, stream_mode="updates"):
        if "__interrupt__" in chunk:
            out.interrupted = True
            break
        for node_name in chunk:
            if node_name != "__interrupt__":
                out.nodes_seen.append(node_name)
    return out


def _classify_actual_behavior(state: dict[str, Any], outcome: _StreamOutcome) -> str:
    """Map final state + stream outcome to one of the four actual behaviors."""
    if outcome.interrupted:
        return "clarify"
    if "refuse" in outcome.nodes_seen:
        return "refuse"
    if "terminate_failure" in outcome.nodes_seen:
        return "error"
    if state.get("answer") is not None and state.get("query_result") is not None:
        return "answer"
    return "error"


async def _run_case(graph: Any, pool: Any, case: GoldenCase) -> CaseResult:
    """Run one case end-to-end and return a graded result."""
    client = WarehouseClient(pool)
    cfg: dict[str, Any] = {"configurable": {"client": client, "thread_id": f"eval-{case.id}"}}

    t0 = time.monotonic()
    try:
        outcome = await _run_stream(graph, cfg, case.question)
        snapshot = await graph.aget_state(cfg)
        state: dict[str, Any] = snapshot.values
    except Exception as exc:  # noqa: BLE001
        latency_ms = round((time.monotonic() - t0) * 1000, 2)
        return CaseResult(
            case_id=case.id,
            category=case.category,
            expected_behavior=case.expected_behavior,
            actual_behavior="error",
            hard_pass=False,
            soft_pass=False,
            reason=f"graph crashed: {exc!s}",
            latency_ms=latency_ms,
        )
    latency_ms = round((time.monotonic() - t0) * 1000, 2)

    # Extract trace metrics
    spans = state.get("trace", [])
    tokens_in = sum(int(s.tokens_in) for s in spans)
    tokens_out = sum(int(s.tokens_out) for s in spans)
    validation_rejections = sum(1 for s in spans if s.node == "validate_sql" and s.error)
    retries = int(state.get("retry_count", 0))
    final_sql = state["sql_draft"].sql if state.get("sql_draft") is not None else None
    actual_row_count = (
        state["query_result"].row_count if state.get("query_result") is not None else None
    )

    actual_behavior = _classify_actual_behavior(state, outcome)

    base = CaseResult(
        case_id=case.id,
        category=case.category,
        expected_behavior=case.expected_behavior,
        actual_behavior=actual_behavior,
        hard_pass=False,
        soft_pass=False,
        retries=retries,
        latency_ms=latency_ms,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        validation_rejections=validation_rejections,
        final_sql=final_sql,
        actual_row_count=actual_row_count,
    )

    # Behavioral cases (clarify / refuse): pass iff actual matches expected.
    if case.expected_behavior in ("clarify", "refuse"):
        passed = actual_behavior == case.expected_behavior
        base.hard_pass = passed
        base.soft_pass = passed
        base.reason = "behavior matched" if passed else f"expected {case.expected_behavior}"
        return base

    # expected_behavior == "answer"
    if actual_behavior != "answer":
        base.reason = f"expected answer, got {actual_behavior}"
        return base

    qr = state["query_result"]
    assert case.golden_sql is not None  # guaranteed by load_golden
    try:
        golden_qr = await _execute_golden(pool, case.golden_sql)
    except Exception as exc:  # noqa: BLE001
        base.reason = f"golden_sql failed: {exc!s}"
        return base
    base.golden_row_count = golden_qr.row_count

    if compare_results(qr, golden_qr):
        base.hard_pass = True
        base.soft_pass = True
        base.reason = f"result-set match ({qr.row_count} rows)"
        return base

    # Soft-pass via LLM judge
    summary = state["answer"].summary if state.get("answer") is not None else ""
    try:
        verdict = _judge_soft_pass(case, summary)
        base.soft_pass = verdict.passes
        base.reason = (
            f"result-set mismatch; soft={'pass' if verdict.passes else 'fail'}: {verdict.reason}"
        )
    except Exception as exc:  # noqa: BLE001
        base.reason = f"result-set mismatch; judge failed: {exc!s}"

    return base


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def run_eval(cases: list[GoldenCase] | None = None) -> list[CaseResult]:
    """Run the full eval suite against the configured warehouse."""
    if cases is None:
        cases = load_golden()

    pool: Any = await asyncpg.create_pool(config.RO_DATABASE_URL, min_size=1, max_size=4)
    graph = build_graph()
    try:
        results: list[CaseResult] = []
        for case in cases:
            result = await _run_case(graph, pool, case)
            print(_format_case_line(result), file=sys.stderr)
            results.append(result)
    finally:
        await pool.close()
    return results


def _format_case_line(r: CaseResult) -> str:
    """Single-line progress update streamed to stderr while the harness runs."""
    icon = "PASS" if r.hard_pass else ("SOFT" if r.soft_pass else "FAIL")
    return (
        f"  [{icon}] {r.case_id:<48s} {r.category:<20s} {r.latency_ms:6.0f} ms  retries={r.retries}"
    )


# ---------------------------------------------------------------------------
# Aggregate metrics (used by both report.py and the CLI summary)
# ---------------------------------------------------------------------------


@dataclass
class CategorySummary:
    category: str
    cases: int
    hard_pass: int
    soft_pass: int
    avg_retries: float
    p50_ms: float
    p95_ms: float


def summarise_by_category(results: list[CaseResult]) -> list[CategorySummary]:
    """Bucket *results* by category and compute per-bucket aggregates."""
    by_cat: dict[str, list[CaseResult]] = {}
    for r in results:
        by_cat.setdefault(r.category, []).append(r)

    summaries: list[CategorySummary] = []
    for category in sorted(by_cat):
        bucket = by_cat[category]
        latencies = [r.latency_ms for r in bucket]
        retries = [r.retries for r in bucket]
        summaries.append(
            CategorySummary(
                category=category,
                cases=len(bucket),
                hard_pass=sum(1 for r in bucket if r.hard_pass),
                soft_pass=sum(1 for r in bucket if r.soft_pass),
                avg_retries=(sum(retries) / len(retries)) if retries else 0.0,
                p50_ms=_percentile(latencies, 50),
                p95_ms=_percentile(latencies, 95),
            )
        )
    return summaries


def _percentile(values: list[float], pct: float) -> float:
    """Simple linear-interpolation percentile (no numpy dependency)."""
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    if lo == hi:
        return s[lo]
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def aggregate_totals(results: list[CaseResult]) -> dict[str, float]:
    """Return overall totals used in the report header."""
    total = len(results)
    if total == 0:
        return {
            "total": 0,
            "hard_pass": 0,
            "soft_pass": 0,
            "tokens_in": 0,
            "tokens_out": 0,
            "validation_rejections": 0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "avg_retries_passing": 0.0,
        }
    latencies = [r.latency_ms for r in results]
    passing_retries = [r.retries for r in results if r.hard_pass]
    return {
        "total": total,
        "hard_pass": sum(1 for r in results if r.hard_pass),
        "soft_pass": sum(1 for r in results if r.soft_pass),
        "tokens_in": sum(r.tokens_in for r in results),
        "tokens_out": sum(r.tokens_out for r in results),
        "validation_rejections": sum(r.validation_rejections for r in results),
        "p50_ms": _percentile(latencies, 50),
        "p95_ms": _percentile(latencies, 95),
        "avg_retries_passing": (statistics.mean(passing_retries) if passing_retries else 0.0),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _main() -> int:
    from evals.report import write_report  # local import to avoid cycle on type-check

    print(f"voyage eval — running {len(load_golden())} cases", file=sys.stderr)
    results = await run_eval()
    write_report(results, _REPORT_PATH)
    totals = aggregate_totals(results)
    hard = int(totals["hard_pass"])
    soft = int(totals["soft_pass"])
    total = int(totals["total"])
    print(
        f"\nDone. {hard}/{total} hard pass, {soft}/{total} soft pass. "
        f"Report: {_REPORT_PATH.relative_to(_REPO_ROOT)}",
        file=sys.stderr,
    )
    return 0 if hard == total else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
