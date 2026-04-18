"""Unit tests for evals.report — markdown rendering."""

from __future__ import annotations

from pathlib import Path

from evals.harness import CaseResult
from evals.report import render_report, write_report


def _r(
    case_id: str,
    category: str,
    *,
    expected: str = "answer",
    actual: str = "answer",
    hard: bool = True,
    soft: bool = True,
    retries: int = 0,
    latency_ms: float = 100.0,
) -> CaseResult:
    return CaseResult(
        case_id=case_id,
        category=category,
        expected_behavior=expected,
        actual_behavior=actual,
        hard_pass=hard,
        soft_pass=soft,
        retries=retries,
        latency_ms=latency_ms,
        tokens_in=10,
        tokens_out=5,
        validation_rejections=0,
    )


class TestRenderReport:
    def test_empty_results_renders_placeholder(self) -> None:
        out = render_report([])
        assert "No cases ran" in out

    def test_summary_includes_pass_counts(self) -> None:
        results = [
            _r("a", "easy", hard=True),
            _r("b", "easy", hard=False, soft=True),
            _r("c", "medium", hard=False, soft=False),
        ]
        out = render_report(results)
        assert "Hard pass: **1/3**" in out
        assert "Soft pass: **2/3**" in out

    def test_per_category_table_present(self) -> None:
        results = [_r("a", "easy"), _r("b", "medium", hard=False, soft=True)]
        out = render_report(results)
        assert "| easy |" in out
        assert "| medium |" in out
        # Per-category percentages render
        assert "100%" in out

    def test_per_case_table_includes_each_id(self) -> None:
        results = [_r("alpha", "easy"), _r("beta", "medium")]
        out = render_report(results)
        assert "| alpha |" in out
        assert "| beta |" in out

    def test_pipes_and_newlines_in_reason_are_sanitised(self) -> None:
        r = _r("x", "easy")
        r.reason = "weird | reason\nwith newline"
        out = render_report([r])
        # No literal newline inside the reason cell — only the row terminator.
        line_with_x = next(line for line in out.splitlines() if "| x |" in line)
        assert "\n" not in line_with_x.split("| x |")[1].split("|")[-2]
        assert "weird / reason" in out

    def test_token_totals_formatted(self) -> None:
        results = [_r("a", "easy"), _r("b", "easy")]
        out = render_report(results)
        # 10 + 10 = 20 in
        assert "Total tokens in: **20**" in out


class TestWriteReport:
    def test_writes_file(self, tmp_path: Path) -> None:
        path = tmp_path / "out" / "latest.md"
        write_report([_r("a", "easy")], path)
        assert path.exists()
        text = path.read_text()
        assert "Voyage BI Copilot — Eval report" in text
