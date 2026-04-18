"""Render a list of :class:`CaseResult` objects to a markdown report.

Pure rendering — does not run the agent or touch the database.  The
harness produces the results; this module only formats them.
"""

from __future__ import annotations

import time
from pathlib import Path

from evals.harness import (
    CaseResult,
    aggregate_totals,
    summarise_by_category,
)


def render_report(results: list[CaseResult]) -> str:
    """Render *results* into the canonical markdown report string."""
    if not results:
        return "# Voyage BI Copilot — Eval report\n\n_No cases ran._\n"

    totals = aggregate_totals(results)
    cats = summarise_by_category(results)

    total = int(totals["total"])
    hard = int(totals["hard_pass"])
    soft = int(totals["soft_pass"])

    lines: list[str] = []
    lines.append("# Voyage BI Copilot — Eval report")
    lines.append("")
    lines.append(f"_Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}_")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Total cases: **{total}**")
    lines.append(f"- Hard pass: **{hard}/{total}** ({_pct(hard, total)})")
    lines.append(f"- Soft pass: **{soft}/{total}** ({_pct(soft, total)})")
    lines.append(f"- p50 latency: **{totals['p50_ms']:.0f} ms**")
    lines.append(f"- p95 latency: **{totals['p95_ms']:.0f} ms**")
    lines.append(f"- Avg retries on passing cases: **{totals['avg_retries_passing']:.2f}**")
    lines.append(f"- Total tokens in: **{int(totals['tokens_in']):,}**")
    lines.append(f"- Total tokens out: **{int(totals['tokens_out']):,}**")
    lines.append(f"- Validation rejections: **{int(totals['validation_rejections'])}**")
    lines.append("")

    lines.append("## Per category")
    lines.append("")
    lines.append("| Category | Cases | Hard pass | Soft pass | Avg retries | p50 ms | p95 ms |")
    lines.append("| -------- | ----: | --------- | --------- | ----------: | -----: | -----: |")
    for c in cats:
        lines.append(
            f"| {c.category} | {c.cases} "
            f"| {c.hard_pass} ({_pct(c.hard_pass, c.cases)}) "
            f"| {c.soft_pass} ({_pct(c.soft_pass, c.cases)}) "
            f"| {c.avg_retries:.2f} | {c.p50_ms:.0f} | {c.p95_ms:.0f} |"
        )
    lines.append("")

    lines.append("## Per case")
    lines.append("")
    lines.append("| ID | Category | Expected | Actual | Hard | Soft | Retries | ms | Reason |")
    lines.append("| -- | -------- | -------- | ------ | :--: | :--: | ------: | -: | ------ |")
    for r in results:
        lines.append(
            f"| {r.case_id} | {r.category} | {r.expected_behavior} "
            f"| {r.actual_behavior} | {_check(r.hard_pass)} | {_check(r.soft_pass)} "
            f"| {r.retries} | {r.latency_ms:.0f} | {_clean(r.reason)} |"
        )
    lines.append("")

    return "\n".join(lines)


def write_report(results: list[CaseResult], path: Path) -> Path:
    """Write the rendered report to *path* and return the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_report(results), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pct(n: int, total: int) -> str:
    return f"{(100.0 * n / total):.0f}%" if total else "0%"


def _check(b: bool) -> str:
    return "yes" if b else "no"


def _clean(text: str) -> str:
    """Strip pipes and newlines so a reason cell never breaks the table."""
    return text.replace("|", "/").replace("\n", " ").strip()
