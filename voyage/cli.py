"""Voyage BI Copilot CLI — Phase 6.

Commands:
    ask          — run the agent against a natural-language question.
                   Streams per-node status, supports HITL clarification,
                   writes a JSONL trace, and optionally pretty-prints the
                   trace with ``--trace``.
    db seed      — generate and load synthetic warehouse data.
    db reset     — drop the warehouse schema, reapply it, and reseed.
    mcp serve    — run the MCP warehouse server over stdio.
    eval         — placeholder for the golden eval suite (Phase 7).

Usage::

    voyage ask "Top 5 markets by revenue last quarter" --trace
    voyage db seed
    voyage db reset --yes
    voyage mcp serve
"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

import asyncpg
import typer
from langgraph.types import Command
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from voyage import config
from voyage.agent.client import WarehouseClient
from voyage.agent.graph import build_graph
from voyage.agent.state import Span, initial_state
from voyage.logging import JsonlSpanLogger, run_log_path

# ---------------------------------------------------------------------------
# Typer apps
# ---------------------------------------------------------------------------

app = typer.Typer(add_completion=False, help="Voyage BI Copilot")
db_app = typer.Typer(add_completion=False, help="Database commands")
mcp_app = typer.Typer(add_completion=False, help="MCP server commands")
app.add_typer(db_app, name="db")
app.add_typer(mcp_app, name="mcp")

console = Console()

_REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# ask
# ---------------------------------------------------------------------------


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural-language question about the warehouse"),
    trace: bool = typer.Option(False, "--trace", help="Print the full span trace at the end"),
) -> None:
    """Run the agent against QUESTION and print the answer."""
    asyncio.run(_ask(question, trace=trace))


def _format_status(span: Span) -> str:
    """Render one node's outcome as a single-line status update."""
    icon = "[red]✗[/red]" if span.error else "[green]✓[/green]"
    suffix = ""
    if span.tokens_in or span.tokens_out:
        suffix += f"  [dim]({span.tokens_in}→{span.tokens_out} tok)[/dim]"
    if span.retry_count:
        suffix += f"  [yellow]retry {span.retry_count}[/yellow]"
    if span.error:
        suffix += f"  [red]{span.error[:80]}[/red]"
    return f"  {icon} {span.node}  [dim]{span.duration_ms:.0f} ms[/dim]{suffix}"


def _extract_interrupt_payload(chunk: dict[str, Any]) -> dict[str, Any] | None:
    raw = chunk.get("__interrupt__")
    if not raw:
        return None
    item = raw[0] if isinstance(raw, (list, tuple)) else raw
    payload = getattr(item, "value", item)
    return payload if isinstance(payload, dict) else {"reason": str(payload)}


async def _stream_segment(
    graph: Any,
    sink: JsonlSpanLogger,
    cfg: dict[str, Any],
    ainput: Any,
) -> dict[str, Any] | None:
    """Stream one segment of the graph; return the interrupt payload, if any."""
    interrupt_payload: dict[str, Any] | None = None
    async for chunk in graph.astream(ainput, config=cfg, stream_mode="updates"):
        if "__interrupt__" in chunk:
            interrupt_payload = _extract_interrupt_payload(chunk)
            continue
        for update in chunk.values():
            if not isinstance(update, dict):
                continue
            for span in update.get("trace", []):
                console.print(_format_status(span))
                sink.write(span)
    return interrupt_payload


async def _ask(question: str, *, trace: bool) -> None:
    run_id = str(uuid.uuid4())[:8]
    log_path = run_log_path(run_id)

    console.print(
        f"[bold]Voyage[/bold]  [dim]run {run_id}  →  {log_path}[/dim]\n[dim]Q:[/dim] {question}\n"
    )

    pool: Any = await asyncpg.create_pool(config.RO_DATABASE_URL, min_size=1, max_size=3)
    client = WarehouseClient(pool)
    graph = build_graph()
    cfg: dict[str, Any] = {"configurable": {"client": client, "thread_id": run_id}}

    try:
        with JsonlSpanLogger(log_path, run_id=run_id) as sink:
            payload = await _stream_segment(graph, sink, cfg, initial_state(question))
            while payload is not None:
                reason = payload.get("reason", "I need a bit more context to answer.")
                console.print(f"\n[bold yellow]Need clarification[/bold yellow]  {reason}")
                user_input = console.input("[bold]> [/bold]")
                payload = await _stream_segment(graph, sink, cfg, Command(resume=user_input))
        snapshot = await graph.aget_state(cfg)
        state: dict[str, Any] = snapshot.values
    finally:
        await pool.close()

    _render_result(state, trace=trace)


def _render_result(state: dict[str, Any], *, trace: bool) -> None:
    sql_draft = state.get("sql_draft")
    if sql_draft:
        console.print("\n[bold cyan]SQL[/bold cyan]")
        console.print(Syntax(sql_draft.sql, "sql", theme="monokai", word_wrap=True))

    qr = state.get("query_result")
    if qr is not None and qr.columns:
        console.print(
            f"\n[bold cyan]Results[/bold cyan]  "
            f"[dim]({qr.row_count} rows, {qr.execution_ms:.0f} ms)[/dim]"
        )
        tbl = Table(show_header=True, header_style="bold")
        for col in qr.columns:
            tbl.add_column(str(col))
        for row in qr.rows[:50]:
            tbl.add_row(*[str(v) for v in row])
        console.print(tbl)
        if qr.truncated:
            console.print("[yellow]Results truncated — increase ROW_LIMIT to see more.[/yellow]")

    answer = state.get("answer")
    if answer is not None:
        console.print(f"\n[bold green]Answer[/bold green]  {answer.summary}")
        for h in answer.highlights:
            console.print(f"  • {h}")

    errors = state.get("errors", [])
    if errors:
        console.print("\n[bold red]Errors[/bold red]")
        for e in errors:
            console.print(f"  [{e.node}] {e.error}")

    if trace:
        _render_trace(state.get("trace", []))


def _render_trace(spans: list[Span]) -> None:
    console.print("\n[bold magenta]Trace[/bold magenta]")
    tbl = Table(show_header=True, header_style="bold")
    tbl.add_column("#", justify="right")
    tbl.add_column("node")
    tbl.add_column("ms", justify="right")
    tbl.add_column("tok in", justify="right")
    tbl.add_column("tok out", justify="right")
    tbl.add_column("retry", justify="right")
    tbl.add_column("error")
    for i, s in enumerate(spans, 1):
        tbl.add_row(
            str(i),
            s.node,
            f"{s.duration_ms:.0f}",
            str(s.tokens_in),
            str(s.tokens_out),
            str(s.retry_count),
            (s.error[:60] if s.error else ""),
        )
    console.print(tbl)


# ---------------------------------------------------------------------------
# db seed / db reset
# ---------------------------------------------------------------------------


@db_app.command("seed")
def db_seed() -> None:
    """Generate and load the synthetic warehouse data."""
    from scripts import seed as seed_mod

    asyncio.run(seed_mod.main())


@db_app.command("reset")
def db_reset(
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt"),
) -> None:
    """Drop and recreate the warehouse schema, then reseed."""
    if not yes:
        msg = "This will DROP SCHEMA warehouse CASCADE and reseed. Continue?"
        if not typer.confirm(msg):
            raise typer.Abort()
    asyncio.run(_db_reset())


async def _db_reset() -> None:
    from scripts import seed as seed_mod

    schema_sql = (_REPO_ROOT / "sql" / "schema.sql").read_text()
    conn: Any = await asyncpg.connect(config.DATABASE_URL)
    try:
        await conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE;")
        console.print("[dim]Dropped warehouse schema.[/dim]")
        await conn.execute(schema_sql)
        console.print("[dim]Re-applied schema.[/dim]")
    finally:
        await conn.close()

    await seed_mod.main()


# ---------------------------------------------------------------------------
# mcp serve
# ---------------------------------------------------------------------------


@mcp_app.command("serve")
def mcp_serve() -> None:
    """Run the MCP warehouse server over stdio."""
    from server.warehouse_mcp import mcp as mcp_server

    mcp_server.run()


# ---------------------------------------------------------------------------
# eval (Phase 7 stub)
# ---------------------------------------------------------------------------


@app.command("eval")
def eval_cmd() -> None:
    """Run the golden eval suite. (Lands in Phase 7.)"""
    console.print(
        "[yellow]The eval harness lands in Phase 7. Track progress in CLAUDE.md → Phase 7.[/yellow]"
    )
    raise typer.Exit(code=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    app()
