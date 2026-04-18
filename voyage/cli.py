"""Voyage BI Copilot CLI — Phase 5.

Commands:
    ask  — run the agent against a natural-language question.  Handles
           HITL clarification interrupts and resumes the graph with the
           user's reply.

Full streaming output, --trace flag, and additional commands (eval,
db seed, db reset, mcp serve) are added in Phase 6.

Usage::

    python -m voyage.cli ask "What is total revenue last month?"
    voyage ask "Top 5 markets by revenue last quarter"
"""

from __future__ import annotations

import asyncio
import uuid
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
from voyage.agent.state import initial_state

app = typer.Typer(add_completion=False, help="Voyage BI Copilot")
console = Console()


# ---------------------------------------------------------------------------
# ask command
# ---------------------------------------------------------------------------


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural-language question about the warehouse"),
) -> None:
    """Run the agent against QUESTION and print the answer."""
    asyncio.run(_ask(question))


def _extract_interrupt(state: dict[str, Any]) -> dict[str, Any] | None:
    """Return the first interrupt payload from a graph result, if any."""
    raw = state.get("__interrupt__")
    if not raw:
        return None
    item = raw[0] if isinstance(raw, (list, tuple)) else raw
    payload = getattr(item, "value", item)
    return payload if isinstance(payload, dict) else {"reason": str(payload)}


async def _ask(question: str) -> None:
    run_id = str(uuid.uuid4())[:8]
    console.print(f"[dim]run {run_id}[/dim]")

    pool: Any = await asyncpg.create_pool(config.RO_DATABASE_URL, min_size=1, max_size=3)
    client = WarehouseClient(pool)
    graph = build_graph()
    cfg: dict[str, Any] = {"configurable": {"client": client, "thread_id": run_id}}

    try:
        state = await graph.ainvoke(initial_state(question), config=cfg)
        # HITL: if the graph paused on an interrupt, prompt and resume.
        while (payload := _extract_interrupt(state)) is not None:
            reason = payload.get("reason", "I need a bit more context to answer.")
            console.print(f"\n[bold yellow]Need clarification[/bold yellow]  {reason}")
            user_input = console.input("[bold]> [/bold]")
            state = await graph.ainvoke(Command(resume=user_input), config=cfg)
    finally:
        await pool.close()

    # --- SQL ------------------------------------------------------------------
    sql_draft = state.get("sql_draft")
    if sql_draft:
        console.print("\n[bold cyan]SQL[/bold cyan]")
        console.print(Syntax(sql_draft.sql, "sql", theme="monokai", word_wrap=True))

    # --- Results --------------------------------------------------------------
    qr = state.get("query_result")
    if qr and qr.columns:
        console.print(
            f"\n[bold cyan]Results[/bold cyan]  [dim]({qr.row_count} rows, {qr.execution_ms} ms)[/dim]"
        )
        tbl = Table(show_header=True, header_style="bold")
        for col in qr.columns:
            tbl.add_column(str(col))
        for row in qr.rows[:50]:
            tbl.add_row(*[str(v) for v in row])
        console.print(tbl)
        if qr.truncated:
            console.print("[yellow]Results truncated — increase ROW_LIMIT to see more.[/yellow]")

    # --- Answer ---------------------------------------------------------------
    answer = state.get("answer")
    if answer:
        console.print(f"\n[bold green]Answer[/bold green]  {answer.summary}")
        for h in answer.highlights:
            console.print(f"  • {h}")

    # --- Errors ---------------------------------------------------------------
    errors = state.get("errors", [])
    if errors:
        console.print("\n[bold red]Errors[/bold red]")
        for e in errors:
            console.print(f"  [{e.node}] {e.error}")


if __name__ == "__main__":
    app()
