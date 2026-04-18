"""Unit tests for voyage.cli — Phase 6 commands and helpers.

The tests use Typer's ``CliRunner`` for command-level coverage and exercise
the small pure helpers (``_format_status``, ``_extract_interrupt_payload``,
``_stream_segment``) directly.  No real database, network, or LLM is hit.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langgraph.types import Interrupt
from typer.testing import CliRunner

from voyage.agent.state import Span
from voyage.cli import (
    _extract_interrupt_payload,
    _format_status,
    _stream_segment,
    app,
)
from voyage.logging import JsonlSpanLogger

runner = CliRunner()


# ---------------------------------------------------------------------------
# Top-level CLI surface
# ---------------------------------------------------------------------------


class TestCliSurface:
    def test_root_help_lists_subcommands(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        for cmd in ("ask", "eval", "db", "mcp"):
            assert cmd in result.output

    def test_db_help_lists_seed_and_reset(self) -> None:
        result = runner.invoke(app, ["db", "--help"])
        assert result.exit_code == 0
        assert "seed" in result.output
        assert "reset" in result.output

    def test_mcp_help_lists_serve(self) -> None:
        result = runner.invoke(app, ["mcp", "--help"])
        assert result.exit_code == 0
        assert "serve" in result.output

    def test_eval_dispatches_to_harness_main(self) -> None:
        # Patch the harness entry point so the test never touches the network.
        with patch("evals.harness._main", new=AsyncMock(return_value=0)) as harness_main:
            result = runner.invoke(app, ["eval"])
        assert result.exit_code == 0
        harness_main.assert_awaited_once()


class TestDbReset:
    def test_aborts_without_yes_flag(self) -> None:
        # Decline the confirmation prompt — should abort, never touch the DB.
        with patch("voyage.cli._db_reset") as do_reset:
            result = runner.invoke(app, ["db", "reset"], input="n\n")
        assert result.exit_code != 0
        do_reset.assert_not_called()

    def test_yes_flag_skips_prompt(self) -> None:
        with patch("voyage.cli._db_reset") as do_reset:
            result = runner.invoke(app, ["db", "reset", "--yes"])
        assert result.exit_code == 0
        do_reset.assert_called_once()


class TestMcpServe:
    def test_dispatches_to_server(self) -> None:
        fake_server = MagicMock()
        with patch.dict("sys.modules"):  # ensure clean import surface
            with patch("server.warehouse_mcp.mcp", fake_server):
                result = runner.invoke(app, ["mcp", "serve"])
        assert result.exit_code == 0
        fake_server.run.assert_called_once()


# ---------------------------------------------------------------------------
# _format_status
# ---------------------------------------------------------------------------


class TestFormatStatus:
    def test_success_uses_green_check(self) -> None:
        s = Span(node="classify_intent", duration_ms=42.0)
        out = _format_status(s)
        assert "classify_intent" in out
        assert "42 ms" in out
        assert "[green]" in out

    def test_error_uses_red_cross(self) -> None:
        s = Span(node="execute_sql", duration_ms=10.0, error="boom")
        out = _format_status(s)
        assert "[red]" in out
        assert "boom" in out

    def test_token_counts_rendered_when_present(self) -> None:
        s = Span(node="draft_sql", duration_ms=80.0, tokens_in=120, tokens_out=40)
        out = _format_status(s)
        assert "120→40" in out

    def test_retry_count_rendered(self) -> None:
        s = Span(node="validate_sql", duration_ms=5.0, retry_count=2)
        out = _format_status(s)
        assert "retry 2" in out


# ---------------------------------------------------------------------------
# _extract_interrupt_payload
# ---------------------------------------------------------------------------


class TestExtractInterruptPayload:
    def test_returns_none_when_no_interrupt(self) -> None:
        assert _extract_interrupt_payload({"foo": "bar"}) is None

    def test_unwraps_tuple_of_interrupt_objects(self) -> None:
        chunk = {"__interrupt__": (Interrupt(value={"reason": "why?"}, id="i1"),)}
        payload = _extract_interrupt_payload(chunk)
        assert payload == {"reason": "why?"}

    def test_handles_non_dict_value(self) -> None:
        chunk = {"__interrupt__": (Interrupt(value="just a string", id="i2"),)}
        payload = _extract_interrupt_payload(chunk)
        assert payload == {"reason": "just a string"}


# ---------------------------------------------------------------------------
# _stream_segment
# ---------------------------------------------------------------------------


class _FakeAsyncIter:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def __aiter__(self) -> _FakeAsyncIter:
        return self

    async def __anext__(self) -> Any:
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


class TestStreamSegment:
    @pytest.mark.asyncio
    async def test_writes_spans_to_logger_and_returns_no_interrupt(self, tmp_path: Path) -> None:
        graph = MagicMock()
        graph.astream = MagicMock(
            return_value=_FakeAsyncIter(
                [
                    {"classify_intent": {"trace": [Span(node="classify_intent", duration_ms=1.0)]}},
                    {"draft_sql": {"trace": [Span(node="draft_sql", duration_ms=2.0)]}},
                ]
            )
        )
        log_path = tmp_path / "stream.jsonl"
        with JsonlSpanLogger(log_path, run_id="r") as sink:
            payload = await _stream_segment(graph, sink, {}, {"question": "?"})
        assert payload is None
        assert len(log_path.read_text().strip().splitlines()) == 2

    @pytest.mark.asyncio
    async def test_returns_interrupt_payload(self, tmp_path: Path) -> None:
        graph = MagicMock()
        graph.astream = MagicMock(
            return_value=_FakeAsyncIter(
                [
                    {"classify_intent": {"trace": [Span(node="classify_intent", duration_ms=1.0)]}},
                    {"__interrupt__": (Interrupt(value={"reason": "need date"}, id="i"),)},
                ]
            )
        )
        with JsonlSpanLogger(tmp_path / "x.jsonl", run_id="r") as sink:
            payload = await _stream_segment(graph, sink, {}, {})
        assert payload == {"reason": "need date"}
