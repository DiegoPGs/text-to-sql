"""Unit tests for voyage.logging — the JSONL span logger."""

from __future__ import annotations

import json
from pathlib import Path

from voyage.agent.state import Span
from voyage.logging import JsonlSpanLogger, run_log_path


class TestRunLogPath:
    def test_path_includes_run_id(self, tmp_path: Path) -> None:
        path = run_log_path("a1b2c3d4", root=tmp_path)
        assert path.parent == tmp_path
        assert path.name.startswith("run-")
        assert "a1b2c3d4" in path.name
        assert path.suffix == ".jsonl"


class TestJsonlSpanLogger:
    def test_writes_one_line_per_span(self, tmp_path: Path) -> None:
        path = tmp_path / "run-test.jsonl"
        with JsonlSpanLogger(path, run_id="run-1") as sink:
            sink.write(Span(node="classify_intent", duration_ms=12.5))
            sink.write(Span(node="draft_sql", duration_ms=40.0, tokens_in=10, tokens_out=20))

        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2
        first = json.loads(lines[0])
        assert first["run_id"] == "run-1"
        assert first["node"] == "classify_intent"
        assert first["duration_ms"] == 12.5
        assert "ts" in first

    def test_write_many(self, tmp_path: Path) -> None:
        path = tmp_path / "run-many.jsonl"
        spans = [Span(node=f"node{i}", duration_ms=float(i)) for i in range(3)]
        with JsonlSpanLogger(path, run_id="r") as sink:
            sink.write_many(spans)
        assert len(path.read_text().strip().splitlines()) == 3

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        path = tmp_path / "deep" / "nested" / "run.jsonl"
        with JsonlSpanLogger(path, run_id="r") as sink:
            sink.write(Span(node="x", duration_ms=1.0))
        assert path.exists()

    def test_close_is_idempotent(self, tmp_path: Path) -> None:
        sink = JsonlSpanLogger(tmp_path / "run.jsonl", run_id="r")
        sink.close()
        sink.close()  # must not raise

    def test_appends_to_existing_file(self, tmp_path: Path) -> None:
        path = tmp_path / "run.jsonl"
        with JsonlSpanLogger(path, run_id="r") as sink:
            sink.write(Span(node="a", duration_ms=1.0))
        with JsonlSpanLogger(path, run_id="r") as sink:
            sink.write(Span(node="b", duration_ms=2.0))
        assert len(path.read_text().strip().splitlines()) == 2
