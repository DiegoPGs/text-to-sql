"""Structured JSON-lines logger for agent runs.

Each agent node emits a :class:`Span` to ``state.trace``.  This module is
the sink that persists those spans to a per-run JSONL file under ``logs/``.

Format (one JSON object per line)::

    {
      "run_id":      "a1b2c3d4",
      "ts":          1736700000.123,        # epoch seconds, span emit time
      "node":        "draft_sql",
      "duration_ms": 421.5,
      "tokens_in":   132,
      "tokens_out":  88,
      "model":       "claude-opus-4-7",
      "retry_count": 0,
      "error":       ""
    }

Designed so a thin exporter can map it to OpenTelemetry or LangSmith later
without changing the call sites.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from types import TracebackType
from typing import IO

from voyage.agent.state import Span

_DEFAULT_LOGS_DIR = Path("logs")


def run_log_path(run_id: str, *, root: Path | None = None) -> Path:
    """Return the canonical log path for *run_id*.

    Format: ``logs/run-YYYYMMDDTHHMMSS-{run_id}.jsonl``.
    """
    base = root if root is not None else _DEFAULT_LOGS_DIR
    stamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    return base / f"run-{stamp}-{run_id}.jsonl"


class JsonlSpanLogger:
    """Append-only JSON-lines sink for :class:`Span` records."""

    def __init__(self, path: Path, *, run_id: str) -> None:
        self.path = path
        self.run_id = run_id
        path.parent.mkdir(parents=True, exist_ok=True)
        # Line-buffered so partial runs are still readable (e.g. on Ctrl-C).
        self._fh: IO[str] = path.open("a", buffering=1, encoding="utf-8")

    def write(self, span: Span) -> None:
        """Serialise *span* and append it as one JSON line."""
        record = {
            "run_id": self.run_id,
            "ts": time.time(),
            **span.model_dump(),
        }
        self._fh.write(json.dumps(record, default=str) + "\n")

    def write_many(self, spans: list[Span]) -> None:
        for span in spans:
            self.write(span)

    def close(self) -> None:
        if not self._fh.closed:
            self._fh.close()

    # --- context-manager sugar ------------------------------------------------

    def __enter__(self) -> JsonlSpanLogger:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
