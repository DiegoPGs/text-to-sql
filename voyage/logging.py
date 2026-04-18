"""Structured JSON-lines span logger.

Every node calls ``log_span`` after it completes.  Spans are appended to
``logs/run-{run_id}.jsonl`` so each run has its own trace file.

The format is designed to be trivially adaptable to OpenTelemetry or
LangSmith with a thin exporter (see CLAUDE.md §Observability).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from voyage.agent.state import Span

_LOGS_DIR = Path(__file__).parent.parent / "logs"


def log_span(run_id: str, span: Span) -> None:
    """Append *span* as a JSON line to ``logs/run-{run_id}.jsonl``.

    Creates the logs directory if it does not exist.  Never raises — a
    logging failure must not crash the agent.
    """
    try:
        _LOGS_DIR.mkdir(exist_ok=True)
        path = _LOGS_DIR / f"run-{run_id}.jsonl"
        record = {"run_id": run_id, **span.model_dump()}
        with path.open("a") as fh:
            fh.write(json.dumps(record) + os.linesep)
    except Exception:  # noqa: BLE001
        pass
