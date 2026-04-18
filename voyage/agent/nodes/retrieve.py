"""retrieve_context node — gather schema + examples the draft node needs.

Retrieval strategy (Phase 4 — no pgvector required):
  1. List all tables via WarehouseClient.
  2. Score each table by keyword overlap with the question; take top-k.
  3. Describe the top-k tables (full schema + sample rows).
  4. Load the full metrics catalog (always included — it is small).
  5. Load few-shot examples from evals/examples.yaml, rank by keyword
     overlap, take top-k.

pgvector-based semantic retrieval is added in a later phase; the keyword
fallback produces good results for straightforward questions.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import yaml
from langchain_core.runnables import RunnableConfig

from voyage.agent.client import WarehouseClient
from voyage.agent.state import AgentState, FewShot, NodeError, Span

_EXAMPLES_PATH = Path(__file__).parent.parent.parent.parent / "evals" / "examples.yaml"

_TOP_K_TABLES = 6
_TOP_K_EXAMPLES = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _keyword_score(text: str, question: str) -> int:
    """Count how many lowercased words from *question* appear in *text*."""
    words = set(question.lower().split())
    return sum(1 for w in words if w in text.lower())


def _rank_tables(
    summaries: list[Any],
    question: str,
    top_k: int,
) -> list[str]:
    """Return names of the *top_k* most relevant tables for *question*."""
    scored = [
        (
            _keyword_score(f"{s.name} {s.description}", question),
            s.name,
        )
        for s in summaries
    ]
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [name for _, name in scored[:top_k]]


def _load_examples(question: str, top_k: int) -> list[FewShot]:
    """Load few-shot examples from YAML and return the top-k by relevance."""
    try:
        raw: dict[str, Any] = yaml.safe_load(_EXAMPLES_PATH.read_text())
        examples: list[FewShot] = [
            FewShot(question=e["question"], sql=e["sql"].strip()) for e in raw.get("examples", [])
        ]
    except Exception:  # noqa: BLE001
        return []

    scored = [(_keyword_score(ex.question, question), ex) for ex in examples]
    scored.sort(key=lambda x: -x[0])
    return [ex for _, ex in scored[:top_k]]


# ---------------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------------


async def retrieve_context(state: AgentState, config: RunnableConfig) -> dict[str, Any]:
    """Populate retrieved_tables, retrieved_examples, and metrics_catalog."""
    t0 = time.monotonic()
    question = state["question"]
    if state["clarification"]:
        question = f"{question} {state['clarification']}"

    client: WarehouseClient = config["configurable"]["client"]

    try:
        summaries = await client.list_tables()
        top_names = _rank_tables(summaries, question, _TOP_K_TABLES)
        schemas = []
        for name in top_names:
            try:
                schemas.append(await client.describe_table(name))
            except Exception:  # noqa: BLE001
                pass

        metrics = client.get_metrics_catalog()
        examples = _load_examples(question, _TOP_K_EXAMPLES)

        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "retrieved_tables": schemas,
            "retrieved_examples": examples,
            "metrics_catalog": metrics,
            "errors": [],
            "trace": [Span(node="retrieve_context", duration_ms=duration)],
        }

    except Exception as exc:  # noqa: BLE001
        duration = round((time.monotonic() - t0) * 1000, 2)
        return {
            "retrieved_tables": [],
            "retrieved_examples": [],
            "metrics_catalog": [],
            "errors": [NodeError(node="retrieve_context", error=str(exc))],
            "trace": [Span(node="retrieve_context", duration_ms=duration, error=str(exc))],
        }
