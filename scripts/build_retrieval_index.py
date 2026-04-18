#!/usr/bin/env python3
"""Build the pgvector retrieval index for the Voyage BI Copilot.

Embeds two kinds of content:
  - table_description: one entry per warehouse table with its schema + purpose
  - few_shot:          question→SQL examples from evals/examples.yaml

Embeddings are produced by OpenAI text-embedding-3-small (1536 dimensions).
The script is idempotent: it deletes existing embeddings before re-inserting.

Run via:  uv run python scripts/build_retrieval_index.py
Requires: OPENAI_API_KEY and DATABASE_URL in .env (or environment).
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import asyncpg
import yaml
from dotenv import load_dotenv
from openai import AsyncOpenAI
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

load_dotenv()

REPO_ROOT = Path(__file__).parent.parent
EXAMPLES_PATH = REPO_ROOT / "evals" / "examples.yaml"
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536

console = Console()

# ---------------------------------------------------------------------------
# Table descriptions — written here so they are stable and curated.
# These become the text that is embedded and later retrieved by the agent.
# ---------------------------------------------------------------------------
TABLE_DESCRIPTIONS: dict[str, str] = {
    "markets": (
        "Table: warehouse.markets\n"
        "Columns: market_id (PK), name (TEXT), state (TEXT), timezone (TEXT), region (TEXT)\n"
        "Description: Vacation rental markets — geographic groupings of properties. "
        "timezone is an IANA string (e.g. 'America/Los_Angeles'). "
        "Use this table when filtering by geography or timezone."
    ),
    "owners": (
        "Table: warehouse.owners\n"
        "Columns: owner_id (PK), name (TEXT), onboarded_at (TIMESTAMPTZ)\n"
        "Description: Property owners enrolled on the platform. "
        "Join with properties to get owner-level metrics."
    ),
    "properties": (
        "Table: warehouse.properties\n"
        "Columns: property_id (PK), market_id (FK→markets), owner_id (FK→owners), "
        "bedrooms (INT), max_occupancy (INT), nightly_base_rate (NUMERIC), "
        "listed_at (TIMESTAMPTZ), delisted_at (TIMESTAMPTZ nullable)\n"
        "Description: Individual rental properties. "
        "delisted_at IS NULL means the property is currently active. "
        "Filter WHERE delisted_at IS NULL to restrict to active listings."
    ),
    "reservations": (
        "Table: warehouse.reservations\n"
        "Columns: reservation_id (PK), property_id (FK→properties), guest_id (INT), "
        "channel (TEXT: airbnb|vrbo|direct|marriott), check_in (DATE), check_out (DATE), "
        "nights (INT), gross_revenue (NUMERIC), net_revenue (NUMERIC), "
        "booking_date (TIMESTAMPTZ), status (TEXT: confirmed|cancelled)\n"
        "Description: All reservations including cancellations. "
        "Always filter status = 'confirmed' for revenue or occupancy metrics. "
        "net_revenue is after channel commission fees; gross_revenue is before. "
        "Use nights for occupancy calculations."
    ),
    "pricing_snapshots": (
        "Table: warehouse.pricing_snapshots\n"
        "Columns: property_id (FK→properties), snapshot_date (DATE), nightly_rate (NUMERIC)\n"
        "Primary key: (property_id, snapshot_date) — one row per property per day.\n"
        "Description: Daily listed nightly rate per property. "
        "Use to analyse pricing trends, average rates, or rate changes over time."
    ),
    "reviews": (
        "Table: warehouse.reviews\n"
        "Columns: review_id (PK), reservation_id (FK→reservations), rating (INT 1–5), "
        "sentiment (TEXT nullable: positive|neutral|negative), submitted_at (TIMESTAMPTZ)\n"
        "Description: Guest reviews for confirmed reservations. "
        "sentiment is a precomputed label and may be NULL for older reviews. "
        "Join with reservations and properties for property-level ratings."
    ),
    "sensor_events": (
        "Table: warehouse.sensor_events\n"
        "Columns: event_id (PK), property_id (FK→properties), "
        "event_type (TEXT: noise_spike|occupancy_mismatch|door_access|lock_failure), "
        "severity (INT 1–5), occurred_at (TIMESTAMPTZ)\n"
        "Description: IoT sensor events. "
        "Useful for cross-domain queries, e.g. properties with noise events and "
        "negative reviews, or lock failures in high-revenue markets."
    ),
}


# ---------------------------------------------------------------------------
# Embedding helper
# ---------------------------------------------------------------------------


async def _embed_batch(
    client: AsyncOpenAI,
    texts: list[str],
) -> list[list[float]]:
    response = await client.embeddings.create(
        input=texts,
        model=EMBEDDING_MODEL,
        dimensions=EMBEDDING_DIM,
    )
    return [item.embedding for item in response.data]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def build_index(conn: asyncpg.Connection) -> None:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        console.print(
            "[yellow]OPENAI_API_KEY is not set — skipping retrieval index build.[/]\n"
            "The agent will still work but schema retrieval will not use semantic search."
        )
        return

    client = AsyncOpenAI(api_key=api_key)

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        # -- Clear existing embeddings --------------------------------------
        task = progress.add_task("Clearing existing embeddings…", total=None)
        await conn.execute(
            "DELETE FROM public.retrieval_embeddings WHERE kind IN ('table_description','few_shot')"
        )

        # -- Embed table descriptions --------------------------------------
        progress.update(task, description="Embedding table descriptions…")
        td_labels = list(TABLE_DESCRIPTIONS.keys())
        td_texts = list(TABLE_DESCRIPTIONS.values())
        td_vectors = await _embed_batch(client, td_texts)

        td_rows: list[tuple[str, str, str, list[float], dict[str, Any]]] = [
            ("table_description", label, text, vec, {"table": label})
            for label, text, vec in zip(td_labels, td_texts, td_vectors, strict=True)
        ]
        await conn.executemany(
            """
            INSERT INTO public.retrieval_embeddings (kind, label, content, embedding, metadata)
            VALUES ($1, $2, $3, $4::vector, $5::jsonb)
            """,
            td_rows,
        )
        console.log(f"  [green]✓[/] {len(td_rows)} table descriptions embedded")

        # -- Embed few-shot examples ----------------------------------------
        progress.update(task, description="Embedding few-shot examples…")
        if not EXAMPLES_PATH.exists():
            console.log(
                "[yellow]  ⚠  evals/examples.yaml not found — skipping few-shot embeddings[/]"
            )
        else:
            raw: dict[str, Any] = yaml.safe_load(EXAMPLES_PATH.read_text())
            examples: list[dict[str, Any]] = raw.get("examples", [])

            fs_labels = [ex["id"] for ex in examples]
            fs_texts = [ex["question"] for ex in examples]
            fs_vectors = await _embed_batch(client, fs_texts)

            fs_rows: list[tuple[str, str, str, list[float], dict[str, Any]]] = [
                ("few_shot", label, question, vec, {"id": label, "sql": ex["sql"]})
                for label, question, vec, ex in zip(
                    fs_labels, fs_texts, fs_vectors, examples, strict=True
                )
            ]
            await conn.executemany(
                """
                INSERT INTO public.retrieval_embeddings (kind, label, content, embedding, metadata)
                VALUES ($1, $2, $3, $4::vector, $5::jsonb)
                """,
                fs_rows,
            )
            console.log(f"  [green]✓[/] {len(fs_rows)} few-shot examples embedded")

        # -- Create IVFFlat index for fast ANN search ----------------------
        progress.update(task, description="Creating vector index…")
        total_rows = await conn.fetchval(
            "SELECT COUNT(*) FROM public.retrieval_embeddings WHERE embedding IS NOT NULL"
        )
        if isinstance(total_rows, int) and total_rows >= 100:
            lists = max(1, total_rows // 10)
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_retrieval_embedding
                ON public.retrieval_embeddings
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = {lists})
                """
            )
            console.log(f"  [green]✓[/] IVFFlat index created (lists={lists})")
        else:
            console.log("  [dim]Skipped IVFFlat index (need ≥100 rows)[/]")

        progress.update(task, description="Done", completed=1, total=1)

    console.print("\n[bold green]Retrieval index built.[/]")


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        console.print("[bold red]ERROR:[/] DATABASE_URL is not set.")
        sys.exit(1)

    conn: asyncpg.Connection = await asyncpg.connect(db_url)
    try:
        await build_index(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
