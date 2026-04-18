#!/usr/bin/env python3
"""Generate and load synthetic warehouse data for the Voyage BI Copilot.

Run via:  make seed
          uv run python scripts/seed.py

Idempotent: truncates all warehouse tables and re-inserts from a fixed random
seed so the dataset is reproducible and eval golden hashes remain stable.

Target cardinalities (from CLAUDE.md):
  markets            10
  owners             50
  properties        200
  reservations   10 000  (~5 % cancelled)
  pricing_snapshots  73 000  (365 per property)
  reviews          3 000
  sensor_events    5 000
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv
from faker import Faker
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

load_dotenv()

console = Console()

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
SEED = 42
fake = Faker()
Faker.seed(SEED)
rng = random.Random(SEED)

# ---------------------------------------------------------------------------
# Seed date — rolling 12-month window ends here
# ---------------------------------------------------------------------------
SEED_DATE = date(2026, 4, 17)
WINDOW_START = SEED_DATE - timedelta(days=365)

# ---------------------------------------------------------------------------
# Target cardinalities
# ---------------------------------------------------------------------------
N_OWNERS = 50
N_PROPERTIES = 200
N_RESERVATIONS = 10_000
N_REVIEWS = 3_000
N_SENSOR_EVENTS = 5_000
CANCELLATION_RATE = 0.05  # ~5 % of reservations are cancelled
REVIEW_RATE = 0.32  # ~32 % of confirmed reservations get a review

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------
MARKETS: list[tuple[str, str, str, str]] = [
    ("Joshua Tree", "CA", "America/Los_Angeles", "West"),
    ("Big Bear", "CA", "America/Los_Angeles", "West"),
    ("Lake Tahoe", "CA", "America/Los_Angeles", "West"),
    ("Sedona", "AZ", "America/Phoenix", "Southwest"),
    ("Scottsdale", "AZ", "America/Phoenix", "Southwest"),
    ("Smoky Mountains", "TN", "America/New_York", "Southeast"),
    ("Outer Banks", "NC", "America/New_York", "Southeast"),
    ("Destin", "FL", "America/Chicago", "Southeast"),
    ("Miami Beach", "FL", "America/New_York", "Southeast"),
    ("Austin", "TX", "America/Chicago", "South"),
]

# channel → (selection weight, fee rate applied to gross revenue)
CHANNELS: dict[str, tuple[float, float]] = {
    "airbnb": (0.40, 0.12),
    "vrbo": (0.30, 0.08),
    "direct": (0.20, 0.03),
    "marriott": (0.10, 0.15),
}

CHANNEL_NAMES = list(CHANNELS.keys())
CHANNEL_WEIGHTS = [v[0] for v in CHANNELS.values()]

BEDROOM_OPTIONS = [1, 2, 3, 4, 5]
BEDROOM_WEIGHTS = [0.25, 0.30, 0.25, 0.12, 0.08]

EVENT_TYPES = ["noise_spike", "occupancy_mismatch", "door_access", "lock_failure"]
EVENT_WEIGHTS = [0.30, 0.20, 0.35, 0.15]

SENTIMENT: dict[int, str] = {
    5: "positive",
    4: "positive",
    3: "neutral",
    2: "negative",
    1: "negative",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, delta))


def _aware_dt(d: date, tz_name: str) -> datetime:
    """Midnight on *d* in the given IANA timezone, returned as UTC-aware."""
    tz = ZoneInfo(tz_name)
    local = datetime(d.year, d.month, d.day, tzinfo=tz)
    return local.astimezone(UTC)


def _nightly_rate(bedrooms: int, market_idx: int) -> Decimal:
    """Base rate with market and bedroom multipliers plus random jitter."""
    base = Decimal("100")
    bedroom_mult = Decimal(str(1.0 + (bedrooms - 1) * 0.35))
    # coastal/resort markets are pricier
    market_mult = Decimal(str(rng.uniform(0.85, 1.40)))
    rate = base * bedroom_mult * market_mult
    return rate.quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# Insertion helpers — each returns the list of inserted PKs
# ---------------------------------------------------------------------------


async def _insert_markets(conn: asyncpg.Connection) -> list[int]:
    rows = [(name, state, tz, region) for name, state, tz, region in MARKETS]
    await conn.executemany(
        """
        INSERT INTO warehouse.markets (name, state, timezone, region)
        VALUES ($1, $2, $3, $4)
        """,
        rows,
    )
    records = await conn.fetch("SELECT market_id FROM warehouse.markets ORDER BY market_id")
    return [r["market_id"] for r in records]


async def _insert_owners(conn: asyncpg.Connection) -> list[int]:
    rows = []
    for _ in range(N_OWNERS):
        onboarded = _aware_dt(
            _rand_date(WINDOW_START - timedelta(days=730), WINDOW_START),
            "UTC",
        )
        rows.append((fake.name(), onboarded))
    await conn.executemany(
        "INSERT INTO warehouse.owners (name, onboarded_at) VALUES ($1, $2)",
        rows,
    )
    records = await conn.fetch("SELECT owner_id FROM warehouse.owners ORDER BY owner_id")
    return [r["owner_id"] for r in records]


async def _insert_properties(
    conn: asyncpg.Connection,
    market_ids: list[int],
    owner_ids: list[int],
) -> list[tuple[int, int, str, Decimal]]:
    """Returns list of (property_id, market_id, market_tz, nightly_base_rate)."""
    rows = []
    market_tz = {mid: MARKETS[i][2] for i, mid in enumerate(market_ids)}

    for i in range(N_PROPERTIES):
        mid = market_ids[i % len(market_ids)]
        oid = rng.choice(owner_ids)
        bedrooms = rng.choices(BEDROOM_OPTIONS, weights=BEDROOM_WEIGHTS, k=1)[0]
        max_occ = bedrooms * 2
        rate = _nightly_rate(bedrooms, i % len(market_ids))
        listed = _aware_dt(
            _rand_date(WINDOW_START - timedelta(days=365), WINDOW_START),
            market_tz[mid],
        )
        # ~8 % of properties are delisted within the window
        delisted: datetime | None = None
        if rng.random() < 0.08:
            dl_date = _rand_date(
                WINDOW_START + timedelta(days=30),
                SEED_DATE - timedelta(days=30),
            )
            delisted = _aware_dt(dl_date, market_tz[mid])

        rows.append((mid, oid, bedrooms, max_occ, rate, listed, delisted))

    await conn.executemany(
        """
        INSERT INTO warehouse.properties
          (market_id, owner_id, bedrooms, max_occupancy,
           nightly_base_rate, listed_at, delisted_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        """,
        rows,
    )
    records = await conn.fetch(
        "SELECT property_id, market_id, nightly_base_rate "
        "FROM warehouse.properties ORDER BY property_id"
    )
    return [
        (r["property_id"], r["market_id"], market_tz[r["market_id"]], r["nightly_base_rate"])
        for r in records
    ]


async def _insert_reservations(
    conn: asyncpg.Connection,
    properties: list[tuple[int, int, str, Decimal]],
) -> tuple[list[int], list[int]]:
    """Returns (confirmed_ids, all_ids)."""
    rows = []
    prop_cycle = list(range(len(properties)))

    for _ in range(N_RESERVATIONS):
        idx = rng.choice(prop_cycle)
        prop_id, _mid, tz_name, base_rate = properties[idx]

        channel = rng.choices(CHANNEL_NAMES, weights=CHANNEL_WEIGHTS, k=1)[0]
        _, fee_rate = CHANNELS[channel]

        check_in = _rand_date(WINDOW_START, SEED_DATE - timedelta(days=2))
        nights = rng.choices(
            [1, 2, 3, 4, 5, 6, 7, 10, 14],
            weights=[5, 10, 15, 15, 12, 8, 12, 8, 5],
            k=1,
        )[0]
        check_out = check_in + timedelta(days=nights)

        gross = (base_rate * Decimal(str(nights))).quantize(Decimal("0.01"))
        fee = (gross * Decimal(str(fee_rate))).quantize(Decimal("0.01"))
        net = gross - fee

        # Booking date is 1–60 days before check-in
        advance_days = rng.randint(1, 60)
        booking_dt = _aware_dt(check_in - timedelta(days=advance_days), tz_name)

        status = "cancelled" if rng.random() < CANCELLATION_RATE else "confirmed"
        guest_id = rng.randint(1, 50_000)

        rows.append(
            (
                prop_id,
                guest_id,
                channel,
                check_in,
                check_out,
                nights,
                gross,
                net,
                booking_dt,
                status,
            )
        )

    await conn.executemany(
        """
        INSERT INTO warehouse.reservations
          (property_id, guest_id, channel, check_in, check_out, nights,
           gross_revenue, net_revenue, booking_date, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """,
        rows,
    )
    records = await conn.fetch(
        "SELECT reservation_id, status FROM warehouse.reservations ORDER BY reservation_id"
    )
    confirmed = [r["reservation_id"] for r in records if r["status"] == "confirmed"]
    all_ids = [r["reservation_id"] for r in records]
    return confirmed, all_ids


async def _insert_pricing_snapshots(
    conn: asyncpg.Connection,
    properties: list[tuple[int, int, str, Decimal]],
) -> None:
    """One row per property per day for 365 days ending on SEED_DATE."""
    rows = []
    dates = [SEED_DATE - timedelta(days=i) for i in range(365)]

    for prop_id, _mid, _tz, base_rate in properties:
        for snap_date in dates:
            # Add seasonal and random daily variation (±15 %)
            seasonal = 1.0 + 0.12 * math.sin(2 * math.pi * snap_date.timetuple().tm_yday / 365)
            jitter = rng.uniform(0.92, 1.08)
            rate = (base_rate * Decimal(str(seasonal * jitter))).quantize(Decimal("0.01"))
            rows.append((prop_id, snap_date, rate))

    # Batch in chunks of 5000 to avoid huge single executemany calls
    chunk = 5_000
    for i in range(0, len(rows), chunk):
        await conn.executemany(
            """
            INSERT INTO warehouse.pricing_snapshots (property_id, snapshot_date, nightly_rate)
            VALUES ($1, $2, $3)
            ON CONFLICT (property_id, snapshot_date) DO NOTHING
            """,
            rows[i : i + chunk],
        )


async def _insert_reviews(
    conn: asyncpg.Connection,
    confirmed_ids: list[int],
) -> None:
    sample_size = min(N_REVIEWS, int(len(confirmed_ids) * REVIEW_RATE))
    sampled = rng.sample(confirmed_ids, sample_size)

    # Fetch check_out dates to place submitted_at after checkout
    records = await conn.fetch(
        "SELECT reservation_id, check_out FROM warehouse.reservations "
        "WHERE reservation_id = ANY($1::int[])",
        sampled,
    )
    checkout_map: dict[int, date] = {r["reservation_id"]: r["check_out"] for r in records}

    rows = []
    for res_id in sampled:
        checkout = checkout_map.get(res_id, SEED_DATE)
        rating = rng.choices([1, 2, 3, 4, 5], weights=[3, 5, 12, 35, 45], k=1)[0]
        # ~10 % of reviews have no sentiment (not yet scored)
        sentiment: str | None = None if rng.random() < 0.10 else SENTIMENT[rating]
        days_after = rng.randint(1, 14)
        submitted = _aware_dt(checkout + timedelta(days=days_after), "UTC")
        rows.append((res_id, rating, sentiment, submitted))

    await conn.executemany(
        "INSERT INTO warehouse.reviews (reservation_id, rating, sentiment, submitted_at) "
        "VALUES ($1, $2, $3, $4)",
        rows,
    )


async def _insert_sensor_events(
    conn: asyncpg.Connection,
    property_ids: list[int],
    market_tz_map: dict[int, str],
) -> None:
    rows = []
    for _ in range(N_SENSOR_EVENTS):
        prop_id = rng.choice(property_ids)
        tz_name = market_tz_map.get(prop_id, "UTC")
        ev_type = rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
        severity = rng.choices([1, 2, 3, 4, 5], weights=[30, 25, 20, 15, 10], k=1)[0]
        occurred = _aware_dt(_rand_date(WINDOW_START, SEED_DATE), tz_name)
        rows.append((prop_id, ev_type, severity, occurred))

    await conn.executemany(
        "INSERT INTO warehouse.sensor_events (property_id, event_type, severity, occurred_at) "
        "VALUES ($1, $2, $3, $4)",
        rows,
    )


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def seed(conn: asyncpg.Connection) -> None:
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Truncating existing data…", total=None)
        await conn.execute(
            """
            TRUNCATE
              warehouse.sensor_events,
              warehouse.reviews,
              warehouse.pricing_snapshots,
              warehouse.reservations,
              warehouse.properties,
              warehouse.owners,
              warehouse.markets
            RESTART IDENTITY CASCADE
            """
        )
        progress.update(task, description="Truncate complete")

        progress.update(task, description="Inserting markets…")
        market_ids = await _insert_markets(conn)
        console.log(f"  [green]✓[/] {len(market_ids)} markets")

        progress.update(task, description="Inserting owners…")
        owner_ids = await _insert_owners(conn)
        console.log(f"  [green]✓[/] {len(owner_ids)} owners")

        progress.update(task, description="Inserting properties…")
        properties = await _insert_properties(conn, market_ids, owner_ids)
        console.log(f"  [green]✓[/] {len(properties)} properties")

        progress.update(task, description="Inserting reservations…")
        confirmed_ids, _all_ids = await _insert_reservations(conn, properties)
        total_res = await conn.fetchval("SELECT COUNT(*) FROM warehouse.reservations")
        cancelled = await conn.fetchval(
            "SELECT COUNT(*) FROM warehouse.reservations WHERE status = 'cancelled'"
        )
        console.log(
            f"  [green]✓[/] {total_res} reservations "
            f"({cancelled} cancelled, {total_res - cancelled} confirmed)"
        )

        progress.update(task, description="Inserting pricing snapshots…")
        await _insert_pricing_snapshots(conn, properties)
        snap_count = await conn.fetchval("SELECT COUNT(*) FROM warehouse.pricing_snapshots")
        console.log(f"  [green]✓[/] {snap_count} pricing snapshots")

        progress.update(task, description="Inserting reviews…")
        await _insert_reviews(conn, confirmed_ids)
        review_count = await conn.fetchval("SELECT COUNT(*) FROM warehouse.reviews")
        console.log(f"  [green]✓[/] {review_count} reviews")

        progress.update(task, description="Inserting sensor events…")
        prop_ids = [p[0] for p in properties]
        # Build property_id → market timezone map via market_id
        records = await conn.fetch(
            "SELECT p.property_id, m.timezone "
            "FROM warehouse.properties p "
            "JOIN warehouse.markets m ON m.market_id = p.market_id"
        )
        market_tz_map: dict[int, str] = {r["property_id"]: r["timezone"] for r in records}
        await _insert_sensor_events(conn, prop_ids, market_tz_map)
        event_count = await conn.fetchval("SELECT COUNT(*) FROM warehouse.sensor_events")
        console.log(f"  [green]✓[/] {event_count} sensor events")

        progress.update(task, description="Done", completed=1, total=1)

    console.print("\n[bold green]Seed complete.[/] Warehouse is ready.")


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        console.print("[bold red]ERROR:[/] DATABASE_URL is not set.")
        sys.exit(1)

    console.print(f"[bold]Connecting to:[/] {db_url.split('@')[-1]}")
    conn: asyncpg.Connection = await asyncpg.connect(db_url)
    try:
        await seed(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
