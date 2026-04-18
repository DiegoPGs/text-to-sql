from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import partial


@dataclass(frozen=True)
class QueryResponse:
    question: str
    sql: str
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    chart: str | None = None


class UnsupportedQuestionError(ValueError):
    """Raised when a question does not match a supported business intent."""


def build_synthetic_warehouse(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        DROP TABLE IF EXISTS listings;
        DROP TABLE IF EXISTS bookings;
        DROP TABLE IF EXISTS reviews;

        CREATE TABLE listings (
            id INTEGER PRIMARY KEY,
            city TEXT NOT NULL,
            neighborhood TEXT NOT NULL,
            bedrooms INTEGER NOT NULL,
            nightly_rate REAL NOT NULL
        );

        CREATE TABLE bookings (
            id INTEGER PRIMARY KEY,
            listing_id INTEGER NOT NULL REFERENCES listings(id),
            check_in TEXT NOT NULL,
            nights INTEGER NOT NULL,
            status TEXT NOT NULL,
            total_amount REAL NOT NULL
        );

        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY,
            listing_id INTEGER NOT NULL REFERENCES listings(id),
            review_score REAL NOT NULL
        );
        """
    )

    listings = [
        (1, "Lisbon", "Alfama", 1, 110.0),
        (2, "Lisbon", "Bairro Alto", 2, 160.0),
        (3, "Porto", "Ribeira", 1, 95.0),
        (4, "Porto", "Cedofeita", 3, 180.0),
        (5, "Faro", "Marina", 2, 130.0),
    ]
    bookings = [
        (1, 1, "2026-01-11", 4, "confirmed", 440.0),
        (2, 1, "2026-02-10", 3, "confirmed", 330.0),
        (3, 2, "2026-01-16", 5, "confirmed", 800.0),
        (4, 3, "2026-01-20", 2, "confirmed", 190.0),
        (5, 4, "2026-03-04", 7, "confirmed", 1260.0),
        (6, 5, "2026-02-14", 4, "confirmed", 520.0),
        (7, 5, "2026-03-18", 2, "cancelled", 260.0),
        (8, 2, "2026-03-22", 6, "confirmed", 960.0),
        (9, 3, "2026-03-30", 5, "confirmed", 475.0),
    ]
    reviews = [
        (1, 1, 4.7),
        (2, 1, 4.8),
        (3, 2, 4.6),
        (4, 3, 4.4),
        (5, 4, 4.9),
        (6, 5, 4.5),
    ]

    connection.executemany(
        "INSERT INTO listings(id, city, neighborhood, bedrooms, nightly_rate) VALUES (?, ?, ?, ?, ?)",
        listings,
    )
    connection.executemany(
        "INSERT INTO bookings(id, listing_id, check_in, nights, status, total_amount) VALUES (?, ?, ?, ?, ?, ?)",
        bookings,
    )
    connection.executemany(
        "INSERT INTO reviews(id, listing_id, review_score) VALUES (?, ?, ?)", reviews
    )
    connection.commit()


class TextToSQLAgent:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def answer(self, question: str) -> QueryResponse:
        normalized = " ".join(question.lower().split())

        if "occupancy" in normalized and "city" in normalized:
            sql = """
                SELECT l.city,
                       ROUND(100.0 * SUM(CASE WHEN b.status = 'confirmed' THEN b.nights ELSE 0 END)
                             / (COUNT(DISTINCT l.id) * 365.0), 2) AS occupancy_rate
                FROM listings l
                LEFT JOIN bookings b ON b.listing_id = l.id
                GROUP BY l.city
                ORDER BY occupancy_rate DESC
            """
            chart_builder = partial(_bar_chart, suffix="%")
        elif "revenue" in normalized and ("top" in normalized or "highest" in normalized):
            sql = """
                SELECT l.city || ' - ' || l.neighborhood AS listing,
                       ROUND(SUM(CASE WHEN b.status = 'confirmed' THEN b.total_amount ELSE 0 END), 2) AS revenue
                FROM listings l
                LEFT JOIN bookings b ON b.listing_id = l.id
                GROUP BY l.id, l.city, l.neighborhood
                ORDER BY revenue DESC
                LIMIT 5
            """
            chart_builder = _bar_chart
        elif "bookings" in normalized and (
            "month" in normalized or "monthly" in normalized or "trend" in normalized
        ):
            sql = """
                SELECT SUBSTR(check_in, 1, 7) AS month,
                       COUNT(*) AS booking_count
                FROM bookings
                WHERE status = 'confirmed'
                GROUP BY SUBSTR(check_in, 1, 7)
                ORDER BY month
            """
            chart_builder = _bar_chart
        elif "rating" in normalized and "city" in normalized:
            sql = """
                SELECT l.city,
                       ROUND(AVG(r.review_score), 2) AS average_rating
                FROM listings l
                JOIN reviews r ON r.listing_id = l.id
                GROUP BY l.city
                ORDER BY average_rating DESC
            """
            chart_builder = None
        else:
            raise UnsupportedQuestionError(
                "Unsupported question. Try occupancy/revenue/bookings trend/average rating questions for cities."
            )

        self._validate_read_only_sql(sql)

        cursor = self.connection.execute(sql)
        columns = tuple(col[0] for col in cursor.description or [])
        rows = tuple(tuple(row) for row in cursor.fetchall())

        chart = None
        if chart_builder and rows:
            chart = chart_builder(rows)

        return QueryResponse(
            question=question,
            sql=_clean_sql(sql),
            columns=columns,
            rows=rows,
            chart=chart,
        )

    @staticmethod
    def _validate_read_only_sql(sql: str) -> None:
        cleaned = _clean_sql(sql).lower()
        if not cleaned.startswith("select"):
            raise ValueError("Only SELECT statements are allowed")
        forbidden = ("insert", "update", "delete", "drop", "alter", "create", "attach", "pragma")
        if any(re.search(rf"\b{keyword}\b", cleaned) for keyword in forbidden):
            raise ValueError("Potentially unsafe SQL detected")


def _clean_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()


def _bar_chart(rows: Sequence[Sequence[object]], suffix: str = "") -> str:
    if not rows:
        return ""

    values: list[float] = []
    labels: list[str] = []
    for row in rows:
        labels.append(str(row[0]))
        values.append(float(row[1]))

    max_value = max(values) or 1.0
    lines: list[str] = []
    for label, value in zip(labels, values, strict=False):
        bar_width = int((value / max_value) * 24)
        bar = "#" * max(1, bar_width)
        lines.append(f"{label:>16} | {bar} {value:.2f}{suffix}")

    return "\n".join(lines)


def format_table(columns: Iterable[str], rows: Sequence[Sequence[object]]) -> str:
    headers = list(columns)
    if not headers:
        return ""

    body = [["" if value is None else str(value) for value in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in body:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(value))

    header = " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    divider = "-+-".join("-" * widths[i] for i in range(len(headers)))
    row_lines = [" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))) for row in body]
    return "\n".join([header, divider, *row_lines])
