# text-to-sql

A production-grade text-to-SQL portfolio project for **vacation rental operations**.

It answers natural-language business questions over a synthetic warehouse and returns:

- generated SQL
- tabular results
- an ASCII chart when the question is trend/ranking-oriented

## Quick start

```bash
python -m text_to_sql "Show the monthly bookings trend"
```

## Supported question patterns

- Occupancy by city
- Top/highest revenue listings
- Monthly bookings trend
- Average rating by city

## Engineering guardrails included

- Read-only SQL enforcement (`SELECT` only)
- Deterministic synthetic warehouse setup for repeatable behavior
- Focused unit tests for SQL + result + chart behavior
