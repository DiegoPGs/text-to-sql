"""Shared pytest fixtures and environment setup.

Sets required environment variables before any module is imported so that
voyage/config.py does not raise RuntimeError during unit tests.
"""

from __future__ import annotations

import os

# Must be set before voyage.config is imported by any test module.
os.environ.setdefault("ANTHROPIC_API_KEY", "test-sk-unit-tests")
os.environ.setdefault("DATABASE_URL", "postgresql://voyage:voyage@localhost:5432/voyage")
os.environ.setdefault(
    "RO_DATABASE_URL", "postgresql://bi_copilot_ro:voyage_ro@localhost:5432/voyage"
)
