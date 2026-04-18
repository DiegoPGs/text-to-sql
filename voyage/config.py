"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Required environment variable {key!r} is not set.")
    return value


def _optional(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# LLM
ANTHROPIC_API_KEY: str = _require("ANTHROPIC_API_KEY")
OPENAI_API_KEY: str = _optional("OPENAI_API_KEY")
MODEL_PRIMARY: str = _optional("MODEL_PRIMARY", "claude-opus-4-6")
MODEL_FALLBACK: str = _optional("MODEL_FALLBACK", "gpt-4o")

# Database
DATABASE_URL: str = _require("DATABASE_URL")
RO_DATABASE_URL: str = _require("RO_DATABASE_URL")

# Safety limits
MAX_COST: int = int(_optional("MAX_COST", "10000"))
ROW_LIMIT: int = int(_optional("ROW_LIMIT", "1000"))
STATEMENT_TIMEOUT_MS: int = int(_optional("STATEMENT_TIMEOUT_MS", "10000"))
