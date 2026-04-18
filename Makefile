.DEFAULT_GOAL := help
.PHONY: help setup up down seed mcp ask eval lint test scan sbom

# ── Colours ───────────────────────────────────────────────────────────────────
BOLD  := \033[1m
RESET := \033[0m

help:           ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BOLD)%-12s$(RESET) %s\n", $$1, $$2}'

# ── Dev environment ───────────────────────────────────────────────────────────
setup:          ## Install deps (uv sync) and activate pre-commit hooks
	uv sync --frozen
	uv run pre-commit install --install-hooks

# ── Database ──────────────────────────────────────────────────────────────────
up:             ## Start Postgres + pgvector (docker compose up -d)
	docker compose up -d
	@echo "Waiting for Postgres to be ready..."
	@docker compose exec postgres sh -c \
		'until pg_isready -U voyage -d voyage; do sleep 1; done'
	@echo "Postgres is ready on localhost:5432"

down:           ## Stop and remove containers (data volume preserved)
	docker compose down

# ── Data ──────────────────────────────────────────────────────────────────────
seed:           ## Generate and load synthetic warehouse data
	uv run python scripts/seed.py

# ── Agent ─────────────────────────────────────────────────────────────────────
mcp:            ## Run the MCP warehouse server (stdio mode)
	uv run python -m server.warehouse_mcp

ask:            ## Run a one-shot query  usage: make ask q="your question"
ifndef q
	$(error Usage: make ask q="your question here")
endif
	uv run voyage ask "$(q)"

# ── Quality ───────────────────────────────────────────────────────────────────
lint:           ## Run ruff (format + lint) and mypy --strict
	uv run ruff format --check .
	uv run ruff check .
	uv run mypy --strict voyage/ server/ scripts/ 2>/dev/null || uv run mypy --strict voyage/

test:           ## Run pytest (unit tests only; skips integration + llm markers)
	uv run pytest -q -m "not integration and not llm"

eval:           ## Run the golden eval suite and write evals/latest.md
	uv run python evals/harness.py

# ── Security and supply chain ─────────────────────────────────────────────────
scan:           ## Scan container image with Trivy (requires trivy CLI)
	trivy image pgvector/pgvector:pg16

sbom:           ## Generate CycloneDX SBOM (writes voyage-sbom.json)
	uv run cyclonedx-py environment --of JSON -o voyage-sbom.json
	@echo "SBOM written to voyage-sbom.json"
