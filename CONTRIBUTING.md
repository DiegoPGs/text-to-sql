# Contributing to Voyage BI Copilot

Thanks for your interest. This is a solo portfolio project, but the repository is
structured as if a team were joining tomorrow. These guidelines exist so that a
future collaborator (or a reviewer) can get productive immediately.

## Branching model

- **Trunk-based**. `main` is always deployable and is protected — no direct
  pushes, no force pushes, linear history enforced.
- Branch names follow the type prefix convention:
  - `feat/<short-description>` — new functionality
  - `fix/<short-description>` — bug fixes
  - `chore/<short-description>` — tooling, deps, config
  - `docs/<short-description>` — documentation only
- Keep branches short-lived. Merge or close within a few days.

## Commit style

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) on every
commit. The commit message is the source of truth for changelog generation.

```
<type>(<optional scope>): <short summary>

<optional body — what and why, not how>

<optional footer — breaking changes, closes #issue>
```

Types: `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `ci`, `perf`.

Examples:

```
feat(agent): add self-correction loop with retry counter
fix(validator): reject queries containing pg_* functions
chore(deps): bump anthropic to 0.21.0
docs: add ARCHITECTURE.md walkthrough
```

The pre-commit Conventional Commits linter will reject a commit that does not
conform. Fix the message, not the hook.

## Local setup

```bash
# 1. Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 2. Sync dependencies and activate the virtual environment
make setup          # runs: uv sync, pre-commit install

# 3. Bring up the database
make up             # docker compose up -d

# 4. Seed synthetic data
make seed

# 5. Run the full local check suite
make lint           # ruff check + ruff format --check + mypy --strict
make test           # pytest -q
```

All five commands must pass on a clean checkout before opening a PR.

## Running a one-shot query

```bash
make ask q="Top 5 markets by revenue last month"
```

## Running the eval suite

```bash
make eval           # writes evals/latest.md
```

Include the eval delta in your PR description if your change touches the agent,
the validator, the retrieval index, or the golden dataset.

## Pull request expectations

1. Open a PR against `main`. Self-review is fine for solo development.
2. Fill in the PR template — do not delete the sections.
3. All CI checks must be green before merge:
   - `ci` — lint, type check, tests
   - `security` — Gitleaks, Snyk, Bandit, pip-audit
4. Squash-merge to keep `main` linear. The squash commit message must be a
   valid Conventional Commit.
5. Delete the branch after merge.

## Code style

- `ruff format` for formatting (enforced by pre-commit and CI).
- `ruff check` for linting (enforced by pre-commit and CI).
- `mypy --strict` for type checking (enforced by pre-commit and CI).
- No `print()` in library code — use the project logger.
- Every LLM call returns a named Pydantic model. No free-form string parsing.
- See `CLAUDE.md §Coding conventions` for the full list.

## Reporting bugs

Use the bug report issue template. For security vulnerabilities, see
[SECURITY.md](SECURITY.md) instead — do not open a public issue.
