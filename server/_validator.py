"""SQL safety validator — the contract that keeps the system safe.

Every SQL query MUST pass through validate_and_sanitize before execution.
No exceptions, not even in tests (CLAUDE.md §Safety rules).

Validation pipeline:
  1. Parse via sqlglot (syntax check + AST construction)
  2. Assert the top-level statement is SELECT
  3. Assert no forbidden sub-statement types (DML/DDL inside CTEs etc.)
  4. Assert no pg_* function calls
  5. Assert no explicitly forbidden functions (dblink, lo_*, COPY, ANALYZE …)
  6. Belt-and-braces regex pass on the lowered SQL text
  7. Inject LIMIT if absent at the top level
  8. Return the cleaned, LIMIT-injected SQL
"""

from __future__ import annotations

import re

import sqlglot
import sqlglot.expressions as exp

from ._models import ValidationResult

# ---------------------------------------------------------------------------
# Deny-lists
# ---------------------------------------------------------------------------

_FORBIDDEN_FUNCS: frozenset[str] = frozenset(
    {
        # File system access
        "pg_read_file",
        "pg_ls_dir",
        "pg_ls_waldir",
        "pg_ls_tmpdir",
        "pg_read_binary_file",
        "pg_stat_file",
        # Remote connections
        "dblink",
        "dblink_exec",
        "dblink_connect",
        "dblink_disconnect",
        # Large-object API
        "lo_import",
        "lo_export",
        "lo_creat",
        "lo_create",
        "lo_open",
        "lo_write",
        "lo_read",
        "lo_lseek",
        "lo_close",
        "lo_unlink",
        # Misc dangerous builtins
        "pg_sleep",
        "pg_cancel_backend",
        "pg_terminate_backend",
    }
)

# DDL / DML / control-flow statement types rejected at the parse-tree level.
# This catches attempts to smuggle DML inside CTEs, e.g.
#   WITH bad AS (DELETE FROM …) SELECT 1
_FORBIDDEN_STMT_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Command,
    exp.Transaction,
    exp.Merge,
    exp.TruncateTable,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_and_sanitize(sql: str, row_limit: int = 1000) -> ValidationResult:
    """Parse, validate, and sanitize *sql*.

    Returns a :class:`ValidationResult`.  When ``ok`` is True, ``result.sql``
    holds the cleaned, LIMIT-injected SQL that is safe to execute.

    Args:
        sql:       Raw SQL string from the LLM or user.
        row_limit: Maximum rows to inject as LIMIT when none is present.
    """
    sql = sql.strip().rstrip(";")

    if not sql:
        return ValidationResult(ok=False, errors=["Empty SQL statement"])

    # -- 1. Parse (syntax + AST) -------------------------------------------
    try:
        statements = sqlglot.parse(
            sql,
            dialect="postgres",
            error_level=sqlglot.ErrorLevel.RAISE,
        )
    except sqlglot.errors.ParseError as exc:
        return ValidationResult(ok=False, errors=[f"SQL parse error: {exc}"])

    if not statements or statements[0] is None:
        return ValidationResult(ok=False, errors=["Could not parse SQL statement"])

    if len(statements) > 1:
        return ValidationResult(
            ok=False,
            errors=["Only a single SQL statement is allowed; received multiple"],
        )

    stmt = statements[0]

    # -- 2. Must be SELECT at the top level --------------------------------
    if not isinstance(stmt, exp.Select):
        stmt_type = type(stmt).__name__
        return ValidationResult(
            ok=False,
            errors=[f"Only SELECT statements are allowed; got {stmt_type}"],
        )

    # -- 3. No forbidden sub-statement types (DML/DDL anywhere in the AST) --
    for forbidden_type in _FORBIDDEN_STMT_TYPES:
        node = stmt.find(forbidden_type)
        if node is not None:
            return ValidationResult(
                ok=False,
                errors=[f"Forbidden operation in query: {forbidden_type.__name__}"],
            )

    # -- 4 & 5. Inspect every function call in the AST ----------------------
    for ast_node in stmt.walk():
        fname = _func_name(ast_node)
        if fname is None:
            continue
        if fname.startswith("pg_"):
            return ValidationResult(
                ok=False,
                errors=[f"pg_* functions are not allowed: {fname}"],
            )
        if fname in _FORBIDDEN_FUNCS:
            return ValidationResult(
                ok=False,
                errors=[f"Forbidden function: {fname}"],
            )

    # -- 6. Belt-and-braces regex pass -------------------------------------
    sql_lower = sql.lower()
    if re.search(r"\bcopy\b", sql_lower):
        return ValidationResult(ok=False, errors=["COPY is not allowed"])
    if re.search(r"\banalyze\b", sql_lower):
        return ValidationResult(ok=False, errors=["ANALYZE is not allowed"])

    # -- 7. Inject LIMIT if absent at the top level ------------------------
    has_limit = stmt.args.get("limit") is not None
    clean_sql = (
        stmt.limit(row_limit).sql(dialect="postgres")
        if not has_limit
        else stmt.sql(dialect="postgres")
    )
    return ValidationResult(ok=True, errors=[], sql=clean_sql)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _func_name(node: exp.Expr | None) -> str | None:
    """Return the lowercased function name if *node* is a function call."""
    if node is None:
        return None
    if isinstance(node, exp.Anonymous):
        return str(node.name).lower()
    if isinstance(node, exp.Func):
        # Known sqlglot function classes expose sql_name()
        try:
            return node.sql_name().lower()
        except Exception:  # noqa: BLE001
            pass
    return None
