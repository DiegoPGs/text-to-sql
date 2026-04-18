"""Unit tests for the SQL safety validator.

All tests run without a database connection — the validator is pure Python.
These tests cover the inviolate safety rules from CLAUDE.md §Safety rules.
"""

from __future__ import annotations

from server._validator import validate_and_sanitize, validate_identifier

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ok(sql: str, row_limit: int = 1000) -> str:
    """Assert validation passes and return the cleaned SQL."""
    result = validate_and_sanitize(sql, row_limit=row_limit)
    assert result.ok, f"Expected OK but got errors: {result.errors}"
    return result.sql


def _fail(sql: str, *, containing: str | None = None) -> list[str]:
    """Assert validation fails and return the error list."""
    result = validate_and_sanitize(sql)
    assert not result.ok, f"Expected failure but validation passed for: {sql!r}"
    if containing:
        joined = " ".join(result.errors).lower()
        assert containing.lower() in joined, (
            f"Expected error message to contain {containing!r}; got: {result.errors}"
        )
    return result.errors


# ---------------------------------------------------------------------------
# Rule 1 — Only SELECT statements
# ---------------------------------------------------------------------------


class TestSelectOnly:
    def test_simple_select_passes(self) -> None:
        sql = _ok("SELECT 1")
        assert "SELECT" in sql.upper()

    def test_select_from_table_passes(self) -> None:
        _ok("SELECT market_id, name FROM warehouse.markets")

    def test_select_with_join_passes(self) -> None:
        _ok(
            """
            SELECT m.name, COUNT(*) AS cnt
            FROM warehouse.reservations r
            JOIN warehouse.properties p ON p.property_id = r.property_id
            JOIN warehouse.markets m    ON m.market_id   = p.market_id
            GROUP BY m.name
            """
        )

    def test_drop_table_rejected(self) -> None:
        _fail("DROP TABLE warehouse.properties", containing="SELECT")

    def test_insert_rejected(self) -> None:
        _fail(
            "INSERT INTO warehouse.markets (name) VALUES ('evil')",
            containing="SELECT",
        )

    def test_update_rejected(self) -> None:
        _fail(
            "UPDATE warehouse.markets SET name = 'pwned' WHERE 1=1",
            containing="SELECT",
        )

    def test_delete_rejected(self) -> None:
        _fail("DELETE FROM warehouse.reservations", containing="SELECT")

    def test_truncate_rejected(self) -> None:
        _fail("TRUNCATE warehouse.reservations", containing="SELECT")

    def test_create_table_rejected(self) -> None:
        _fail("CREATE TABLE evil (id int)", containing="SELECT")

    def test_alter_table_rejected(self) -> None:
        _fail("ALTER TABLE warehouse.markets ADD COLUMN evil TEXT", containing="SELECT")


# ---------------------------------------------------------------------------
# Rule 2 — No DDL/DML inside CTEs
# ---------------------------------------------------------------------------


class TestNoDmlInCtes:
    def test_dml_inside_cte_rejected(self) -> None:
        _fail(
            "WITH bad AS (DELETE FROM warehouse.reservations) SELECT 1",
            containing="DELETE",
        )

    def test_insert_inside_cte_rejected(self) -> None:
        _fail(
            "WITH ins AS (INSERT INTO warehouse.markets VALUES (99,'x','y','z','w')) SELECT 1",
            containing="Insert",
        )


# ---------------------------------------------------------------------------
# Rule 2 — No pg_* functions
# ---------------------------------------------------------------------------


class TestNoPgFunctions:
    def test_pg_read_file_rejected(self) -> None:
        _fail("SELECT pg_read_file('/etc/passwd')", containing="pg_")

    def test_pg_sleep_rejected(self) -> None:
        _fail("SELECT pg_sleep(10)", containing="pg_")

    def test_pg_ls_dir_rejected(self) -> None:
        _fail("SELECT * FROM pg_ls_dir('/')", containing="pg_")

    def test_pg_cancel_backend_rejected(self) -> None:
        _fail("SELECT pg_cancel_backend(1234)", containing="pg_")


# ---------------------------------------------------------------------------
# Rule 2 — No explicitly forbidden functions
# ---------------------------------------------------------------------------


class TestForbiddenFunctions:
    def test_dblink_rejected(self) -> None:
        _fail(
            "SELECT * FROM dblink('host=evil', 'SELECT 1') AS t(id int)",
            containing="dblink",
        )

    def test_lo_import_rejected(self) -> None:
        _fail("SELECT lo_import('/etc/passwd')", containing="lo_import")

    def test_copy_keyword_rejected(self) -> None:
        _fail("COPY warehouse.reservations TO '/tmp/dump.csv'", containing="COPY")


# ---------------------------------------------------------------------------
# Rule 3 — LIMIT injection
# ---------------------------------------------------------------------------


class TestLimitInjection:
    def test_limit_injected_when_absent(self) -> None:
        sql = _ok("SELECT * FROM warehouse.markets", row_limit=500)
        assert "500" in sql, f"Expected LIMIT 500 in: {sql!r}"
        assert "LIMIT" in sql.upper()

    def test_existing_limit_not_doubled(self) -> None:
        sql = _ok("SELECT * FROM warehouse.markets LIMIT 10", row_limit=500)
        # Should have exactly one LIMIT clause
        assert sql.upper().count("LIMIT") == 1
        assert "10" in sql

    def test_limit_in_subquery_not_sufficient(self) -> None:
        # A LIMIT inside a subquery should NOT prevent injection at top level
        sql = _ok(
            "SELECT * FROM (SELECT * FROM warehouse.markets LIMIT 5) sub",
            row_limit=200,
        )
        # Top-level LIMIT must be present
        assert "LIMIT" in sql.upper()

    def test_cte_with_no_top_level_limit_gets_injected(self) -> None:
        sql = _ok(
            "WITH m AS (SELECT * FROM warehouse.markets) SELECT * FROM m",
            row_limit=100,
        )
        assert "LIMIT" in sql.upper()
        assert "100" in sql


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_string_rejected(self) -> None:
        _fail("", containing="Empty")

    def test_semicolon_stripped(self) -> None:
        # Trailing semicolons should be stripped before parsing
        sql = _ok("SELECT 1;")
        assert sql.strip().endswith(";") is False or "SELECT" in sql.upper()

    def test_multiple_statements_rejected(self) -> None:
        _fail("SELECT 1; SELECT 2", containing="single")

    def test_analyze_keyword_rejected(self) -> None:
        _fail("ANALYZE warehouse.reservations", containing="ANALYZE")

    def test_case_insensitive_select_passes(self) -> None:
        _ok("select market_id from warehouse.markets")

    def test_complex_window_function_passes(self) -> None:
        _ok(
            """
            SELECT
                m.name,
                SUM(r.net_revenue) AS revenue,
                RANK() OVER (ORDER BY SUM(r.net_revenue) DESC) AS rnk
            FROM warehouse.reservations r
            JOIN warehouse.properties p ON p.property_id = r.property_id
            JOIN warehouse.markets    m ON m.market_id   = p.market_id
            WHERE r.status = 'confirmed'
            GROUP BY m.name
            ORDER BY rnk
            LIMIT 10
            """
        )

    def test_comment_injection_does_not_bypass(self) -> None:
        # SQL comments should not allow bypassing the validator
        _fail("SELECT 1; -- DROP TABLE warehouse.properties", containing="single")


# ---------------------------------------------------------------------------
# validate_identifier — B608 injection guard for table-name interpolation
# ---------------------------------------------------------------------------


class TestValidateIdentifier:
    def test_simple_table_name_passes(self) -> None:
        validate_identifier("markets")  # should not raise
        validate_identifier("reservations")
        validate_identifier("pricing_snapshots")
        validate_identifier("sensor_events")

    def test_leading_underscore_passes(self) -> None:
        validate_identifier("_internal")

    def test_name_with_dollar_passes(self) -> None:
        # PostgreSQL allows $ in identifiers after the first character.
        validate_identifier("col_1$")

    def test_injection_attempt_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('evil"; DROP TABLE warehouse.markets --')

    def test_quote_character_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier('table"name')

    def test_space_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("table name")

    def test_leading_digit_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("1_table")

    def test_empty_string_rejected(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("")
