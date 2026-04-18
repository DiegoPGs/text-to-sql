import sqlite3
import sys

from .agent import TextToSQLAgent, build_synthetic_warehouse, format_table


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python -m text_to_sql \"<question>\"")
        return 1

    question = " ".join(sys.argv[1:])
    connection = sqlite3.connect(":memory:")
    build_synthetic_warehouse(connection)

    agent = TextToSQLAgent(connection)
    result = agent.answer(question)

    print("SQL:")
    print(result.sql)
    print()
    print("Result:")
    print(format_table(result.columns, result.rows))
    if result.chart:
        print()
        print("Chart:")
        print(result.chart)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
