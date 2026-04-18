import sqlite3
import unittest

from text_to_sql import TextToSQLAgent, UnsupportedQuestionError, build_synthetic_warehouse


class TextToSQLAgentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        build_synthetic_warehouse(self.connection)
        self.agent = TextToSQLAgent(self.connection)

    def tearDown(self) -> None:
        self.connection.close()

    def test_monthly_bookings_generates_sql_rows_and_chart(self) -> None:
        result = self.agent.answer("Show the monthly bookings trend")

        self.assertIn("SELECT SUBSTR(check_in, 1, 7) AS month", result.sql)
        self.assertEqual(result.columns, ("month", "booking_count"))
        self.assertGreater(len(result.rows), 0)
        self.assertIsNotNone(result.chart)
        self.assertIn("2026-01", result.chart)

    def test_average_rating_returns_rows_without_chart(self) -> None:
        result = self.agent.answer("What is the average rating by city?")

        self.assertEqual(result.columns, ("city", "average_rating"))
        self.assertGreater(len(result.rows), 0)
        self.assertIsNone(result.chart)

    def test_unsupported_question_raises_error(self) -> None:
        with self.assertRaises(UnsupportedQuestionError):
            self.agent.answer("Delete all cancelled bookings")


if __name__ == "__main__":
    unittest.main()
