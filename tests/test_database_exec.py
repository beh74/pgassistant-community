import unittest
from unittest.mock import Mock

from apps.home.database import format_sql_execution_output


class DatabaseExecOutputTests(unittest.TestCase):
    def test_format_sql_execution_output_with_rows(self):
        cursor = Mock()
        cursor.description = [("pg_reload_conf",)]
        cursor.fetchall.return_value = [(True,)]
        cursor.rowcount = 1

        output = format_sql_execution_output(cursor, [])
        self.assertIn("pg_reload_conf", output)
        self.assertIn("True", output)

    def test_format_sql_execution_output_with_notices(self):
        cursor = Mock()
        cursor.description = None
        cursor.rowcount = -1

        output = format_sql_execution_output(
            cursor,
            ["INFO:  vacuuming \"public.orders\"\n", "INFO:  finished vacuuming\n"],
        )
        self.assertIn('vacuuming "public.orders"', output)
        self.assertIn("finished vacuuming", output)

    def test_format_sql_execution_output_empty(self):
        cursor = Mock()
        cursor.description = None
        cursor.rowcount = -1

        self.assertEqual(format_sql_execution_output(cursor, []), "")


if __name__ == "__main__":
    unittest.main()
