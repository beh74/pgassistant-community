import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "apps" / "home" / "fillfactor_advisor.py"
SPEC = importlib.util.spec_from_file_location("fillfactor_advisor_test_module", MODULE_PATH)
fillfactor_advisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fillfactor_advisor)


class _Cursor:
    def __init__(self, columns, rows):
        self.description = [(column,) for column in columns]
        self.rows = rows

    def execute(self, _sql, _params):
        return None

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        return None


class _Connection:
    def __init__(self, responses):
        self.responses = list(responses)
        self.rollback_count = 0

    def cursor(self):
        columns, rows = self.responses.pop(0)
        return _Cursor(columns, rows)

    def rollback(self):
        self.rollback_count += 1


class FillfactorAdvisorTests(unittest.TestCase):
    def test_extracts_columns_from_parsed_update_target(self):
        columns = fillfactor_advisor._updated_columns_for_table(
            'UPDATE public.orders SET "status" = $1, updated_at = now() WHERE id = $2',
            schema_name="public",
            table_name="orders",
        )
        self.assertEqual(columns, {"status", "updated_at"})

    def test_extracts_grouped_assignments_and_update_from(self):
        columns = fillfactor_advisor._updated_columns_for_table(
            """
            UPDATE public.orders AS o
            SET (status, updated_at) = ($1, now())
            FROM public.customers AS c
            WHERE o.customer_id = c.id
            """,
            schema_name="public",
            table_name="orders",
        )
        self.assertEqual(columns, {"status", "updated_at"})

    def test_ignores_update_targeting_another_table_or_schema(self):
        wrong_table = fillfactor_advisor._updated_columns_for_table(
            "UPDATE public.order_items SET status = $1",
            schema_name="public",
            table_name="orders",
        )
        wrong_schema = fillfactor_advisor._updated_columns_for_table(
            "UPDATE archive.orders SET status = $1",
            schema_name="public",
            table_name="orders",
        )
        self.assertEqual(wrong_table, set())
        self.assertEqual(wrong_schema, set())

    def test_preserves_quoted_identifier_case(self):
        columns = fillfactor_advisor._updated_columns_for_table(
            'UPDATE public."Orders" SET "Status" = $1',
            schema_name="public",
            table_name="Orders",
        )
        self.assertEqual(columns, {"Status"})

    def test_reports_indexed_updates_vacuum_pressure_and_long_transactions(self):
        connection = _Connection([
            (("index_count", "indexed_columns"), [(3, ["id", "status"])]),
            (("query",), [("UPDATE orders SET status = $1 WHERE id = $2",)]),
            (("n_live_tup", "n_dead_tup", "last_autovacuum", "dead_pct"), [(100_000, 25_000, None, 20.0)]),
            (("transaction_count", "oldest_seconds"), [(2, 900)]),
        ])

        result = fillfactor_advisor.run_fillfactor_checks(connection, "public", "orders")

        self.assertTrue(result["success"])
        self.assertEqual(result["checks"]["indexed_updates"]["status"], "warning")
        self.assertIn("status", result["checks"]["indexed_updates"]["summary"])
        self.assertEqual(result["checks"]["autovacuum"]["status"], "warning")
        self.assertEqual(result["checks"]["long_transactions"]["status"], "warning")


if __name__ == "__main__":
    unittest.main()
