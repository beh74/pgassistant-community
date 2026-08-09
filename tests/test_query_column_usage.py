import unittest

from apps.home.query_column_usage import build_table_usage, extract_table_column_roles


class QueryColumnUsageTests(unittest.TestCase):
    def test_extracts_qualified_column_roles(self):
        result = extract_table_column_roles(
            """
            SELECT o.id, o.total
            FROM public.orders AS o
            JOIN public.customers AS c ON c.id = o.customer_id
            WHERE o.created_at >= $1
            ORDER BY o.total
            """,
            "public",
            "orders",
            ["id", "total", "customer_id", "created_at"],
        )

        self.assertIn("projection", result["roles"]["id"])
        self.assertIn("join", result["roles"]["customer_id"])
        self.assertIn("filter", result["roles"]["created_at"])
        self.assertIn("group_sort", result["roles"]["total"])

    def test_does_not_assign_ambiguous_unqualified_columns(self):
        result = extract_table_column_roles(
            "SELECT id FROM public.orders o JOIN public.customers c ON o.customer_id = c.id",
            "public",
            "orders",
            ["id", "customer_id"],
        )

        self.assertIn("id", result["ambiguous_columns"])
        self.assertIn("join", result["roles"]["customer_id"])

    def test_aggregates_pgss_calls(self):
        result = build_table_usage(
            [{
                "queryid": 42,
                "query": "SELECT id FROM public.orders WHERE customer_id = $1",
                "calls": 12,
                "total_exec_time": 30,
            }],
            "public",
            "orders",
            ["id", "customer_id"],
        )

        self.assertEqual(result["summary"]["statements"], 1)
        self.assertEqual(result["summary"]["calls"], 12)
        self.assertTrue(all(column["calls"] == 12 for column in result["columns"]))


if __name__ == "__main__":
    unittest.main()
