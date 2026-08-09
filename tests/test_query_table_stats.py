import unittest

from apps.home.query_table_stats import (
    aggregate_by_table,
    aggregate_pgss_rows,
    normalize_table_references,
)


class QueryTableStatsTest(unittest.TestCase):
    def test_aggregates_statement_types_and_workload_by_table(self):
        rows = [
            {
                "tables": ["public.orders", "public.customers"],
                "operation_type": "select",
                "calls": 10,
                "rows": 30,
                "total_exec_time": 50,
            },
            {
                "tables": ["public.orders"],
                "operation_type": "update",
                "calls": 2,
                "rows": 2,
                "total_exec_time": 10,
            },
        ]

        stats = aggregate_by_table(rows)

        self.assertEqual(stats[0]["table"], "public.orders")
        self.assertEqual(stats[0]["query_count"], 2)
        self.assertEqual(stats[0]["select_count"], 1)
        self.assertEqual(stats[0]["update_count"], 1)
        self.assertEqual(stats[0]["calls"], 12)
        self.assertEqual(stats[0]["rows"], 32)
        self.assertEqual(stats[0]["total_exec_time"], 60)
        self.assertEqual(stats[0]["mean_exec_time"], 5)
        self.assertEqual(
            stats[0]["operations"],
            [
                {
                    "operation": "select",
                    "query_count": 1,
                    "calls": 10,
                    "rows": 30,
                    "total_exec_time": 50.0,
                    "mean_exec_time": 5.0,
                },
                {
                    "operation": "update",
                    "query_count": 1,
                    "calls": 2,
                    "rows": 2,
                    "total_exec_time": 10.0,
                    "mean_exec_time": 5.0,
                },
            ],
        )

    def test_counts_each_table_only_once_per_statement(self):
        stats = aggregate_by_table(
            [{"tables": ["orders", "orders"], "operation_type": "select", "calls": 3}]
        )

        self.assertEqual(stats[0]["query_count"], 1)
        self.assertEqual(stats[0]["calls"], 3)

    def test_excludes_postgresql_internal_tables_before_limiting(self):
        rows = [
            {
                "query": "SELECT * FROM pg_catalog.pg_class",
                "calls": 1000,
                "rows": 1000,
                "total_exec_time": 100,
            },
            {
                "query": "SELECT * FROM public.orders",
                "calls": 20,
                "rows": 20,
                "total_exec_time": 10,
            },
            {
                "query": "SELECT * FROM customers",
                "calls": 10,
                "rows": 10,
                "total_exec_time": 5,
            },
        ]

        stats = aggregate_pgss_rows(rows, limit=1, exclude_internal=True)

        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]["table"], "public.orders")
        self.assertNotIn("pg_catalog.pg_class", [item["table"] for item in stats])

    def test_resolves_unique_unqualified_names_and_filters_internal_tables(self):
        tables = normalize_table_references(
            ["orders", "public.orders", "pg_catalog.pg_class", "pg_stat_activity"],
            relation_names=["public.orders", "sales.customers"],
        )

        self.assertEqual(tables, ["public.orders"])

    def test_keeps_ambiguous_unqualified_names_unmodified(self):
        tables = normalize_table_references(
            ["orders"],
            relation_names=["public.orders", "archive.orders"],
        )

        self.assertEqual(tables, ["orders"])


if __name__ == "__main__":
    unittest.main()
