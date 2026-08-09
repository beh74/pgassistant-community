import unittest

from apps.home.query_table_stats import (
    _mermaid_entity_ids,
    aggregate_by_table,
    aggregate_pgss_rows,
    build_query_activity_graph,
    normalize_table_references,
)


class _GraphCursor:
    def __init__(self):
        self.query_number = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql):
        self.query_number += 1

    def fetchall(self):
        if self.query_number == 1:
            return [
                ("public", "orders", "id", 1, True, False),
                ("public", "orders", "customer_id", 2, False, True),
                ("public", "orders", "created_at", 3, False, False),
                ("public", "customers", "id", 1, True, False),
            ]
        return [
            ("public", "orders", "public", "customers", "orders_customer_fk")
        ]


class _GraphConnection:
    def cursor(self):
        return _GraphCursor()


class QueryTableStatsTest(unittest.TestCase):
    def test_mermaid_ids_stay_readable_unless_names_really_collide(self):
        identifiers = _mermaid_entity_ids(
            ["public.categories", "sales.order-items", "sales.order_items"]
        )

        self.assertEqual(identifiers["public.categories"], "public_categories")
        self.assertRegex(identifiers["sales.order-items"], r"^sales_order_items_[0-9a-f]{7}$")
        self.assertRegex(identifiers["sales.order_items"], r"^sales_order_items_[0-9a-f]{7}$")
        self.assertNotEqual(
            identifiers["sales.order-items"], identifiers["sales.order_items"]
        )

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

    def test_builds_activity_graph_with_keys_used_columns_and_total_time_color(self):
        graph, error = build_query_activity_graph(
            _GraphConnection(),
            [{
                "query": (
                    "SELECT o.id FROM public.orders o "
                    "JOIN public.customers c ON c.id = o.customer_id "
                    "WHERE o.created_at >= $1"
                ),
                "queryid": 42,
                "calls": 10,
                "total_exec_time": 500,
            }],
            [{
                "table": "public.orders",
                "calls": 10,
                "total_exec_time": 500,
                "mean_exec_time": 50,
            }],
        )

        self.assertEqual(error, "")
        self.assertIn('text id PK "used: SELECT"', graph)
        self.assertIn('text customer_id FK "used: join"', graph)
        self.assertIn('text created_at "used: filter"', graph)
        self.assertIn("orders_customer_fk", graph)
        self.assertIn("queryTime5", graph)

    def test_graph_color_scale_ignores_unresolved_tables(self):
        graph, error = build_query_activity_graph(
            _GraphConnection(),
            [{
                "query": "SELECT id FROM public.orders",
                "queryid": 42,
                "calls": 10,
                "total_exec_time": 500,
            }],
            [
                {"table": "public.orders", "total_exec_time": 500},
                {"table": "missing.unresolved", "total_exec_time": 100000},
            ],
        )

        self.assertEqual(error, "")
        orders_class = next(
            line for line in graph.splitlines()
            if line.strip().startswith("class public_orders ")
        )
        self.assertTrue(orders_class.endswith("queryTime5"))


if __name__ == "__main__":
    unittest.main()
