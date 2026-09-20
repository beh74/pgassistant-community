import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_helpers_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_name = "apps.home.alalyze_advisor_helpers"

    sys.modules.setdefault("apps", types.ModuleType("apps"))
    sys.modules.setdefault("apps.home", types.ModuleType("apps.home"))
    sys.modules.setdefault("apps.home.database", types.ModuleType("apps.home.database"))

    threshold_name = "apps.home.index_advisor_thresholds"
    threshold_spec = importlib.util.spec_from_file_location(
        threshold_name, repo_root / "apps" / "home" / "index_advisor_thresholds.py",
    )
    threshold_module = importlib.util.module_from_spec(threshold_spec)
    sys.modules[threshold_name] = threshold_module
    threshold_spec.loader.exec_module(threshold_module)

    spec = importlib.util.spec_from_file_location(
        module_name,
        repo_root / "apps" / "home" / "alalyze_advisor_helpers.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # Drop shim modules so later tests can import the real apps.home package.
    for name in (
        module_name,
        threshold_name,
        "apps.home.database",
        "apps.home",
        "apps",
    ):
        sys.modules.pop(name, None)

    return module


helpers = _load_helpers_module()


class SimpleFilterParsingTest(unittest.TestCase):
    def parse(self, expression):
        return helpers.parse_simple_filter_predicates(expression, "o", "orders")

    def test_null_checks(self):
        self.assertEqual(self.parse("(o.deleted_at IS NULL) AND (o.code IS NOT NULL)"),
                         ([{"column": "deleted_at", "operator": "IS NULL"},
                           {"column": "code", "operator": "IS NOT NULL"}], True))

    def test_quoted_identifiers_and_literals_preserve_boolean_structure(self):
        self.assertEqual(self.parse('("o"."Customer ID" = \'A AND B (OR)\')'),
                         ([{"column": "Customer ID", "operator": "="}], True))
        self.assertEqual(self.parse('"o"."a""b" = 2')[0][0]["column"], 'a"b')

    def test_in_and_postgresql_any_forms(self):
        for expression, operator in (
            ("o.id IN (1, 2, $1)", "IN"),
            ("o.id = ANY ('{1,2}'::integer[])", "ANY"),
            ("o.id = ANY (ARRAY[1, 2])", "ANY"),
            ("o.id = ANY ($1)", "ANY"),
        ):
            with self.subTest(expression=expression):
                self.assertEqual(self.parse(expression),
                                 ([{"column": "id", "operator": operator}], True))

    def test_retains_only_mandatory_supported_conjuncts(self):
        predicates, complete = self.parse("(o.id = 1) AND ((lower(o.name) = 'x') AND (o.code IS NULL))")
        self.assertFalse(complete)
        self.assertEqual([p["column"] for p in predicates], ["id", "code"])

    def test_or_branches_are_not_mandatory(self):
        self.assertEqual(self.parse("o.id = 1 OR o.code = 2 AND o.active = TRUE"), ([], False))
        predicates, complete = self.parse("o.id = 1 AND (o.code = 2 OR o.code = 3)")
        self.assertFalse(complete)
        self.assertEqual(predicates, [{"column": "id", "operator": "="}])

    def test_rejects_unsupported_operands(self):
        for expression in ("o.id IN (SELECT id FROM other)", "o.id = ANY(o.ids)",
                           "o.id = o.other_id", "o.name ILIKE '%x%'", "o.id NOT IN (1, 2)",
                           "other.id = 2", "o.id = ALL ('{1,2}'::integer[])"):
            with self.subTest(expression=expression):
                self.assertEqual(self.parse(expression), ([], False))

    def test_multi_value_predicates_are_not_ranked_as_single_equalities(self):
        with patch.object(helpers, "load_column_stats", return_value=None):
            columns = helpers.reorder_index_candidate_columns(None, "public", "orders", [
                {"column": "range_key", "operator": ">"},
                {"column": "list_key", "operator": "ANY"},
                {"column": "null_key", "operator": "IS NULL"},
            ])
        self.assertEqual(columns, ["null_key", "list_key", "range_key"])

    def test_sql_escapes_quoted_identifiers(self):
        sql = helpers.build_create_index_sql("public", "orders", ['a"b'])
        self.assertIn('("a""b")', sql)
        self.assertIn('"pga_idx_orders_a""b"', sql)


def _stats(column, n_distinct):
    return helpers.ColumnStats(
        schema="public",
        table="orders",
        column=column,
        null_frac=0.0,
        n_distinct=n_distinct,
        most_common_vals=None,
        most_common_freqs=None,
        histogram_bounds=None,
    )


class ReorderIndexCandidateColumnsTest(unittest.TestCase):
    def test_uses_negative_n_distinct_as_row_fraction(self):
        stats_by_column = {
            "status": _stats("status", 10),
            "country": _stats("country", 15),
            "customer_id": _stats("customer_id", -0.3),
        }

        def fake_load_column_stats(_con, _schema, _table, column):
            return stats_by_column[column]

        with patch.object(helpers, "load_column_stats", fake_load_column_stats):
            self.assertEqual(
                helpers.reorder_index_candidate_columns(
                    con=None,
                    schema="public",
                    table="orders",
                    predicates=[
                        {"column": "status", "operator": "="},
                        {"column": "country", "operator": "="},
                        {"column": "customer_id", "operator": "="},
                    ],
                    table_rows=1000,
                ),
                ["customer_id", "country", "status"],
            )

    def test_keeps_operator_precedence(self):
        stats_by_column = {
            "created_at": _stats("created_at", -0.8),
            "status": _stats("status", 10),
        }

        def fake_load_column_stats(_con, _schema, _table, column):
            return stats_by_column[column]

        with patch.object(helpers, "load_column_stats", fake_load_column_stats):
            self.assertEqual(
                helpers.reorder_index_candidate_columns(
                    con=None,
                    schema="public",
                    table="orders",
                    predicates=[
                        {"column": "created_at", "operator": ">="},
                        {"column": "status", "operator": "="},
                    ],
                    table_rows=1000,
                ),
                ["status", "created_at"],
            )
