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

    spec = importlib.util.spec_from_file_location(
        module_name,
        repo_root / "apps" / "home" / "alalyze_advisor_helpers.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


helpers = _load_helpers_module()


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
