"""Regression cases for alternatives to an already-used index."""
import importlib.util
import copy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def load_advisor():
    root = Path(__file__).resolve().parents[1] / "apps" / "home"
    with patch.dict(sys.modules):
        for name in ("apps", "apps.home", "apps.home.database"):
            sys.modules[name] = types.ModuleType(name)
        for name in ("index_advisor_thresholds", "alalyze_advisor_helpers", "analyze_advisor"):
            spec = importlib.util.spec_from_file_location("apps.home." + name, root / (name + ".py"))
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
        return module


advisor = load_advisor()
helpers = advisor.helpers

# Reduced from the Northwind generic plan for query 4608506514630080376.
NORTHWIND_PLAN = {
    "Node Type": "Bitmap Heap Scan", "Parallel Aware": False,
    "Relation Name": "orders", "Schema": "public", "Alias": "orders",
    "Startup Cost": 128.85, "Total Cost": 4463.78, "Plan Rows": 104,
    "Recheck Cond": "((orders.employee_id = $2) AND (orders.order_date >= $3))",
    "Filter": "((orders.customer_id)::text = $1)",
    "Plans": [{
        "Node Type": "Bitmap Index Scan", "Plan Rows": 9253,
        "Index Name": "pga_idx_orders_customer_id_employee_id_order_date",
        "Index Cond": "((orders.employee_id = $2) AND (orders.order_date >= $3))",
    }],
}


class IndexedScanTests(unittest.TestCase):
    def evaluate(self, columns=("a",), **overrides):
        fields = dict(schema="public", table="orders", alias="o",
                      node_type="Index Scan", index_name="old_idx",
                      index_cond="(a > 42)", recheck_cond=None, filter_expr="(b = 7)",
                      actual_rows=10, plan_rows=10, actual_loops=1,
                      startup_cost=0, total_cost=100, rows_removed_by_filter=1000,
                      shared_hit_blocks=0, shared_read_blocks=0, actual_total_time=5)
        extra_index = overrides.pop("extra_index", None)
        fields.update(overrides)
        indexes = [dict(index_name="old_idx", columns=list(columns),
                        indexdef="CREATE INDEX old_idx ON public.orders USING btree (" + ", ".join(columns) + ")")]
        if extra_index:
            indexes.append(extra_index)
        meta = helpers.TableMeta("public", "orders", 100000, 1000, 8192000, indexes)
        with patch.object(helpers, "load_column_stats", return_value=None):
            return advisor.evaluate_scan_candidate(None, helpers.ScanFinding(**fields), meta)

    def test_adds_selective_residual_column_for_all_indexed_paths(self):
        for node_type in ("Index Scan", "Index Only Scan", "Bitmap Heap Scan"):
            with self.subTest(node_type=node_type):
                conditions = dict(index_cond=None, recheck_cond="(a > 42)") if node_type == "Bitmap Heap Scan" else {}
                rec = self.evaluate(node_type=node_type, **conditions)
                self.assertEqual(rec.candidate_columns, ["b", "a"])
                self.assertEqual(rec.confidence, "review")
                self.assertIn('("b", "a")', rec.create_index_sql)
                self.assertEqual(rec.used_index_name, "old_idx")

    def test_reorders_existing_range_then_equality_keys(self):
        rec = self.evaluate(columns=("a", "b"))
        self.assertIsNotNone(rec.create_index_sql)
        self.assertIn("moves a residual equality", rec.reason)

    def test_counts_repeated_scans(self):
        rec = self.evaluate(actual_rows=1, rows_removed_by_filter=9,
                            actual_total_time=0.02, actual_loops=1000)
        self.assertIsNotNone(rec.create_index_sql)
        self.assertIn("9000 tuples", rec.reason)

    def test_sequential_scan_uses_cumulative_time(self):
        rec = self.evaluate(node_type="Seq Scan", index_name=None, index_cond=None,
                            actual_total_time=0.2, actual_loops=50000)
        self.assertIsNotNone(rec.create_index_sql)
        self.assertIn("10000 ms cumulative", rec.reason)

    def test_fast_single_sequential_scan_is_still_skipped(self):
        rec = self.evaluate(node_type="Seq Scan", index_name=None, index_cond=None,
                            actual_total_time=0.2, actual_loops=1)
        self.assertIsNone(rec.create_index_sql)

    def test_sequential_scan_cumulative_time_boundary(self):
        for loops, recommended in ((4, False), (5, True)):
            with self.subTest(loops=loops):
                rec = self.evaluate(node_type="Seq Scan", index_name=None, index_cond=None,
                                    actual_total_time=0.2, actual_loops=loops)
                self.assertEqual(rec.create_index_sql is not None, recommended)

    def test_new_simple_predicates_produce_scan_candidates(self):
        for expression in ("b IS NULL", "b IS NOT NULL", "b IN (1, 2)",
                           "b = ANY ('{1,2}'::integer[])", '"b" = 7'):
            with self.subTest(expression=expression):
                rec = self.evaluate(node_type="Seq Scan", index_name=None,
                                    index_cond=None, filter_expr=expression)
                self.assertEqual(rec.candidate_columns, ["b"])
                self.assertIsNotNone(rec.create_index_sql)

    def test_partial_filter_candidate_requires_review(self):
        rec = self.evaluate(node_type="Seq Scan", index_name=None, index_cond=None,
                            filter_expr="b = 7 AND lower(name) = 'x'")
        self.assertEqual(rec.confidence, "review")
        self.assertEqual(rec.candidate_columns, ["b"])
        self.assertIn("whole filter", rec.reason)
        self.assertIsNotNone(rec.create_index_sql)

    def test_or_filter_does_not_produce_a_scan_candidate(self):
        rec = self.evaluate(node_type="Seq Scan", index_name=None, index_cond=None,
                            filter_expr="b = 7 OR c = 8")
        self.assertIsNone(rec.create_index_sql)

    def test_does_not_recommend_without_evidence_of_gain(self):
        for fields in (dict(filter_expr=None), dict(rows_removed_by_filter=0),
                       dict(actual_total_time=0.01), dict(actual_loops=0),
                       dict(filter_expr="(lower(b) = 'x')"),
                       dict(actual_rows=900, rows_removed_by_filter=100)):
            with self.subTest(fields=fields):
                self.assertIsNone(self.evaluate(**fields).create_index_sql)

    def test_does_not_duplicate_existing_candidate(self):
        rec = self.evaluate(extra_index=dict(index_name="better_idx", columns=["b", "a"]))
        self.assertIsNone(rec.create_index_sql)
        self.assertEqual(rec.existing_index_match, "better_idx")

    def test_does_not_reorder_only_equality_keys(self):
        rec = self.evaluate(columns=("b", "a"), index_cond="(a = 42)")
        self.assertIsNone(rec.create_index_sql)

    def evaluate_northwind(self, plan):
        scans = []
        helpers.walk_plan_collect_findings(plan, scans, [])
        index_name = NORTHWIND_PLAN["Plans"][0]["Index Name"]
        meta = helpers.TableMeta("public", "orders", 250000, 5000, 40960000, [
            dict(index_name=index_name, columns=["employee_id", "order_date"],
                 indexdef=f"CREATE INDEX {index_name} ON public.orders USING btree (employee_id, order_date)")
        ])
        with patch.object(helpers, "load_column_stats", return_value=None):
            return advisor.evaluate_scan_candidate(None, scans[0], meta)

    def test_northwind_generic_plan_proposes_alternative_from_estimates(self):
        rec = self.evaluate_northwind(NORTHWIND_PLAN)
        self.assertEqual(rec.confidence, "review")
        self.assertEqual(set(rec.candidate_columns), {"customer_id", "employee_id", "order_date"})
        self.assertEqual(rec.candidate_columns[-1], "order_date")
        self.assertEqual(rec.used_index_name, NORTHWIND_PLAN["Plans"][0]["Index Name"])
        self.assertIn("9253 rows", rec.reason)
        self.assertIn("104 rows", rec.reason)
        self.assertIn("estimates only", rec.reason)
        self.assertIsNone(rec.row_estimation_reason)
        self.assertIsNotNone(rec.create_index_sql)

    def test_generic_estimates_do_not_override_actual_or_unexecuted_scans(self):
        for loops in (0, 1):
            plan = copy.deepcopy(NORTHWIND_PLAN)
            plan.update({"Actual Loops": loops, "Actual Rows": 104,
                         "Actual Total Time": 5, "Rows Removed by Filter": 0})
            self.assertIsNone(self.evaluate_northwind(plan).create_index_sql)

    def test_generic_filter_threshold_includes_half_the_rows(self):
        for kept, recommended in ((104, True), (156, True), (157, False)):
            with self.subTest(kept=kept):
                plan = copy.deepcopy(NORTHWIND_PLAN)
                plan["Plans"][0]["Plan Rows"] = 312
                plan["Plan Rows"] = kept
                rec = self.evaluate_northwind(plan)
                self.assertEqual(rec.create_index_sql is not None, recommended)
                if recommended:
                    self.assertEqual(rec.confidence, "review")

    def test_generic_filter_still_requires_100_discarded_rows(self):
        plan = copy.deepcopy(NORTHWIND_PLAN)
        plan["Plans"][0]["Plan Rows"] = 198
        plan["Plan Rows"] = 99
        self.assertIsNone(self.evaluate_northwind(plan).create_index_sql)

    def test_generic_plan_requires_reliable_selective_estimates(self):
        for change in ({"Plan Rows": 8000}, {"Parallel Aware": True},
                       {"Plans": []}, {"Plans": [{"Node Type": "BitmapAnd", "Plan Rows": 9253}]}):
            plan = copy.deepcopy(NORTHWIND_PLAN)
            plan.update(change)
            with self.subTest(change=change):
                rec = self.evaluate_northwind(plan)
                self.assertIsNone(rec.create_index_sql)
                self.assertIsNone(rec.row_estimation_reason)

    def test_alternative_sql_avoids_existing_name_collision(self):
        rec = self.evaluate(extra_index=dict(index_name="pga_idx_orders_b_a", columns=["a"]))
        self.assertIn('"pga_idx_orders_b_a_2"', rec.create_index_sql)

    def test_collision_suffix_survives_postgresql_identifier_truncation(self):
        table = "é" * 40
        base = ("pga_idx_" + table + "_a").encode()[:63].decode(errors="ignore")
        sql = helpers.build_create_index_sql("public", table, ["a"], [dict(index_name=base)])
        name = sql.split('"')[1]
        self.assertTrue(name.endswith("_2"))
        self.assertLessEqual(len(name.encode()), 63)


if __name__ == "__main__":
    unittest.main()
