import unittest

from apps.home.table_autovacuum_advisor import (
    _normalize_issue_types,
    _primary_issue_type,
    assess_cluster_load,
    build_batch_actions,
    build_analyze_sql,
    build_autovacuum_tuning_sql,
    build_criteria_help,
    build_global_autovacuum_recommendations,
    build_restore_script,
    build_table_calculation_help,
    build_tuning_rationale,
    compute_autovacuum_tuning,
    build_vacuum_sql,
    get_batch_execution_risks,
    get_execution_risks,
    get_rule_sql,
    normalize_stale_days,
    render_rule_sql,
)
from apps.home.global_advisor import load_recommendation_catalog
from pathlib import Path


class TableAutovacuumAdvisorTests(unittest.TestCase):
    def test_normalize_stale_days(self):
        self.assertEqual(normalize_stale_days(None), 30)
        self.assertEqual(normalize_stale_days("14"), 14)
        self.assertEqual(normalize_stale_days(999), 365)

    def test_build_analyze_sql(self):
        self.assertEqual(build_analyze_sql("public", "orders"), 'ANALYZE "public"."orders";')

    def test_build_vacuum_sql(self):
        self.assertEqual(
            build_vacuum_sql("public", "orders"),
            'VACUUM VERBOSE "public"."orders";',
        )

    def test_build_autovacuum_tuning_sql_large_table(self):
        sql = build_autovacuum_tuning_sql(
            "public",
            "events",
            n_live_tup=12_000_000,
            n_dead_tup=800_000,
            modified_since_analyze_ratio=0.4,
            vacuum_urgency=2.5,
            never_analyzed=True,
        )
        self.assertIn("autovacuum_analyze_scale_factor = 0.005", sql)
        self.assertIn("autovacuum_vacuum_scale_factor = 0.005", sql)

    def test_build_autovacuum_tuning_sql_many_dead_tuples(self):
        sql = build_autovacuum_tuning_sql(
            "public",
            "logs",
            n_live_tup=2_000_000,
            n_dead_tup=1_500_000,
            table_size_bytes=2 * 1024**3,
        )
        self.assertIn("autovacuum_vacuum_scale_factor = 0.005", sql)
        self.assertIn("autovacuum_vacuum_threshold = 200", sql)

    def test_build_tuning_rationale_dead_tuples(self):
        rationale = build_tuning_rationale(
            n_live_tup=12_000_000,
            n_dead_tup=900_000,
            table_size_bytes=20 * 1024**3,
            vacuum_urgency=2.1,
        )
        self.assertIn("dead tuple", rationale.lower())
        self.assertIn("critical", rationale.lower())

    def test_assess_cluster_load_critical(self):
        level = assess_cluster_load(
            total_dead_tuples=12_000_000,
            high_dead_pressure_tables=25,
            critical_vacuum_count=6,
            flagged_table_count=60,
            active_autovacuum_workers=3,
            current_max_workers=3,
        )
        self.assertEqual(level, "critical")

    def test_build_global_autovacuum_recommendations_high_load(self):
        result = build_global_autovacuum_recommendations(
            load_level="high",
            metrics={"user_table_count": 800, "total_dead_tuples": 2_000_000},
            current_settings={
                "autovacuum": {"setting": "on", "context": "sighup"},
                "autovacuum_max_workers": {"setting": "3", "context": "postmaster"},
                "autovacuum_naptime": {"setting": "60", "unit": "s", "context": "sighup"},
                "autovacuum_vacuum_scale_factor": {"setting": "0.2", "context": "sighup"},
                "autovacuum_analyze_scale_factor": {"setting": "0.1", "context": "sighup"},
                "autovacuum_vacuum_threshold": {"setting": "50", "context": "sighup"},
                "autovacuum_analyze_threshold": {"setting": "50", "context": "sighup"},
                "autovacuum_vacuum_cost_delay": {"setting": "2", "unit": "ms", "context": "sighup"},
                "autovacuum_vacuum_cost_limit": {"setting": "-1", "context": "sighup"},
                "log_autovacuum_min_duration": {"setting": "-1", "context": "sighup"},
                "max_worker_processes": {"setting": "16", "context": "postmaster"},
            },
        )
        params = {item["parameter"] for item in result["recommendations"]}
        self.assertIn("autovacuum_vacuum_scale_factor", params)
        self.assertIn("autovacuum_max_workers", params)
        self.assertIn("ALTER SYSTEM SET autovacuum_vacuum_scale_factor = 0.05;", result["script_sql"])
        self.assertTrue(result["restart_required"])

    def test_get_execution_risks_vacuum(self):
        risks = get_execution_risks("vacuum", table_name="public.orders")
        self.assertTrue(any("I/O" in risk for risk in risks))
        self.assertTrue(any("public.orders" in risk for risk in risks))

    def test_get_execution_risks_alter_system_postmaster(self):
        risks = get_execution_risks(
            "alter_system",
            parameter="autovacuum_max_workers",
            context="postmaster",
        )
        self.assertTrue(any("restart" in risk.lower() for risk in risks))

    def test_primary_issue_type_prefers_vacuum(self):
        self.assertEqual(
            _primary_issue_type(["stale_analyze", "never_vacuumed"]),
            "never_vacuumed",
        )

    def test_normalize_issue_types_pg_array(self):
        self.assertEqual(
            _normalize_issue_types("{never_vacuumed,stale_analyze}"),
            ["never_vacuumed", "stale_analyze"],
        )

    def test_get_batch_execution_risks(self):
        risks = get_batch_execution_risks("all", 12)
        self.assertTrue(any("12 flagged table" in risk for risk in risks))
        self.assertTrue(any("VACUUM" in risk for risk in risks))

    def test_build_batch_actions(self):
        actions = build_batch_actions([
            {"object_name": "public.t1", "alter_available": False},
            {"object_name": "public.t2", "alter_available": True},
        ])
        self.assertEqual(actions["table_count"], 2)
        self.assertEqual(actions["alter_eligible_count"], 1)
        self.assertIn("risks", actions["vacuum"])

    def test_build_restore_script_without_alter(self):
        sql = build_restore_script('ANALYZE "public"."t";', 'VACUUM VERBOSE "public"."t";', "")
        self.assertNotIn("Tune per-table", sql)
        self.assertLess(sql.index("ANALYZE"), sql.index("VACUUM"))

    def test_build_criteria_help_includes_vacuum_pressure(self):
        help_doc = build_criteria_help(7)
        self.assertEqual(help_doc["stale_days"], 7)
        self.assertEqual(build_criteria_help()["stale_days"], 30)
        vacuum_section = next(s for s in help_doc["sections"] if s["id"] == "vacuum")
        joined = " ".join(vacuum_section["points"])
        self.assertIn("needs_vacuum_pressure", joined)
        self.assertIn("10,000", joined)

    def test_compute_autovacuum_tuning_includes_formulas(self):
        tuning = compute_autovacuum_tuning(n_live_tup=2_000_000, n_dead_tup=150_000)
        self.assertIn("vacuum_formula", tuning)
        self.assertIn("analyze_formula", tuning)
        self.assertGreater(tuning["vacuum_trigger_at"], 0)

    def test_build_table_calculation_help_includes_alter_preview(self):
        tuning = compute_autovacuum_tuning(n_live_tup=500_000, n_dead_tup=20_000)
        help_doc = build_table_calculation_help(
            stale_days=7,
            issue_types=["stale_vacuum"],
            never_vacuumed=False,
            stale_vacuum=True,
            needs_vacuum_pressure=True,
            n_live_tup=500_000,
            n_dead_tup=20_000,
            alter_available=False,
            alter_skip_reason="Run ANALYZE first.",
            tuning=tuning,
        )
        self.assertTrue(help_doc["alter"]["preview_only"])
        self.assertIn("vacuum_formula", help_doc["alter"])

    def test_maintenance_flagged_sql_uses_vacuum_pressure(self):
        catalog = load_recommendation_catalog(
            str(Path(__file__).resolve().parents[1] / "advisor_enriched.yml")
        )
        sql = get_rule_sql(
            catalog,
            "table_autovacuum_maintenance_flagged",
            stale_days=7,
            min_table_size_bytes=1048576,
        )
        self.assertIn("needs_vacuum_pressure", sql)
        self.assertIn("last_any_analyze", sql)

    def test_render_rule_sql_replaces_parameters(self):
        sql = render_rule_sql(
            "SELECT {{stale_days}}::int, {{min_table_size_bytes}}::bigint;",
            stale_days=14,
            min_table_size_bytes=1048576,
        )
        self.assertIn("14::int", sql)
        self.assertIn("1048576::bigint", sql)

    def test_get_rule_sql_from_advisor_enriched(self):
        yaml_path = Path(__file__).resolve().parents[1] / "advisor_enriched.yml"
        catalog = load_recommendation_catalog(str(yaml_path))
        sql = get_rule_sql(
            catalog,
            "table_autovacuum_maintenance_flagged",
            stale_days=7,
            min_table_size_bytes=1048576,
        )
        self.assertIn("never_vacuumed", sql)
        self.assertIn("7::int AS stale_days", sql)
        cluster_sql = get_rule_sql(catalog, "table_autovacuum_cluster_load")
        self.assertIn("active_autovacuum_workers", cluster_sql)

    def test_long_running_transactions_are_limited_to_connected_database(self):
        yaml_path = Path(__file__).resolve().parents[1] / "advisor_enriched.yml"
        catalog = load_recommendation_catalog(str(yaml_path))
        rule = next(
            definition
            for definition in catalog
            if definition.get("id") == "long_running_transactions"
        )

        self.assertIn("a.datname = current_database()", rule["sql"])


if __name__ == "__main__":
    unittest.main()
