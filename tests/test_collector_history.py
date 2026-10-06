import unittest
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from apps.home.collector_history import (
    _build_collection_timeline,
    _query_statement_type,
    build_workload_measurement_prompt,
    build_history,
    build_workload_correlation,
    current_plan_rows,
    normalize_team,
    recommendation_fingerprints,
)


def history_row(collected_at, fingerprint=None, action="", **values):
    return {
        "run_id": values.pop("run_id", uuid4()),
        "collected_at": collected_at,
        "finding_fingerprint": fingerprint,
        "action_fingerprint": action,
        "team": values.pop("team", "DEV"),
        "priority": values.pop("priority", "HIGH"),
        "source": values.pop("source", "global_advisor"),
        "sources": values.pop("sources", ["global_advisor"]),
        "advisor_id": values.pop("advisor_id", fingerprint),
        "action_type": values.pop("action_type", "REVIEW_ONLY"),
        "scope_name": values.pop("scope_name", "public.orders"),
        "object_name": values.pop("object_name", "public.orders"),
        "title": values.pop("title", f"Recommendation {fingerprint}"),
        "description": values.pop("description", "Review this finding."),
        **values,
    }


class CollectorHistoryTests(unittest.TestCase):
    def test_workload_prompt_contains_only_selected_and_previous_measurements(self):
        prompt = build_workload_measurement_prompt(
            {
                "run_id": "selected", "collected_at": "2026-08-24T12:00:00Z",
                "workload": {"calls": 20}, "recommendations": {"new": []},
                "environment": {"version": "17"}, "whole_timeline": ["must-not-leak"],
            },
            {
                "run_id": "previous", "collected_at": "2026-08-24T11:00:00Z",
                "workload": {"calls": 10}, "unrelated": "must-not-leak-either",
            },
        )

        context = json.loads(prompt.split("Collector evidence:\n", 1)[1])
        self.assertEqual(set(context), {"selected_measurement", "previous_measurement"})
        self.assertEqual(context["selected_measurement"]["run_id"], "selected")
        self.assertEqual(context["previous_measurement"]["run_id"], "previous")
        self.assertEqual(
            context["selected_measurement"]["environment"], {"version": "17"}
        )
        self.assertEqual(
            context["previous_measurement"]["recommendations"], {"active_count": None}
        )
        self.assertNotIn("whole_timeline", context["selected_measurement"])
        self.assertNotIn("unrelated", context["previous_measurement"])

    def test_query_statement_type_uses_postgresql_parser(self):
        self.assertEqual(_query_statement_type("SELECT * FROM orders"), "SELECT")
        self.assertEqual(_query_statement_type("WITH source AS (SELECT 1) INSERT INTO events SELECT * FROM source"), "INSERT")
        self.assertEqual(_query_statement_type("UPDATE orders SET status = 'done'"), "UPDATE")
        self.assertEqual(_query_statement_type("DELETE FROM orders WHERE id = 1"), "DELETE")
        self.assertEqual(_query_statement_type("VACUUM orders"), "OTHERS")
        self.assertEqual(_query_statement_type("not valid sql"), "OTHERS")

    def timeline(self, samples, hours=None, days=None):
        start = datetime(2026, 10, 6, tzinfo=timezone.utc)
        rows = []
        for i, sample in enumerate(samples):
            for key, calls, time in sample:
                rows.append({"run_id": str(i), "collected_at": start + timedelta(hours=hours[i] if hours else i),
                             "queryid": key, "calls": calls, "total_exec_time_ms": time, "query": "SELECT 1"})
        return _build_collection_timeline(rows, [], [], days=days)

    def test_seven_active_queries_do_not_get_diluted_by_43_idle_queries(self):
        samples = [[(str(k), 10000 + (500*i if k < 7 else 0),
                     100000 + (500*i*2 if k < 7 else 0)) for k in range(50)] for i in range(3)]
        points = self.timeline(samples)
        self.assertIsNone(points[0]["workload"]["calls"])
        self.assertIsNone(points[0]["workload"]["statement_calls"])
        self.assertIsNone(points[1]["workload"]["calls_change_pct"])
        value = points[2]["workload"]
        self.assertEqual(value["calls"], 3500)
        self.assertEqual(value["average_time_ms"], 2)
        self.assertEqual(value["active_queries"], 7)
        self.assertEqual(value["coverage"]["comparable_queries"], 50)
        self.assertEqual(value["statement_calls"]["SELECT"], 3500)
        self.assertEqual(value["calls_change_pct"], 0)
        self.assertEqual(value["change_pct"], 0)

    def test_comparison_uses_shared_queries_not_changed_ranking(self):
        points = self.timeline([
            [("a", 100, 1000), ("b", 100, 1000)],
            [("a", 110, 1020), ("b", 200, 1900), ("c", 10000, 90000)],
            [("a", 120, 1060), ("c", 11000, 100000)],
        ])
        value = points[2]["workload"]
        self.assertEqual(value["calls"], 1010)
        self.assertEqual(value["comparison"]["query_count"], 1)
        self.assertEqual(value["comparison"]["calls"], 10)
        self.assertEqual(value["comparison"]["previous_calls"], 10)
        self.assertEqual(value["calls_change_pct"], 0)
        self.assertEqual(value["change_pct"], 100)
        self.assertEqual(value["coverage"]["exclusions"]["missing"], 1)

    def test_reset_gap_and_returning_queries_are_excluded(self):
        points = self.timeline([
            [("a", 100, 1000)], [("a", 5, 50)],
            [("b", 50, 500)], [("a", 20, 200)], [("a", 30, 220)],
        ])
        self.assertIsNone(points[1]["workload"]["calls"])
        self.assertEqual(points[1]["workload"]["coverage"]["exclusions"]["reset_or_invalid"], 1)
        self.assertIsNone(points[3]["workload"]["calls"])
        self.assertEqual(points[4]["workload"]["calls"], 10)
        self.assertIsNone(points[4]["workload"]["calls_change_pct"])

    def test_zero_activity_and_unequal_durations(self):
        points = self.timeline([[('a', 10, 100)], [('a', 20, 120)], [('a', 30, 140)]], hours=[0, 1, 3])
        self.assertEqual(points[2]['workload']['calls_change_pct'], 0)
        self.assertEqual(points[2]['workload']['calls_per_minute_change_pct'], -50)
        idle = self.timeline([[('a', 10, 100)], [('a', 10, 100)], [('a', 20, 120)]])
        self.assertEqual(idle[1]['workload']['calls'], 0)
        self.assertIsNone(idle[1]['workload']['average_time_ms'])
        self.assertIsNone(idle[2]['workload']['calls_change_pct'])
        self.assertIsNone(idle[2]['workload']['change_pct'])

    def test_collection_period_filter_preserves_comparisons(self):
        samples = [[('a', 100+i*10, 1000+i*20)] for i in range(5)]
        complete = self.timeline(samples, hours=[0, 24, 48, 72, 96])
        filtered = self.timeline(samples, hours=[0, 24, 48, 72, 96], days=1)
        self.assertEqual(filtered, complete[2:])

    def test_duplicate_ids_and_invalid_counters_are_not_measured(self):
        points = self.timeline([[('a', 10, 100), ('b', 10, 100)],
                                [('a', 20, 200), ('a', 30, 300), ('b', 10, 150)]])
        value = points[1]['workload']
        self.assertIsNone(value['calls'])
        self.assertEqual(value['coverage']['exclusions']['ambiguous'], 2)
        self.assertEqual(value['coverage']['exclusions']['reset_or_invalid'], 1)

    def test_collection_timeline_exposes_recommendation_details(self):
        collected_at = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        result = _build_collection_timeline([{
            "run_id": "run-1", "collected_at": collected_at, "queryid": "42",
            "calls": 10, "total_exec_time_ms": 50, "query": "select 42",
        }], [{
            "run_id": "run-1", "finding_fingerprint": "finding", "title": "Review index",
            "description": "The index could help this lookup.",
            "recommendation_sql": "CREATE INDEX CONCURRENTLY example_idx ON example(id);",
            "query_ids": ["42"],
        }], [], days=None)

        recommendation = result[0]["recommendations"]["new"][0]
        self.assertEqual(recommendation["description"], "The index could help this lookup.")
        self.assertIn("CREATE INDEX", recommendation["recommendation_sql"])

    def test_collection_timeline_ranks_query_changes_by_weighted_workload_impact(self):
        points = self.timeline([
            [('small', 100, 1000), ('large', 100, 1000)],
            [('small', 101, 1010), ('large', 1100, 11000)],
            [('small', 102, 1030), ('large', 2100, 22000)],
        ])
        changes = points[2]['workload']['query_changes']
        self.assertEqual([item['queryid'] for item in changes], ['large'])
        self.assertEqual(changes[0]['impact_time_ms'], 1000)
        self.assertEqual(changes[0]['total_time_ms'], 11000)
        self.assertEqual(changes[0]['workload_share_pct'], 99.82)

    def test_correlates_exact_run_before_temporal_and_builds_trend(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        result = build_workload_correlation([{
            "run_id": "plan-1", "collected_at": now,
            "finding_fingerprint": "finding-1", "action_fingerprint": "action-1",
            "title": "Add index", "queryid": "42", "exact_run_match": True,
            "query_id_source": "recommendation",
            "workload_release": "2026.08", "previous_workload_release": "2026.07",
            "postgres_version": "17.6", "previous_postgres_version": "17.5",
            "postgres_settings_changed": True,
            "workload_run_id": "plan-1", "workload_collected_at": now,
            "calls": 100, "mean_exec_time_ms": 5, "total_exec_time_ms": 500,
            "share_total_time": 12.5, "share_io": 8,
            "before_collected_at": now - timedelta(days=1),
            "before_mean_exec_time_ms": 10, "before_calls": 80,
            "after_collected_at": now + timedelta(days=1),
            "after_mean_exec_time_ms": 5, "after_calls": 100,
            "previous_latest_collected_at": now,
            "previous_latest_mean_exec_time_ms": 5, "previous_latest_calls": 20,
            "latest_collected_at": now + timedelta(minutes=10),
            "latest_mean_exec_time_ms": 9, "latest_calls": 20,
            "latest_workload_release": "2026.08",
            "latest_previous_workload_release": "2026.07",
            "latest_postgres_version": "19beta3",
            "latest_previous_postgres_version": "18.1",
            "latest_postgres_settings_changed": True,
            "latest_previous_postgres_settings": {
                "shared_buffers": "1GB", "work_mem": "4MB",
            },
            "latest_postgres_settings": {
                "shared_buffers": "128MB", "work_mem": "4MB",
            },
        }], target_id="orders", days=30)

        self.assertEqual(result["summary"]["exact_run_matches"], 1)
        self.assertEqual(result["correlations"][0]["match"]["type"], "run_id")
        self.assertEqual(result["correlations"][0]["query_id_source"], "recommendation")
        self.assertTrue(result["correlations"][0]["postgres_context"]["confounding_change"])
        self.assertEqual(result["summary"]["context_changes"], 1)
        self.assertTrue(result["summary"]["environment_baseline_available"])
        self.assertEqual(result["correlations"][0]["trend"]["mean_exec_time_change_pct"], -50)
        self.assertEqual(result["correlations"][0]["trend"]["direction"], "improved")
        self.assertEqual(result["correlations"][0]["latest_trend"]["direction"], "degraded")
        self.assertEqual(result["correlations"][0]["latest_trend"]["mean_exec_time_change_pct"], 80)
        self.assertEqual(result["environment_impact"]["trend_counts"]["degraded"], 1)
        self.assertEqual(result["environment_impact"]["setting_changes"], [{
            "name": "shared_buffers",
            "before": "1GB",
            "after": "128MB",
            "possible_impact": "May change cache hit rates and physical I/O pressure.",
        }])

    def test_marks_environment_baseline_unavailable_instead_of_reporting_zero(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        result = build_workload_correlation([{
            "run_id": "plan-1", "collected_at": now,
            "finding_fingerprint": "finding-1", "queryid": "42",
            "latest_postgres_version": "19beta3",
        }], target_id="orders", days=30)

        self.assertTrue(result["summary"]["environment_context_available"])
        self.assertFalse(result["summary"]["environment_baseline_available"])
        self.assertFalse(result["correlations"][0]["postgres_context"]["baseline_available"])

    def test_builds_environment_changes_as_a_period_timeline(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        result = build_workload_correlation(
            [], target_id="orders", days=30,
            environment_rows=[
                {
                    "run_id": "change-2", "collected_at": now,
                    "previous_collected_at": now - timedelta(days=1),
                    "postgres_version": "19beta3", "previous_postgres_version": "19beta3",
                    "postgres_settings": {"work_mem": "8MB", "shared_buffers": "256MB"},
                    "previous_postgres_settings": {"work_mem": "4MB", "shared_buffers": "256MB"},
                },
                {
                    "run_id": "change-1", "collected_at": now - timedelta(days=1),
                    "previous_collected_at": now - timedelta(days=2),
                    "postgres_version": "19beta3", "previous_postgres_version": "18.1",
                    "postgres_settings": {"work_mem": "4MB", "shared_buffers": "256MB"},
                    "previous_postgres_settings": {"work_mem": "4MB", "shared_buffers": "128MB"},
                },
            ],
        )

        self.assertEqual(result["summary"]["context_changes"], 2)
        self.assertEqual([event["run_id"] for event in result["environment_history"]], ["change-2", "change-1"])
        self.assertEqual(result["environment_history"][0]["setting_changes"][0]["name"], "work_mem")
        self.assertTrue(result["environment_history"][1]["version_changed"])

    def test_builds_new_modified_corrected_and_reopened_events(self):
        now = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
        first_run, second_run, third_run = uuid4(), uuid4(), uuid4()
        rows = [
            history_row(now - timedelta(days=20), "a", "action-a1", run_id=first_run),
            history_row(
                now - timedelta(days=20),
                "b",
                "action-b",
                run_id=first_run,
                recommendation_sql="VACUUM (ANALYZE) public.orders;",
            ),
            history_row(now - timedelta(days=10), "a", "action-a2", run_id=second_run),
            history_row(now - timedelta(days=10), "c", "action-c", run_id=second_run),
            history_row(now, "b", "action-b", run_id=third_run),
            history_row(now, "c", "action-c", run_id=third_run),
        ]

        result = build_history(
            rows,
            target_id="orders-production",
            days=15,
            team="ALL",
            now=now,
        )

        self.assertEqual(result["snapshots"], 2)
        self.assertEqual(result["summary"]["active"], 2)
        self.assertEqual(result["summary"]["corrected"], 2)
        self.assertEqual(result["summary"]["new"], 2)
        self.assertEqual(result["summary"]["modified"], 1)
        self.assertEqual(result["summary"]["resolution_rate"], 33)
        self.assertEqual(result["timeline"][0]["corrected"], 1)
        self.assertEqual(result["timeline"][0]["modified"], 1)
        self.assertTrue(result["timeline"][0]["has_previous"])
        self.assertEqual(
            result["timeline"][0]["correction_packages"][0]["recommendations"][0]["finding_fingerprint"],
            "b",
        )
        self.assertEqual(
            result["timeline"][0]["correction_packages"][0]["recommendations"][0]["recommendation_sql"],
            "VACUUM (ANALYZE) public.orders;",
        )
        self.assertEqual(
            {
                item["finding_fingerprint"]
                for package in result["timeline"][0]["change_packages"]
                for item in package["recommendations"]
            },
            {"a", "c"},
        )
        self.assertTrue(next(item for item in result["additions"] if item["finding_fingerprint"] == "b")["reopened"])
        self.assertTrue(next(item for item in result["corrections"] if item["finding_fingerprint"] == "b")["reopened"])

    def test_empty_snapshot_can_confirm_all_previous_findings_as_corrected(self):
        now = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
        first_run, second_run = uuid4(), uuid4()
        rows = [
            history_row(now - timedelta(days=2), "a", "action-a", run_id=first_run),
            history_row(now, None, run_id=second_run),
        ]

        result = build_history(
            rows,
            target_id="orders-production",
            days=7,
            team="OPS",
            now=now,
        )

        self.assertEqual(result["summary"]["active"], 0)
        self.assertEqual(result["summary"]["corrected"], 1)
        self.assertEqual(result["summary"]["resolution_rate"], 100)

    def test_team_alias_is_normalized(self):
        self.assertEqual(normalize_team("DEVOPS"), "DEV_OPS")
        self.assertEqual(normalize_team("DEV/OPS"), "DEV_OPS")

    def test_invalid_team_is_rejected(self):
        with self.assertRaises(ValueError):
            normalize_team("DBA")

    def test_latest_compares_only_the_last_snapshot_transition(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        rows = [
            history_row(now - timedelta(days=10), "old", run_id=uuid4()),
            history_row(now - timedelta(days=1), "latest", run_id=uuid4()),
            history_row(now, None, run_id="current", is_current=True),
        ]

        result = build_history(rows, target_id="orders", days="latest", team="ALL", now=now)

        self.assertEqual(result["snapshots"], 1)
        self.assertEqual(result["summary"]["corrected"], 1)
        self.assertEqual(result["corrections"][0]["finding_fingerprint"], "latest")

    def test_live_plan_uses_collector_fingerprints_and_keeps_packages(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        recommendation = {
            "advisor_id": "missing_index",
            "category_id": "INDEX",
            "action_type": "CREATE_INDEX",
            "schema_name": "public",
            "table_name": "orders",
            "object_name": "public.orders",
            "sql": "CREATE INDEX idx_orders_status ON public.orders (status)",
            "team": "DEV_OPS",
            "priority": "HIGH",
            "title": "Create the orders status index",
        }
        plan = {
            "tasks": [{
                "title": "Improve order access paths",
                "phase": 20,
                "workstream": "INDEX_STRATEGY",
                "team": "DEV_OPS",
                "recommendations": [recommendation],
            }],
            "errors": [],
        }

        rows = current_plan_rows(plan, target_id="orders-production", team="ALL", now=now)
        expected_finding, expected_action = recommendation_fingerprints("orders-production", recommendation)

        self.assertEqual(rows[0]["finding_fingerprint"], expected_finding)
        self.assertEqual(rows[0]["action_fingerprint"], expected_action)
        self.assertEqual(rows[0]["package_title"], "Improve order access paths")
        self.assertTrue(rows[0]["is_current"])

    def test_live_snapshot_immediately_marks_missing_finding_as_corrected(self):
        now = datetime(2026, 8, 24, 12, tzinfo=timezone.utc)
        previous_run = uuid4()
        rows = [
            history_row(
                now - timedelta(days=1),
                "fixed",
                "action-fixed",
                run_id=previous_run,
                package_title="Protect order data",
                phase_number=10,
            ),
            history_row(now, None, run_id="current", is_current=True),
        ]

        result = build_history(rows, target_id="orders-production", days=7, team="ALL", now=now)

        self.assertEqual(result["summary"]["corrected"], 1)
        self.assertTrue(result["corrections"][0]["live_comparison"])
        self.assertEqual(result["correction_packages"][0]["title"], "Protect order data")
        self.assertTrue(result["timeline"][-1]["is_current"])


if __name__ == "__main__":
    unittest.main()
