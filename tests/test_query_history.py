import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from apps.home import query_history


class FakeCursor:
    def __init__(self, results):
        self.results = iter(results)
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, parameters=None):
        self.executions.append((sql, parameters))

    def fetchall(self):
        return next(self.results)


class FakeConnection:
    def __init__(self, results):
        self.cursor_instance = FakeCursor(results)
        self.closed = False

    def cursor(self, **_kwargs):
        return self.cursor_instance

    def close(self):
        self.closed = True


class QueryHistoryTests(unittest.TestCase):
    def test_builds_global_performance_and_keeps_queries_that_left_top_50(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = []
        # Query 1 improves: interval latency moves from 10 ms to 5 ms.
        for offset, calls, total in ((0, 100, 1000), (1, 110, 1100), (2, 120, 1170), (3, 130, 1220)):
            rows.append({"queryid": "1", "collected_at": start + timedelta(days=offset),
                         "calls": calls, "total_exec_time_ms": total})
        # Query 2 regresses, then no longer appears in the latest Top 50.
        for offset, calls, total in ((0, 50, 500), (1, 60, 600), (2, 70, 800)):
            rows.append({"queryid": "2", "collected_at": start + timedelta(days=offset),
                         "calls": calls, "total_exec_time_ms": total})

        result = query_history.build_performance_evolution(
            rows, target_id="production", days=None, now=start + timedelta(days=3)
        )

        self.assertEqual(result["summary"]["improved"], 1)
        self.assertEqual(result["summary"]["degraded"], 1)
        self.assertEqual(result["summary"]["left_top_50"], 1)
        self.assertEqual(result["gains"][0]["queryid"], "1")
        self.assertEqual(result["regressions"][0]["ranking_status"], "left_top_50")
        self.assertEqual(result["timeline"][0]["calls"], 20)
        self.assertEqual(result["timeline"][0]["calls_per_minute"], round(20 / 1440, 3))
        self.assertEqual(result["timeline"][0]["interval_seconds"], 86400)

    def test_global_verdict_is_activity_weighted_and_ignores_tiny_absolute_impact(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = []
        # The current interval is 200x slower, but only adds 3.98 ms in total.
        for offset, calls, total in ((0, 100, 1), (1, 101, 1.02), (2, 102, 5.0)):
            rows.append({"queryid": "tiny", "collected_at": start + timedelta(days=offset),
                         "calls": calls, "total_exec_time_ms": total})

        result = query_history.build_performance_evolution(
            rows, target_id="production", days=None, now=start + timedelta(days=2)
        )

        self.assertGreater(result["summary"]["global_change_pct"], 10000)
        self.assertEqual(result["summary"]["global_verdict"], "stable")
        self.assertAlmostEqual(result["summary"]["estimated_saved_time_ms"], -3.97, places=2)

    def test_global_verdict_uses_current_call_volume(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = []
        # One busy query regresses by 20%; one quiet query improves by 50%.
        for queryid, values in {
            "busy": ((0, 0, 0), (1, 1000, 10000), (2, 2000, 22000)),
            "quiet": ((0, 0, 0), (1, 1, 100), (2, 2, 150)),
        }.items():
            for offset, calls, total in values:
                rows.append({"queryid": queryid, "collected_at": start + timedelta(days=offset),
                             "calls": calls, "total_exec_time_ms": total})

        result = query_history.build_performance_evolution(
            rows, target_id="production", days=None, now=start + timedelta(days=2)
        )

        self.assertEqual(result["summary"]["global_verdict"], "degraded")
        self.assertGreater(result["summary"]["global_change_pct"], 18)
        self.assertLess(result["summary"]["global_change_pct"], 20)
        self.assertEqual(result["regressions"][0]["queryid"], "busy")
        self.assertGreater(result["regressions"][0]["activity_share_pct"], 99)
        self.assertEqual(result["regressions"][0]["current_total_time_ms"], 12000)

    def test_top_lists_hide_single_call_with_negligible_total_impact(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = []
        for queryid, values in {
            "busy-stable": ((0, 0, 0), (1, 10000, 100000), (2, 20000, 200000)),
            "tiny-regression": ((0, 0, 0), (1, 1, 0.01), (2, 2, 0.03)),
        }.items():
            for offset, calls, total in values:
                rows.append({"queryid": queryid, "collected_at": start + timedelta(days=offset),
                             "calls": calls, "total_exec_time_ms": total})

        result = query_history.build_performance_evolution(
            rows, target_id="production", days=None, now=start + timedelta(days=2)
        )

        self.assertEqual(result["summary"]["degraded"], 1)
        self.assertEqual(result["regressions"], [])

    def test_ignores_counter_resets_in_performance_intervals(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = [
            {"queryid": "1", "collected_at": start, "calls": 100, "total_exec_time_ms": 1000},
            {"queryid": "1", "collected_at": start + timedelta(days=1), "calls": 5, "total_exec_time_ms": 50},
            {"queryid": "1", "collected_at": start + timedelta(days=2), "calls": 10, "total_exec_time_ms": 100},
        ]
        result = query_history.build_performance_evolution(rows, target_id="production", days=None)
        self.assertEqual(result["summary"]["comparable"], 1)
        self.assertEqual(result["summary"]["stable"], 1)

    def test_latest_period_uses_only_the_last_two_collections(self):
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        rows = [
            {"queryid": "1", "collected_at": start + timedelta(days=offset),
             "calls": calls, "total_exec_time_ms": total}
            for offset, calls, total in (
                (0, 0, 0), (1, 10, 100), (2, 20, 300), (3, 30, 600)
            )
        ]

        result = query_history.build_performance_evolution(
            rows, target_id="production", days=0, now=start + timedelta(days=3)
        )

        self.assertEqual(result["period"]["from"], start + timedelta(days=2))
        self.assertEqual(result["summary"]["comparable"], 1)
        self.assertEqual(result["gains"], [])
        self.assertEqual(result["regressions"][0]["queryid"], "1")

    def test_first_snapshot_is_the_baseline_for_interval_gains(self):
        start = datetime(2026, 9, 1, tzinfo=timezone.utc)
        rows = [
            {"queryid": "4608506514630080376", "collected_at": start,
             "calls": 500, "total_exec_time_ms": 3321.06},
            {"queryid": "4608506514630080376", "collected_at": start + timedelta(minutes=3),
             "calls": 1000, "total_exec_time_ms": 3664.94},
            {"queryid": "4608506514630080376", "collected_at": start + timedelta(minutes=10),
             "calls": 1500, "total_exec_time_ms": 4016.91},
        ]

        result = query_history.build_performance_evolution(
            rows, target_id="northwind-demo", days=None, now=start + timedelta(minutes=10)
        )

        gain = result["gains"][0]
        self.assertEqual(gain["queryid"], "4608506514630080376")
        self.assertAlmostEqual(gain["baseline_avg_time_ms"], 6.642, places=3)
        self.assertAlmostEqual(gain["current_avg_time_ms"], 0.704, places=3)
        self.assertAlmostEqual(gain["change_pct"], -89.4, places=1)
        self.assertGreater(gain["estimated_saved_time_ms"], 2900)

    def test_loads_available_metrics_with_average_time_as_default(self):
        collected_at = datetime(2026, 8, 28, tzinfo=timezone.utc)
        connection = FakeConnection([
            [{
                "collected_at": collected_at,
                "mean_exec_time_ms": Decimal("12.5"),
                "calls": 42,
                **{name: None for name in query_history.METRIC_NAMES if name not in {"mean_exec_time_ms", "calls"}},
            }],
        ])
        with patch.object(query_history.collector_history, "_connect", return_value=connection):
            result = query_history.load_query_history("123", "application-production")

        self.assertTrue(result["success"])
        self.assertEqual(result["target_id"], "application-production")
        self.assertEqual(result["default_metric"], "mean_exec_time_ms")
        self.assertEqual([metric["key"] for metric in result["metrics"]], ["mean_exec_time_ms", "calls"])
        self.assertEqual(result["points"][0]["values"]["mean_exec_time_ms"], 12.5)
        self.assertTrue(connection.closed)

    def test_returns_empty_result_when_query_was_not_collected(self):
        connection = FakeConnection([[]])
        with patch.object(query_history.collector_history, "_connect", return_value=connection):
            result = query_history.load_query_history("missing", "application-production")

        self.assertEqual(result["points"], [])
        self.assertIn("No historical metrics", result["message"])

    def test_query_activity_template_contains_history_dialog(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "apps" / "templates" / "home" / "topqueries.html"
        ).read_text(encoding="utf-8")

        self.assertIn('id="queryHistoryModal"', template)
        self.assertIn('class="form-select" id="queryHistoryMetric"', template)
        self.assertIn('data-queryid="{{ row[\'queryid\'] }}"', template)
        self.assertIn("payload.default_metric", template)

    def test_database_template_owns_collector_target_selection(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "apps" / "templates" / "home" / "database.html"
        ).read_text(encoding="utf-8")
        executive_template = (
            Path(__file__).resolve().parents[1]
            / "apps" / "templates" / "home" / "executive_plan.html"
        ).read_text(encoding="utf-8")

        self.assertIn('id="connection-collector-tab"', template)
        self.assertIn('name="target_id"', template)
        self.assertIn('id="save-collector-target"', template)
        self.assertIn('id="collector-connection-error"', template)
        self.assertIn("showCollectorError", template)
        self.assertNotIn('id="history-target"', executive_template)

    def test_query_ranking_template_contains_performance_evolution_tab(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "apps" / "templates" / "home" / "rankqueries.html"
        ).read_text(encoding="utf-8")
        self.assertIn("{% if performance_evolution_enabled %}", template)
        self.assertIn('id="performance-tab"', template)
        self.assertIn("/api/v1/query_ranking/performance", template)
        self.assertIn("Left Top 50", template)
        self.assertIn('id="performance-activity-chart"', template)
        self.assertIn("calls_per_minute", template)
        self.assertIn('class="nav nav-tabs tune-nav-tabs"', template)
        self.assertIn('class="btn btn-sm performance-period-btn" data-period="latest"', template)
        self.assertIn('data-period="15"', template)
        self.assertIn('data-bs-target="#rankQueryHistoryModal"', template)
        self.assertIn('id="rankQueryHistoryMetric"', template)
        self.assertIn("api_query_activity_history", template)


if __name__ == "__main__":
    unittest.main()
