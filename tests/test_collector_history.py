import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from apps.home.collector_history import (
    build_history,
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
    def test_builds_new_modified_corrected_and_reopened_events(self):
        now = datetime(2026, 8, 23, 12, tzinfo=timezone.utc)
        first_run, second_run, third_run = uuid4(), uuid4(), uuid4()
        rows = [
            history_row(now - timedelta(days=20), "a", "action-a1", run_id=first_run),
            history_row(now - timedelta(days=20), "b", "action-b", run_id=first_run),
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
