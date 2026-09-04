import unittest
from unittest.mock import patch

from flask import Flask

from apps.home import route_api


class ExecutivePlanApiTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "test-secret"
        self.app.register_blueprint(route_api.blueprint)
        self.client = self.app.test_client()

    def test_returns_complete_executive_plan_for_request_database(self):
        plan = {
            "status": "ok",
            "database": "application",
            "phases": [{"number": 10, "tasks": []}],
            "tasks": [],
            "errors": [],
            "summary": {"recommendations_collected": 4},
        }
        db_config = {"db_uri": "postgresql://user:secret@db/application"}

        with patch.object(
            route_api.executive_plan,
            "build_executive_plan",
            return_value=plan,
        ) as build_plan:
            response = self.client.get(
                "/api/v1/executive_plan",
                json={"db_config": db_config},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), plan)
        build_plan.assert_called_once_with(db_config)

    def test_rejects_request_without_database_configuration(self):
        response = self.client.post("/api/v1/executive_plan", json={})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["error"], "Database is not connected.")

    def test_returns_json_error_when_plan_generation_fails(self):
        with patch.object(
            route_api.executive_plan,
            "build_executive_plan",
            side_effect=RuntimeError("advisor failed"),
        ):
            response = self.client.get(
                "/api/v1/executive_plan",
                json={"db_config": {"db_name": "application"}},
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.get_json(), {"success": False, "error": "advisor failed"})

    def test_history_targets_returns_suggested_collector_target(self):
        targets = {
            "database": {"db_name": "application"},
            "database_filter": "application",
            "suggested_target_id": "application-production",
            "requires_selection": False,
            "targets": [{"target_id": "application-production"}],
        }
        with self.client.session_transaction() as flask_session:
            flask_session["db_name"] = "application"
            flask_session["target_id"] = "application-production"

        with (
            patch.object(route_api.collector_history, "is_configured", return_value=True),
            patch.object(route_api.collector_history, "list_targets", return_value=targets) as list_targets,
        ):
            response = self.client.get("/api/v1/collector/targets")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["suggested_target_id"], "application-production")
        self.assertEqual(list_targets.call_args.kwargs, {"query": "", "limit": 50})

    def test_history_endpoint_accepts_all_period_and_devops_team(self):
        history = {
            "status": "ok",
            "target_id": "application-production",
            "team": "DEV_OPS",
            "days": None,
            "snapshots": 2,
        }
        with self.client.session_transaction() as flask_session:
            flask_session["db_name"] = "application"
            flask_session["target_id"] = "application-production"

        with (
            patch.object(route_api.collector_history, "is_configured", return_value=True),
            patch.object(route_api.collector_history, "target_exists", return_value=True),
            patch.object(route_api.collector_history, "load_history", return_value=history) as load_history,
            patch.object(route_api.executive_plan, "build_executive_plan", return_value={"tasks": [], "errors": []}),
        ):
            response = self.client.get(
                "/api/v1/executive_plan/history",
                query_string={
                    "period": "all",
                    "team": "DEVOPS",
                },
            )

        self.assertEqual(response.status_code, 200)
        load_history.assert_called_once_with(
            "application-production",
            days=None,
            team="DEVOPS",
            current_plan={"tasks": [], "errors": []},
        )
    def test_history_endpoint_rejects_unknown_target_before_building_current_plan(self):
        with self.client.session_transaction() as flask_session:
            flask_session["db_name"] = "application"
            flask_session["target_id"] = "anything"

        with (
            patch.object(route_api.collector_history, "is_configured", return_value=True),
            patch.object(route_api.collector_history, "target_exists", return_value=False),
            patch.object(route_api.executive_plan, "build_executive_plan") as build_plan,
        ):
            response = self.client.get(
                "/api/v1/executive_plan/history",
                query_string={"period": "30"},
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.get_json()["error"], "Unknown collector target.")
        build_plan.assert_not_called()

    def test_query_activity_history_uses_active_database_session(self):
        history = {
            "success": True,
            "queryid": "123",
            "default_metric": "mean_exec_time_ms",
            "metrics": [{"key": "mean_exec_time_ms", "label": "Average time", "unit": "ms"}],
            "points": [],
        }
        with self.client.session_transaction() as flask_session:
            flask_session["db_name"] = "application"
            flask_session["target_id"] = "application-production"

        with (
            patch.object(route_api.collector_history, "is_configured", return_value=True),
            patch.object(route_api.collector_history, "target_exists", return_value=True),
            patch.object(route_api.query_history, "load_query_history", return_value=history) as load_history,
        ):
            response = self.client.get(
                "/api/v1/query_activity/history",
                query_string={"queryid": "123"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["default_metric"], "mean_exec_time_ms")
        load_history.assert_called_once_with("123", "application-production")

    def test_query_activity_history_is_hidden_without_collector(self):
        with patch.object(route_api.collector_history, "is_configured", return_value=False):
            response = self.client.get("/api/v1/query_activity/history?queryid=123")

        self.assertEqual(response.status_code, 404)

    def test_collector_target_selection_is_saved_in_session(self):
        with (
            patch.object(route_api.collector_history, "is_configured", return_value=True),
            patch.object(route_api.collector_history, "target_exists", return_value=True),
        ):
            response = self.client.post(
                "/api/v1/collector/target",
                json={"target_id": "application-production"},
            )

        self.assertEqual(response.status_code, 200)
        with self.client.session_transaction() as flask_session:
            self.assertEqual(flask_session["target_id"], "application-production")


if __name__ == "__main__":
    unittest.main()
