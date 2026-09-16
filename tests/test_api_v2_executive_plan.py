import os
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from apps.api_v2 import blueprint
from apps.api_v2 import executive_plan as route_executive_plan


class ExecutivePlanV2ApiTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.register_blueprint(blueprint)
        self.client = self.app.test_client()
        self.token_patch = patch.dict(os.environ, {}, clear=False)
        self.token_patch.start()
        os.environ.pop("PGA_API_TOKEN", None)

    def tearDown(self):
        self.token_patch.stop()

    def test_builds_plan_from_database_uri_without_web_session(self):
        connection = MagicMock()
        recommendation = {"source": "autovacuum", "title": "Analyze orders"}
        task = {
            "id": "task-1",
            "recommendations": [recommendation],
            "recommendation_groups": [
                {"scope_name": "public.orders", "recommendations": [recommendation]}
            ],
        }
        plan = {
            "status": "ok",
            "database": "application",
            "phases": [{"number": 10, "tasks": [task]}],
            "tasks": [task],
            "errors": [],
            "summary": {"tasks": 0},
            "postgres_context": {"available": True, "server_version": "18.3"},
        }
        with (
            patch.object(
                route_executive_plan.database,
                "connectdb",
                return_value=(connection, "OK"),
            ) as connect,
            patch.object(
                route_executive_plan.executive_plan,
                "build_executive_plan",
                return_value=plan,
            ) as build_plan,
        ):
            response = self.client.post(
                "/api/v2/executive-plan",
                json={"database_uri": "postgresql://user:secret@db:5432/application"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["phases"][0]["task_ids"], ["task-1"])
        self.assertNotIn("tasks", payload["phases"][0])
        self.assertEqual(payload["tasks"][0]["recommendations"], [recommendation])
        self.assertNotIn("recommendation_groups", payload["tasks"][0])
        db_config = {
            "db_uri": "postgresql://user:secret@db:5432/application",
            "db_name": "application",
        }
        connect.assert_called_once_with(db_config)
        connection.close.assert_called_once_with()
        build_plan.assert_called_once_with(db_config)

    def test_each_recommendation_is_serialized_only_once(self):
        recommendation = {"source": "global_advisor", "advisor_id": "setting"}
        task = {
            "id": "task-1",
            "recommendations": [recommendation],
            "recommendation_groups": [
                {"scope_name": "Database", "recommendations": [recommendation]}
            ],
        }

        serialized = route_executive_plan._serialize_plan(
            {"phases": [{"number": 30, "tasks": [task]}], "tasks": [task]}
        )

        self.assertEqual(serialized["tasks"][0]["recommendations"], [recommendation])
        self.assertEqual(serialized["phases"][0]["task_ids"], ["task-1"])
        self.assertEqual(str(serialized).count("global_advisor"), 1)

    def test_requires_database_uri(self):
        response = self.client.post("/api/v2/executive-plan", json={})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "'database_uri' is required.")

    def test_returns_safe_error_when_connection_fails(self):
        with (
            patch.object(
                route_executive_plan.database,
                "connectdb",
                return_value=(None, "password=secret connection refused"),
            ),
            patch.object(
                route_executive_plan.executive_plan,
                "build_executive_plan",
            ) as build_plan,
        ):
            response = self.client.post(
                "/api/v2/executive-plan",
                json={"database_uri": "postgresql://user:secret@db/application"},
            )

        self.assertEqual(response.status_code, 422)
        self.assertNotIn("secret", response.get_data(as_text=True))
        build_plan.assert_not_called()

    def test_uses_the_shared_conditional_bearer_authentication(self):
        os.environ["PGA_API_TOKEN"] = "integration-secret"
        missing = self.client.post(
            "/api/v2/executive-plan",
            json={"database_uri": "postgresql://user:secret@db/application"},
        )

        connection = MagicMock()
        with (
            patch.object(
                route_executive_plan.database,
                "connectdb",
                return_value=(connection, "OK"),
            ),
            patch.object(
                route_executive_plan.executive_plan,
                "build_executive_plan",
                return_value={"status": "ok"},
            ),
        ):
            valid = self.client.post(
                "/api/v2/executive-plan",
                json={"database_uri": "postgresql://user:secret@db/application"},
                headers={"Authorization": "Bearer integration-secret"},
            )

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(valid.status_code, 200)

    def test_route_is_documented_in_the_v2_swagger_specification(self):
        response = self.client.get("/api/v2/swagger.json")

        self.assertEqual(response.status_code, 200)
        operation = response.get_json()["paths"]["/executive-plan"]["post"]
        self.assertEqual(operation["security"], [{"BearerAuth": []}])


if __name__ == "__main__":
    unittest.main()
