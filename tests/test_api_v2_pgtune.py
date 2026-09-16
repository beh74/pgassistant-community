import os
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from apps.api_v2 import blueprint
from apps.api_v2 import pgtune as route_pgtune


class PgTuneV2ApiTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.register_blueprint(blueprint)
        self.client = self.app.test_client()
        self.token_patch = patch.dict(os.environ, {}, clear=False)
        self.token_patch.start()
        os.environ.pop("PGA_API_TOKEN", None)

    def tearDown(self):
        self.token_patch.stop()

    @staticmethod
    def _tuner(recommendations=None, alter_system_sql=""):
        tuner = MagicMock()
        tuner.get_pg_tune.return_value = recommendations or {"shared_buffers": "2GB"}
        tuner.get_alter_system.return_value = alter_system_sql
        return tuner

    def test_generates_recommendations_without_database_or_token(self):
        tuner = self._tuner()
        with patch.object(route_pgtune.pgtune, "pgTune", return_value=tuner) as pg_tune:
            response = self.client.post(
                "/api/v2/pgtune",
                json={"cpu": 4, "memory_mb": 8192, "postgresql_version": "17"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["source"], "request")
        self.assertEqual(payload["recommendations"], {"shared_buffers": "2GB"})
        pg_tune.assert_called_once_with("17", 4, "8192MB", "ssd", "web", 100)

    def test_requires_bearer_token_only_when_configured(self):
        os.environ["PGA_API_TOKEN"] = "integration-secret"

        missing = self.client.post("/api/v2/pgtune", json={"cpu": 2, "memory_mb": 2048})
        invalid = self.client.post(
            "/api/v2/pgtune",
            json={"cpu": 2, "memory_mb": 2048},
            headers={"Authorization": "Bearer wrong"},
        )

        tuner = self._tuner()
        with patch.object(route_pgtune.pgtune, "pgTune", return_value=tuner):
            valid = self.client.post(
                "/api/v2/pgtune",
                json={"cpu": 2, "memory_mb": 2048},
                headers={"Authorization": "Bearer integration-secret"},
            )

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(missing.headers["WWW-Authenticate"], "Bearer")
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(valid.status_code, 200)

    def test_uses_database_detection_and_returns_alter_system_sql(self):
        connection = MagicMock()
        resources = {"cpu": 8, "memory_mb": 16384, "environment": "docker"}
        running = {"max_connections": "250", "shared_buffers": "128MB"}
        tuner = self._tuner(alter_system_sql="ALTER SYSTEM SET shared_buffers='4GB';\n")

        with (
            patch.object(route_pgtune.database, "connectdb", return_value=(connection, "OK")),
            patch.object(
                route_pgtune.pgtune_resource_detector,
                "detect_postgresql_resources",
                return_value=resources,
            ),
            patch.object(
                route_pgtune.database,
                "get_pg_tune_parameter",
                return_value=(running, "16"),
            ),
            patch.object(route_pgtune.pgtune, "pgTune", return_value=tuner) as pg_tune,
        ):
            response = self.client.post(
                "/api/v2/pgtune",
                json={"database_uri": "postgresql://user:secret@db:5432/application"},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["source"], "database")
        self.assertEqual(payload["detected_resources"], resources)
        self.assertEqual(payload["current_values"], running)
        self.assertIn("ALTER SYSTEM", payload["alter_system_sql"])
        self.assertNotIn("database_uri", payload)
        connection.close.assert_called_once()
        pg_tune.assert_called_once_with("16", 8, "16384MB", "ssd", "web", 250)

    def test_rejects_missing_sizing_without_database_uri(self):
        response = self.client.post("/api/v2/pgtune", json={})

        self.assertEqual(response.status_code, 400)
        self.assertIn("cpu", response.get_json()["error"])

    def test_exposes_swagger_documentation_for_the_v2_route(self):
        specification = self.client.get("/api/v2/swagger.json")
        documentation = self.client.get("/api/v2/docs")

        self.assertEqual(specification.status_code, 200)
        self.assertIn("/pgtune", specification.get_json()["paths"])
        self.assertIn("BearerAuth", specification.get_json()["securityDefinitions"])
        self.assertEqual(documentation.status_code, 200)


if __name__ == "__main__":
    unittest.main()
