import os
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from flask import Flask

from apps.api_v2 import blueprint
from apps.api_v2 import executive_plan as endpoint
from apps.home import executive_plan_report as reports


class ExecutivePlanReportApiTests(unittest.TestCase):
    def setUp(self):
        app = Flask(__name__)
        app.register_blueprint(blueprint)
        self.client = app.test_client()
        token_patch = patch.dict(os.environ, {}, clear=False)
        token_patch.start()
        self.addCleanup(token_patch.stop)
        os.environ.pop("PGA_API_TOKEN", None)
        self.payload = {"database_uri": "postgresql://user:secret@db/application", "teams": ["DEV", "OPS"]}

    def test_downloads_pdf_for_each_team_selection_and_ai_option_without_session(self):
        for teams in (["DEV"], ["OPS"], ["DEV", "OPS"]):
            for include in (False, True):
                with (
                    self.subTest(teams=teams, include=include),
                    patch.object(endpoint.database, "connectdb", return_value=(MagicMock(), "OK")) as connect,
                    patch.object(reports.executive_plan, "build_executive_plan", return_value={"phases": []}) as plan,
                    patch.object(reports.database_design, "get_analysis_prompt", return_value="Schema prompt") as prompt,
                    patch.object(reports.llm, "query_chatgpt", return_value="AI analysis") as llm,
                    patch.object(reports.executive_plan_pdf, "build_executive_plan_pdf", return_value=BytesIO(b"%PDF-test")) as build,
                ):
                    response = self.client.post("/api/v2/executive-plan/report.pdf", json={**self.payload, "teams": teams, "include_db_design": include})
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.mimetype, "application/pdf")
                    self.assertEqual(response.data, b"%PDF-test")
                    self.assertIn("attachment; filename=pgassistant-executive-plan-application.pdf", response.headers["Content-Disposition"])
                    config = {"db_uri": self.payload["database_uri"], "db_name": "application"}
                    connect.assert_called_once_with(config)
                    connect.return_value[0].close.assert_called_once_with()
                    plan.assert_called_once_with(config)
                    build.assert_called_once_with({"phases": []}, teams, db_design_markdown="AI analysis" if include else None)
                    if include:
                        prompt.assert_called_once_with(config)
                        llm.assert_called_once_with("Schema prompt", render_html=False)
                    else:
                        prompt.assert_not_called()
                        llm.assert_not_called()

    def test_defaults_to_no_ai_and_deduplicates_teams(self):
        with (
            patch.object(endpoint, "_check_connection"),
            patch.object(reports, "build_report", return_value=BytesIO(b"%PDF-test")) as build,
        ):
            response = self.client.post("/api/v2/executive-plan/report.pdf", json={**self.payload, "teams": ["DEV", "DEV"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(build.call_args.args[1], ["DEV"])
        self.assertEqual(build.call_args.kwargs, {"include_db_design": False})

    def test_serves_real_pdf_from_shared_renderer(self):
        with (
            patch.object(endpoint, "_check_connection"),
            patch.object(reports.executive_plan, "build_executive_plan", return_value={"database": "application", "phases": []}),
            patch.object(reports.database_design, "get_analysis_prompt", return_value="Schema prompt"),
            patch.object(reports.llm, "query_chatgpt", return_value="# Design review\nReview the **orders** table."),
        ):
            response = self.client.post("/api/v2/executive-plan/report.pdf", json={**self.payload, "include_db_design": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data.startswith(b"%PDF"))
        self.assertGreater(len(response.data), 5000)

    def test_invalid_requests_are_rejected_before_database_or_llm_access(self):
        invalid = [None, [], "text", {}, {**self.payload, "database_uri": "https://db"}]
        invalid += [{**self.payload, "teams": teams} for teams in (None, [], "DEV", ["QA"], ["DEV", "QA"], [{}], [True])]
        invalid += [{**self.payload, "include_db_design": option} for option in (None, "true", "false", 0, 1, [], {})]
        with patch.object(endpoint, "_check_connection") as connect, patch.object(reports, "build_report") as build:
            for payload in invalid:
                with self.subTest(payload=payload):
                    response = self.client.post("/api/v2/executive-plan/report.pdf", json=payload)
                    self.assertEqual(response.status_code, 400)
                    self.assertFalse(response.get_json()["success"])
            connect.assert_not_called()
            build.assert_not_called()

    def test_authentication_runs_before_generation(self):
        os.environ["PGA_API_TOKEN"] = "api-secret"
        with patch.object(endpoint, "_check_connection") as connect, patch.object(reports, "build_report", return_value=BytesIO(b"%PDF-test")) as build:
            for headers in ({}, {"Authorization": "Bearer wrong"}):
                response = self.client.post("/api/v2/executive-plan/report.pdf", json=self.payload, headers=headers)
                self.assertEqual(response.status_code, 401)
            connect.assert_not_called()
            build.assert_not_called()
            response = self.client.post("/api/v2/executive-plan/report.pdf", json=self.payload, headers={"Authorization": "Bearer api-secret"})
            self.assertEqual(response.status_code, 200)

    def test_connection_errors_do_not_expose_credentials(self):
        with patch.object(endpoint.database, "connectdb", return_value=(None, "secret connection refused")), patch.object(reports, "build_report") as build:
            response = self.client.post("/api/v2/executive-plan/report.pdf", json=self.payload)
        self.assertEqual(response.status_code, 422)
        self.assertNotIn("secret", response.get_data(as_text=True))
        build.assert_not_called()

    def test_generation_errors_return_safe_json(self):
        with patch.object(endpoint, "_check_connection"), patch.object(reports, "build_report", side_effect=RuntimeError("secret LLM error")):
            response = self.client.post("/api/v2/executive-plan/report.pdf", json=self.payload)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.mimetype, "application/json")
        self.assertNotIn("secret", response.get_data(as_text=True))

    def test_empty_requested_ai_analysis_is_not_silently_omitted(self):
        with (
            patch.object(reports.executive_plan, "build_executive_plan", return_value={}),
            patch.object(reports.database_design, "get_analysis_prompt", return_value="Prompt"),
            patch.object(reports.llm, "query_chatgpt", return_value="  "),
            patch.object(reports.executive_plan_pdf, "build_executive_plan_pdf") as build,
        ):
            with self.assertRaisesRegex(ValueError, "empty"):
                reports.build_report({}, ["DEV"], include_db_design=True)
        build.assert_not_called()

    def test_swagger_documents_pdf_and_options(self):
        response = self.client.get("/api/v2/swagger.json")
        self.assertEqual(response.status_code, 200)
        spec = response.get_json()
        operation = spec["paths"]["/executive-plan/report.pdf"]["post"]
        self.assertEqual(operation["security"], [{"BearerAuth": []}])
        self.assertEqual(operation["produces"], ["application/pdf"])
        self.assertEqual(operation["responses"]["200"]["schema"], {"type": "file"})
        properties = spec["definitions"]["ExecutivePlanReportRequest"]["allOf"][1]["properties"]
        self.assertEqual(properties["teams"]["items"]["enum"], ["DEV", "OPS"])
        self.assertFalse(properties["include_db_design"]["default"])


if __name__ == "__main__":
    unittest.main()
