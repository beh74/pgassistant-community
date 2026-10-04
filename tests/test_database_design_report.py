import unittest
from io import BytesIO
from unittest.mock import Mock, patch

from flask import Flask

from apps.home import database_design, report_pdf, route_reports, routes_helpers


class DatabaseDesignPromptTests(unittest.TestCase):
    def test_edited_prompt_does_not_query_database(self):
        with patch.object(database_design.database, "connectdb") as connect:
            self.assertEqual(database_design.get_analysis_prompt({}, "  Custom prompt  "), "Custom prompt")
        connect.assert_not_called()

    def test_collects_workload_and_closes_connection(self):
        connection = Mock()
        config = {"db_name": "example"}
        with (
            patch.object(database_design.database, "connectdb", return_value=(connection, "OK")),
            patch.object(database_design.query_table_stats, "load_top_table_workload", return_value=[{"table": "orders"}]) as workload,
            patch.object(database_design.schema_helper, "get_database_schema_llm_context", return_value={"llm_prompt": "Generated prompt"}) as context,
        ):
            self.assertEqual(database_design.get_analysis_prompt(config), "Generated prompt")
        workload.assert_called_once_with(config, limit=None)
        context.assert_called_once_with(connection, table_workload=[{"table": "orders"}])
        connection.close.assert_called_once_with()

    def test_context_failure_still_closes_connection(self):
        connection = Mock()
        with (
            patch.object(database_design.database, "connectdb", return_value=(connection, "OK")),
            patch.object(database_design.query_table_stats, "load_top_table_workload", side_effect=RuntimeError("workload failed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "workload failed"):
                database_design.get_analysis_prompt({})
        connection.close.assert_called_once_with()

    def test_connection_failure_is_reported(self):
        with patch.object(database_design.database, "connectdb", return_value=(None, "connection refused")):
            with self.assertRaisesRegex(RuntimeError, "connection refused"):
                database_design.get_analysis_prompt({})


class DatabaseDesignReportTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "test"
        self.app.register_blueprint(route_reports.blueprint)
        self.client = self.app.test_client()

    def connect(self):
        with self.client.session_transaction() as session:
            session.update(db_connected=True, db_name="example/db")

    def test_requires_connection_before_calling_llm(self):
        with patch.object(route_reports.llm, "query_chatgpt") as query:
            response = self.client.post("/database-analyze/report.pdf")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.location.endswith("/database.html"))
        query.assert_not_called()

    def test_downloads_only_llm_analysis_using_edited_prompt(self):
        self.connect()
        with (
            patch.object(route_reports.llm, "query_chatgpt", return_value="# Schema findings\nReview **orders**.") as query,
            patch.object(route_reports.executive_plan, "build_executive_plan") as plan,
            patch.object(database_design.database, "connectdb") as connect,
        ):
            response = self.client.post("/database-analyze/report.pdf", data={"llm_prompt": "Custom prompt"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertTrue(response.data.startswith(b"%PDF"))
        self.assertIn("pgassistant-database-design-example-db.pdf", response.headers["Content-Disposition"])
        query.assert_called_once_with("Custom prompt", render_html=False)
        plan.assert_not_called()
        connect.assert_not_called()

    def test_missing_prompt_uses_shared_context_builder(self):
        self.connect()
        with (
            patch.object(database_design, "get_analysis_prompt", return_value="Generated prompt") as prompt,
            patch.object(route_reports.llm, "query_chatgpt", return_value="Analysis") as query,
            patch.object(report_pdf, "build_database_design_pdf", return_value=BytesIO(b"%PDF-test")),
        ):
            response = self.client.post("/database-analyze/report.pdf")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(prompt.call_args.args[1], "")
        query.assert_called_once_with("Generated prompt", render_html=False)

    def test_llm_failure_returns_error_page_not_pdf(self):
        self.connect()
        with (
            patch.object(route_reports.llm, "query_chatgpt", side_effect=RuntimeError("LLM unavailable")),
            patch.object(route_reports, "render_template", return_value="Error") as render,
            patch.object(report_pdf, "build_database_design_pdf") as build,
        ):
            response = self.client.post("/database-analyze/report.pdf", data={"llm_prompt": "Prompt"})
        self.assertEqual(response.status_code, 500)
        self.assertEqual(render.call_args.args[0], "home/page-500.html")
        build.assert_not_called()

    def test_executive_report_reuses_context_and_keeps_optional_analysis(self):
        self.connect()
        for include in (False, True):
            with (
                self.subTest(include=include),
                patch.object(route_reports.executive_plan, "build_executive_plan", return_value={"tasks": []}) as plan,
                patch.object(database_design, "get_analysis_prompt", return_value="Shared prompt") as prompt,
                patch.object(route_reports.llm, "query_chatgpt", return_value="Analysis") as query,
                patch.object(route_reports.executive_plan_report.executive_plan_pdf, "build_executive_plan_pdf", return_value=BytesIO(b"%PDF-test")) as build,
            ):
                response = self.client.post("/executive-plan/report.pdf", data={"teams": "DEV", "include_db_design": "1" if include else "0"})
                self.assertEqual(response.status_code, 200)
                plan.assert_called_once()
                self.assertEqual(prompt.call_count, int(include))
                self.assertEqual(query.call_count, int(include))
                build.assert_called_once_with({"tasks": []}, ["DEV"], db_design_markdown="Analysis" if include else None)

    def test_html_analysis_reuses_prompt_and_preserves_html_output(self):
        with self.app.test_request_context(method="POST", data={"llm_prompt": "Custom prompt"}):
            from flask import session
            session["db_name"] = "example"
            with (
                patch.object(database_design, "get_analysis_prompt", return_value="Custom prompt") as prompt,
                patch.object(routes_helpers.llm, "query_chatgpt", return_value="<p>Analysis</p>") as query,
                patch.object(routes_helpers, "render_template", return_value="HTML") as render,
            ):
                self.assertEqual(routes_helpers.handle_database_analyze_llm_post("unused", "database_analyze_llm.html"), "HTML")
            prompt.assert_called_once_with(session, "Custom prompt")
            query.assert_called_once_with("Custom prompt")
            self.assertEqual(render.call_args.kwargs["chatgpt_response"], "<p>Analysis</p>")


class DatabaseDesignPdfTests(unittest.TestCase):
    def test_empty_analysis_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "empty"):
            report_pdf.build_database_design_pdf("example", "  ")

    def test_standalone_report_has_no_plan_sections(self):
        with patch.object(report_pdf, "finish_report") as finish:
            report_pdf.build_database_design_pdf("example", "# Findings\n**Orders** needs an index.")
        story = finish.call_args.args[2]
        text = "\n".join(item.getPlainText() for item in story if hasattr(item, "getPlainText"))
        self.assertIn("Database Design Analysis", text)
        self.assertIn("Orders needs an index.", text)
        self.assertNotIn("Plan overview", text)
        self.assertNotIn("Executive Plan", text)
        self.assertNotIn("No work package", text)


if __name__ == "__main__":
    unittest.main()
