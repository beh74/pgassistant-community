import unittest
from unittest.mock import Mock, patch

from flask import Flask, session

from apps.home import routes_helpers


class DashboardConnectionTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.secret_key = "test"

    def test_connection_failure_returns_original_error_and_allows_reconnection(self):
        for multi_db in (False, True):
            with self.subTest(multi_db=multi_db), self.app.test_request_context():
                session.update(db_connected=True, db_name="example", multi_db=multi_db)
                with (
                    patch.object(routes_helpers.database, "get_db_info", return_value={
                        "profile": [], "error": "connection refused",
                    }),
                    patch.object(routes_helpers.database, "connectdb") as connect,
                    patch.object(routes_helpers, "render_template", return_value="connection page") as render,
                    patch.object(routes_helpers.collector_history, "is_configured", return_value=False),
                ):
                    result = routes_helpers.handle_dashboard_get("dashboard.html")

                self.assertEqual(result, "connection page")
                self.assertFalse(session["db_connected"])
                connect.assert_not_called()
                self.assertEqual(render.call_args.args, ("home/database.html",))
                context = render.call_args.kwargs
                self.assertEqual(context["dbinfo"]["error"], "connection refused")
                self.assertEqual(context["segment"], "database.html")
                self.assertEqual(context["connection_form"]["db_name"], "example")
                self.assertEqual(context["connection_form"]["multi_db"], multi_db)

    def test_success_keeps_dashboard_and_closes_architecture_connection(self):
        with self.app.test_request_context():
            session["db_connected"] = True
            connection = Mock()
            info = {"cache": 99, "profile": [], "error": None}
            architecture = {"type": "Standalone"}
            with (
                patch.object(routes_helpers.database, "get_db_info", return_value=info),
                patch.object(routes_helpers.database, "connectdb", return_value=(connection, "OK")),
                patch.object(routes_helpers.schema_helper, "get_database_architecture", return_value=architecture),
                patch.object(routes_helpers, "render_template", return_value="dashboard") as render,
            ):
                self.assertEqual(routes_helpers.handle_dashboard_get("dashboard.html"), "dashboard")

            render.assert_called_once_with("home/dashboard.html", segment="dashboard.html", dbinfo=info)
            self.assertEqual(info["architecture"], architecture)
            self.assertTrue(session["db_connected"])
            connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
