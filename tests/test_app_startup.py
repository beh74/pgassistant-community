import unittest
from unittest.mock import patch

from apps import create_app
from apps.config import ProductionConfig


class AppStartupTests(unittest.TestCase):
    def setUp(self):
        # Application imports normally initialize the persisted LLM settings.
        with patch("apps.home.config.init_or_load_env"):
            self.app = create_app(ProductionConfig)
        self.app.config.update(TESTING=True, SECRET_KEY="test")
        self.client = self.app.test_client()

    def test_entry_points_redirect_to_database_connection(self):
        for path in ("/", "/index"):
            with self.subTest(path=path):
                response = self.client.get(path)
                self.assertEqual(response.status_code, 302)
                self.assertEqual(response.headers["Location"], "/database.html")

    def test_database_page_and_tools_keep_working_with_sessions(self):
        with self.client.session_transaction() as session:
            session["db_name"] = "example"
            session["db_connected"] = False
        self.assertEqual(self.client.get("/database.html").status_code, 200)
        response = self.client.get("/tools")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b'href="/logout"', response.data)
        with self.client.session_transaction() as session:
            self.assertEqual(session["db_name"], "example")

    def test_removed_account_routes_and_settings_return_not_found(self):
        for path in ("/login", "/register", "/logout", "/github", "/index/github", "/settings.html"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 404)


if __name__ == "__main__":
    unittest.main()
