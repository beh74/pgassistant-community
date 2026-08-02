import unittest

from werkzeug.datastructures import ImmutableMultiDict

from apps.home.database import (
    _uri_with_database,
    apply_pgss_database_filter,
    get_resolved_database_name,
    inject_pgss_current_db_filter,
    resolve_db_config,
)
from apps.home.routes_helpers import _db_config_from_form, get_cluster_database_names


class DatabaseMultiDbTests(unittest.TestCase):
    def test_resolved_database_name_uses_active_database_in_multi_db_mode(self):
        config = {
            "db_name": "postgres",
            "active_db": "application",
            "multi_db": True,
        }

        self.assertEqual(get_resolved_database_name(config), "application")

    def test_resolved_database_name_keeps_database_in_single_db_mode(self):
        config = {
            "db_name": "postgres",
            "active_db": "ignored",
            "multi_db": False,
        }

        self.assertEqual(get_resolved_database_name(config), "postgres")

    def test_cached_cluster_database_names_are_sorted_alphabetically(self):
        session_obj = {
            "cluster_databases": ["zeta", "Alpha", "beta", "analytics"],
        }

        self.assertEqual(
            get_cluster_database_names(session_obj),
            ["Alpha", "analytics", "beta", "zeta"],
        )

    def test_uri_with_database_replaces_existing_db(self):
        uri = "postgresql://user:pass@localhost:5432/postgres?connect_timeout=5"
        updated = _uri_with_database(uri, "northwind")
        self.assertIn("/northwind", updated)

    def test_inject_pgss_current_db_filter_before_order_by(self):
        sql = (
            "SELECT query FROM pg_stat_statements "
            "WHERE calls > 0 ORDER BY total_exec_time DESC LIMIT 50"
        )
        filtered = inject_pgss_current_db_filter(sql)
        self.assertIn("current_database()", filtered)
        self.assertIn("ORDER BY total_exec_time DESC", filtered)
        self.assertLess(filtered.index("current_database()"), filtered.index("ORDER BY"))

    def test_apply_pgss_database_filter_uses_explicit_db_name(self):
        sql = (
            "SELECT query FROM pg_stat_statements "
            "WHERE calls > 0 ORDER BY total_exec_time DESC LIMIT 50"
        )
        filtered = apply_pgss_database_filter(sql, "northwind")
        self.assertIn("datname = 'northwind'", filtered)
        self.assertLess(filtered.index("northwind"), filtered.index("ORDER BY"))

    def test_apply_pgss_database_filter_appends_to_existing_where(self):
        sql = "select query from pg_stat_statements where queryid=123"
        filtered = apply_pgss_database_filter(sql, "appdb")
        self.assertIn("where queryid=123", filtered.lower())
        self.assertIn("datname = 'appdb'", filtered)

    def test_apply_pgss_database_filter_handles_cte_with_outer_cross_join(self):
        sql = (
            "WITH head AS ("
            "SELECT query FROM pg_stat_statements WHERE calls > 0"
            "), agg AS (SELECT count(*) AS total FROM head) "
            "SELECT total FROM agg CROSS JOIN (SELECT 1) AS totals "
            "ORDER BY total DESC"
        )

        filtered = apply_pgss_database_filter(sql, "appdb")

        self.assertIn("FROM (SELECT * FROM pg_stat_statements WHERE dbid", filtered)
        self.assertNotIn("CROSS JOIN (SELECT 1) AS totals  AND", filtered)
        self.assertIn("ORDER BY total DESC", filtered)

    def test_uri_with_database_removes_dbname_query_param(self):
        uri = "postgresql://user:pass@localhost:5432/postgres?dbname=postgres&connect_timeout=5"
        updated = _uri_with_database(uri, "northwind")
        self.assertIn("/northwind", updated)
        self.assertNotIn("dbname=", updated)

    def test_inject_pgss_current_db_filter_skips_when_dbid_present(self):
        sql = "SELECT query FROM pg_stat_statements WHERE dbid = 123 ORDER BY 1"
        self.assertEqual(sql, inject_pgss_current_db_filter(sql))

    def test_db_config_from_form_ignores_stale_active_db(self):
        session_obj = {
            "db_host": "old-host",
            "db_port": "5432",
            "db_name": "postgres",
            "db_user": "postgres",
            "db_password": "secret",
            "multi_db": True,
            "active_db": "legacy_app",
            "cluster_databases": ["legacy_app", "legacy_other"],
        }
        form = ImmutableMultiDict(
            [
                ("db_host", "new-host"),
                ("db_port", "5433"),
                ("db_name", "appdb"),
                ("db_user", "app"),
                ("db_password", "newsecret"),
                ("multi_db", "on"),
            ]
        )
        merged = _db_config_from_form(form, session_obj)
        self.assertNotIn("active_db", merged)
        self.assertNotIn("cluster_databases", merged)
        self.assertEqual(merged["db_host"], "new-host")
        self.assertEqual(merged["db_name"], "appdb")
        resolved = resolve_db_config(merged)
        self.assertEqual(resolved["db_name"], "appdb")


if __name__ == "__main__":
    unittest.main()
