import unittest

from apps.home.database import (
    _uri_with_database,
    apply_pgss_database_filter,
    inject_pgss_current_db_filter,
)


class DatabaseMultiDbTests(unittest.TestCase):
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

    def test_uri_with_database_removes_dbname_query_param(self):
        uri = "postgresql://user:pass@localhost:5432/postgres?dbname=postgres&connect_timeout=5"
        updated = _uri_with_database(uri, "northwind")
        self.assertIn("/northwind", updated)
        self.assertNotIn("dbname=", updated)

    def test_inject_pgss_current_db_filter_skips_when_dbid_present(self):
        sql = "SELECT query FROM pg_stat_statements WHERE dbid = 123 ORDER BY 1"
        self.assertEqual(sql, inject_pgss_current_db_filter(sql))


if __name__ == "__main__":
    unittest.main()
