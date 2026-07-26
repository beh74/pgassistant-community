import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _load_database_module():
    repo_root = Path(__file__).resolve().parents[1]
    module_name = "apps.home.database_pgss_test"

    sys.modules.setdefault(
        "apps.home.sqlhelper",
        types.ModuleType("apps.home.sqlhelper"),
    )

    spec = importlib.util.spec_from_file_location(
        module_name,
        repo_root / "apps" / "home" / "database.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


database = _load_database_module()


class _Cursor:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.executed = []
        self.current_response = None
        self.closed = False

    def execute(self, sql):
        self.executed.append(sql)
        self.current_response = next(self.responses, None)

    def fetchone(self):
        return self.current_response

    def close(self):
        self.closed = True


class _Connection:
    def __init__(self, responses):
        self.cursor_instance = _Cursor(responses)

    def cursor(self):
        return self.cursor_instance


class EnsurePgStatStatementsTest(unittest.TestCase):
    def test_installed_extension_does_not_execute_create_extension(self):
        connection = _Connection([(True,)])

        available, error = database.ensure_pg_stat_statements(connection)

        self.assertTrue(available)
        self.assertIsNone(error)
        self.assertEqual(len(connection.cursor_instance.executed), 1)
        self.assertNotIn(
            "CREATE EXTENSION",
            connection.cursor_instance.executed[0],
        )
        self.assertTrue(connection.cursor_instance.closed)

    def test_missing_extension_reports_admin_action_for_read_only_role(self):
        connection = _Connection([(False,), ("on",)])

        available, error = database.ensure_pg_stat_statements(connection)

        self.assertFalse(available)
        self.assertIn("administrator must run", error)
        self.assertEqual(len(connection.cursor_instance.executed), 2)
        self.assertFalse(
            any(
                "CREATE EXTENSION" in sql
                for sql in connection.cursor_instance.executed
            )
        )

    def test_missing_extension_can_be_installed_by_read_write_role(self):
        connection = _Connection([(False,), ("off",), None])

        available, error = database.ensure_pg_stat_statements(connection)

        self.assertTrue(available)
        self.assertIsNone(error)
        self.assertIn(
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
            connection.cursor_instance.executed[2],
        )


if __name__ == "__main__":
    unittest.main()
