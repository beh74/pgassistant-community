"""Exercise the settings reader without importing unrelated application dependencies."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock


class PgTuneSettingsTests(unittest.TestCase):
    def test_reads_available_parameters_and_preserves_version_detection(self):
        source = Path(__file__).resolve().parents[1] / 'apps/home/database.py'
        tree = ast.parse(source.read_text())
        function = next(node for node in tree.body
                        if isinstance(node, ast.FunctionDef) and node.name == 'get_pg_tune_parameter')
        connection = Mock()
        connection.cursor.return_value.fetchall.return_value = [
            ('shared_buffers', '16GB'), ('jit', 'on'), ('wal_compression', 'off'),
            ('autovacuum_work_mem', '-1'), ('io_method', 'worker')]
        namespace = {
            'connectdb': Mock(return_value=(connection, 'OK')),
            'db_query': Mock(return_value=([{'server_version': '18.1'}], None)),
            'get_pg_major_version': Mock(return_value=18),
        }
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
        values, major = namespace['get_pg_tune_parameter']({})
        self.assertEqual(major, '18')
        self.assertEqual(values['io_method'], 'worker')
        self.assertEqual(values['autovacuum_work_mem'], '-1')
        query, params = connection.cursor.return_value.execute.call_args.args
        self.assertIn('WHERE name = ANY(%s)', query)
        for setting in ('jit', 'wal_compression', 'autovacuum_work_mem', 'io_method'):
            self.assertIn(setting, params[0])
        connection.cursor.return_value.close.assert_called_once()
        connection.close.assert_called_once()
