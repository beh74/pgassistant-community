import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    'pgtune_manual_features', Path(__file__).resolve().parents[1] / 'apps/home/pgtune.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class ManualFeaturesTests(unittest.TestCase):
    def tuner(self, compression='lz4', method='io_uring'):
        tuner = module.pgTune('18', 2, '64GB', 'ssd', 'web', 200)
        tuner.db_tune = {'wal_compression': compression, 'io_method': method, 'jit': 'off'}
        return tuner

    def test_compose_options_are_yaml_comments_outside_command(self):
        output = self.tuner().get_docker_cmd(
            {'db_name': 'test', 'db_port': 5432, 'db_user': 'test'}, '18')
        uncommented = '\n'.join(line for line in output.splitlines()
                                if not line.lstrip().startswith('#'))
        self.assertNotIn('wal_compression', uncommented)
        self.assertNotIn('io_method', uncommented)
        self.assertIn("-c jit='off'", uncommented)
        self.assertIn("# Optional after verification: -c wal_compression='lz4'", output)
        self.assertIn("# Optional after verification: -c io_method='io_uring'", output)
        self.assertIn('LZ4 build support', output)
        self.assertIn('kernel/container permission', output)
        self.assertNotIn('#', output.split('    command: >\n', 1)[1])

    def test_apply_options_are_sql_comments_even_with_missing_current_settings(self):
        for current in ({}, {'jit': 'on', 'io_method': 'worker', 'wal_compression': 'off'}):
            with self.subTest(current=current):
                output = self.tuner().get_alter_system(current)
                executable = '\n'.join(line for line in output.splitlines()
                                       if not line.lstrip().startswith('--'))
                self.assertNotIn('wal_compression', executable)
                self.assertNotIn('io_method', executable)
                self.assertIn("-- Optional after verification: ALTER SYSTEM SET wal_compression='lz4';", output)
                self.assertIn("-- Optional after verification: ALTER SYSTEM SET io_method='io_uring';", output)
                if current:
                    self.assertIn("ALTER SYSTEM SET jit='off';", executable)

    def test_postgresql_conf_exports_complete_baseline_with_manual_options_commented(self):
        tuner = self.tuner()
        tuner.db_tune.update({'max_connections': '200', 'shared_buffers': '16GB'})
        output = tuner.get_postgresql_conf()
        active = '\n'.join(line for line in output.splitlines() if not line.startswith('#'))
        self.assertIn('max_connections = 200', active)
        self.assertIn("shared_buffers = '16GB'", active)
        self.assertIn("jit = 'off'", active)
        self.assertNotIn('wal_compression', active)
        self.assertNotIn('io_method', active)
        self.assertIn("# wal_compression = 'lz4'", output)
        self.assertIn("# io_method = 'io_uring'", output)
        self.assertIn('LZ4 build support', output)
        self.assertIn('kernel/container permission', output)
        self.assertIn("wal_compression = 'on'", self.tuner(compression='on').get_postgresql_conf())

    def test_plain_wal_compression_remains_enabled(self):
        tuner = self.tuner(compression='on', method='worker')
        output = tuner.get_alter_system({'wal_compression': 'off', 'io_method': 'sync', 'jit': 'on'})
        executable = '\n'.join(line for line in output.splitlines() if not line.startswith('--'))
        self.assertIn("ALTER SYSTEM SET wal_compression='on';", executable)
        self.assertNotIn('io_method', executable)
