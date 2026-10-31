import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock

SPEC = importlib.util.spec_from_file_location(
    'hugepages', Path(__file__).resolve().parents[1] / 'apps/home/pgtune_hugepages.py')
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class HugePagesTests(unittest.TestCase):
    def test_margin_rounds_up_and_default_pool_guidance(self):
        result = module.build_guidance({'shared_memory_size_in_huge_pages': '3170',
                                       'huge_page_size': '0', 'shared_memory_type': 'mmap'})
        self.assertEqual(result['recommended_pages'], 3329)
        self.assertIn('vm.nr_hugepages = 3329', ' '.join(result['actions']))

    def test_unavailable_counts_never_generate_reservation(self):
        for count in (None, '-1', '0', 'invalid'):
            result = module.build_guidance({'shared_memory_size_in_huge_pages': count})
            self.assertIsNone(result['recommended_pages'])
            self.assertNotIn('vm.nr_hugepages =', ' '.join(result['actions']))

    def test_non_default_size_does_not_use_default_sysctl(self):
        result = module.build_guidance({'shared_memory_size_in_huge_pages': '100',
                                       'huge_page_size': '1GB'}, True)
        text = ' '.join(result['actions'])
        self.assertNotIn('vm.nr_hugepages =', text)
        self.assertIn('pending restarts', text)
        self.assertIn('matching pool', text)

    def test_inspection_handles_missing_version_specific_settings(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = [('huge_pages', 'try', False)]
        cursor.fetchone.return_value = (False,)
        result = module.inspect_huge_pages(connection)
        self.assertEqual(result['settings'], {'huge_pages': 'try'})
        self.assertIsNone(result['recommended_pages'])
        cursor.close.assert_called_once()

    def test_inspection_closes_cursor_on_error(self):
        connection = Mock()
        cursor = connection.cursor.return_value
        cursor.execute.side_effect = RuntimeError('unavailable')
        with self.assertRaises(RuntimeError):
            module.inspect_huge_pages(connection)
        cursor.close.assert_called_once()
