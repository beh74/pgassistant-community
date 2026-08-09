import unittest

from apps.home.column_statistics import _parse_array, _parse_float_array


class ColumnStatisticsTests(unittest.TestCase):
    def test_parses_quoted_postgresql_array_values(self):
        self.assertEqual(
            _parse_array('{"North, America",Europe,"A\\"B"}'),
            ["North, America", "Europe", 'A"B'],
        )

    def test_parses_frequency_array(self):
        self.assertEqual(_parse_float_array("{0.5,0.25}"), [0.5, 0.25])

    def test_empty_array(self):
        self.assertEqual(_parse_array("{}"), [])


if __name__ == "__main__":
    unittest.main()
