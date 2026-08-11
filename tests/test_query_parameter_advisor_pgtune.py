import unittest

from apps.home.query_parameter_advisor import _build_pgtune_recommendation


class QueryParameterAdvisorPgTuneTests(unittest.TestCase):
    def setUp(self):
        self.resources = {
            "cpu": 4,
            "memory_mb": 8192,
            "environment": "docker",
        }

    def test_ignores_differences_at_or_below_twenty_percent(self):
        recommendation = _build_pgtune_recommendation(
            {
                "shared_buffers": "1800MB",
                "work_mem": "10MB",
                "default_statistics_target": "120",
            },
            {
                "shared_buffers": "2GB",
                "work_mem": "10MB",
                "default_statistics_target": "100",
            },
            self.resources,
        )

        self.assertEqual(recommendation, {})

    def test_returns_one_sql_script_for_all_significant_changes(self):
        recommendation = _build_pgtune_recommendation(
            {
                "shared_buffers": "1GB",
                "effective_cache_size": "3GB",
                "random_page_cost": "4",
            },
            {
                "shared_buffers": "2GB",
                "effective_cache_size": "6GB",
                "random_page_cost": "1.1",
            },
            self.resources,
        )

        self.assertEqual(len(recommendation["changes"]), 3)
        sql = recommendation["alter_system_sql"]
        self.assertEqual(sql.count("ALTER SYSTEM SET"), 3)
        self.assertEqual(sql.count("SELECT pg_reload_conf();"), 1)
        self.assertIn("Generated from pgTune", sql)
        self.assertEqual(recommendation["source"], "pgtune")
        self.assertTrue(recommendation["restart_required"])
        self.assertIn("shared_buffers", recommendation["restart_parameters"])


if __name__ == "__main__":
    unittest.main()
