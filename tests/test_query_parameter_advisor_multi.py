import unittest
from unittest.mock import MagicMock, patch

from apps.home import query_parameter_advisor


class QueryParameterAdvisorMultiDatabaseTests(unittest.TestCase):
    @patch.object(query_parameter_advisor, "_build_recommendations", return_value=[])
    @patch.object(query_parameter_advisor, "_fetch_pg_stat_statements_rows", return_value=[])
    @patch.object(query_parameter_advisor.database, "get_resolved_database_name", return_value="application")
    @patch.object(query_parameter_advisor.database, "get_monitoring_db_config")
    @patch.object(query_parameter_advisor.database, "is_multi_db", return_value=True)
    @patch.object(query_parameter_advisor.database, "get_pg_tune_parameter", return_value=({}, 16))
    @patch.object(query_parameter_advisor, "_get_postgres_major_version", return_value=16)
    @patch.object(query_parameter_advisor.database, "connectdb")
    def test_reads_pgss_from_monitoring_database_and_plans_on_active_database(
        self,
        connectdb,
        _major_version,
        _pg_tune,
        _is_multi_db,
        get_monitoring_db_config,
        _database_name,
        fetch_pgss,
        _build_recommendations,
    ):
        plan_conn = MagicMock(name="plan_conn")
        stats_conn = MagicMock(name="stats_conn")
        connectdb.side_effect = [(plan_conn, "OK"), (stats_conn, "OK")]
        config = {"db_name": "postgres", "active_db": "application", "multi_db": True}
        monitoring_config = {"db_name": "postgres", "multi_db": False}
        get_monitoring_db_config.return_value = monitoring_config

        result = query_parameter_advisor.analyze_query_parameter_workload(config)

        self.assertTrue(result["success"])
        self.assertEqual(connectdb.call_args_list[0].args[0], config)
        self.assertEqual(connectdb.call_args_list[1].args[0], monitoring_config)
        fetch_pgss.assert_called_once_with(stats_conn, "application")
        stats_conn.close.assert_called_once_with()
        plan_conn.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
