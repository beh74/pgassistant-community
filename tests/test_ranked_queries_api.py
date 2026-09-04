import unittest
from unittest.mock import patch

from apps.home import api_helper


class RankedQueriesApiHelperTests(unittest.TestCase):
    def test_ranked_query_api_returns_up_to_50_queries(self):
        rows = [{"queryid": str(index)} for index in range(75)]

        with (
            patch.object(api_helper.database, "get_rank_queries", return_value=rows),
            patch.object(api_helper.ranking, "rank_queries", return_value=rows),
        ):
            result = api_helper.get_rank_top_50_queries({})

        self.assertEqual(len(result), 50)
        self.assertEqual(result[-1]["queryid"], "49")

    def test_status_response_returns_up_to_50_queries(self):
        rows = [{"queryid": str(index)} for index in range(60)]

        with (
            patch.object(
                api_helper.database,
                "get_rank_queries_status",
                return_value=(rows, None),
            ),
            patch.object(api_helper.ranking, "rank_queries", return_value=rows),
        ):
            result = api_helper.get_rank_top_50_queries_status({})

        self.assertEqual(len(result["ranked_queries"]), 50)
        self.assertTrue(result["pg_stat_statements_available"])

    def test_legacy_helper_name_uses_the_top_50_limit(self):
        self.assertIs(api_helper.get_rank_top_10_queries, api_helper.get_rank_top_50_queries)


if __name__ == "__main__":
    unittest.main()
