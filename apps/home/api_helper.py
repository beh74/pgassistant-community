"""Prepare ranked query and advisor dashboard results for API responses."""

from . import database
from . import ranking
from . import global_advisor


RANKED_QUERY_API_LIMIT = 50


def get_rank_top_50_queries(session):
    """Fetch and rank the queries exposed to the Collector API."""
    rows = database.get_rank_queries(session)
    ranked_queries = ranking.rank_queries(rows)
    return ranked_queries[:RANKED_QUERY_API_LIMIT]


def get_rank_top_50_queries_status(session):
    """Return ranked queries plus pg_stat_statements availability details."""
    rows, warning = database.get_rank_queries_status(session)
    ranked_queries = ranking.rank_queries(rows)[:RANKED_QUERY_API_LIMIT]
    return {
        "ranked_queries": ranked_queries,
        "pg_stat_statements_available": warning is None,
        "pg_stat_statements_warning": warning,
    }


# Compatibility aliases: existing reports and deployed Collectors still use the
# historical top-10 names. They now receive the same top-50 dataset; consumers
# that only need 10 rows continue to apply their own display limit.
get_rank_top_10_queries = get_rank_top_50_queries
get_rank_top_10_queries_status = get_rank_top_50_queries_status


def get_top_10_global_advisor_recommendations(session, yaml_path="advisor_enriched.yml"):
    """Fetches the top 10 global advisor recommendations from the database and ranks them using the global_advisor module."""

    result = global_advisor.run_global_advisor(session, yaml_path=yaml_path)
    print("Global Advisor Recommendations: ", result["recommendations"])
    return result["recommendations"]


def get_dev_advisor_dashboard(session, yaml_path="advisor_enriched.yml"):
    """Return DEV team advisor data prepared for the dashboard."""
    return global_advisor.run_dev_advisor_dashboard(session, yaml_path=yaml_path)


def get_postgresql_version_advisor(session):
    """Return PostgreSQL version guidance from the Global Advisor logic."""
    return global_advisor.get_postgresql_version_advisor(session)
