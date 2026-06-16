from . import database
from . import ranking
from . import global_advisor


def get_rank_top_10_queries (session):
    '''Fetches the top 10 queries from the database and ranks them using the ranking module.'''        
    rows = database.get_rank_queries(session)
    ranked_queries = ranking.rank_queries(rows)

    
    return ranked_queries[:10]


def get_top_10_global_advisor_recommendations(session, yaml_path="advisor_enriched.yml"):
    """Fetches the top 10 global advisor recommendations from the database and ranks them using the global_advisor module."""

    result = global_advisor.run_global_advisor(session, yaml_path=yaml_path)
    print("Global Advisor Recommendations: ", result["recommendations"])
    return result["recommendations"]


"""
curl -X GET http://localhost:8080/api/v1/rank_top_10_queries \
  -H "Content-Type: application/json" \
  -d '{
    "db_config": {
      "db_host": "host.docker.internal",
      "db_port": 5420,
      "db_name": "northwind",
      "db_user": "postgres",
      "db_password": "demo"
    }
  }'

  
curl -X GET http://localhost:8080/api/v1/rank_top_10_queries \
  -H "Content-Type: application/json" \
  -d '{
    "db_config": {
      "db_uri": "postgresql://postgres:demo@host.docker.internal:5421/demo?options=-c%20search_path%3Dapp%2Cbookings"
    }
  }'  
"""