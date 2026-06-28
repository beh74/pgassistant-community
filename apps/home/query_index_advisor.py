# -*- encoding: utf-8 -*-
"""Index advisor orchestration for top ranked queries."""
from __future__ import annotations

from typing import Any, Dict, List

from . import analyze_advisor
from . import database
from . import ranking
from . import sqlhelper


INTERNAL_SCHEMAS = {"pg_catalog", "information_schema"}


def _get_postgres_major_version(db_config: Dict[str, Any]) -> int:
    configured_version = db_config.get("version")
    if configured_version:
        try:
            return int(str(configured_version).split(".")[0])
        except (TypeError, ValueError):
            pass

    conn, status = database.connectdb(db_config)
    if conn is None:
        raise RuntimeError(status or "Unable to connect to database.")

    try:
        rows, _description = database.db_query(conn, "db_version")
        version = rows[0]["server_version"] if rows else ""
        return database.get_pg_major_version(str(version))
    finally:
        conn.close()


def _sort_recommendations(recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    priority = {
        "safe": 0,
        "review": 1,
        "none": 2,
        "info": 3,
    }
    return sorted(
        recommendations or [],
        key=lambda rec: priority.get(rec.get("confidence"), 99),
    )


def _query_text_for_row(db_config: Dict[str, Any], row: Dict[str, Any]) -> str:
    if row.get("query"):
        return row["query"]
    return database.get_pgstat_query_by_id(db_config, str(row.get("queryid")))


def _queryid_as_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _generic_plan_for_query(db_config: Dict[str, Any], query: str) -> Any:
    normalized_query = sqlhelper.normalize_query_for_parameter_analysis(query)
    explain_sql = (
        "EXPLAIN (GENERIC_PLAN TRUE, VERBOSE TRUE, SETTINGS TRUE, FORMAT JSON) "
        + normalized_query
    )
    rows = database.generic_select_with_sql(db_config, explain_sql)
    if not rows:
        raise RuntimeError("Generic plan returned no rows.")
    return rows[0]["QUERY PLAN"]


def _walk_plan_nodes(node: Any):
    if isinstance(node, list):
        for item in node:
            yield from _walk_plan_nodes(item)
        return

    if not isinstance(node, dict):
        return

    if "Plan" in node and isinstance(node["Plan"], dict):
        yield from _walk_plan_nodes(node["Plan"])
        return

    yield node

    for child in node.get("Plans") or []:
        yield from _walk_plan_nodes(child)


def _plan_uses_internal_schema(plan_json: Any) -> bool:
    for node in _walk_plan_nodes(plan_json):
        schema = str(node.get("Schema") or "").strip()
        if schema in INTERNAL_SCHEMAS or schema.startswith("pg_toast"):
            return True
    return False


def analyze_top_ranked_query_indexes(
    db_config: Dict[str, Any],
    limit: int = 10,
) -> Dict[str, Any]:
    """Run generic plans for top ranked queries and analyze index opportunities."""
    pg_major_version = _get_postgres_major_version(db_config)
    if pg_major_version < 16:
        return {
            "success": True,
            "supported": False,
            "required_version": 16,
            "postgres_major_version": pg_major_version,
            "message": "Generic plans require PostgreSQL 16 or newer.",
            "results": [],
            "summary": {
                "queries_planned": 0,
                "queries_analyzed": 0,
                "queries_skipped_internal": 0,
                "queries_without_recommendation": 0,
                "queries_failed": 0,
                "recommendations": 0,
                "actionable_recommendations": 0,
            },
        }

    rows = database.get_rank_queries(db_config)
    ranked_queries = ranking.rank_queries(rows)[:limit]

    results = []
    queries_planned = 0
    queries_advisor_run = 0
    queries_skipped_internal = 0
    queries_without_recommendation = 0
    queries_failed = 0
    recommendation_count = 0
    actionable_count = 0

    for row in ranked_queries:
        queryid = _queryid_as_text(row.get("queryid"))
        row["queryid"] = queryid
        result = {
            "queryid": queryid,
            "query": row.get("query") or "",
            "ranking": row,
            "ok": False,
            "error": None,
            "recommendations": [],
            "actionable_recommendations": [],
            "observations": [],
        }

        if not queryid:
            queries_failed += 1
            continue

        try:
            query = _query_text_for_row(db_config, row)
            if not query.strip():
                raise RuntimeError("Query text not found in pg_stat_statements.")
            result["query"] = query

            plan_json = _generic_plan_for_query(db_config, query)
            queries_planned += 1

            if _plan_uses_internal_schema(plan_json):
                queries_skipped_internal += 1
                continue

            queries_advisor_run += 1
            advisor_result = analyze_advisor.analyze_plan_for_safe_indexes(
                plan_json,
                db_config,
                queryid,
            )

            recommendations = _sort_recommendations(
                advisor_result.get("recommendations") or []
            )
            actionable = _sort_recommendations(
                advisor_result.get("actionable_recommendations") or []
            )
            if not actionable:
                queries_without_recommendation += 1
                continue

            observations = advisor_result.get("observations") or []

            result.update(
                {
                    "ok": bool(advisor_result.get("ok")),
                    "message": advisor_result.get("message"),
                    "recommendations": recommendations,
                    "actionable_recommendations": actionable,
                    "observations": observations,
                    "query_stats": advisor_result.get("query_stats"),
                }
            )
            recommendation_count += len(recommendations)
            actionable_count += len(actionable)
            results.append(result)
        except Exception as exc:
            result["error"] = str(exc)
            queries_failed += 1

    return {
        "success": True,
        "supported": True,
        "required_version": 16,
        "postgres_major_version": pg_major_version,
        "query_limit": limit,
        "results": results,
        "summary": {
            "queries_planned": queries_planned,
            "queries_analyzed": queries_advisor_run,
            "queries_skipped_internal": queries_skipped_internal,
            "queries_without_recommendation": queries_without_recommendation,
            "queries_failed": queries_failed,
            "recommendations": recommendation_count,
            "actionable_recommendations": actionable_count,
        },
    }
