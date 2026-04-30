import json
import yaml
from typing import Any, Dict, List, Optional

from .global_advisor_models import (
    GlobalRecommendation,
    RecommendationCategory,
    ObjectType,
)

from .database import (
    connectdb,
    db_fetch_json,
)

def sql_literal(value: str) -> str:
    """
    Safely quotes a Python string as a SQL literal.
    Minimal helper for internal generated SQL.
    """
    return "'" + value.replace("'", "''") + "'"

def get_estimated_table_rows(conn, schema_name: str, table_name: str) -> Optional[int]:
    """
    Returns the estimated number of live rows for a table.

    Uses pg_stat_user_tables, so it does not scan or analyze the table.
    """
    if not schema_name or not table_name:
        return None

    sql = f"""
        SELECT COALESCE(n_live_tup, 0) AS estimated_rows
        FROM pg_stat_user_tables
        WHERE schemaname = {sql_literal(schema_name)}
          AND relname = {sql_literal(table_name)}
        LIMIT 1;
    """

    raw_result = db_fetch_json(conn, sql)
    rows = json.loads(raw_result)

    if not rows:
        return None

    return rows[0].get("estimated_rows")

def enrich_recommendation_with_table_stats(conn, rec: GlobalRecommendation) -> GlobalRecommendation:
    """
    Adds estimated table row count when schema/table are known.
    """
    if rec.schema_name and rec.table_name:
        rec.estimated_rows = get_estimated_table_rows(
            conn,
            rec.schema_name,
            rec.table_name,
        )

    return rec

def load_recommendation_catalog(yaml_path: str) -> List[Dict[str, Any]]:
    """
    Loads the Global Advisor recommendation catalog from a YAML file.
    """
    with open(yaml_path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}

    return data.get("recommendations", [])


def compute_rank(confidence: int, impact: int, effort: int) -> int:
    """
    Computes a normalized rank between 0 and 100.

    Higher impact and confidence increase the rank.
    Higher effort decreases the rank.
    """
    rank = int(
        0.5 * impact
        + 0.3 * confidence
        + 0.2 * (100 - effort)
    )

    return max(0, min(100, rank))


def get_mapped_value(
    row: Dict[str, Any],
    mapping: Dict[str, str],
    field_name: str,
    default: Optional[Any] = None,
) -> Any:
    """
    Returns a value from a SQL result row using the YAML result_mapping section.
    """
    column_name = mapping.get(field_name)

    if not column_name:
        return default

    return row.get(column_name, default)


def build_recommendation_from_row(
    definition: Dict[str, Any],
    row: Dict[str, Any],
) -> GlobalRecommendation:
    """
    Builds one GlobalRecommendation from one SQL result row.
    """
    mapping = definition.get("result_mapping", {})

    confidence = int(definition.get("confidence", 50))
    impact = int(definition.get("impact", 50))
    effort = int(definition.get("effort", 50))

    rank = compute_rank(confidence, impact, effort)

    return GlobalRecommendation(
        rank=rank,

        category_id=RecommendationCategory(definition.get("category_id", "OTHER")),
        label=definition.get("label"),
        source=definition.get("source", definition.get("id", "unknown")),

        object_type=ObjectType(definition.get("object_type", "OTHER")),
        object_id=get_mapped_value(row, mapping, "object_id"),
        object_name=get_mapped_value(row, mapping, "object_name"),

        schema_name=get_mapped_value(row, mapping, "schema_name"),
        table_name=get_mapped_value(row, mapping, "table_name"),
        index_name=get_mapped_value(row, mapping, "index_name"),
        column_name=get_mapped_value(row, mapping, "column_name"),

        schema_id=get_mapped_value(row, mapping, "schema_id"),
        table_id=get_mapped_value(row, mapping, "table_id"),
        query_id=get_mapped_value(row, mapping, "query_id"),

        improvement_sql=get_mapped_value(row, mapping, "improvement_sql"),

        recommendation_note=get_mapped_value(row, mapping, "recommendation_note"),

        confidence=confidence,
        impact=impact,
        effort=effort,

        manual_review_required=definition.get("manual_review_required", True),
    )


def run_sql_recommendation(
    conn,
    definition: Dict[str, Any],
) -> List[GlobalRecommendation]:
    """
    Executes one SQL-based recommendation definition and returns recommendations.
    """
    sql = definition.get("sql")

    if not sql:
        return []

    raw_result = db_fetch_json(conn, sql)
    rows = json.loads(raw_result)

    recommendations = []

    for row in rows:
        recommendation = build_recommendation_from_row(definition, row)
        recommendation = enrich_recommendation_with_table_stats(conn, recommendation)
        recommendations.append(recommendation)

    return recommendations


def run_global_advisor(
    db_config: Dict[str, Any],
    yaml_path: str,
) -> Dict[str, Any]:
    """
    Runs the Global Advisor against a PostgreSQL database.

    :param db_config: Database connection configuration.
    :param yaml_path: Path to the YAML recommendation catalog.
    :return: Dictionary containing status, recommendations, and execution details.
    """
    catalog = load_recommendation_catalog(yaml_path)

    conn, status_message = connectdb(db_config)

    if conn is None:
        return {
            "status": "error",
            "message": status_message,
            "recommendations": [],
            "errors": [],
        }

    all_recommendations: List[GlobalRecommendation] = []
    errors: List[Dict[str, str]] = []

    try:
        for definition in catalog:
            recommendation_id = definition.get("id", "unknown")

            if not definition.get("enabled_by_default", True):
                continue

            try:
                recommendations = run_sql_recommendation(conn, definition)
                all_recommendations.extend(recommendations)

            except Exception as exc:
                errors.append({
                    "recommendation_id": recommendation_id,
                    "error": str(exc),
                })

        all_recommendations.sort(
            key=lambda rec: rec.rank,
            reverse=True,
        )

        return {
            "status": "ok",
            "message": "Global Advisor completed",
            "recommendations": all_recommendations,
            "errors": errors,
        }

    finally:
        try:
            conn.close()
        except Exception:
            pass