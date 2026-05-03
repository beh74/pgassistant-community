import json
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

import yaml

from .global_advisor_models import (
    ActionSafety,
    ActionType,
    AdvisorGroup,
    AdvisorOutcome,
    GlobalRecommendation,
    ObjectType,
    RecommendationCategory,
    RiskLevel,
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


def yaml_bool(definition: Dict[str, Any], field_name: str, default: bool = False) -> bool:
    """
    Normalizes boolean values loaded from YAML.
    """
    value = definition.get(field_name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "1", "on"}
    return bool(value)


def safe_enum_value(enum_cls, value: Any, default: str) -> str:
    """
    Returns a valid enum value. Keeps catalog loading tolerant during upgrades.
    """
    candidate = value or default
    try:
        return enum_cls(candidate).value
    except ValueError:
        return default


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

        recommendation_id=definition.get("id"),
        category_id=RecommendationCategory(definition.get("category_id", "OTHER")),
        label=definition.get("label"),
        source=definition.get("source", definition.get("id", "unknown")),

        outcome_id=safe_enum_value(AdvisorOutcome, definition.get("outcome_id"), "OTHER"),
        advisor_group=safe_enum_value(AdvisorGroup, definition.get("advisor_group"), "OTHER"),

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

        why_it_matters=definition.get("why_it_matters"),
        fix_strategy=definition.get("fix_strategy"),
        expected_benefit=definition.get("expected_benefit"),

        confidence=confidence,
        impact=impact,
        effort=effort,

        manual_review_required=yaml_bool(definition, "manual_review_required", True),
        risk_level=safe_enum_value(RiskLevel, definition.get("risk_level"), "UNKNOWN"),
        action_type=safe_enum_value(ActionType, definition.get("action_type"), "OTHER"),
        action_safety=safe_enum_value(ActionSafety, definition.get("action_safety"), "UNKNOWN"),
        requires_lock=yaml_bool(definition, "requires_lock", False),
        requires_maintenance_window=yaml_bool(definition, "requires_maintenance_window", False),
        can_generate_sql=yaml_bool(definition, "can_generate_sql", True),
        can_auto_apply=yaml_bool(definition, "can_auto_apply", False),
        tags=definition.get("tags", []) or [],
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


def _counter_by_attr(recommendations: List[GlobalRecommendation], attr_name: str) -> Dict[str, int]:
    counter = Counter()
    for rec in recommendations:
        value = getattr(rec, attr_name, None)
        if hasattr(value, "value"):
            value = value.value
        counter[str(value or "UNKNOWN")] += 1
    return dict(counter)


def summarize_recommendations(
    recommendations: List[GlobalRecommendation],
    errors: Optional[List[Dict[str, str]]] = None,
    checks_total: Optional[int] = None,
    duration_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Builds the decision layer for the Global Advisor.

    This summary is intended for the top of the Global Advisor page:
    - counts by priority/category/outcome/risk/action type
    - top priorities
    - quick wins
    - manual review workload
    - short deterministic advisor message
    """
    errors = errors or []
    sorted_recs = sorted(recommendations, key=lambda rec: rec.rank, reverse=True)

    priority_counts = _counter_by_attr(sorted_recs, "priority")
    category_counts = _counter_by_attr(sorted_recs, "category_id")
    outcome_counts = _counter_by_attr(sorted_recs, "outcome_id")
    group_counts = _counter_by_attr(sorted_recs, "advisor_group")
    risk_counts = _counter_by_attr(sorted_recs, "risk_level")
    action_type_counts = _counter_by_attr(sorted_recs, "action_type")

    quick_wins = [
        rec for rec in sorted_recs
        if (rec.impact or 0) >= 60
        and (rec.effort or 100) <= 30
        and (rec.confidence or 0) >= 75
        and rec.risk_level.value in {"LOW", "MEDIUM"}
    ]

    safe_to_review = [
        rec for rec in sorted_recs
        if rec.action_safety.value in {"SAFE_TO_APPLY", "SAFE_TO_REVIEW"}
        and not rec.requires_maintenance_window
    ]

    manual_review = [rec for rec in sorted_recs if rec.manual_review_required]
    maintenance_window = [rec for rec in sorted_recs if rec.requires_maintenance_window]
    high_risk = [rec for rec in sorted_recs if rec.risk_level.value == "HIGH"]

    top_outcome = max(outcome_counts.items(), key=lambda item: item[1])[0] if outcome_counts else None
    top_category = max(category_counts.items(), key=lambda item: item[1])[0] if category_counts else None

    if not sorted_recs:
        advisor_message = "No active advisor recommendation was found for this database."
    else:
        advisor_message = (
            f"The advisor found {len(sorted_recs)} recommendation(s). "
            f"Start with the {len(priority_counts) and priority_counts.get('HIGH', 0)} high-priority item(s), "
            f"then review quick wins and recommendations requiring a maintenance window."
        )

    return {
        "total": len(sorted_recs),
        "advisor_message": advisor_message,
        "priority_counts": priority_counts,
        "category_counts": category_counts,
        "outcome_counts": outcome_counts,
        "group_counts": group_counts,
        "risk_counts": risk_counts,
        "action_type_counts": action_type_counts,
        "manual_review_required": len(manual_review),
        "requires_maintenance_window": len(maintenance_window),
        "high_risk": len(high_risk),
        "quick_wins_count": len(quick_wins),
        "safe_to_review_count": len(safe_to_review),
        "top_outcome": top_outcome,
        "top_category": top_category,
        "top_recommendations": [rec.to_dict() for rec in sorted_recs[:5]],
        "quick_wins": [rec.to_dict() for rec in quick_wins[:5]],
        "safe_to_review": [rec.to_dict() for rec in safe_to_review[:5]],
        "by_group": {
            group: [rec.to_dict() for rec in group_recs[:10]]
            for group, group_recs in _group_recommendations_by(sorted_recs, "advisor_group").items()
        },
        "execution": {
            "checks_total": checks_total,
            "checks_failed": len(errors),
            "checks_success": None if checks_total is None else max(0, checks_total - len(errors)),
            "duration_ms": duration_ms,
        },
    }


def _group_recommendations_by(
    recommendations: List[GlobalRecommendation],
    attr_name: str,
) -> Dict[str, List[GlobalRecommendation]]:
    grouped: Dict[str, List[GlobalRecommendation]] = defaultdict(list)
    for rec in recommendations:
        value = getattr(rec, attr_name, None)
        if hasattr(value, "value"):
            value = value.value
        grouped[str(value or "UNKNOWN")].append(rec)
    return dict(grouped)


def run_global_advisor(
    db_config: Dict[str, Any],
    yaml_path: str,
) -> Dict[str, Any]:
    """
    Runs the Global Advisor against a PostgreSQL database.

    :param db_config: Database connection configuration.
    :param yaml_path: Path to the YAML recommendation catalog.
    :return: Dictionary containing status, recommendations, summary, and execution details.
    """
    started_at = time.monotonic()
    catalog = load_recommendation_catalog(yaml_path)

    conn, status_message = connectdb(db_config)

    if conn is None:
        return {
            "status": "error",
            "message": status_message,
            "recommendations": [],
            "summary": summarize_recommendations([], [], checks_total=len(catalog), duration_ms=0),
            "errors": [],
        }

    all_recommendations: List[GlobalRecommendation] = []
    errors: List[Dict[str, str]] = []
    enabled_checks = 0

    try:
        for definition in catalog:
            recommendation_id = definition.get("id", "unknown")

            if not definition.get("enabled_by_default", True):
                continue

            enabled_checks += 1

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

        duration_ms = int((time.monotonic() - started_at) * 1000)
        summary = summarize_recommendations(
            all_recommendations,
            errors=errors,
            checks_total=enabled_checks,
            duration_ms=duration_ms,
        )

        return {
            "status": "ok",
            "message": "Global Advisor completed",
            "recommendations": all_recommendations,
            "summary": summary,
            "errors": errors,
        }

    finally:
        try:
            conn.close()
        except Exception:
            pass
