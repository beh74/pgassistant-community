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
    AdvisorTeam,
    GlobalRecommendation,
    ObjectType,
    PriorityLevel,
    RecommendationCategory,
    RiskLevel,
)

from .database import (
    connectdb,
    db_fetch_json,
)
from .pg_version import get_postgresql_upgrade_recommendation


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
        team=safe_enum_value(AdvisorTeam, definition.get("team"), "OPS"),

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



def run_postgresql_version_recommendation(
    conn,
) -> List[GlobalRecommendation]:
    """
    Check the installed PostgreSQL release against the latest minor release
    available for its major branch.

    A recommendation is returned when:
    - a newer minor release is available for the installed major branch; or
    - the installed major branch is no longer supported.

    No recommendation is returned when the installed release is current and
    its major branch is still supported.
    """
    sql = """
        SELECT
            d.oid AS object_id,
            d.datname AS object_name,
            current_setting('server_version') AS installed_version
        FROM pg_database AS d
        WHERE d.datname = current_database();
    """

    raw_result = db_fetch_json(conn, sql)

    if isinstance(raw_result, str):
        rows = json.loads(raw_result)
    elif isinstance(raw_result, list):
        rows = raw_result
    elif isinstance(raw_result, dict):
        rows = [raw_result]
    else:
        raise RuntimeError(
            "Unexpected result type returned while detecting the "
            "PostgreSQL server version."
        )

    if not rows:
        return []

    row = rows[0]

    installed_version = str(
        row.get("installed_version") or ""
    ).strip()

    if not installed_version:
        raise RuntimeError(
            "Unable to detect the PostgreSQL server version."
        )

    version_status = get_postgresql_upgrade_recommendation(
        installed_version
    )

    # An unsupported major branch must generate a recommendation even when
    # its latest available minor release is already installed.
    if (
        not version_status.upgrade_recommended
        and version_status.supported
    ):
        return []

    unsupported_branch = not version_status.supported

    minor_update_available = (
        version_status.installed_version
        != version_status.latest_minor_version
    )

    if unsupported_branch:
        label = (
            f"PostgreSQL {version_status.major_version} is no longer supported"
        )

        tags = [
            "postgresql-version",
            "upgrade",
            "end-of-life",
            "major-upgrade",
        ]

        why_it_matters = (
            "This PostgreSQL major branch is no longer supported by the "
            "PostgreSQL project and no longer receives security, reliability, "
            "bug, or data-integrity fixes."
        )

        if minor_update_available:
            fix_strategy = (
                f"Upgrade first from PostgreSQL "
                f"{version_status.installed_version} to the final minor release "
                f"{version_status.latest_minor_version} of the current branch "
                "when required by the migration strategy. Then plan a major "
                "upgrade to a supported PostgreSQL branch. Review application "
                "and extension compatibility, test the migration, validate "
                "backup and rollback procedures, and schedule an appropriate "
                "maintenance window."
            )
        else:
            fix_strategy = (
                "Plan a major upgrade to a supported PostgreSQL branch. "
                "Review application and extension compatibility, test the "
                "migration, validate backup and rollback procedures, and "
                "schedule an appropriate maintenance window."
            )

        expected_benefit = (
            "Return to a supported PostgreSQL branch and regain access to "
            "current security, reliability, bug, and data-integrity fixes."
        )

        risk_level = "HIGH"
        impact = 90
        effort = 60

    else:
        label = (
            f"Upgrade PostgreSQL {version_status.installed_version} "
            f"to {version_status.latest_minor_version}"
        )

        tags = [
            "postgresql-version",
            "upgrade",
            "minor-release",
        ]

        why_it_matters = (
            "The installed PostgreSQL release is not the latest minor release "
            "available for its major branch. PostgreSQL minor releases include "
            "security, reliability, bug, and data-integrity fixes."
        )

        fix_strategy = (
            f"Review the release notes between PostgreSQL "
            f"{version_status.installed_version} and "
            f"{version_status.latest_minor_version}. Test the update in a "
            "representative environment, verify operating-system package and "
            "extension compatibility, validate backups, and schedule a "
            "controlled PostgreSQL restart."
        )

        expected_benefit = (
            "Access to the latest security, reliability, bug, and "
            "data-integrity fixes available for the installed PostgreSQL "
            "major branch."
        )

        risk_level = "MEDIUM"
        impact = 70
        effort = 35

    definition: Dict[str, Any] = {
        "id": "PostgreSQL release upgrade",
        "source": "postgresql_version_check",
        "category_id": "MAINTENANCE",
        "object_type": "DATABASE",
        "outcome_id": "OPERABILITY",
        "advisor_group": "MAINTENANCE_RISKS",
        "team": "OPS",
        "risk_level": risk_level,

        # Replace OTHER with UPGRADE if ActionType.UPGRADE is available.
        "action_type": "OTHER",
        "action_safety": "SAFE_TO_REVIEW",
        "requires_lock": False,
        "requires_maintenance_window": True,
        "can_generate_sql": False,
        "can_auto_apply": False,
        "enabled_by_default": True,
        "manual_review_required": True,

        "confidence": 95,
        "impact": impact,
        "effort": effort,

        "label": label,
        "tags": tags,
        "why_it_matters": why_it_matters,
        "fix_strategy": fix_strategy,
        "expected_benefit": expected_benefit,

        "result_mapping": {
            "object_id": "object_id",
            "object_name": "object_name",
            "recommendation_note": "recommendation_note",
            "current_value": "current_value",
            "recommended_value": "recommended_value",
        },
    }

    recommended_value = (
        "Upgrade to a supported PostgreSQL major branch"
        if unsupported_branch
        else version_status.latest_minor_version
    )

    recommendation_row = {
        "object_id": row.get("object_id"),
        "object_name": row.get("object_name"),
        "current_value": version_status.installed_version,
        "recommended_value": recommended_value,
        "recommendation_note": version_status.recommendation,
    }

    recommendation = build_recommendation_from_row(
        definition,
        recommendation_row,
    )

    # Unsupported PostgreSQL major versions are always top-priority OPS items,
    # independently of the generic confidence/impact/effort scoring formula.
    if unsupported_branch:
        recommendation.rank = 100
        recommendation.priority = PriorityLevel.HIGH

    return [recommendation]


def get_postgresql_version_advisor(
    db_config: Dict[str, Any],
) -> Dict[str, Any]:
    conn, status_message = connectdb(db_config)

    if conn is None:
        return {
            "status": "error",
            "message": status_message,
        }

    try:
        sql = """
            SELECT current_setting('server_version') AS installed_version;
        """
        rows = json.loads(db_fetch_json(conn, sql))
        installed_version = str(rows[0].get("installed_version") or "").strip()

        version_status = get_postgresql_upgrade_recommendation(installed_version)
        recommendations = run_postgresql_version_recommendation(conn)

        return {
            "status": "ok",
            "installed_version": version_status.installed_version,
            "major_version": version_status.major_version,
            "latest_minor_version": version_status.latest_minor_version,
            "latest_release_date": version_status.latest_release_date,
            "supported": version_status.supported,
            "end_of_life_date": version_status.end_of_life_date,
            "upgrade_recommended": version_status.upgrade_recommended,
            "recommendation_level": version_status.recommendation_level,
            "recommendation": version_status.recommendation,
            "global_advisor_recommendation": (
                recommendations[0].to_dict()
                if recommendations
                else None
            ),
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
    team_counts = _counter_by_attr(sorted_recs, "team")

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
        "team_counts": team_counts,
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


def get_dashboard_scope_counts(db_config: Dict[str, Any]) -> Dict[str, int]:
    conn, _ = connectdb(db_config)

    default_scope = {
        "table_count": 0,
        "index_count": 0,
        "foreign_key_count": 0,
        "column_count": 0,
    }

    if conn is None:
        return default_scope

    try:
        sql = """
            WITH user_tables AS (
                SELECT c.oid
                FROM pg_class AS c
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND n.nspname NOT LIKE 'pg_toast%'
            )
            SELECT
                (SELECT COUNT(*) FROM user_tables) AS table_count,
                (
                    SELECT COUNT(*)
                    FROM pg_index AS i
                    JOIN user_tables AS t ON t.oid = i.indrelid
                ) AS index_count,
                (
                    SELECT COUNT(*)
                    FROM pg_constraint AS c
                    JOIN user_tables AS t ON t.oid = c.conrelid
                    WHERE c.contype = 'f'
                ) AS foreign_key_count,
                (
                    SELECT COUNT(*)
                    FROM pg_attribute AS a
                    JOIN user_tables AS t ON t.oid = a.attrelid
                    WHERE a.attnum > 0
                      AND NOT a.attisdropped
                ) AS column_count;
        """
        rows = json.loads(db_fetch_json(conn, sql))
        if not rows:
            return default_scope

        return {
            "table_count": int(rows[0].get("table_count") or 0),
            "index_count": int(rows[0].get("index_count") or 0),
            "foreign_key_count": int(rows[0].get("foreign_key_count") or 0),
            "column_count": int(rows[0].get("column_count") or 0),
        }
    except Exception:
        return default_scope
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_dashboard_scope_for_recommendation(
    rec: GlobalRecommendation,
    scope_counts: Dict[str, int],
) -> int:
    fk_based_recommendations = {
        "foreign_key_columns_different_data_types",
        "missing_useful_foreign_key_indexes",
    }

    if rec.recommendation_id in fk_based_recommendations:
        return scope_counts.get("foreign_key_count", 0)

    if rec.object_type.value == "INDEX":
        return scope_counts.get("index_count", 0)

    if rec.object_type.value == "COLUMN":
        return scope_counts.get("column_count", 0)

    return scope_counts.get("table_count", 0)


def compute_dashboard_score(
    grouped_recommendations: Dict[str, List[GlobalRecommendation]],
    scope_counts: Dict[str, int],
) -> Dict[str, Any]:
    severity_weights = {
        "HIGH": 100.0,
        "MEDIUM": 40.0,
        "LOW": 15.0,
    }
    max_penalty_by_type = 25.0

    total_penalty = 0.0
    penalties_by_type = {}

    for recommendation_type, type_recs in grouped_recommendations.items():
        priority_counts = _counter_by_attr(type_recs, "priority")
        weighted_issues = sum(
            priority_counts.get(priority, 0) * weight
            for priority, weight in severity_weights.items()
        )
        scope_count = get_dashboard_scope_for_recommendation(
            type_recs[0],
            scope_counts,
        )

        if scope_count > 0:
            penalty = weighted_issues / scope_count
            if weighted_issues > 0:
                penalty = max(1.0, penalty)
        else:
            penalty = (
                priority_counts.get("HIGH", 0) * 4
                + priority_counts.get("MEDIUM", 0) * 2
                + priority_counts.get("LOW", 0)
            )

        penalty = min(max_penalty_by_type, penalty)
        total_penalty += penalty

        penalties_by_type[recommendation_type] = {
            "scope_count": scope_count,
            "score_penalty": round(penalty, 1),
        }

    total_penalty = min(100.0, total_penalty)

    return {
        "database_score": max(0, min(100, round(100 - total_penalty))),
        "score_penalty": round(total_penalty, 1),
        "penalties_by_type": penalties_by_type,
    }


def run_global_advisor(
    db_config: Dict[str, Any],
    yaml_path: str,
    team_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Runs the Global Advisor against a PostgreSQL database.

    :param db_config: Database connection configuration.
    :param yaml_path: Path to the YAML recommendation catalog.
    :return: Dictionary containing status, recommendations, summary, and execution details.
    """
    started_at = time.monotonic()
    catalog = load_recommendation_catalog(yaml_path)
    wanted_team = team_filter.upper() if team_filter else None

    if wanted_team:
        catalog = [
            definition for definition in catalog
            if str(definition.get("team", "OPS")).upper() == wanted_team
        ]

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

    # The PostgreSQL release check is implemented in Python rather than YAML.
    enabled_checks = 0 if wanted_team else 1

    try:
        if not wanted_team:
            try:
                all_recommendations.extend(
                    run_postgresql_version_recommendation(conn)
                )
            except Exception as exc:
                errors.append({
                    "recommendation_id": "PostgreSQL release upgrade",
                    "error": str(exc),
                })

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


def build_team_dashboard_summary(
    recommendations: List[GlobalRecommendation],
    team: str,
    scope_counts: Optional[Dict[str, int]] = None,
    errors: Optional[List[Dict[str, str]]] = None,
    duration_ms: Optional[int] = None,
) -> Dict[str, Any]:
    sorted_recs = sorted(recommendations, key=lambda rec: rec.rank, reverse=True)
    average_rank = (
        round(sum(rec.rank for rec in sorted_recs) / len(sorted_recs), 1)
        if sorted_recs
        else 0.0
    )
    scope_counts = scope_counts or {}
    grouped_by_type = _group_recommendations_by(sorted_recs, "recommendation_id")
    score = compute_dashboard_score(grouped_by_type, scope_counts)
    by_type = []

    for recommendation_type, type_recs in grouped_by_type.items():
        priority_counts = _counter_by_attr(type_recs, "priority")
        score_details = score["penalties_by_type"].get(recommendation_type, {})
        by_type.append({
            "recommendation_id": recommendation_type,
            "label": type_recs[0].label or type_recs[0].title or recommendation_type,
            "advisor_group": type_recs[0].advisor_group.value,
            "total": len(type_recs),
            "average_rank": round(sum(rec.rank for rec in type_recs) / len(type_recs), 1),
            "scope_count": score_details.get("scope_count", 0),
            "score_penalty": score_details.get("score_penalty", 0),
            "priority_counts": priority_counts,
            "highest_priority": (
                "HIGH" if priority_counts.get("HIGH")
                else "MEDIUM" if priority_counts.get("MEDIUM")
                else "LOW"
            ),
        })

    by_type.sort(
        key=lambda item: (
            item["priority_counts"].get("HIGH", 0),
            item["priority_counts"].get("MEDIUM", 0),
            item["average_rank"],
            item["total"],
        ),
        reverse=True,
    )

    return {
        "team": team,
        "total": len(sorted_recs),
        "average_rank": average_rank,
        "database_score": score["database_score"],
        "score_penalty": score["score_penalty"],
        "score_scope": scope_counts,
        "priority_counts": _counter_by_attr(sorted_recs, "priority"),
        "by_type": by_type,
        "errors": errors or [],
        "duration_ms": duration_ms,
    }


def run_dev_advisor_dashboard(
    db_config: Dict[str, Any],
    yaml_path: str = "advisor_enriched.yml",
) -> Dict[str, Any]:
    result = run_global_advisor(
        db_config,
        yaml_path=yaml_path,
        team_filter=AdvisorTeam.DEV.value,
    )

    recommendations = result.get("recommendations", [])
    scope_counts = get_dashboard_scope_counts(db_config)
    summary = build_team_dashboard_summary(
        recommendations,
        AdvisorTeam.DEV.value,
        scope_counts=scope_counts,
        errors=result.get("errors", []),
        duration_ms=(result.get("summary") or {}).get("execution", {}).get("duration_ms"),
    )

    return {
        "status": result.get("status", "ok"),
        "message": result.get("message", ""),
        "summary": summary,
    }
