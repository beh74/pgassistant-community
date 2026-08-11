# -*- encoding: utf-8 -*-
"""Build an ordered implementation plan from the existing advisors."""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Iterable

import yaml

from . import database
from . import global_advisor
from . import query_index_advisor
from . import query_parameter_advisor
from . import table_autovacuum_advisor


DEFAULT_RULES_PATH = Path(__file__).resolve().parents[2] / "executive_plan_rules.yml"
DEFAULT_ADVISOR_PATH = Path(__file__).resolve().parents[2] / "advisor_enriched.yml"


def load_rules(path: str | Path = DEFAULT_RULES_PATH) -> dict[str, Any]:
    """Load and minimally validate the planning policy."""
    with Path(path).open(encoding="utf-8") as stream:
        rules = yaml.safe_load(stream) or {}
    if rules.get("version") != 1:
        raise ValueError("Unsupported Executive Plan rules version.")
    if not isinstance(rules.get("rules"), list):
        raise ValueError("Executive Plan rules must contain a rules list.")
    return rules


def collect_advisor_results(db_config: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Run the public entry point of each existing advisor independently."""
    results: dict[str, Any] = {}
    errors: list[dict[str, str]] = []
    calls = (
        (
            "global_advisor",
            lambda: global_advisor.run_global_advisor(
                db_config,
                yaml_path=str(DEFAULT_ADVISOR_PATH),
            ),
        ),
        (
            "index_advisor",
            lambda: query_index_advisor.analyze_top_ranked_query_indexes(db_config, limit=10),
        ),
        (
            "parameter_advisor",
            lambda: query_parameter_advisor.analyze_query_parameter_workload(db_config),
        ),
        (
            "autovacuum",
            lambda: table_autovacuum_advisor.run_table_autovacuum_advisor(db_config),
        ),
    )
    for source, call in calls:
        try:
            results[source] = call()
        except Exception as exc:
            results[source] = {}
            errors.append({"source": source, "error": str(exc)})
    return results, errors


def _enum_value(value: Any) -> Any:
    return getattr(value, "value", value)


def _clean_sql(value: Any) -> str:
    return str(value or "").strip()


def _base_advice(source: str, database_name: str, **values: Any) -> dict[str, Any]:
    advice = {
        "source": source,
        "database": database_name,
        "advisor_id": "",
        "category_id": "OTHER",
        "action_type": "REVIEW_ONLY",
        "team": "DEV_OPS",
        "priority": "MEDIUM",
        "impact": 50,
        "confidence": 50,
        "effort": 50,
        "schema_name": "",
        "table_name": "",
        "object_name": database_name,
        "title": "Review recommendation",
        "description": "",
        "sql": "",
        "query_ids": [],
        "scope": "database",
        "urgency": "normal",
        "requires_restart": False,
        "requires_maintenance_window": False,
    }
    advice.update(values)
    schema_name = str(advice.get("schema_name") or "")
    table_name = str(advice.get("table_name") or "")
    advice["scope_name"] = (
        f"{schema_name}.{table_name}"
        if schema_name and table_name
        else table_name or str(advice.get("object_name") or database_name)
    )
    return advice


def _normalize_global(result: dict[str, Any], database_name: str) -> list[dict[str, Any]]:
    normalized = []
    for recommendation in result.get("recommendations") or []:
        row = recommendation.to_dict() if hasattr(recommendation, "to_dict") else dict(recommendation)
        schema = str(row.get("schema_name") or "")
        table = str(row.get("table_name") or "")
        object_name = str(row.get("object_name") or "")
        description = str(row.get("recommendation_note") or row.get("description") or "")
        if row.get("recommendation_id") == "Important PostgreSQL settings disabled or suboptimal":
            parameter_match = re.match(r"^([a-z][a-z0-9_]*)\s+is\b", description, flags=re.IGNORECASE)
            if parameter_match:
                object_name = parameter_match.group(1)
        if not object_name and table:
            object_name = f"{schema}.{table}" if schema else table
        normalized.append(
            _base_advice(
                "global_advisor",
                database_name,
                advisor_id=str(row.get("recommendation_id") or row.get("source") or ""),
                category_id=str(_enum_value(row.get("category_id")) or "OTHER"),
                action_type=str(_enum_value(row.get("action_type")) or "REVIEW_ONLY"),
                team=str(_enum_value(row.get("team")) or "DEV_OPS"),
                priority=str(_enum_value(row.get("priority")) or "MEDIUM"),
                impact=int(row.get("impact") or row.get("rank") or 50),
                confidence=int(row.get("confidence") or 50),
                effort=int(row.get("effort") or 50),
                schema_name=schema,
                table_name=table,
                object_name=object_name or database_name,
                title=str(row.get("title") or row.get("label") or "Database recommendation"),
                description=description,
                sql=_clean_sql(row.get("improvement_sql")),
                query_ids=[str(row["query_id"])] if row.get("query_id") else [],
                risk_level=str(_enum_value(row.get("risk_level")) or "UNKNOWN"),
                requires_lock=bool(row.get("requires_lock")),
                requires_maintenance_window=bool(row.get("requires_maintenance_window")),
            )
        )
    return normalized


def _normalize_indexes(result: dict[str, Any], database_name: str) -> list[dict[str, Any]]:
    normalized = []
    for query_result in result.get("results") or []:
        query_id = str(query_result.get("queryid") or "")
        for row in query_result.get("actionable_recommendations") or []:
            schema = str(row.get("schema") or "")
            table = str(row.get("table") or "")
            object_name = f"{schema}.{table}" if schema else table
            confidence_label = str(row.get("confidence") or "review").lower()
            confidence = 90 if confidence_label == "safe" else 70 if confidence_label == "review" else 40
            normalized.append(
                _base_advice(
                    "index_advisor",
                    database_name,
                    advisor_id=str(row.get("recommendation_type") or "index_opportunity"),
                    category_id="INDEX",
                    action_type="CREATE_INDEX",
                    team="DEV_OPS",
                    priority="HIGH" if confidence_label == "safe" else "MEDIUM",
                    impact=70,
                    confidence=confidence,
                    effort=35,
                    schema_name=schema,
                    table_name=table,
                    object_name=object_name or database_name,
                    title=f"Review index opportunity on {object_name}",
                    description=str(row.get("reason") or ""),
                    sql=_clean_sql(row.get("create_index_sql")),
                    query_ids=[query_id] if query_id else [],
                    scope="table",
                )
            )
    return normalized


def _normalize_parameters(result: dict[str, Any], database_name: str) -> list[dict[str, Any]]:
    normalized = []
    for row in result.get("recommendations") or []:
        parameter = str(row.get("parameter") or "PostgreSQL parameters")
        confidence_label = str(row.get("confidence") or "review").lower()
        is_pgtune = str(row.get("source") or "").lower() == "pgtune"
        normalized.append(
            _base_advice(
                "parameter_advisor",
                database_name,
                advisor_id="pgtune" if is_pgtune else parameter,
                category_id="CONFIGURATION",
                action_type="CONFIG_CHANGE",
                team="OPS",
                priority="HIGH" if confidence_label == "high" else "MEDIUM",
                impact=70 if confidence_label == "high" else 55,
                confidence=85 if confidence_label == "high" else 65,
                effort=30,
                object_name=parameter,
                title=(
                    "Review the pgTune configuration baseline"
                    if is_pgtune
                    else f"Review parameter {parameter}"
                ),
                description=str(row.get("reason") or ""),
                evidence=list(row.get("evidence") or []),
                sql=_clean_sql(row.get("alter_system_sql")),
                scope="cluster",
                requires_restart=(
                    bool(row.get("restart_required")) if is_pgtune else True
                ),
                requires_maintenance_window=(
                    bool(row.get("restart_required")) if is_pgtune else True
                ),
            )
        )
    return normalized


def _normalize_autovacuum(result: dict[str, Any], database_name: str) -> list[dict[str, Any]]:
    normalized = []
    for row in result.get("tables") or []:
        schema = str(row.get("schema_name") or "")
        table = str(row.get("table_name") or "")
        object_name = str(row.get("object_name") or f"{schema}.{table}")
        urgency_value = float(row.get("vacuum_urgency") or 0)
        never_analyzed = bool(row.get("never_analyzed"))
        normalized.append(
            _base_advice(
                "autovacuum",
                database_name,
                advisor_id=str(row.get("issue_type") or "table_autovacuum"),
                category_id="STATISTICS" if never_analyzed else "MAINTENANCE",
                action_type="ANALYZE" if never_analyzed else "VACUUM",
                team="OPS",
                priority=str(row.get("priority") or "MEDIUM"),
                impact=90 if urgency_value >= 2 else 65,
                confidence=85,
                effort=30,
                schema_name=schema,
                table_name=table,
                object_name=object_name,
                title=f"Restore table maintenance for {object_name}",
                description=str(row.get("recommendation_note") or row.get("tuning_rationale") or ""),
                sql=_clean_sql(row.get("script_sql")),
                scope="table",
                urgency="critical" if urgency_value >= 2 else "elevated" if urgency_value >= 1 else "normal",
            )
        )

    global_tuning = result.get("global_tuning") or {}
    for row in global_tuning.get("recommendations") or []:
        parameter = str(row.get("parameter") or "autovacuum")
        normalized.append(
            _base_advice(
                "autovacuum",
                database_name,
                advisor_id=parameter,
                category_id="CONFIGURATION",
                action_type="CONFIG_CHANGE",
                team="OPS",
                priority="HIGH" if global_tuning.get("load_level") in {"high", "critical"} else "MEDIUM",
                impact=75,
                confidence=75,
                effort=35,
                object_name=parameter,
                title=f"Review autovacuum parameter {parameter}",
                description=str(row.get("rationale") or ""),
                sql=_clean_sql(row.get("sql")),
                scope="cluster",
                urgency="critical" if global_tuning.get("load_level") == "critical" else "normal",
                requires_restart=str(row.get("context") or "") == "postmaster",
            )
        )
    return normalized


def normalize_advisor_results(results: dict[str, Any], database_name: str) -> list[dict[str, Any]]:
    """Convert all advisor payloads to the same small planning model."""
    return (
        _normalize_global(results.get("global_advisor") or {}, database_name)
        + _normalize_indexes(results.get("index_advisor") or {}, database_name)
        + _normalize_parameters(results.get("parameter_advisor") or {}, database_name)
        + _normalize_autovacuum(results.get("autovacuum") or {}, database_name)
    )


def _matches(advice: dict[str, Any], match: dict[str, Any]) -> bool:
    return all(advice.get(key) == value for key, value in match.items())


def classify_advices(advices: Iterable[dict[str, Any]], policy: dict[str, Any]) -> list[dict[str, Any]]:
    """Apply the first matching policy rule to each normalized recommendation."""
    defaults = policy.get("defaults") or {}
    classified = []
    for advice in advices:
        classification = dict(defaults)
        rule_classification: dict[str, Any] = {}
        matched_rule = "defaults"
        for rule in policy.get("rules") or []:
            if _matches(advice, rule.get("match") or {}):
                rule_classification = rule.get("classify") or {}
                classification.update(rule_classification)
                matched_rule = str(rule.get("id") or "rule")
                break
        item = dict(advice)
        item.update(classification)
        item["planning_rule"] = matched_rule
        item["team"] = str(rule_classification.get("team") or advice.get("team") or defaults.get("team") or "DEV_OPS")
        item["priority"] = str(rule_classification.get("priority") or advice.get("priority") or defaults.get("priority") or "MEDIUM")
        classified.append(item)
    return classified


def _signature(advice: dict[str, Any]) -> str:
    if advice.get("action_type") == "CONFIG_CHANGE":
        identity = "|".join(
            str(advice.get(key) or "").lower()
            for key in ("database", "action_type", "object_name")
        )
        return hashlib.sha1(identity.encode("utf-8")).hexdigest()

    sql = re.sub(r"\s+", " ", advice.get("sql") or "").strip().lower()
    if advice.get("action_type") == "CREATE_INDEX" and sql:
        index_target = re.search(r"\bon\s+([^\s(]+)\s*\(([^)]*)\)", sql, flags=re.IGNORECASE)
        if index_target:
            relation = index_target.group(1).replace('"', "")
            columns = re.sub(r"[\s\"]+", "", index_target.group(2))
            identity = "|".join(
                (
                    str(advice.get("database") or "").lower(),
                    "create_index",
                    relation,
                    columns,
                )
            )
            return hashlib.sha1(identity.encode("utf-8")).hexdigest()

    identity = "|".join(
        str(advice.get(key) or "")
        for key in ("database", "action_type", "schema_name", "table_name", "advisor_id")
    )
    return hashlib.sha1(f"{identity}|{sql}".encode("utf-8")).hexdigest()


def deduplicate_advices(advices: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge exact actions while retaining every affected query and source."""
    merged: dict[str, dict[str, Any]] = {}
    for advice in advices:
        signature = _signature(advice)
        if signature not in merged:
            item = dict(advice)
            item["sources"] = [advice["source"]]
            merged[signature] = item
            continue
        current = merged[signature]
        current["query_ids"] = sorted(set(current.get("query_ids") or []) | set(advice.get("query_ids") or []))
        current["sources"] = sorted(set(current.get("sources") or []) | {advice["source"]})
        current["impact"] = max(int(current.get("impact") or 0), int(advice.get("impact") or 0))
        current["confidence"] = min(int(current.get("confidence") or 100), int(advice.get("confidence") or 100))
        current["requires_restart"] = bool(current.get("requires_restart") or advice.get("requires_restart"))
        current["requires_maintenance_window"] = bool(
            current.get("requires_maintenance_window") or advice.get("requires_maintenance_window")
        )
    return list(merged.values())


def _format_title(template: str, advice: dict[str, Any]) -> str:
    values = {key: str(value or "") for key, value in advice.items()}
    try:
        return template.format_map(values)
    except (KeyError, ValueError):
        return template


def group_tasks(advices: Iterable[dict[str, Any]], policy: dict[str, Any]) -> list[dict[str, Any]]:
    """Group recommendations into deployable work packages."""
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for advice in advices:
        fields = advice.get("group_by") or ["database", "workstream", "object_name"]
        base_key = tuple(advice.get(field) or "" for field in fields)
        max_items = max(1, int(advice.get("max_items") or 1000))
        matching_count = sum(len(items) for key, items in groups.items() if key[: len(base_key)] == base_key)
        batch = matching_count // max_items
        groups.setdefault(base_key + (batch,), []).append(advice)

    phases = policy.get("phases") or {}
    tasks = []
    for key, items in groups.items():
        first = items[0]
        phase_value = first.get("phase")
        phase = int(90 if phase_value is None else phase_value)
        phase_meta = phases.get(phase) or phases.get(str(phase)) or {}
        title = _format_title(
            str(first.get("task_title") or first.get("phase_name") or "Review recommendations"),
            first,
        )
        task_id = hashlib.sha1(repr(key).encode("utf-8")).hexdigest()[:10]
        priority_values = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
        priority = max(
            (str(item.get("priority") or "MEDIUM") for item in items),
            key=lambda value: priority_values.get(value, 0),
        )
        score = max(
            round(
                0.45 * int(item.get("impact") or 50)
                + 0.30 * int(item.get("confidence") or 50)
                + 0.15 * priority_values.get(str(item.get("priority")), 2) * 25
                + 0.10 * (100 - int(item.get("effort") or 50))
            )
            for item in items
        )
        recommendation_groups: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            scope_name = str(item.get("scope_name") or item.get("object_name") or item.get("database") or "Database")
            recommendation_groups.setdefault(scope_name, []).append(item)
        tasks.append(
            {
                "id": task_id,
                "phase": phase,
                "phase_name": phase_meta.get("name") or first.get("phase_name") or "Manual review",
                "phase_rationale": phase_meta.get("rationale") or "",
                "title": title,
                "team": first.get("team") or "DEV_OPS",
                "priority": priority,
                "score": score,
                "workstream": first.get("workstream") or "REVIEW",
                "scope_name": first.get("scope_name") or first.get("object_name") or first.get("database"),
                "recommendations": items,
                "recommendation_groups": [
                    {"scope_name": scope_name, "recommendations": recommendations}
                    for scope_name, recommendations in recommendation_groups.items()
                ],
                "recommendation_count": len(items),
                "sql_count": sum(1 for item in items if item.get("sql")),
                "query_ids": sorted({query_id for item in items for query_id in item.get("query_ids") or []}),
                "sources": sorted({source for item in items for source in item.get("sources") or [item["source"]]}),
                "requires_restart": any(item.get("requires_restart") for item in items),
                "requires_maintenance_window": any(item.get("requires_maintenance_window") for item in items),
            }
        )
    return sorted(tasks, key=lambda task: (task["phase"], -task["score"], task["title"]))


def build_plan_from_results(
    results: dict[str, Any],
    database_name: str,
    *,
    policy: dict[str, Any] | None = None,
    errors: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Pure planning entry point, useful for the UI, API and tests."""
    policy = policy or load_rules()
    normalized = normalize_advisor_results(results, database_name)
    classified = classify_advices(normalized, policy)
    deduplicated = deduplicate_advices(classified)
    tasks = group_tasks(deduplicated, policy)
    phases = []
    for task in tasks:
        if not phases or phases[-1]["number"] != task["phase"]:
            phases.append(
                {
                    "number": task["phase"],
                    "name": task["phase_name"],
                    "rationale": task["phase_rationale"],
                    "tasks": [],
                    "sql_count": 0,
                    "teams": [],
                    "requires_restart": False,
                    "requires_maintenance_window": False,
                }
            )
        phases[-1]["tasks"].append(task)
        phases[-1]["sql_count"] += task["sql_count"]
        phases[-1]["requires_restart"] = bool(
            phases[-1]["requires_restart"] or task["requires_restart"]
        )
        phases[-1]["requires_maintenance_window"] = bool(
            phases[-1]["requires_maintenance_window"] or task["requires_maintenance_window"]
        )
        if task["team"] not in phases[-1]["teams"]:
            phases[-1]["teams"].append(task["team"])

    for phase in phases:
        phase["team"] = phase["teams"][0] if len(phase["teams"]) == 1 else "DEV_OPS"
    return {
        "status": "ok",
        "database": database_name,
        "phases": phases,
        "tasks": tasks,
        "errors": errors or [],
        "summary": {
            "recommendations_collected": len(normalized),
            "recommendations_after_deduplication": len(deduplicated),
            "tasks": len(tasks),
            "phases": len(phases),
            "teams": {
                team: sum(1 for task in tasks if task["team"] == team)
                for team in ("DEV", "OPS", "DEV_OPS")
            },
        },
    }


def build_executive_plan(db_config: dict[str, Any]) -> dict[str, Any]:
    """Collect current advisor results and build the Executive Plan."""
    database_name = database.get_resolved_database_name(db_config)
    results, errors = collect_advisor_results(db_config)
    return build_plan_from_results(results, database_name, errors=errors)
