"""Read-only Executive Plan history from an optional pgAssistant Collector."""
from __future__ import annotations

import os
import hashlib
import json
import re
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import psycopg2
from pglast import ast, parse_sql
from pglast.parser import ParseError as PglastParseError
from psycopg2.extensions import parse_dsn
from psycopg2.extras import RealDictCursor

from . import database


ALLOWED_PERIODS = {1, 7, 15, 30}
ALLOWED_TEAMS = {"ALL", "DEV", "DEV_OPS", "OPS"}


class CollectorHistoryError(RuntimeError):
    pass


def is_configured() -> bool:
    return bool((os.getenv("COLLECTOR_URI") or "").strip())


def _connect():
    uri = (os.getenv("COLLECTOR_URI") or "").strip()
    if not uri:
        raise CollectorHistoryError("Executive Plan history is not configured.")
    try:
        connection = psycopg2.connect(
            uri,
            connect_timeout=3,
            application_name="pgAssistant Community history",
        )
        connection.set_session(readonly=True, autocommit=True)
        return connection
    except psycopg2.Error as exc:
        raise CollectorHistoryError("Unable to connect to the collector repository.") from exc


def database_identity(db_config: dict[str, Any]) -> dict[str, Any]:
    config = database.resolve_db_config(database.normalize_db_config(db_config))
    uri = (config.get("db_uri") or "").strip()
    if uri:
        try:
            parsed = parse_dsn(uri)
        except psycopg2.Error:
            parsed = {}
        return {
            "db_host": parsed.get("host"),
            "db_port": int(parsed.get("port") or 5432),
            "db_name": parsed.get("dbname") or config.get("db_name"),
            "db_user": parsed.get("user"),
        }
    return {
        "db_host": config.get("db_host"),
        "db_port": int(config.get("db_port") or 5432),
        "db_name": config.get("db_name"),
        "db_user": config.get("db_user"),
    }


def list_targets(
    db_config: dict[str, Any], *, query: str = "", limit: int = 50
) -> dict[str, Any]:
    identity = database_identity(db_config)
    limit = max(1, min(int(limit), 50))
    search = str(query or "").strip()
    database_name = str(identity.get("db_name") or "")
    if not search:
        search = database_name
    pattern = f"%{search}%"
    connection = _connect()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (runs.target_id)
                        runs.target_id,
                        runs.target_name,
                        runs.environment,
                        runs.target_group,
                        runs.db_host,
                        runs.db_port,
                        runs.db_name,
                        runs.db_user,
                        runs.metadata,
                        coalesce(runs.finished_at, runs.started_at) AS last_collected_at,
                        runs.status AS last_plan_status,
                        count(*) OVER (PARTITION BY runs.target_id) AS snapshot_count
                    FROM pga_collection_run AS runs
                    ORDER BY runs.target_id, runs.started_at DESC
                )
                SELECT *
                FROM latest
                WHERE target_id ILIKE %s
                   OR coalesce(target_name, '') ILIKE %s
                   OR coalesce(environment, '') ILIKE %s
                   OR coalesce(target_group, '') ILIKE %s
                   OR (
                       lower(coalesce(db_host, '')) = lower(%s)
                       AND db_port = %s
                       AND lower(coalesce(db_name, '')) = lower(%s)
                       AND lower(coalesce(db_user, '')) = lower(%s)
                   )
                ORDER BY lower(coalesce(nullif(target_name, ''), target_id)), lower(target_id)
                LIMIT %s
                """,
                (
                    pattern, pattern, pattern, pattern,
                    str(identity.get("db_host") or ""), identity.get("db_port"),
                    database_name, str(identity.get("db_user") or ""),
                    limit + 1,
                ),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as exc:
        raise CollectorHistoryError("The collector repository schema is not compatible.") from exc
    finally:
        connection.close()

    has_more = len(rows) > limit
    rows = rows[:limit]
    for target in rows:
        target["exact_database_match"] = all(
            _same(target.get(key), identity.get(key))
            for key in ("db_host", "db_port", "db_name", "db_user")
        )
        target["database_name_match"] = bool(
            database_name
            and (
                _same(target.get("db_name"), database_name)
                or database_name.lower() in str(target.get("target_id") or "").lower()
            )
        )

    rows.sort(key=lambda target: (
        str(target.get("target_name") or target.get("target_id") or "").lower(),
        str(target.get("target_id") or "").lower(),
    ))
    exact_matches = [target for target in rows if target["exact_database_match"]]
    name_matches = [target for target in rows if target["database_name_match"]]
    suggested_target_id = None
    if len(exact_matches) == 1:
        suggested_target_id = exact_matches[0]["target_id"]
    elif not exact_matches and len(name_matches) == 1:
        suggested_target_id = name_matches[0]["target_id"]

    return {
        "database": identity,
        "database_filter": database_name,
        "suggested_target_id": suggested_target_id,
        "requires_selection": suggested_target_id is None,
        "query": search,
        "limit": limit,
        "has_more": has_more,
        "targets": rows,
    }


def target_exists(target_id: str) -> bool:
    target_id = str(target_id or "").strip()
    if not target_id:
        return False
    connection = _connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pga_collection_run WHERE target_id = %s LIMIT 1",
                (target_id,),
            )
            return cursor.fetchone() is not None
    except psycopg2.Error as exc:
        raise CollectorHistoryError("Unable to validate the collector target.") from exc
    finally:
        connection.close()


def load_history(
    target_id: str,
    *,
    days: int | str | None,
    team: str = "ALL",
    now: datetime | None = None,
    current_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_id = str(target_id or "").strip()
    if not target_id:
        raise ValueError("A collector target_id is required.")
    if days != "latest" and days is not None and days not in ALLOWED_PERIODS:
        raise ValueError("History period must be latest, 1, 7, 15, 30 days or all.")
    team = normalize_team(team)

    connection = _connect()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            team_join = "" if team == "ALL" else "AND recommendations.team = %s"
            parameters: list[Any] = [target_id]
            if team != "ALL":
                parameters.append(team)
            cursor.execute(
                f"""
                SELECT
                    plans.run_id,
                    plans.collected_at,
                    recommendations.finding_fingerprint,
                    recommendations.action_fingerprint,
                    recommendations.team,
                    recommendations.priority,
                    recommendations.source,
                    recommendations.sources,
                    recommendations.advisor_id,
                    recommendations.action_type,
                    recommendations.scope_name,
                    recommendations.object_name,
                    recommendations.title,
                    recommendations.description,
                    recommendations.recommendation_sql,
                    recommendations.query_ids,
                    tasks.title AS package_title,
                    tasks.phase_number,
                    tasks.workstream,
                    tasks.task_order AS package_order
                FROM pga_executive_plan_snapshot AS plans
                LEFT JOIN pga_executive_plan_recommendation_snapshot AS recommendations
                  ON recommendations.run_id = plans.run_id
                 {team_join}
                LEFT JOIN pga_executive_plan_task_snapshot AS tasks
                  ON tasks.run_id = recommendations.run_id
                 AND tasks.task_id = recommendations.task_id
                WHERE plans.target_id = %s
                  AND plans.status = 'ok'
                ORDER BY plans.collected_at, recommendations.id
                """,
                tuple(parameters[1:] + parameters[:1]) if team != "ALL" else tuple(parameters),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as exc:
        raise CollectorHistoryError("Unable to read Executive Plan history.") from exc
    finally:
        connection.close()

    current_status = "not_requested"
    if current_plan is not None:
        current_status = "partial" if current_plan.get("errors") else "complete"
        if current_status == "complete":
            rows.extend(current_plan_rows(current_plan, target_id=target_id, team=team, now=now))
    result = build_history(rows, target_id=target_id, days=days, team=team, now=now)
    result["current_comparison"] = {
        "status": current_status,
        "collected_at": (now or datetime.now(timezone.utc)) if current_status == "complete" else None,
        "errors": current_plan.get("errors", []) if current_plan else [],
    }
    return result


def build_history(
    rows: Iterable[dict[str, Any]],
    *,
    target_id: str,
    days: int | str | None,
    team: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    snapshots: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    for row in rows:
        run_id = row.get("run_id")
        snapshot = snapshots.setdefault(
            run_id,
            {
                "run_id": str(run_id),
                "collected_at": row.get("collected_at"),
                "findings": {},
                "is_current": bool(row.get("is_current")),
            },
        )
        fingerprint = row.get("finding_fingerprint")
        if fingerprint:
            snapshot["findings"][str(fingerprint)] = _finding_payload(row)

    ordered = sorted(snapshots.values(), key=lambda item: item["collected_at"])
    if not ordered:
        return _empty_history(target_id, days, team)

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=days) if isinstance(days, int) else None
    first_visible_index = 0
    if days == "latest":
        first_visible_index = max(0, len(ordered) - 1)
    elif cutoff is not None:
        first_visible_index = next(
            (index for index, item in enumerate(ordered) if item["collected_at"] >= cutoff),
            len(ordered),
        )
    baseline = ordered[first_visible_index - 1] if first_visible_index > 0 else None
    visible = ordered[first_visible_index:]
    if not visible:
        visible = [ordered[-1]]
        baseline = ordered[-2] if len(ordered) > 1 else None

    first_seen: dict[str, datetime] = {}
    ever_seen: set[str] = set()
    for snapshot in ordered:
        for fingerprint in snapshot["findings"]:
            ever_seen.add(fingerprint)
            first_seen.setdefault(fingerprint, snapshot["collected_at"])

    previous = baseline["findings"] if baseline else {}
    eligible = set(previous)
    timeline = []
    corrections = []
    additions = []
    modifications = []
    seen_before_period = set().union(
        *(set(snapshot["findings"]) for snapshot in ordered[:first_visible_index])
    ) if first_visible_index else set()

    for snapshot_index, snapshot in enumerate(visible):
        current = snapshot["findings"]
        current_keys = set(current)
        previous_keys = set(previous)
        new_keys = current_keys - previous_keys
        corrected_keys = previous_keys - current_keys
        persistent_keys = current_keys & previous_keys
        modified_keys = {
            fingerprint
            for fingerprint in persistent_keys
            if current[fingerprint].get("action_fingerprint")
            != previous[fingerprint].get("action_fingerprint")
        }
        eligible.update(current_keys)
        point_additions = []
        point_corrections = []
        point_modifications = []

        for fingerprint in sorted(new_keys):
            finding = dict(current[fingerprint])
            finding["detected_at"] = snapshot["collected_at"]
            finding["first_seen_at"] = first_seen[fingerprint]
            finding["reopened"] = fingerprint in seen_before_period
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            additions.append(finding)
            point_additions.append(finding)
        for fingerprint in sorted(corrected_keys):
            finding = dict(previous[fingerprint])
            finding["corrected_at"] = snapshot["collected_at"]
            finding["first_seen_at"] = first_seen.get(fingerprint)
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            corrections.append(finding)
            point_corrections.append(finding)
        for fingerprint in sorted(modified_keys):
            finding = dict(current[fingerprint])
            finding["modified_at"] = snapshot["collected_at"]
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            modifications.append(finding)
            point_modifications.append(finding)

        timeline.append({
            "run_id": snapshot["run_id"],
            "collected_at": snapshot["collected_at"],
            "active": len(current_keys),
            "new": len(new_keys),
            "corrected": len(corrected_keys),
            "modified": len(modified_keys),
            "persistent": len(persistent_keys - modified_keys),
            "is_current": bool(snapshot.get("is_current")),
            "has_previous": baseline is not None or snapshot_index > 0,
            "correction_packages": _group_events(
                point_corrections, "corrected_at"
            ),
            "change_packages": _group_events(
                point_additions + point_modifications, "detected_at"
            ),
        })
        seen_before_period.update(current_keys)
        previous = current

    latest = visible[-1]
    latest_keys = set(latest["findings"])
    resolved_now = eligible - latest_keys
    resolution_rate = round(100 * len(resolved_now) / len(eligible)) if eligible else 0
    for finding in corrections:
        finding["reopened"] = finding["finding_fingerprint"] in latest_keys

    return {
        "status": "ok",
        "target_id": target_id,
        "team": team,
        "days": days,
        "snapshots": len(visible),
        "collector_snapshots": sum(1 for snapshot in visible if not snapshot.get("is_current")),
        "period": {
            "from": visible[0]["collected_at"],
            "to": visible[-1]["collected_at"],
        },
        "summary": {
            "active": len(latest_keys),
            "corrected": len(corrections),
            "new": len(additions),
            "modified": len(modifications),
            "resolved_now": len(resolved_now),
            "eligible": len(eligible),
            "resolution_rate": resolution_rate,
        },
        "timeline": timeline,
        "corrections": sorted(corrections, key=lambda item: item["corrected_at"], reverse=True),
        "additions": sorted(additions, key=lambda item: item["detected_at"], reverse=True),
        "modifications": sorted(modifications, key=lambda item: item["modified_at"], reverse=True),
        "current": sorted(
            latest["findings"].values(),
            key=lambda item: (_priority_order(item.get("priority")), item.get("title") or ""),
        ),
        "correction_packages": _group_events(corrections, "corrected_at"),
        "change_packages": _group_events(additions + modifications, "detected_at"),
    }


def current_plan_rows(
    plan: dict[str, Any], *, target_id: str, team: str, now: datetime | None = None
) -> list[dict[str, Any]]:
    """Convert the live plan to the collector snapshot shape for a direct comparison."""
    collected_at = now or datetime.now(timezone.utc)
    run_id = "current"
    rows: list[dict[str, Any]] = []
    for package_order, task in enumerate(plan.get("tasks") or [], start=1):
        task_team = normalize_team(task.get("team") or "DEV_OPS")
        if team != "ALL" and task_team != team:
            continue
        for recommendation in task.get("recommendations") or []:
            finding, action = recommendation_fingerprints(target_id, recommendation)
            rows.append({
                **recommendation,
                "run_id": run_id,
                "collected_at": collected_at,
                "finding_fingerprint": finding,
                "action_fingerprint": action,
                "package_title": task.get("title") or "Other recommendations",
                "phase_number": task.get("phase"),
                "workstream": task.get("workstream"),
                "package_order": package_order,
                "is_current": True,
            })
    if not rows:
        rows.append({"run_id": run_id, "collected_at": collected_at, "is_current": True})
    return rows


def recommendation_fingerprints(target_id: str, recommendation: dict[str, Any]) -> tuple[str, str]:
    """Mirror the collector's stable recommendation identity algorithm."""
    sql = re.sub(r"\s+", " ", str(recommendation.get("sql") or "")).strip().lower()
    action_type = str(recommendation.get("action_type") or "")
    semantic_target = ""
    if action_type == "CREATE_INDEX" and sql:
        match = re.search(r"\bon\s+([^\s(]+)\s*\(([^)]*)\)", sql, flags=re.IGNORECASE)
        if match:
            semantic_target = "%s|%s" % (
                match.group(1).replace('"', ""),
                re.sub(r'[\s"]+', "", match.group(2)),
            )
    parts = (
        target_id,
        recommendation.get("advisor_id"),
        recommendation.get("category_id"),
        action_type,
        recommendation.get("schema_name"),
        recommendation.get("table_name"),
        recommendation.get("object_name"),
        semantic_target,
    )
    finding = hashlib.sha256("|".join(str(part or "") for part in parts).encode()).hexdigest()
    action = hashlib.sha256(f"{finding}|{sql}".encode()).hexdigest()
    return finding, action


def _group_events(events: Iterable[dict[str, Any]], date_key: str) -> list[dict[str, Any]]:
    packages: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
    def event_date(item: dict[str, Any]) -> datetime:
        return (
            item.get(date_key)
            or item.get("modified_at")
            or item.get("detected_at")
            or datetime.min.replace(tzinfo=timezone.utc)
        )
    for event in sorted(events, key=event_date, reverse=True):
        key = (event.get("package_title"), event.get("phase_number"), event.get("team"))
        package = packages.setdefault(key, {
            "title": event.get("package_title") or "Other recommendations",
            "phase": event.get("phase_number"),
            "team": event.get("team"),
            "workstream": event.get("workstream"),
            "recommendations": [],
        })
        package["recommendations"].append(event)
    return list(packages.values())


def normalize_team(value: str) -> str:
    team = str(value or "ALL").strip().upper().replace("/", "_")
    if team == "DEVOPS":
        team = "DEV_OPS"
    if team not in ALLOWED_TEAMS:
        raise ValueError("Team must be ALL, DEV, DEV_OPS or OPS.")
    return team


def _finding_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "finding_fingerprint": str(row.get("finding_fingerprint")),
        "action_fingerprint": str(row.get("action_fingerprint") or ""),
        "team": row.get("team"),
        "priority": row.get("priority"),
        "source": row.get("source"),
        "sources": row.get("sources") or [],
        "advisor_id": row.get("advisor_id"),
        "action_type": row.get("action_type"),
        "scope_name": row.get("scope_name"),
        "object_name": row.get("object_name"),
        "title": row.get("title") or "Untitled recommendation",
        "description": row.get("description"),
        "recommendation_sql": row.get("recommendation_sql") or row.get("sql"),
        "query_ids": _normalize_query_ids(row.get("query_ids")),
        "package_title": row.get("package_title") or "Other recommendations",
        "phase_number": row.get("phase_number"),
        "workstream": row.get("workstream"),
        "package_order": row.get("package_order"),
    }


def _empty_history(target_id: str, days: int | str | None, team: str) -> dict[str, Any]:
    return {
        "status": "ok",
        "target_id": target_id,
        "team": team,
        "days": days,
        "snapshots": 0,
        "collector_snapshots": 0,
        "period": {"from": None, "to": None},
        "summary": {
            "active": 0,
            "corrected": 0,
            "new": 0,
            "modified": 0,
            "resolved_now": 0,
            "eligible": 0,
            "resolution_rate": 0,
        },
        "timeline": [],
        "corrections": [],
        "additions": [],
        "modifications": [],
        "current": [],
        "correction_packages": [],
        "change_packages": [],
        "current_comparison": {"status": "not_requested", "collected_at": None, "errors": []},
    }


def _same(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return False
    return str(left).strip().lower() == str(right).strip().lower()


def _priority_order(value: Any) -> int:
    return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}.get(
        str(value or "").upper(),
        4,
    )


def _normalize_query_ids(value: Any) -> list[str]:
    """Return stable, non-empty query IDs from JSONB or live-plan values."""
    if value is None:
        return []
    if isinstance(value, (str, int)):
        value = [value]
    return sorted({str(item).strip() for item in value if str(item).strip()})


def load_workload_correlation(
    target_id: str, *, days: int | None = 30, window_hours: int = 24
) -> dict[str, Any]:
    """Correlate Executive Plan recommendations with ranked-query snapshots.

    A workload row from the same collection run is preferred. Repositories that
    collected the two datasets in separate runs fall back to the closest snapshot
    inside ``window_hours``. The nearest observations before and after the
    recommendation provide an observed trend; they do not imply deployment.
    """
    target_id = str(target_id or "").strip()
    if not target_id:
        raise ValueError("Select a Collector target in Database connection settings first.")
    if days is not None and days not in ALLOWED_PERIODS | {90}:
        raise ValueError("Correlation period must be 1, 7, 15, 30, 90 days or all.")
    window_hours = max(1, min(int(window_hours), 168))

    connection = _connect()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                WITH collection_context AS (
                    SELECT runs.run_id,
                           coalesce(runs.finished_at, runs.started_at) AS context_collected_at,
                           coalesce(
                             runs.metadata->>'release',
                             runs.metadata->>'application_release',
                             runs.metadata->>'deployment_version',
                             payload.raw_payload#>>'{postgres_context,release}'
                           ) AS workload_release,
                           coalesce(
                             runs.metadata->>'postgresql_version',
                             runs.metadata->>'postgres_version',
                             runs.metadata->>'server_version',
                             runs.metadata->>'postgres_release',
                             payload.raw_payload#>>'{postgres_context,server_version}'
                           ) AS postgres_version,
                           coalesce(
                             runs.metadata->'postgresql_settings',
                             runs.metadata->'postgres_settings',
                             runs.metadata->'settings',
                             runs.metadata->'configuration',
                             payload.raw_payload#>'{postgres_context,settings}'
                           ) AS postgres_settings,
                           lag(coalesce(
                             runs.metadata->>'release',
                             runs.metadata->>'application_release',
                             runs.metadata->>'deployment_version',
                             payload.raw_payload#>>'{postgres_context,release}'
                           )) OVER (ORDER BY coalesce(runs.finished_at, runs.started_at)) AS previous_workload_release,
                           lag(coalesce(
                             runs.metadata->>'postgresql_version',
                             runs.metadata->>'postgres_version',
                             runs.metadata->>'server_version',
                             runs.metadata->>'postgres_release',
                             payload.raw_payload#>>'{postgres_context,server_version}'
                           )) OVER (ORDER BY coalesce(runs.finished_at, runs.started_at)) AS previous_postgres_version,
                           lag(coalesce(
                             runs.metadata->'postgresql_settings',
                             runs.metadata->'postgres_settings',
                             runs.metadata->'settings',
                             runs.metadata->'configuration',
                             payload.raw_payload#>'{postgres_context,settings}'
                           )) OVER (ORDER BY coalesce(runs.finished_at, runs.started_at)) AS previous_postgres_settings
                    FROM pga_collection_run AS runs
                    LEFT JOIN LATERAL (
                        SELECT stored.raw_payload
                        FROM pga_collection_payload AS stored
                        WHERE stored.run_id = runs.run_id
                          AND stored.job_type = 'executive_plan'
                        ORDER BY stored.collected_at DESC
                        LIMIT 1
                    ) AS payload ON true
                    WHERE runs.target_id = %s
                ), latest_environment AS (
                    SELECT current_context.*,
                           previous_context.workload_release AS latest_previous_workload_release,
                           previous_context.postgres_version AS latest_previous_postgres_version,
                           previous_context.postgres_settings AS latest_previous_postgres_settings
                    FROM collection_context AS current_context
                    LEFT JOIN LATERAL (
                        SELECT prior.*
                        FROM collection_context AS prior
                        WHERE prior.context_collected_at < current_context.context_collected_at
                          AND (prior.workload_release IS NOT NULL
                               OR prior.postgres_version IS NOT NULL
                               OR prior.postgres_settings IS NOT NULL)
                        ORDER BY prior.context_collected_at DESC
                        LIMIT 1
                    ) AS previous_context ON true
                    WHERE current_context.workload_release IS NOT NULL
                       OR current_context.postgres_version IS NOT NULL
                       OR current_context.postgres_settings IS NOT NULL
                    ORDER BY current_context.context_collected_at DESC
                    LIMIT 1
                ), recommendations AS (
                    SELECT plans.run_id, plans.collected_at,
                           rec.finding_fingerprint, rec.action_fingerprint,
                           rec.title, rec.priority, rec.impact, rec.confidence,
                           rec.advisor_id,
                           context.postgres_version,
                           context.previous_postgres_version,
                           context.workload_release,
                           context.previous_workload_release,
                           CASE
                             WHEN context.postgres_settings IS NULL
                               OR context.previous_postgres_settings IS NULL THEN NULL
                             ELSE context.postgres_settings IS DISTINCT FROM context.previous_postgres_settings
                           END AS postgres_settings_changed,
                           CASE
                             WHEN jsonb_typeof(rec.query_ids) = 'array'
                              AND jsonb_array_length(rec.query_ids) > 0
                             THEN 'recommendation'
                             WHEN task_scope.recommendation_count = 1
                              AND jsonb_typeof(task.query_ids) = 'array'
                              AND jsonb_array_length(task.query_ids) > 0
                             THEN 'task'
                             ELSE 'none'
                           END AS query_id_source,
                           CASE
                             WHEN jsonb_typeof(rec.query_ids) = 'array'
                              AND jsonb_array_length(rec.query_ids) > 0
                             THEN rec.query_ids
                             WHEN task_scope.recommendation_count = 1
                              AND jsonb_typeof(task.query_ids) = 'array'
                             THEN task.query_ids
                             ELSE '[]'::jsonb
                           END AS query_ids
                    FROM pga_executive_plan_snapshot AS plans
                    JOIN pga_executive_plan_recommendation_snapshot AS rec
                      ON rec.run_id = plans.run_id
                    LEFT JOIN pga_executive_plan_task_snapshot AS task
                      ON task.run_id = rec.run_id AND task.task_id = rec.task_id
                    LEFT JOIN LATERAL (
                        SELECT count(*) AS recommendation_count
                        FROM pga_executive_plan_recommendation_snapshot AS sibling
                        WHERE sibling.run_id = rec.run_id AND sibling.task_id = rec.task_id
                    ) AS task_scope ON true
                    LEFT JOIN collection_context AS context ON context.run_id = plans.run_id
                    WHERE plans.target_id = %s AND plans.status = 'ok'
                      AND (%s IS NULL OR plans.collected_at >= now() - (%s * interval '1 day'))
                ), expanded AS (
                    SELECT recommendations.*, query_ids.queryid
                    FROM recommendations
                    CROSS JOIN LATERAL jsonb_array_elements_text(
                        coalesce(recommendations.query_ids, '[]'::jsonb)
                    ) AS query_ids(queryid)
                )
                SELECT expanded.*,
                       matched.run_id AS workload_run_id,
                       matched.collected_at AS workload_collected_at,
                       matched.calls, matched.mean_exec_time_ms,
                       matched.total_exec_time_ms, matched.share_total_time,
                       matched.share_io,
                       (matched.run_id = expanded.run_id) AS exact_run_match,
                       before_sample.collected_at AS before_collected_at,
                       before_sample.mean_exec_time_ms AS before_mean_exec_time_ms,
                       before_sample.calls AS before_calls,
                       after_sample.collected_at AS after_collected_at,
                       after_sample.mean_exec_time_ms AS after_mean_exec_time_ms,
                       after_sample.calls AS after_calls,
                       latest_intervals.latest_collected_at,
                       latest_intervals.latest_mean_exec_time_ms,
                       latest_intervals.latest_calls,
                       latest_intervals.previous_latest_collected_at,
                       latest_intervals.previous_latest_mean_exec_time_ms,
                       latest_intervals.previous_latest_calls,
                       latest_context.workload_release AS latest_workload_release,
                       latest_context.latest_previous_workload_release,
                       latest_context.postgres_version AS latest_postgres_version,
                       latest_context.latest_previous_postgres_version,
                       latest_context.postgres_settings AS latest_postgres_settings,
                       latest_context.latest_previous_postgres_settings,
                       CASE
                         WHEN latest_context.postgres_settings IS NULL
                           OR latest_context.latest_previous_postgres_settings IS NULL THEN NULL
                         ELSE latest_context.postgres_settings IS DISTINCT FROM latest_context.latest_previous_postgres_settings
                       END AS latest_postgres_settings_changed
                FROM expanded
                LEFT JOIN LATERAL (
                    SELECT workload.*
                    FROM pga_ranked_query_snapshot AS workload
                    WHERE workload.target_id = %s
                      AND workload.queryid::text = expanded.queryid
                      AND (workload.run_id = expanded.run_id OR
                           abs(extract(epoch FROM workload.collected_at - expanded.collected_at))
                               <= %s * 3600)
                    ORDER BY (workload.run_id = expanded.run_id) DESC,
                             abs(extract(epoch FROM workload.collected_at - expanded.collected_at))
                    LIMIT 1
                ) AS matched ON true
                LEFT JOIN LATERAL (
                    SELECT workload.collected_at, workload.mean_exec_time_ms, workload.calls
                    FROM pga_ranked_query_snapshot AS workload
                    WHERE workload.target_id = %s AND workload.queryid::text = expanded.queryid
                      AND workload.collected_at < expanded.collected_at
                    ORDER BY workload.collected_at DESC LIMIT 1
                ) AS before_sample ON true
                LEFT JOIN LATERAL (
                    SELECT workload.collected_at, workload.mean_exec_time_ms, workload.calls
                    FROM pga_ranked_query_snapshot AS workload
                    WHERE workload.target_id = %s AND workload.queryid::text = expanded.queryid
                      AND workload.collected_at > expanded.collected_at
                    ORDER BY workload.collected_at LIMIT 1
                ) AS after_sample ON true
                LEFT JOIN LATERAL (
                    WITH recent AS (
                        SELECT workload.collected_at, workload.calls,
                               workload.total_exec_time_ms
                        FROM pga_ranked_query_snapshot AS workload
                        WHERE workload.target_id = %s
                          AND workload.queryid::text = expanded.queryid
                        ORDER BY workload.collected_at DESC
                        LIMIT 20
                    ), samples AS (
                        SELECT recent.*,
                               lag(calls) OVER (ORDER BY collected_at) AS previous_calls,
                               lag(total_exec_time_ms) OVER (ORDER BY collected_at) AS previous_total_exec_time_ms
                        FROM recent
                    ), intervals AS (
                        SELECT collected_at,
                               calls - previous_calls AS interval_calls,
                               (total_exec_time_ms - previous_total_exec_time_ms)
                                 / (calls - previous_calls) AS interval_mean_exec_time_ms
                        FROM samples
                        WHERE previous_calls IS NOT NULL
                          AND calls > previous_calls
                          AND total_exec_time_ms >= previous_total_exec_time_ms
                    ), ranked AS (
                        SELECT intervals.*,
                               row_number() OVER (ORDER BY collected_at DESC) AS interval_rank
                        FROM intervals
                    )
                    SELECT max(collected_at) FILTER (WHERE interval_rank = 1) AS latest_collected_at,
                           max(interval_mean_exec_time_ms) FILTER (WHERE interval_rank = 1) AS latest_mean_exec_time_ms,
                           max(interval_calls) FILTER (WHERE interval_rank = 1) AS latest_calls,
                           max(collected_at) FILTER (WHERE interval_rank = 2) AS previous_latest_collected_at,
                           max(interval_mean_exec_time_ms) FILTER (WHERE interval_rank = 2) AS previous_latest_mean_exec_time_ms,
                           max(interval_calls) FILTER (WHERE interval_rank = 2) AS previous_latest_calls
                    FROM ranked
                    WHERE interval_rank <= 2
                ) AS latest_intervals ON true
                LEFT JOIN latest_environment AS latest_context ON true
                ORDER BY expanded.collected_at DESC, expanded.finding_fingerprint, expanded.queryid
                """,
                (target_id, target_id, days, days, target_id, window_hours,
                 target_id, target_id, target_id),
            )
            rows = [dict(row) for row in cursor.fetchall()]
            environment_rows = _load_environment_rows(
                cursor, target_id=target_id, days=days
            )
            workload_rows = _load_workload_timeline_rows(cursor, target_id=target_id)
            recommendation_rows = _load_recommendation_timeline_rows(
                cursor, target_id=target_id
            )
    except psycopg2.Error as exc:
        raise CollectorHistoryError(
            "Unable to correlate Executive Plan and workload history."
        ) from exc
    finally:
        connection.close()
    result = build_workload_correlation(
        rows, target_id=target_id, days=days, window_hours=window_hours,
        environment_rows=environment_rows,
    )
    result["workload_timeline"] = _build_collection_timeline(
        workload_rows, recommendation_rows, result["environment_history"], days=days
    )
    return result


def _load_workload_timeline_rows(cursor: Any, *, target_id: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        WITH recent AS (
            SELECT DISTINCT collected_at
            FROM pga_ranked_query_snapshot
            WHERE target_id = %s
            ORDER BY collected_at DESC LIMIT 200
        )
        SELECT run_id, collected_at, queryid::text AS queryid, query AS query_sql,
               calls, total_exec_time_ms, share_total_time, share_io
        FROM pga_ranked_query_snapshot
        WHERE target_id = %s AND collected_at IN (SELECT collected_at FROM recent)
        ORDER BY collected_at, queryid
        """,
        (target_id, target_id),
    )
    return [dict(row) for row in cursor.fetchall()]


def _load_recommendation_timeline_rows(cursor: Any, *, target_id: str) -> list[dict[str, Any]]:
    cursor.execute(
        """
        SELECT plans.run_id, plans.collected_at, rec.finding_fingerprint,
               rec.action_fingerprint, rec.title, rec.description,
               rec.recommendation_sql, rec.priority, rec.query_ids
        FROM pga_executive_plan_snapshot AS plans
        JOIN pga_executive_plan_recommendation_snapshot AS rec
          ON rec.run_id = plans.run_id
        WHERE plans.target_id = %s AND plans.status = 'ok'
        ORDER BY plans.collected_at, rec.finding_fingerprint
        """,
        (target_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _load_environment_rows(cursor: Any, *, target_id: str, days: int | None) -> list[dict[str, Any]]:
    cursor.execute(
        """
        WITH contexts AS (
            SELECT runs.run_id,
                   coalesce(runs.finished_at, runs.started_at) AS collected_at,
                   coalesce(runs.metadata->>'release',
                            runs.metadata->>'application_release',
                            runs.metadata->>'deployment_version',
                            payload.raw_payload#>>'{postgres_context,release}') AS workload_release,
                   coalesce(runs.metadata->>'postgresql_version',
                            runs.metadata->>'postgres_version',
                            runs.metadata->>'server_version',
                            runs.metadata->>'postgres_release',
                            payload.raw_payload#>>'{postgres_context,server_version}') AS postgres_version,
                   coalesce(runs.metadata->'postgresql_settings',
                            runs.metadata->'postgres_settings',
                            runs.metadata->'settings',
                            runs.metadata->'configuration',
                            payload.raw_payload#>'{postgres_context,settings}') AS postgres_settings
            FROM pga_collection_run AS runs
            LEFT JOIN LATERAL (
                SELECT stored.raw_payload
                FROM pga_collection_payload AS stored
                WHERE stored.run_id = runs.run_id AND stored.job_type = 'executive_plan'
                ORDER BY stored.collected_at DESC LIMIT 1
            ) AS payload ON true
            WHERE runs.target_id = %s
        )
        SELECT current_context.*,
               previous_context.collected_at AS previous_collected_at,
               previous_context.workload_release AS previous_workload_release,
               previous_context.postgres_version AS previous_postgres_version,
               previous_context.postgres_settings AS previous_postgres_settings
        FROM contexts AS current_context
        LEFT JOIN LATERAL (
            SELECT prior.* FROM contexts AS prior
            WHERE prior.collected_at < current_context.collected_at
              AND (prior.workload_release IS NOT NULL OR prior.postgres_version IS NOT NULL
                   OR prior.postgres_settings IS NOT NULL)
            ORDER BY prior.collected_at DESC LIMIT 1
        ) AS previous_context ON true
        WHERE (current_context.workload_release IS NOT NULL
               OR current_context.postgres_version IS NOT NULL
               OR current_context.postgres_settings IS NOT NULL)
          AND (%s IS NULL OR current_context.collected_at >= now() - (%s * interval '1 day'))
        ORDER BY current_context.collected_at DESC
        """,
        (target_id, days, days),
    )
    return [dict(row) for row in cursor.fetchall()]


def build_workload_correlation(
    rows: Iterable[dict[str, Any]], *, target_id: str, days: int | None,
    window_hours: int = 24,
    environment_rows: Iterable[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    correlations = []
    exact = temporal = trends = 0
    context_change_keys: set[Any] = set()
    context_available = baseline_available = False
    for source in rows:
        row = dict(source)
        match_type = "none"
        if row.get("workload_collected_at") is not None:
            match_type = "run_id" if row.get("exact_run_match") else "temporal"
            exact += match_type == "run_id"
            temporal += match_type == "temporal"
        before = _number_or_none(row.get("before_mean_exec_time_ms"))
        after = _number_or_none(row.get("after_mean_exec_time_ms"))
        change_pct = None
        if before is not None and after is not None and before > 0:
            change_pct = round((after - before) * 100 / before, 2)
        version_changed = bool(
            row.get("postgres_version") and row.get("previous_postgres_version")
            and row.get("postgres_version") != row.get("previous_postgres_version")
        )
        release_changed = bool(
            row.get("workload_release") and row.get("previous_workload_release")
            and row.get("workload_release") != row.get("previous_workload_release")
        )
        settings_changed = row.get("postgres_settings_changed") is True
        latest_version_changed = bool(
            row.get("latest_postgres_version") and row.get("latest_previous_postgres_version")
            and row.get("latest_postgres_version") != row.get("latest_previous_postgres_version")
        )
        latest_release_changed = bool(
            row.get("latest_workload_release") and row.get("latest_previous_workload_release")
            and row.get("latest_workload_release") != row.get("latest_previous_workload_release")
        )
        latest_settings_changed = row.get("latest_postgres_settings_changed") is True
        setting_changes = _postgres_setting_changes(
            row.get("latest_previous_postgres_settings"),
            row.get("latest_postgres_settings"),
        )
        row_context_available = bool(
            row.get("latest_workload_release") or row.get("latest_postgres_version")
            or row.get("latest_postgres_settings_changed") is not None
        )
        row_baseline_available = bool(
            row.get("latest_previous_workload_release")
            or row.get("latest_previous_postgres_version")
            or row.get("latest_postgres_settings_changed") is not None
        )
        context_available = context_available or row_context_available
        baseline_available = baseline_available or row_baseline_available
        if latest_release_changed or latest_version_changed or latest_settings_changed:
            context_change_keys.add(row.get("latest_collected_at"))
        latest_before = _number_or_none(row.get("previous_latest_mean_exec_time_ms"))
        latest_after = _number_or_none(row.get("latest_mean_exec_time_ms"))
        latest_change_pct = None
        if latest_before is not None and latest_after is not None and latest_before > 0:
            latest_change_pct = round((latest_after - latest_before) * 100 / latest_before, 2)
            trends += 1
        correlations.append({
            "run_id": str(row.get("run_id") or ""),
            "collected_at": row.get("collected_at"),
            "finding_fingerprint": str(row.get("finding_fingerprint") or ""),
            "action_fingerprint": str(row.get("action_fingerprint") or ""),
            "title": row.get("title"),
            "priority": row.get("priority"),
            "impact": row.get("impact"),
            "confidence": row.get("confidence"),
            "advisor_id": row.get("advisor_id"),
            "queryid": str(row.get("queryid") or ""),
            "query_id_source": row.get("query_id_source") or "recommendation",
            "detection_postgres_context": {
                "release": row.get("workload_release"),
                "previous_release": row.get("previous_workload_release"),
                "release_changed": release_changed,
                "version": row.get("postgres_version"),
                "previous_version": row.get("previous_postgres_version"),
                "version_changed": version_changed,
                "settings_changed": settings_changed,
                "settings_comparison_available": row.get("postgres_settings_changed") is not None,
                "confounding_change": release_changed or version_changed or settings_changed,
            },
            "match": {
                "type": match_type,
                "run_id": str(row.get("workload_run_id") or "") or None,
                "collected_at": row.get("workload_collected_at"),
                "calls": _number_or_none(row.get("calls")),
                "mean_exec_time_ms": _number_or_none(row.get("mean_exec_time_ms")),
                "total_exec_time_ms": _number_or_none(row.get("total_exec_time_ms")),
                "share_total_time": _number_or_none(row.get("share_total_time")),
                "share_io": _number_or_none(row.get("share_io")),
            },
            "trend": {
                "before_collected_at": row.get("before_collected_at"),
                "before_mean_exec_time_ms": before,
                "before_calls": _number_or_none(row.get("before_calls")),
                "after_collected_at": row.get("after_collected_at"),
                "after_mean_exec_time_ms": after,
                "after_calls": _number_or_none(row.get("after_calls")),
                "mean_exec_time_change_pct": change_pct,
                "direction": (
                    "improved" if change_pct is not None and change_pct < -1
                    else "degraded" if change_pct is not None and change_pct > 1
                    else "stable" if change_pct is not None else "unavailable"
                ),
            },
            "latest_trend": {
                "before_collected_at": row.get("previous_latest_collected_at"),
                "before_mean_exec_time_ms": latest_before,
                "before_calls": _number_or_none(row.get("previous_latest_calls")),
                "after_collected_at": row.get("latest_collected_at"),
                "after_mean_exec_time_ms": latest_after,
                "after_calls": _number_or_none(row.get("latest_calls")),
                "mean_exec_time_change_pct": latest_change_pct,
                "direction": (
                    "improved" if latest_change_pct is not None and latest_change_pct < -1
                    else "degraded" if latest_change_pct is not None and latest_change_pct > 1
                    else "stable" if latest_change_pct is not None else "unavailable"
                ),
            },
            "postgres_context": {
                "release": row.get("latest_workload_release"),
                "previous_release": row.get("latest_previous_workload_release"),
                "release_changed": latest_release_changed,
                "version": row.get("latest_postgres_version"),
                "previous_version": row.get("latest_previous_postgres_version"),
                "version_changed": latest_version_changed,
                "settings_changed": latest_settings_changed,
                "setting_changes": setting_changes,
                "settings_comparison_available": row.get("latest_postgres_settings_changed") is not None,
                "baseline_available": row_baseline_available,
                "confounding_change": (
                    latest_release_changed or latest_version_changed or latest_settings_changed
                ),
            },
        })
    latest_query_trends = {}
    for item in correlations:
        trend = item["latest_trend"]
        if trend["mean_exec_time_change_pct"] is not None:
            latest_query_trends[item["queryid"]] = trend
    trend_counts = {
        direction: sum(
            trend["direction"] == direction for trend in latest_query_trends.values()
        )
        for direction in ("improved", "degraded", "stable")
    }
    environment_history = _build_environment_history(environment_rows or [])
    environment = (
        environment_history[0] if environment_history
        else correlations[0]["postgres_context"] if correlations else {}
    )
    environment_impact = {
        **environment,
        "observed_query_count": len(latest_query_trends),
        "trend_counts": trend_counts,
        "notice": (
            "These workload movements occurred around the environment snapshot. "
            "They indicate correlation, not proven causation."
        ),
    }
    history_change_count = sum(
        item["release_changed"] or item["version_changed"] or item["settings_changed"]
        for item in environment_history
    )
    history_baseline_available = any(
        item["baseline_available"] for item in environment_history
    )
    unique_query_links = {
        (item["finding_fingerprint"], item["queryid"]) for item in correlations
    }
    unique_comparable_trends = {
        (item["finding_fingerprint"], item["queryid"])
        for item in correlations
        if item["latest_trend"]["mean_exec_time_change_pct"] is not None
    }
    return {
        "status": "ok", "target_id": target_id, "days": days,
        "window_hours": window_hours,
        "summary": {
            "query_links": len(unique_query_links), "exact_run_matches": exact,
            "temporal_matches": temporal,
            "unmatched": len(correlations) - exact - temporal,
            "comparable_trends": len(unique_comparable_trends),
            "context_changes": history_change_count if environment_history else len(context_change_keys),
            "environment_context_available": bool(environment_history) or context_available,
            "environment_baseline_available": (
                history_baseline_available if environment_history else baseline_available
            ),
        },
        "correlations": correlations,
        "environment_impact": environment_impact,
        "environment_history": environment_history,
        "trend_note": (
            "Before/after compares observed workload snapshots around the recommendation "
            "date; it does not prove that the recommendation was deployed."
        ),
    }


def _number_or_none(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return float(value)


def _query_statement_type(query_sql: Any) -> str:
    """Classify a PostgreSQL statement using pglast's native parser."""
    sql = str(query_sql or "").strip()
    if not sql:
        return "OTHERS"
    try:
        statements = parse_sql(sql)
    except (PglastParseError, TypeError, ValueError):
        return "OTHERS"
    if len(statements) != 1:
        return "OTHERS"
    statement = statements[0].stmt
    return {
        ast.SelectStmt: "SELECT",
        ast.InsertStmt: "INSERT",
        ast.UpdateStmt: "UPDATE",
        ast.DeleteStmt: "DELETE",
    }.get(type(statement), "OTHERS")


def _build_collection_timeline(
    workload_rows: Iterable[dict[str, Any]],
    recommendation_rows: Iterable[dict[str, Any]],
    environment_history: Iterable[dict[str, Any]],
    *, days: int | None,
) -> list[dict[str, Any]]:
    workload = sorted(
        (dict(row) for row in workload_rows),
        key=lambda row: (row.get("collected_at"), str(row.get("queryid") or "")),
    )
    timestamps = sorted({row.get("collected_at") for row in workload if row.get("collected_at")})
    if not timestamps:
        return []
    if days is not None:
        cutoff = timestamps[-1] - timedelta(days=days)
        first_visible = next((index for index, stamp in enumerate(timestamps) if stamp >= cutoff), 0)
        timestamps = timestamps[max(0, first_visible - 1):]
    visible = set(timestamps)
    rows_by_time: dict[Any, list[dict[str, Any]]] = {}
    for row in workload:
        if row.get("collected_at") in visible:
            rows_by_time.setdefault(row["collected_at"], []).append(row)

    recommendations_by_run: dict[str, dict[str, dict[str, Any]]] = {}
    for source in recommendation_rows:
        row = dict(source)
        recommendations_by_run.setdefault(str(row.get("run_id") or ""), {})[
            str(row.get("finding_fingerprint") or "")
        ] = row
    environment_by_run = {
        str(item.get("run_id") or ""): item for item in environment_history
    }

    previous_counters: dict[str, dict[str, Any]] = {}
    previous_intervals: dict[str, float] = {}
    previous_recommendations: dict[str, dict[str, Any]] = {}
    previous_global_average = None
    previous_global_calls = None
    previous_active_queries = None
    timeline = []
    for stamp in timestamps:
        snapshot_rows = rows_by_time.get(stamp, [])
        run_id = str(snapshot_rows[0].get("run_id") or "") if snapshot_rows else ""
        interval_queries = {}
        interval_calls = interval_time = 0.0
        statement_calls = {
            "SELECT": 0.0, "INSERT": 0.0, "UPDATE": 0.0,
            "DELETE": 0.0, "OTHERS": 0.0,
        }
        for row in snapshot_rows:
            queryid = str(row.get("queryid") or "")
            calls = _number_or_none(row.get("calls"))
            total_time = _number_or_none(row.get("total_exec_time_ms"))
            previous = previous_counters.get(queryid)
            average = None
            interval_query_calls = None
            if previous is None and calls is not None and total_time is not None and calls > 0:
                average = float(total_time) / float(calls)
                interval_query_calls = float(calls)
                interval_calls += float(calls)
                interval_time += float(total_time)
            if previous and None not in (calls, total_time, previous.get("calls"), previous.get("time")):
                delta_calls = float(calls) - float(previous["calls"])
                delta_time = float(total_time) - float(previous["time"])
                if delta_calls > 0 and delta_time >= 0:
                    average = delta_time / delta_calls
                    interval_query_calls = delta_calls
                    interval_calls += delta_calls
                    interval_time += delta_time
            if average is not None:
                statement_type = _query_statement_type(
                    row.get("query_sql") if "query_sql" in row else row.get("query")
                )
                statement_calls[statement_type] += float(interval_query_calls)
                prior_average = previous_intervals.get(queryid)
                change_pct = (
                    (average - prior_average) * 100 / prior_average
                    if prior_average is not None and prior_average > 0 else None
                )
                interval_queries[queryid] = {
                    "queryid": queryid,
                    "sql": row.get("query_sql") if "query_sql" in row else row.get("query"),
                    "average_time_ms": round(average, 4),
                    "calls": round(interval_query_calls, 2),
                    "total_time_ms": round(average * interval_query_calls, 2),
                    "change_pct": round(change_pct, 2) if change_pct is not None else None,
                    "impact_time_ms": (
                        round(abs(average - prior_average) * interval_query_calls, 2)
                        if prior_average is not None else None
                    ),
                }
                previous_intervals[queryid] = average
            if calls is not None and total_time is not None:
                previous_counters[queryid] = {"calls": calls, "time": total_time}

        global_average = interval_time / interval_calls if interval_calls > 0 else None
        global_change = (
            (global_average - previous_global_average) * 100 / previous_global_average
            if global_average is not None and previous_global_average not in (None, 0) else None
        )
        if global_average is not None:
            previous_global_average = global_average
        active_query_count = len(interval_queries)
        calls_change = (
            (interval_calls - previous_global_calls) * 100 / previous_global_calls
            if previous_global_calls not in (None, 0) else None
        )
        active_queries_change = (
            (active_query_count - previous_active_queries) * 100 / previous_active_queries
            if previous_active_queries not in (None, 0) else None
        )

        current_recommendations = recommendations_by_run.get(run_id, {})
        new_fingerprints = current_recommendations.keys() - previous_recommendations.keys()
        resolved_fingerprints = previous_recommendations.keys() - current_recommendations.keys()
        def recommendation_event(row: dict[str, Any], status: str) -> dict[str, Any]:
            query_impacts = []
            for queryid in _normalize_query_ids(row.get("query_ids")):
                impact = interval_queries.get(queryid)
                query_impacts.append({
                    "queryid": queryid,
                    "average_time_ms": impact.get("average_time_ms") if impact else None,
                    "change_pct": impact.get("change_pct") if impact else None,
                })
            return {
                "finding_fingerprint": str(row.get("finding_fingerprint") or ""),
                "title": row.get("title"), "priority": row.get("priority"),
                "description": row.get("description"),
                "recommendation_sql": row.get("recommendation_sql"),
                "status": status, "query_impacts": query_impacts,
                "evidence": "observed" if query_impacts else "unavailable",
            }
        new_items = [recommendation_event(current_recommendations[key], "new") for key in new_fingerprints]
        resolved_items = [
            recommendation_event(previous_recommendations[key], "no_longer_detected")
            for key in resolved_fingerprints
        ]
        for item in interval_queries.values():
            item["workload_share_pct"] = (
                round(item["total_time_ms"] * 100 / interval_time, 2)
                if interval_time > 0 else None
            )
        ranked_changes = sorted(
            (
                item for item in interval_queries.values()
                if item["impact_time_ms"] is not None
                and item["workload_share_pct"] is not None
                and item["workload_share_pct"] >= 1
            ),
            key=lambda item: (item["impact_time_ms"], item["total_time_ms"]), reverse=True,
        )[:8]
        timeline.append({
            "run_id": run_id, "collected_at": stamp,
            "workload": {
                "average_time_ms": round(global_average, 4) if global_average is not None else None,
                "calls": round(interval_calls, 2), "total_time_ms": round(interval_time, 2),
                "change_pct": round(global_change, 2) if global_change is not None else None,
                "calls_change_pct": round(calls_change, 2) if calls_change is not None else None,
                "active_queries": active_query_count,
                "active_queries_change_pct": (
                    round(active_queries_change, 2) if active_queries_change is not None else None
                ),
                "statement_calls": {
                    key: round(value, 2) for key, value in statement_calls.items()
                },
                "query_changes": ranked_changes,
            },
            "recommendations": {
                "new": new_items, "no_longer_detected": resolved_items,
                "active_count": len(current_recommendations),
            },
            "environment": environment_by_run.get(run_id),
        })
        previous_global_calls = interval_calls
        previous_active_queries = active_query_count
        previous_recommendations = current_recommendations
    return timeline


def _workload_prompt_measurement(
    point: dict[str, Any] | None, *, include_recommendation_events: bool = True
) -> dict[str, Any] | None:
    """Return only fields belonging to one workload measurement."""
    if point is None:
        return None
    recommendations = point.get("recommendations") or {}
    result = {
        "run_id": point.get("run_id"),
        "collected_at": point.get("collected_at"),
        "environment": point.get("environment"),
        "workload": point.get("workload") or {},
    }
    result["recommendations"] = (
        recommendations if include_recommendation_events
        else {"active_count": recommendations.get("active_count")}
    )
    return result


def build_workload_measurement_prompt(point: dict[str, Any], previous: dict[str, Any] | None = None) -> str:
    """Build context strictly limited to a selected measurement and its predecessor."""
    context = {
        "selected_measurement": _workload_prompt_measurement(point),
        "previous_measurement": _workload_prompt_measurement(
            previous, include_recommendation_events=False
        ),
    }
    return (
        "Analyze only the selected pgAssistant Collector workload measurement as a PostgreSQL "
        "expert. Use the immediately preceding measurement only as its comparison baseline; "
        "do not infer broader trends beyond these two measurements. "
        "Start by explicitly identifying every selected-measurement environment change, "
        "especially PostgreSQL version upgrades and setting changes, before discussing workload. "
        "Write a concise operational synthesis in Markdown with: (1) executive summary, "
        "(2) workload changes in execution time, calls and active queries, (3) environment "
        "changes that may explain them, (4) new and no-longer-detected advisor recommendations "
        "including concrete SQL where supplied, and (5) prioritized next checks. Do not claim "
        "that a recommendation was applied merely because it is no longer detected. Treat all "
        "before/after relationships as observational, call out missing evidence, and never invent "
        "SQL or parameter values.\n\nCollector evidence:\n"
        + json.dumps(context, default=str, ensure_ascii=False, indent=2)
    )


_SETTING_IMPACT_HINTS = {
    "shared_buffers": "May change cache hit rates and physical I/O pressure.",
    "effective_cache_size": "Changes planner estimates and can alter index usage.",
    "work_mem": "May change whether sorts and hash operations spill to disk.",
    "maintenance_work_mem": "Affects maintenance operations such as VACUUM and index creation.",
    "max_connections": "Can change memory pressure and connection contention.",
    "max_worker_processes": "Limits the worker pool available to background and parallel work.",
    "max_parallel_workers": "Limits parallel execution across the whole server.",
    "max_parallel_workers_per_gather": "Changes the parallelism available to one query.",
    "max_parallel_maintenance_workers": "Changes parallelism for maintenance operations.",
    "effective_io_concurrency": "Influences asynchronous I/O and bitmap heap scan costing.",
    "random_page_cost": "Can change the planner's preference for index scans.",
    "default_statistics_target": "Can change estimate quality and planning overhead.",
    "wal_buffers": "May affect WAL throughput for write-heavy workloads.",
    "checkpoint_completion_target": "Changes how checkpoint I/O is spread over time.",
    "max_wal_size": "Can change checkpoint frequency under write load.",
    "min_wal_size": "Changes how much WAL is retained for reuse.",
    "huge_pages": "Can affect memory management overhead.",
}


def _postgres_setting_changes(previous: Any, current: Any) -> list[dict[str, Any]]:
    if not isinstance(previous, dict) or not isinstance(current, dict):
        return []
    changes = []
    for name in sorted(set(previous) | set(current)):
        before, after = previous.get(name), current.get(name)
        if before == after:
            continue
        changes.append({
            "name": name,
            "before": before,
            "after": after,
            "possible_impact": _SETTING_IMPACT_HINTS.get(
                name, "May influence planning, resource usage, or execution behavior."
            ),
        })
    return changes


def _build_environment_history(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    history = []
    for source in rows:
        row = dict(source)
        changes = _postgres_setting_changes(
            row.get("previous_postgres_settings"), row.get("postgres_settings")
        )
        baseline_available = bool(
            row.get("previous_workload_release") or row.get("previous_postgres_version")
            or isinstance(row.get("previous_postgres_settings"), dict)
        )
        release_changed = bool(
            row.get("workload_release") and row.get("previous_workload_release")
            and row["workload_release"] != row["previous_workload_release"]
        )
        version_changed = bool(
            row.get("postgres_version") and row.get("previous_postgres_version")
            and row["postgres_version"] != row["previous_postgres_version"]
        )
        if baseline_available and not (release_changed or version_changed or changes):
            continue
        history.append({
            "run_id": str(row.get("run_id") or ""),
            "collected_at": row.get("collected_at"),
            "previous_collected_at": row.get("previous_collected_at"),
            "release": row.get("workload_release"),
            "previous_release": row.get("previous_workload_release"),
            "release_changed": release_changed,
            "version": row.get("postgres_version"),
            "previous_version": row.get("previous_postgres_version"),
            "version_changed": version_changed,
            "settings_changed": bool(changes),
            "setting_changes": changes,
            "baseline_available": baseline_available,
        })
    return history
