"""Read-only Executive Plan history from an optional pgAssistant Collector."""
from __future__ import annotations

import os
import hashlib
import re
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import psycopg2
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
                    recommendations.description
                    ,tasks.title AS package_title
                    ,tasks.phase_number
                    ,tasks.workstream
                    ,tasks.task_order AS package_order
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

    for snapshot in visible:
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

        for fingerprint in sorted(new_keys):
            finding = dict(current[fingerprint])
            finding["detected_at"] = snapshot["collected_at"]
            finding["first_seen_at"] = first_seen[fingerprint]
            finding["reopened"] = fingerprint in seen_before_period
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            additions.append(finding)
        for fingerprint in sorted(corrected_keys):
            finding = dict(previous[fingerprint])
            finding["corrected_at"] = snapshot["collected_at"]
            finding["first_seen_at"] = first_seen.get(fingerprint)
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            corrections.append(finding)
        for fingerprint in sorted(modified_keys):
            finding = dict(current[fingerprint])
            finding["modified_at"] = snapshot["collected_at"]
            finding["live_comparison"] = bool(snapshot.get("is_current"))
            modifications.append(finding)

        timeline.append({
            "run_id": snapshot["run_id"],
            "collected_at": snapshot["collected_at"],
            "active": len(current_keys),
            "new": len(new_keys),
            "corrected": len(corrected_keys),
            "modified": len(modified_keys),
            "persistent": len(persistent_keys - modified_keys),
            "is_current": bool(snapshot.get("is_current")),
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
