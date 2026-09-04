"""Read-only query metric history from an optional pgAssistant Collector."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from . import collector_history


METRICS = (
    ("mean_exec_time_ms", "Average time", "ms"),
    ("min_exec_time_ms", "Minimum time", "ms"),
    ("max_exec_time_ms", "Maximum time", "ms"),
    ("stddev_exec_time_ms", "Time standard deviation", "ms"),
    ("total_exec_time_ms", "Total execution time", "ms"),
    ("calls", "Calls", ""),
    ("rows", "Rows", ""),
    ("rows_per_call", "Rows per call", ""),
    ("cache_hit_ratio", "Cache hit ratio", "%"),
    ("cache_miss_share", "Cache miss share", "%"),
    ("share_calls", "Share of calls", "%"),
    ("share_total_time", "Share of total time", "%"),
    ("share_io", "Share of I/O", "%"),
    ("shared_blks_hit", "Shared blocks hit", "blocks"),
    ("shared_blks_read", "Shared blocks read", "blocks"),
    ("shared_blks_written", "Shared blocks written", "blocks"),
    ("total_blks_read", "Total blocks read", "blocks"),
    ("total_blks_written", "Total blocks written", "blocks"),
    ("temp_blks_read", "Temporary blocks read", "blocks"),
    ("temp_blks_written", "Temporary blocks written", "blocks"),
    ("local_blks_hit", "Local blocks hit", "blocks"),
    ("local_blks_read", "Local blocks read", "blocks"),
    ("local_blks_written", "Local blocks written", "blocks"),
    ("wal_bytes", "WAL generated", "bytes"),
    ("wal_records", "WAL records", ""),
    ("wal_fpi", "WAL full-page images", ""),
)
METRIC_NAMES = tuple(metric[0] for metric in METRICS)

# A relative change from a near-zero baseline can be spectacular while having no
# practical effect.  The workload verdict therefore requires both a meaningful
# relative change and at least this much execution time per comparison interval.
MIN_MATERIAL_IMPACT_MS = 100.0
MIN_MATERIAL_CHANGE_PCT = 1.0
MIN_QUERY_IMPACT_MS = 10.0
MIN_QUERY_ACTIVITY_CALLS = 10
MIN_QUERY_TOTAL_TIME_MS = 100.0


def _number(value: Any) -> int | float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, int):
        return value
    return float(value)


def load_query_history(queryid: str, target_id: str) -> dict[str, Any]:
    queryid = str(queryid or "").strip()
    if not queryid:
        raise ValueError("A query ID is required.")
    target_id = str(target_id or "").strip()
    if not target_id:
        raise ValueError("Select a Collector target in Database connection settings first.")

    connection = collector_history._connect()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            columns = ", ".join(METRIC_NAMES)
            cursor.execute(
                f"""
                SELECT collected_at, {columns}
                FROM (
                    SELECT collected_at, {columns}
                    FROM pga_ranked_query_snapshot
                    WHERE target_id = %s AND queryid = %s
                    ORDER BY collected_at DESC
                    LIMIT 2000
                ) AS recent_history
                ORDER BY collected_at
                """,
                (target_id, queryid),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as exc:
        raise collector_history.CollectorHistoryError(
            "Unable to read query history from the Collector repository."
        ) from exc
    finally:
        connection.close()

    if not rows:
        result = _empty_result(queryid, "No historical metrics were found for this query.")
        result["target_id"] = target_id
        return result

    available = [
        {"key": key, "label": label, "unit": unit}
        for key, label, unit in METRICS
        if any(row.get(key) is not None for row in rows)
    ]
    points = [
        {
            "collected_at": row["collected_at"],
            "values": {key: _number(row.get(key)) for key, _, _ in METRICS if row.get(key) is not None},
        }
        for row in rows
    ]
    default_metric = (
        "mean_exec_time_ms"
        if any(metric["key"] == "mean_exec_time_ms" for metric in available)
        else available[0]["key"]
    )
    return {
        "success": True,
        "queryid": queryid,
        "target_id": target_id,
        "default_metric": default_metric,
        "metrics": available,
        "points": points,
    }


def load_performance_evolution(
    target_id: str, *, days: int | None = 30, snapshot_limit: int = 200
) -> dict[str, Any]:
    """Build a workload-wide comparison from ranked-query counter deltas."""
    target_id = str(target_id or "").strip()
    if not target_id:
        raise ValueError("Select a Collector target in Database connection settings first.")
    if days is not None and days not in {0, 1, 7, 15, 30, 90}:
        raise ValueError("Performance period must be latest, 1, 7, 15, 30, 90 days or all.")
    snapshot_limit = max(2, min(int(snapshot_limit), 500))

    connection = collector_history._connect()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                WITH recent_snapshots AS (
                    SELECT DISTINCT collected_at
                    FROM pga_ranked_query_snapshot
                    WHERE target_id = %s
                    ORDER BY collected_at DESC
                    LIMIT %s
                )
                SELECT queryid, collected_at, calls, total_exec_time_ms,
                       mean_exec_time_ms
                FROM pga_ranked_query_snapshot
                WHERE target_id = %s
                  AND collected_at IN (SELECT collected_at FROM recent_snapshots)
                ORDER BY collected_at, queryid
                """,
                (target_id, snapshot_limit, target_id),
            )
            rows = [dict(row) for row in cursor.fetchall()]
    except psycopg2.Error as exc:
        raise collector_history.CollectorHistoryError(
            "Unable to read query performance history from the Collector repository."
        ) from exc
    finally:
        connection.close()
    return build_performance_evolution(rows, target_id=target_id, days=days)


def build_performance_evolution(
    rows: list[dict[str, Any]], *, target_id: str, days: int | None = 30,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare interval latency for every query observed in consecutive snapshots."""
    if not rows:
        return _empty_performance(target_id, days)
    ordered = sorted(rows, key=lambda row: (row.get("collected_at"), str(row.get("queryid"))))
    timestamps = sorted({row["collected_at"] for row in ordered if row.get("collected_at")})
    if len(timestamps) < 2:
        return _empty_performance(target_id, days, "At least two collections are required.")

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None and timestamps[-1].tzinfo is not None:
        now = now.replace(tzinfo=timezone.utc)
    elif now.tzinfo is not None and timestamps[-1].tzinfo is None:
        now = now.replace(tzinfo=None)
    cutoff = now - timedelta(days=days) if days not in {None, 0} else None
    # The first snapshot supplies the baseline and the second supplies the latest
    # counter interval, so two collections are sufficient for "Latest".
    visible_timestamps = (timestamps[-2:] if days == 0 else
                          [stamp for stamp in timestamps if cutoff is None or stamp >= cutoff])
    if len(visible_timestamps) < 2:
        visible_timestamps = timestamps[-2:]
    visible_set = set(visible_timestamps)
    position = {stamp: index for index, stamp in enumerate(visible_timestamps)}
    by_query: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ordered:
        queryid = str(row.get("queryid") or "").strip()
        if queryid and row.get("collected_at") in visible_set:
            by_query[queryid].append(row)

    query_intervals: dict[str, list[dict[str, Any]]] = defaultdict(list)
    timeline_totals: dict[datetime, dict[str, float]] = defaultdict(
        lambda: {"calls": 0.0, "time": 0.0, "queries": 0.0}
    )
    for queryid, query_rows in by_query.items():
        query_rows.sort(key=lambda row: row["collected_at"])
        first = query_rows[0]
        first_calls = _number(first.get("calls"))
        first_time = _number(first.get("total_exec_time_ms"))
        if first_calls is not None and first_time is not None and first_calls > 0 and first_time >= 0:
            query_intervals[queryid].append({
                "collected_at": first["collected_at"],
                "avg_time_ms": float(first_time) / float(first_calls),
                "calls": float(first_calls),
                "total_time_ms": float(first_time),
                "baseline_snapshot": True,
            })
        for previous, current in zip(query_rows, query_rows[1:]):
            previous_at, current_at = previous["collected_at"], current["collected_at"]
            if position[current_at] != position[previous_at] + 1:
                continue
            values = tuple(_number(row.get(key)) for row, key in (
                (previous, "calls"), (current, "calls"),
                (previous, "total_exec_time_ms"), (current, "total_exec_time_ms"),
            ))
            if any(value is None for value in values):
                continue
            previous_calls, current_calls, previous_time, current_time = values
            delta_calls = float(current_calls) - float(previous_calls)
            delta_time = float(current_time) - float(previous_time)
            if delta_calls < 0 or delta_time < 0:
                query_intervals[queryid].clear()
                if current_calls > 0 and current_time >= 0:
                    query_intervals[queryid].append({
                        "collected_at": current_at,
                        "avg_time_ms": float(current_time) / float(current_calls),
                        "calls": float(current_calls),
                        "total_time_ms": float(current_time),
                        "baseline_snapshot": True,
                    })
                continue
            if delta_calls == 0:
                continue
            interval = {"collected_at": current_at, "avg_time_ms": delta_time / delta_calls,
                        "calls": delta_calls, "total_time_ms": delta_time}
            query_intervals[queryid].append(interval)
            totals = timeline_totals[current_at]
            totals["calls"] += delta_calls
            totals["time"] += delta_time
            totals["queries"] += 1

    latest_at = visible_timestamps[-1]
    latest_ids = {str(row.get("queryid")) for row in ordered if row.get("collected_at") == latest_at}
    comparisons = []
    baseline_expected_ms = 0.0
    current_actual_ms = 0.0
    for queryid, intervals in query_intervals.items():
        if len(intervals) < 2:
            continue
        baseline, current = intervals[0], intervals[-1]
        baseline_ms, current_ms = baseline["avg_time_ms"], current["avg_time_ms"]
        if baseline_ms <= 0:
            continue
        change_pct = 100 * (current_ms - baseline_ms) / baseline_ms
        saved_ms = (baseline_ms - current_ms) * current["calls"]
        baseline_expected_ms += baseline_ms * current["calls"]
        current_actual_ms += current_ms * current["calls"]
        classification = "improved" if change_pct <= -10 else "degraded" if change_pct >= 10 else "stable"
        comparisons.append({
            "queryid": queryid, "baseline_avg_time_ms": round(baseline_ms, 3),
            "current_avg_time_ms": round(current_ms, 3), "change_pct": round(change_pct, 1),
            "estimated_saved_time_ms": round(saved_ms, 2),
            "current_total_time_ms": round(current_ms * current["calls"], 2),
            "current_interval_calls": int(current["calls"]), "classification": classification,
            "ranking_status": "ranked" if queryid in latest_ids else "left_top_50",
            "last_observed_at": by_query[queryid][-1]["collected_at"],
        })

    timeline = [
        {
            "collected_at": stamp,
            "avg_time_ms": round(values["time"] / values["calls"], 3),
            "calls": int(values["calls"]),
            "calls_per_minute": round(
                values["calls"] * 60
                / (stamp - visible_timestamps[position[stamp] - 1]).total_seconds(), 3
            ),
            "interval_seconds": round(
                (stamp - visible_timestamps[position[stamp] - 1]).total_seconds(), 1
            ),
            "queries": int(values["queries"]),
        }
        for stamp, values in sorted(timeline_totals.items()) if values["calls"] > 0
        and position[stamp] > 0
        and (stamp - visible_timestamps[position[stamp] - 1]).total_seconds() > 0
    ]
    # Compare like with like: estimate how much the current calls would have cost
    # at each query's baseline latency, then compare that with their actual cost.
    # This prevents a changing Top 50/query mix from dominating the verdict and
    # weights each query by its activity in the current interval.
    impact_ms = current_actual_ms - baseline_expected_ms
    global_change = (
        round(100 * impact_ms / baseline_expected_ms, 1)
        if baseline_expected_ms > 0 else None
    )
    material_threshold_ms = max(
        MIN_MATERIAL_IMPACT_MS,
        baseline_expected_ms * MIN_MATERIAL_CHANGE_PCT / 100,
    )
    if not comparisons or global_change is None or abs(impact_ms) < material_threshold_ms:
        global_verdict = "stable"
    else:
        global_verdict = "improved" if impact_ms < 0 else "degraded"
    total_current_calls = sum(item["current_interval_calls"] for item in comparisons)
    for item in comparisons:
        item["activity_share_pct"] = round(
            100 * item["current_interval_calls"] / total_current_calls, 2
        ) if total_current_calls else 0
        item["total_time_share_pct"] = round(
            100 * item["current_total_time_ms"] / current_actual_ms, 2
        ) if current_actual_ms else 0

    # Top lists are impact lists, not percentage lists. A low-volume query only
    # appears when its absolute time impact is material; busy or costly queries
    # are also surfaced through their call count or total execution time.
    def is_material_query(item: dict[str, Any]) -> bool:
        return (
            abs(item["estimated_saved_time_ms"]) >= MIN_QUERY_IMPACT_MS
            or item["current_interval_calls"] >= MIN_QUERY_ACTIVITY_CALLS
            or item["current_total_time_ms"] >= MIN_QUERY_TOTAL_TIME_MS
        )

    improved = sorted(
        (item for item in comparisons
         if item["classification"] == "improved" and is_material_query(item)),
        key=lambda item: item["estimated_saved_time_ms"], reverse=True,
    )
    degraded = sorted(
        (item for item in comparisons
         if item["classification"] == "degraded" and is_material_query(item)),
        key=lambda item: item["estimated_saved_time_ms"],
    )
    return {
        "success": True, "target_id": target_id, "days": days,
        "period": {"from": visible_timestamps[0], "to": latest_at},
        "summary": {
            "global_change_pct": global_change, "global_verdict": global_verdict,
            "baseline_expected_time_ms": round(baseline_expected_ms, 2),
            "current_actual_time_ms": round(current_actual_ms, 2),
            "improved": sum(item["classification"] == "improved" for item in comparisons),
            "degraded": sum(item["classification"] == "degraded" for item in comparisons),
            "stable": sum(item["classification"] == "stable" for item in comparisons),
            "comparable": len(comparisons),
            "left_top_50": sum(item["ranking_status"] == "left_top_50" for item in comparisons),
            "estimated_saved_time_ms": round(-impact_ms, 2),
        },
        "timeline": timeline, "gains": improved[:10], "regressions": degraded[:10],
        "message": None if comparisons else "Not enough comparable query intervals were found.",
    }


def _empty_performance(target_id: str, days: int | None, message: str = "No query performance history was found.") -> dict[str, Any]:
    return {
        "success": True, "target_id": target_id, "days": days,
        "period": {"from": None, "to": None},
        "summary": {"global_change_pct": None, "global_verdict": "stable",
                    "baseline_expected_time_ms": 0, "current_actual_time_ms": 0,
                    "improved": 0, "degraded": 0,
                    "stable": 0, "comparable": 0, "left_top_50": 0,
                    "estimated_saved_time_ms": 0},
        "timeline": [], "gains": [], "regressions": [], "message": message,
    }


def _empty_result(queryid: str, message: str) -> dict[str, Any]:
    return {
        "success": True,
        "queryid": queryid,
        "target_id": None,
        "default_metric": None,
        "metrics": [],
        "points": [],
        "message": message,
    }
