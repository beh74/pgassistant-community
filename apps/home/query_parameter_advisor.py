# -*- encoding: utf-8 -*-
"""Workload-based advisor for pgTune-style PostgreSQL parameters."""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List

from . import database
from . import pgtune
from . import pgtune_resource_detector
from . import sqlhelper


PGTUNE_PARAMETERS = [
    "max_connections",
    "shared_buffers",
    "effective_cache_size",
    "maintenance_work_mem",
    "checkpoint_completion_target",
    "wal_buffers",
    "default_statistics_target",
    "random_page_cost",
    "effective_io_concurrency",
    "work_mem",
    "huge_pages",
    "min_wal_size",
    "max_wal_size",
    "max_worker_processes",
    "max_parallel_workers_per_gather",
    "max_parallel_workers",
    "max_parallel_maintenance_workers",
]

INTERNAL_SCHEMAS = {"pg_catalog", "information_schema"}
BLOCK_SIZE_BYTES = 8192
PGTUNE_RELATIVE_THRESHOLD = 0.20
PGTUNE_MEMORY_PARAMETERS = {
    "shared_buffers",
    "effective_cache_size",
    "maintenance_work_mem",
    "wal_buffers",
    "work_mem",
    "min_wal_size",
    "max_wal_size",
}
PGTUNE_RESTART_PARAMETERS = {
    "max_connections",
    "shared_buffers",
    "wal_buffers",
    "huge_pages",
    "max_worker_processes",
}


def _to_float(value: Any, default: float = 0.0) -> float:
    """Convert a database value to float while keeping aggregation resilient."""
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    """Convert a database value to int while tolerating empty or invalid input."""
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _json_value(value: Any) -> Any:
    """Normalize values returned by psycopg2 so they can be JSON serialized."""
    if isinstance(value, Decimal):
        return float(value)
    return value


def _bytes_pretty(value: float) -> str:
    """Format a byte count for API/UI summaries."""
    size = float(value or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size) < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _format_setting_literal(value: str) -> str:
    """Escape a setting value for an ALTER SYSTEM proposal."""
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _parse_pg_memory_bytes(value: Any) -> int | None:
    """Parse common PostgreSQL memory settings such as 4MB or 1GB to bytes."""
    if value is None:
        return None

    raw = str(value).strip().replace(" ", "")
    if not raw:
        return None

    lower = raw.lower()
    units = [
        ("tb", 1024**4),
        ("gb", 1024**3),
        ("mb", 1024**2),
        ("kb", 1024),
        ("b", 1),
    ]

    for suffix, multiplier in units:
        if lower.endswith(suffix):
            number = lower[: -len(suffix)]
            try:
                return int(float(number) * multiplier)
            except ValueError:
                return None

    try:
        return int(float(lower))
    except ValueError:
        return None


def _memory_setting_from_bytes(value: int) -> str:
    """Render a byte count back to a compact PostgreSQL memory setting."""
    if value >= 1024**3 and value % (1024**3) == 0:
        return f"{value // (1024**3)}GB"
    if value >= 1024**2:
        return f"{max(1, round(value / (1024**2)))}MB"
    if value >= 1024:
        return f"{max(1, round(value / 1024))}kB"
    return str(value)


def _propose_double_memory(current_value: Any, fallback: str) -> str:
    """Propose a conservative memory increase by doubling the current value."""
    bytes_value = _parse_pg_memory_bytes(current_value)
    if not bytes_value:
        return fallback
    return _memory_setting_from_bytes(bytes_value * 2)


def _setting_values_equal(current_value: Any, proposed_value: Any) -> bool:
    """Return True when a recommendation would propose the already active value."""
    if current_value is None or proposed_value is None:
        return False

    current = str(current_value).strip().lower()
    proposed = str(proposed_value).strip().lower()

    if current == proposed:
        return True

    try:
        return float(current) == float(proposed)
    except ValueError:
        return False


def _comparable_setting_value(parameter: str, value: Any) -> float | None:
    """Normalize a pgTune/current value for relative difference calculations."""
    if parameter in PGTUNE_MEMORY_PARAMETERS:
        parsed = _parse_pg_memory_bytes(value)
        return float(parsed) if parsed is not None else None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _build_pgtune_recommendation(
    running_values: Dict[str, Any],
    tuned_values: Dict[str, Any],
    resources: Dict[str, Any],
    threshold: float = PGTUNE_RELATIVE_THRESHOLD,
) -> Dict[str, Any]:
    """Build one pgTune recommendation containing all significant changes."""
    changes = []
    for parameter in PGTUNE_PARAMETERS:
        if parameter not in running_values or parameter not in tuned_values:
            continue
        current = _comparable_setting_value(parameter, running_values[parameter])
        proposed = _comparable_setting_value(parameter, tuned_values[parameter])
        if current is None or proposed is None or proposed == 0:
            continue
        difference = abs(current - proposed) / abs(proposed)
        if difference <= threshold:
            continue
        changes.append({
            "parameter": parameter,
            "current_value": running_values[parameter],
            "proposed_value": tuned_values[parameter],
            "difference_pct": round(difference * 100, 1),
        })

    if not changes:
        return {}

    restart_parameters = [
        change["parameter"]
        for change in changes
        if change["parameter"] in PGTUNE_RESTART_PARAMETERS
    ]
    sql_lines = [
        "-- Generated from pgTune estimates; review before applying.",
        *[
            f"ALTER SYSTEM SET {change['parameter']} = "
            f"{_format_setting_literal(change['proposed_value'])};"
            for change in changes
        ],
        "SELECT pg_reload_conf();",
    ]
    if restart_parameters:
        sql_lines.append(
            "-- Restart PostgreSQL to activate: " + ", ".join(restart_parameters)
        )
    evidence = [
        "Source: pgTune (https://pgtune.leopard.in.ua/).",
        f"Detected resources: {resources['cpu']} CPU(s), {resources['memory_mb']} MB memory.",
        f"Detected environment: {resources['environment']}.",
        "Assumptions: Web application workload and SSD storage.",
        f"Only differences greater than {round(threshold * 100)}% are included.",
    ]
    if restart_parameters:
        evidence.append(
            "PostgreSQL restart required for: " + ", ".join(restart_parameters) + "."
        )
    evidence.extend(
        f"{change['parameter']}: {change['current_value']} → "
        f"{change['proposed_value']} ({change['difference_pct']}% difference)"
        for change in changes
    )
    return {
        "parameter": "pgTune configuration",
        "source": "pgtune",
        "current_value": f"{len(changes)} setting(s) outside tolerance",
        "proposed_value": "Combined pgTune baseline",
        "confidence": "review",
        "reason": (
            "The active PostgreSQL configuration differs significantly from a "
            "pgTune baseline calculated from the automatically detected resources."
        ),
        "evidence": evidence,
        "changes": changes,
        "restart_required": bool(restart_parameters),
        "restart_parameters": restart_parameters,
        "alter_system_sql": "\n".join(sql_lines),
    }


def _run_pgtune_baseline(
    connection,
    pg_major_version: int,
    running_values: Dict[str, Any],
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Detect server resources and run pgTune with documented defaults."""
    resources = pgtune_resource_detector.detect_postgresql_resources(connection)
    tuner = pgtune.pgTune(
        str(pg_major_version),
        resources["cpu"],
        f"{resources['memory_mb']}MB",
        "ssd",
        "web",
        running_values.get("max_connections") or 100,
    )
    return resources, tuner.get_pg_tune()


def _get_postgres_major_version(db_config: Dict[str, Any]) -> int:
    """Resolve the PostgreSQL major version from session config or the database."""
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


def _fetch_pg_stat_statements_rows(conn, database_name: str = "") -> List[Dict[str, Any]]:
    """Read the workload snapshot used to aggregate runtime counters."""
    sql = """
        SELECT
            queryid::text AS queryid,
            query,
            calls::bigint AS calls,
            rows::bigint AS rows,
            total_exec_time::float8 AS total_exec_time,
            mean_exec_time::float8 AS mean_exec_time,
            shared_blks_hit::bigint AS shared_blks_hit,
            shared_blks_read::bigint AS shared_blks_read,
            shared_blks_written::bigint AS shared_blks_written,
            local_blks_hit::bigint AS local_blks_hit,
            local_blks_read::bigint AS local_blks_read,
            local_blks_written::bigint AS local_blks_written,
            temp_blks_read::bigint AS temp_blks_read,
            temp_blks_written::bigint AS temp_blks_written,
            wal_records::bigint AS wal_records,
            wal_fpi::bigint AS wal_fpi,
            wal_bytes::float8 AS wal_bytes
        FROM pg_stat_statements
        WHERE query IS NOT NULL
          AND btrim(query) <> ''
          AND calls > 0
          AND lower(query) NOT LIKE '/* launched by pgassistant */%'
        ORDER BY total_exec_time DESC
    """
    qualified_relation, relation_error = database.get_pg_stat_statements_relation(conn)
    if not qualified_relation:
        raise RuntimeError(relation_error or "pg_stat_statements is unavailable.")
    sql = database.prepare_pgss_sql(
        sql,
        conn,
        database_name,
        qualified_relation=qualified_relation,
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        return [
            {columns[index]: _json_value(value) for index, value in enumerate(row)}
            for row in cur.fetchall()
        ]


def _generic_plan_for_query(conn, query: str) -> Any:
    """Ask PostgreSQL 16+ for a generic JSON plan for a normalized query."""
    normalized_query = sqlhelper.normalize_query_for_parameter_analysis(query)
    explain_sql = (
        "EXPLAIN (GENERIC_PLAN TRUE, VERBOSE TRUE, SETTINGS TRUE, FORMAT JSON) "
        + normalized_query
    )
    with conn.cursor() as cur:
        cur.execute(explain_sql)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Generic plan returned no rows.")
    return row[0]


def _walk_plan_nodes(node: Any):
    """Yield every plan node from PostgreSQL's nested JSON plan structure."""
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
    """Detect plans that target PostgreSQL internal schemas and should be skipped."""
    for node in _walk_plan_nodes(plan_json):
        schema = str(node.get("Schema") or "").strip()
        if schema in INTERNAL_SCHEMAS or schema.startswith("pg_toast"):
            return True
    return False


def _empty_plan_metrics() -> Dict[str, Any]:
    """Return the initial metrics bucket used for one plan or the full workload."""
    return {
        "nodes": 0,
        "sort_nodes": 0,
        "hash_nodes": 0,
        "hash_join_nodes": 0,
        "aggregate_nodes": 0,
        "seq_scan_nodes": 0,
        "index_scan_nodes": 0,
        "bitmap_scan_nodes": 0,
        "materialize_nodes": 0,
        "gather_nodes": 0,
        "parallel_aware_nodes": 0,
        "workers_planned": 0,
        "total_plan_cost": 0.0,
        "max_plan_cost": 0.0,
        "plan_rows": 0.0,
    }


def _collect_plan_metrics(plan_json: Any) -> Dict[str, Any]:
    """Count useful structural signals from a generic plan."""
    metrics = _empty_plan_metrics()

    for node in _walk_plan_nodes(plan_json):
        metrics["nodes"] += 1
        node_type = str(node.get("Node Type") or "")

        if node_type in {"Sort", "Incremental Sort"}:
            metrics["sort_nodes"] += 1
        if node_type in {"Hash", "Hash Join"}:
            metrics["hash_nodes"] += 1
        if node_type == "Hash Join":
            metrics["hash_join_nodes"] += 1
        if node_type in {"Aggregate", "GroupAggregate", "HashAggregate", "MixedAggregate"}:
            metrics["aggregate_nodes"] += 1
        if node_type == "Seq Scan":
            metrics["seq_scan_nodes"] += 1
        if node_type in {"Index Scan", "Index Only Scan"}:
            metrics["index_scan_nodes"] += 1
        if node_type in {"Bitmap Heap Scan", "Bitmap Index Scan"}:
            metrics["bitmap_scan_nodes"] += 1
        if node_type == "Materialize":
            metrics["materialize_nodes"] += 1
        if node_type in {"Gather", "Gather Merge"}:
            metrics["gather_nodes"] += 1

        if node.get("Parallel Aware"):
            metrics["parallel_aware_nodes"] += 1

        metrics["workers_planned"] += _to_int(node.get("Workers Planned"), 0)
        total_cost = _to_float(node.get("Total Cost"), 0.0)
        metrics["total_plan_cost"] += total_cost
        metrics["max_plan_cost"] = max(metrics["max_plan_cost"], total_cost)
        metrics["plan_rows"] += _to_float(node.get("Plan Rows"), 0.0)

    return metrics


def _sum_rows(rows: Iterable[Dict[str, Any]], key: str) -> float:
    """Sum a numeric field from pg_stat_statements rows."""
    return sum(_to_float(row.get(key), 0.0) for row in rows)


def _aggregate_statement_metrics(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate runtime counters from the pg_stat_statements workload snapshot."""
    calls = _sum_rows(rows, "calls")
    total_exec_time = _sum_rows(rows, "total_exec_time")
    shared_hit = _sum_rows(rows, "shared_blks_hit")
    shared_read = _sum_rows(rows, "shared_blks_read")
    temp_read = _sum_rows(rows, "temp_blks_read")
    temp_written = _sum_rows(rows, "temp_blks_written")
    wal_bytes = _sum_rows(rows, "wal_bytes")
    total_shared = shared_hit + shared_read

    return {
        "queries_seen": len(rows),
        "calls": int(calls),
        "total_exec_time_ms": round(total_exec_time, 2),
        "shared_blks_hit": int(shared_hit),
        "shared_blks_read": int(shared_read),
        "shared_cache_hit_ratio_pct": (
            round(100.0 * shared_hit / total_shared, 2) if total_shared else None
        ),
        "temp_blks_read": int(temp_read),
        "temp_blks_written": int(temp_written),
        "temp_bytes": int((temp_read + temp_written) * BLOCK_SIZE_BYTES),
        "temp_pretty": _bytes_pretty((temp_read + temp_written) * BLOCK_SIZE_BYTES),
        "wal_bytes": int(wal_bytes),
        "wal_pretty": _bytes_pretty(wal_bytes),
        "queries_with_temp": sum(
            1 for row in rows
            if _to_int(row.get("temp_blks_read"), 0) + _to_int(row.get("temp_blks_written"), 0) > 0
        ),
        "queries_with_shared_reads": sum(
            1 for row in rows if _to_int(row.get("shared_blks_read"), 0) > 0
        ),
    }


def _merge_plan_metrics(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """Accumulate one query's plan metrics into the workload-level metrics."""
    for key, value in source.items():
        if key == "max_plan_cost":
            target[key] = max(target.get(key, 0.0), value)
        else:
            target[key] = target.get(key, 0) + value


def _make_recommendation(
    parameter: str,
    current_value: Any,
    proposed_value: str | None,
    confidence: str,
    reason: str,
    evidence: List[str],
) -> Dict[str, Any]:
    """Build a single recommendation payload and optional SQL proposal."""
    if _setting_values_equal(current_value, proposed_value):
        return {}

    alter_system_sql = None
    if proposed_value:
        alter_system_sql = (
            f"ALTER SYSTEM SET {parameter} = {_format_setting_literal(proposed_value)};\n"
            "SELECT pg_reload_conf();"
        )

    return {
        "parameter": parameter,
        "current_value": current_value,
        "proposed_value": proposed_value,
        "confidence": confidence,
        "reason": reason,
        "evidence": evidence,
        "alter_system_sql": alter_system_sql,
    }


def _build_recommendations(
    running_values: Dict[str, Any],
    statement_metrics: Dict[str, Any],
    plan_metrics: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply the advisor rules and return only actionable parameter reviews."""
    recommendations = []
    total_queries = max(statement_metrics.get("queries_seen") or 0, 1)
    temp_bytes = statement_metrics.get("temp_bytes") or 0
    queries_with_temp = statement_metrics.get("queries_with_temp") or 0
    sort_nodes = plan_metrics.get("sort_nodes") or 0
    hash_nodes = plan_metrics.get("hash_nodes") or 0
    aggregate_nodes = plan_metrics.get("aggregate_nodes") or 0
    seq_scan_nodes = plan_metrics.get("seq_scan_nodes") or 0
    gather_nodes = plan_metrics.get("gather_nodes") or 0
    cache_hit = statement_metrics.get("shared_cache_hit_ratio_pct")
    shared_reads = statement_metrics.get("shared_blks_read") or 0
    wal_bytes = statement_metrics.get("wal_bytes") or 0

    if temp_bytes > 0 or sort_nodes + hash_nodes + aggregate_nodes > total_queries:
        proposed = _propose_double_memory(running_values.get("work_mem"), "64MB")
        recommendation = _make_recommendation(
            "work_mem",
            running_values.get("work_mem"),
            proposed,
            "high" if temp_bytes > 0 else "review",
            "The workload uses temporary blocks and/or many memory-sensitive plan nodes.",
            [
                f"Temp usage: {statement_metrics.get('temp_pretty')}",
                f"Queries with temp usage: {queries_with_temp}",
                f"Sort nodes: {sort_nodes}",
                f"Hash nodes: {hash_nodes}",
                f"Aggregate nodes: {aggregate_nodes}",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

    if cache_hit is not None and cache_hit < 95 and shared_reads > 0:
        recommendation = _make_recommendation(
            "effective_cache_size",
            running_values.get("effective_cache_size"),
            None,
            "review",
            "Shared block reads are significant and cache hit ratio is below the usual target for OLTP workloads.",
            [
                f"Shared cache hit ratio: {cache_hit}%",
                f"Shared blocks read: {int(shared_reads)}",
                "Set this with pgTune using realistic RAM and OS cache assumptions.",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

    if shared_reads > 0 and seq_scan_nodes > 0:
        recommendation = _make_recommendation(
            "random_page_cost",
            running_values.get("random_page_cost"),
            "1.1",
            "review",
            "The plans include sequential scans and the workload performs shared reads. On SSD storage, PostgreSQL often benefits from a lower random_page_cost.",
            [
                f"Sequential scan nodes: {seq_scan_nodes}",
                f"Shared blocks read: {int(shared_reads)}",
                "Use this only if the database storage is SSD/NVMe and the current value is still HDD-oriented.",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

        recommendation = _make_recommendation(
            "effective_io_concurrency",
            running_values.get("effective_io_concurrency"),
            "200",
            "review",
            "The workload reads from storage. On SSD/NVMe, higher effective_io_concurrency can help bitmap and prefetch-heavy access patterns.",
            [
                f"Shared blocks read: {int(shared_reads)}",
                f"Bitmap scan nodes: {plan_metrics.get('bitmap_scan_nodes') or 0}",
                "Keep conservative values for HDD or constrained cloud storage.",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

    parallel_candidates = seq_scan_nodes + aggregate_nodes + sort_nodes
    if parallel_candidates >= max(5, total_queries * 0.15) and gather_nodes == 0:
        recommendation = _make_recommendation(
            "max_parallel_workers_per_gather",
            running_values.get("max_parallel_workers_per_gather"),
            "2",
            "review",
            "Several plans look parallelizable, but the generic plans did not choose Gather nodes.",
            [
                f"Potentially parallel-friendly nodes: {parallel_candidates}",
                f"Gather nodes: {gather_nodes}",
                "Review together with max_parallel_workers and max_worker_processes.",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

    if wal_bytes > 1024 * 1024 * 1024:
        recommendation = _make_recommendation(
            "max_wal_size",
            running_values.get("max_wal_size"),
            _propose_double_memory(running_values.get("max_wal_size"), "4GB"),
            "review",
            "pg_stat_statements reports high WAL volume for the captured workload.",
            [
                f"WAL generated: {statement_metrics.get('wal_pretty')}",
                "Confirm with checkpoint statistics before changing WAL/checkpoint parameters.",
            ],
        )
        if recommendation:
            recommendations.append(recommendation)

    return recommendations


def analyze_query_parameter_workload(db_config: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point used by the API to analyze workload-level parameter signals."""
    pg_major_version = _get_postgres_major_version(db_config)
    running_values, running_major = database.get_pg_tune_parameter(db_config)

    result = {
        "success": True,
        "supported": True,
        "workload_analysis_supported": pg_major_version >= 16,
        "required_version": 16,
        "postgres_major_version": pg_major_version,
        "pg_tune_parameter_names": PGTUNE_PARAMETERS,
        "pg_tune_parameters": running_values,
        "results": [],
        "recommendations": [],
        "pgtune": {
            "available": False,
            "source": "pgTune",
            "threshold_pct": round(PGTUNE_RELATIVE_THRESHOLD * 100),
        },
        "summary": {},
    }

    if pg_major_version < 16:
        result["message"] = (
            "pgTune analysis is available, but generic workload plans require "
            "PostgreSQL 16 or newer."
        )

    plan_conn, status = database.connectdb(db_config)
    if plan_conn is None:
        raise RuntimeError(status or "Unable to connect to database.")

    stats_conn = plan_conn
    if pg_major_version >= 16 and database.is_multi_db(db_config):
        stats_conn, stats_status = database.connectdb(
            database.get_monitoring_db_config(db_config)
        )
        if stats_conn is None:
            plan_conn.close()
            raise RuntimeError(
                stats_status or "Unable to connect to the monitoring database."
            )

    db_name = database.get_resolved_database_name(db_config)
    plan_metrics = _empty_plan_metrics()
    planned_rows = []
    queries_planned = 0
    queries_failed = 0
    queries_skipped_internal = 0

    try:
        pgtune_recommendation = {}
        try:
            resources, tuned_values = _run_pgtune_baseline(
                plan_conn,
                pg_major_version,
                running_values,
            )
            pgtune_recommendation = _build_pgtune_recommendation(
                running_values,
                tuned_values,
                resources,
            )
            result["pgtune"] = {
                "available": True,
                "source": "pgTune",
                "resources": resources,
                "assumptions": {"database_type": "web", "storage": "ssd"},
                "threshold_pct": round(PGTUNE_RELATIVE_THRESHOLD * 100),
                "tuned_values": tuned_values,
                "significant_changes": pgtune_recommendation.get("changes", []),
                "sql": pgtune_recommendation.get("alter_system_sql"),
            }
        except Exception as exc:
            result["pgtune"]["error"] = str(exc)

        if pg_major_version < 16:
            recommendations = [pgtune_recommendation] if pgtune_recommendation else []
            result.update({
                "recommendations": recommendations,
                "summary": {
                    "queries_seen": 0,
                    "queries_planned": 0,
                    "queries_failed": 0,
                    "queries_skipped_internal": 0,
                    "recommendations": len(recommendations),
                    "plan_metrics": plan_metrics,
                    "pg_tune_major_version": running_major,
                },
            })
            return result

        pgss_rows = _fetch_pg_stat_statements_rows(stats_conn, db_name)
        statement_metrics = _aggregate_statement_metrics(pgss_rows)

        for row in pgss_rows:
            query = row.get("query") or ""
            query_result = {
                "queryid": str(row.get("queryid")) if row.get("queryid") is not None else None,
                "calls": _to_int(row.get("calls"), 0),
                "total_exec_time": _to_float(row.get("total_exec_time"), 0.0),
                "temp_blks_read": _to_int(row.get("temp_blks_read"), 0),
                "temp_blks_written": _to_int(row.get("temp_blks_written"), 0),
                "ok": False,
                "error": None,
                "plan_metrics": _empty_plan_metrics(),
            }

            try:
                plan_json = _generic_plan_for_query(plan_conn, query)
                if _plan_uses_internal_schema(plan_json):
                    queries_skipped_internal += 1
                    continue

                metrics = _collect_plan_metrics(plan_json)
                _merge_plan_metrics(plan_metrics, metrics)
                query_result["ok"] = True
                query_result["plan_metrics"] = metrics
                queries_planned += 1
            except Exception as exc:
                query_result["error"] = str(exc)
                queries_failed += 1

            planned_rows.append(query_result)

        recommendations = _build_recommendations(
            running_values,
            statement_metrics,
            plan_metrics,
        )
        if result["pgtune"].get("available"):
            pgtune_parameters = {
                change["parameter"]
                for change in pgtune_recommendation.get("changes", [])
            }
            recommendations = [
                recommendation
                for recommendation in recommendations
                if recommendation.get("parameter") not in pgtune_parameters
            ]
            # pgTune owns the single executable configuration script. Workload
            # findings remain visible as review evidence without competing SQL.
            for recommendation in recommendations:
                recommendation["alter_system_sql"] = None
            if pgtune_recommendation:
                recommendations.insert(0, pgtune_recommendation)

        result.update({
            "results": planned_rows,
            "recommendations": recommendations,
            "summary": {
                **statement_metrics,
                "queries_planned": queries_planned,
                "queries_failed": queries_failed,
                "queries_skipped_internal": queries_skipped_internal,
                "recommendations": len(recommendations),
                "plan_metrics": plan_metrics,
                "pg_tune_major_version": running_major,
            },
        })
        return result
    finally:
        if stats_conn is not plan_conn:
            stats_conn.close()
        plan_conn.close()
