"""Per-table analyze and autovacuum tuning advisor based on advisor_enriched.yml rules."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .database import connectdb, db_fetch_json
from .global_advisor import load_recommendation_catalog

MAINTENANCE_FLAGGED_RULE_ID = "table_autovacuum_maintenance_flagged"
CLUSTER_LOAD_RULE_ID = "table_autovacuum_cluster_load"
VACUUM_URGENCY_RULE_ID = "autovacuum_dead_tuple_vacuum_urgency"
DEFAULT_STALE_DAYS = 7
DEFAULT_MIN_TABLE_BYTES = 1024 * 1024
DEAD_TUPLE_MIN_ABSOLUTE = 10_000
DEAD_TUPLE_MIN_RATIO = 0.05
DEAD_TUPLE_HIGH_ABSOLUTE = 100_000
LARGE_TABLE_BYTES = 1024**3
LARGE_TABLE_DEAD_RATIO = 0.20


def normalize_stale_days(value: str | int | None) -> int:
    try:
        days = int(value or DEFAULT_STALE_DAYS)
    except (TypeError, ValueError):
        days = DEFAULT_STALE_DAYS
    return max(1, min(365, days))


def _find_rule(catalog: list[dict[str, Any]], rule_id: str) -> dict[str, Any] | None:
    for item in catalog:
        if item.get("id") == rule_id:
            return item
    return None


def render_rule_sql(sql_template: str, **params: Any) -> str:
    rendered = sql_template
    for key, value in params.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
    return rendered


def get_rule_sql(catalog: list[dict[str, Any]], rule_id: str, **params: Any) -> str:
    rule = _find_rule(catalog, rule_id)
    if not rule or not rule.get("sql"):
        raise ValueError(f"Advisor rule not found or missing SQL: {rule_id}")
    return render_rule_sql(rule["sql"], **params)


ISSUE_SORT_ORDER = {
    "never_vacuumed": 0,
    "never_analyzed": 1,
    "stale_vacuum": 2,
    "stale_analyze": 3,
}


def _normalize_issue_types(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value if item]
    if isinstance(raw_value, str):
        raw_value = raw_value.strip()
        if raw_value.startswith("{") and raw_value.endswith("}"):
            inner = raw_value[1:-1]
            if not inner:
                return []
            return [part.strip().strip('"') for part in inner.split(",") if part.strip()]
    return []


def _primary_issue_type(issue_types: list[str]) -> str:
    if not issue_types:
        return "stale_analyze"
    return min(issue_types, key=lambda item: ISSUE_SORT_ORDER.get(item, 99))


def _quote_ident(value: str) -> str:
    value = (value or "").replace('"', '""')
    return f'"{value}"'


def build_analyze_sql(schema_name: str, table_name: str) -> str:
    return f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(table_name)};"


def build_vacuum_sql(schema_name: str, table_name: str) -> str:
    return f"VACUUM {_quote_ident(schema_name)}.{_quote_ident(table_name)};"


def compute_autovacuum_tuning(
    *,
    n_live_tup: int = 0,
    n_dead_tup: int = 0,
    table_size_bytes: int = 0,
    modified_since_analyze_ratio: float = 0.0,
    vacuum_urgency: float = 0.0,
    never_analyzed: bool = False,
) -> dict[str, Any]:
    """Compute per-table autovacuum thresholds and the rules applied."""
    analyze_scale_factor = 0.05
    analyze_threshold = 1000
    vacuum_scale_factor = 0.05
    vacuum_threshold = 1000
    adjustments: list[str] = []

    is_huge = n_live_tup >= 10_000_000 or table_size_bytes >= 10 * 1024**3
    is_large = n_live_tup >= 1_000_000 or table_size_bytes >= 1024**3
    dead_ratio = n_dead_tup / max(n_live_tup, 1)

    if is_huge:
        analyze_scale_factor = 0.01
        analyze_threshold = 5000
        vacuum_scale_factor = 0.02
        vacuum_threshold = 5000
        adjustments.append("Very large table (≥ 10M rows or ≥ 10 GiB): base thresholds reduced.")
    elif is_large:
        analyze_scale_factor = 0.02
        analyze_threshold = 2000
        vacuum_scale_factor = 0.03
        vacuum_threshold = 2000
        adjustments.append("Large table (≥ 1M rows or ≥ 1 GiB): base thresholds reduced.")
    else:
        adjustments.append("Default starting thresholds (scale factor 0.05, threshold 1000).")

    if never_analyzed or modified_since_analyze_ratio >= 0.30:
        analyze_scale_factor = min(analyze_scale_factor, 0.02)
        analyze_threshold = max(analyze_threshold, 500)
        if never_analyzed:
            adjustments.append("Never analyzed: analyze thresholds tightened.")
        else:
            adjustments.append(
                f"≥ 30% rows modified since analyze ({modified_since_analyze_ratio * 100:.1f}%): "
                "analyze thresholds tightened."
            )

    if vacuum_urgency >= 2:
        vacuum_scale_factor = 0.01
        vacuum_threshold = max(vacuum_threshold, 2000)
        adjustments.append(f"Vacuum urgency critical ({vacuum_urgency:.2f}): vacuum thresholds aggressively reduced.")
    elif vacuum_urgency >= 1:
        vacuum_scale_factor = min(vacuum_scale_factor, 0.02)
        adjustments.append(f"Elevated vacuum urgency ({vacuum_urgency:.2f}): vacuum scale factor reduced.")

    if is_huge and (dead_ratio >= 0.02 or n_dead_tup >= 500_000):
        vacuum_scale_factor = min(vacuum_scale_factor, 0.005)
        vacuum_threshold = min(vacuum_threshold, 500)
        analyze_scale_factor = min(analyze_scale_factor, 0.005)
        analyze_threshold = min(analyze_threshold, 500)
        adjustments.append(
            f"High dead pressure on very large table ({n_dead_tup:,} dead, {dead_ratio * 100:.1f}% of live): "
            "vacuum/analyze thresholds lowered."
        )
    elif is_large and (dead_ratio >= 0.05 or n_dead_tup >= 100_000):
        vacuum_scale_factor = min(vacuum_scale_factor, 0.01)
        vacuum_threshold = min(vacuum_threshold, 1000)
        analyze_scale_factor = min(analyze_scale_factor, 0.01)
        analyze_threshold = min(analyze_threshold, 1000)
        adjustments.append(
            f"High dead pressure on large table ({n_dead_tup:,} dead, {dead_ratio * 100:.1f}% of live): "
            "vacuum/analyze thresholds lowered."
        )

    if n_dead_tup >= 1_000_000:
        vacuum_scale_factor = min(vacuum_scale_factor, 0.005)
        vacuum_threshold = min(vacuum_threshold, 200)
        adjustments.append(f"≥ 1M dead tuples ({n_dead_tup:,}): vacuum thresholds minimized.")

    vacuum_trigger_at = int(round(vacuum_threshold + vacuum_scale_factor * n_live_tup))
    analyze_trigger_at = int(round(analyze_threshold + analyze_scale_factor * n_live_tup))

    return {
        "analyze_scale_factor": analyze_scale_factor,
        "analyze_threshold": analyze_threshold,
        "vacuum_scale_factor": vacuum_scale_factor,
        "vacuum_threshold": vacuum_threshold,
        "adjustments": adjustments,
        "size_class": "huge" if is_huge else "large" if is_large else "normal",
        "dead_ratio_pct": round(dead_ratio * 100, 2),
        "vacuum_trigger_at": vacuum_trigger_at,
        "analyze_trigger_at": analyze_trigger_at,
        "vacuum_formula": (
            f"dead_tuples ≥ {vacuum_threshold} + {vacuum_scale_factor:g} × {n_live_tup:,} "
            f"= {vacuum_trigger_at:,}"
        ),
        "analyze_formula": (
            f"modified_rows ≥ {analyze_threshold} + {analyze_scale_factor:g} × {n_live_tup:,} "
            f"= {analyze_trigger_at:,}"
        ),
    }


def build_autovacuum_tuning_sql(
    schema_name: str,
    table_name: str,
    *,
    n_live_tup: int = 0,
    n_dead_tup: int = 0,
    table_size_bytes: int = 0,
    modified_since_analyze_ratio: float = 0.0,
    vacuum_urgency: float = 0.0,
    never_analyzed: bool = False,
) -> str:
    tuning = compute_autovacuum_tuning(
        n_live_tup=n_live_tup,
        n_dead_tup=n_dead_tup,
        table_size_bytes=table_size_bytes,
        modified_since_analyze_ratio=modified_since_analyze_ratio,
        vacuum_urgency=vacuum_urgency,
        never_analyzed=never_analyzed,
    )
    return (
        f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(table_name)} SET (\n"
        f"  autovacuum_analyze_scale_factor = {tuning['analyze_scale_factor']},\n"
        f"  autovacuum_analyze_threshold = {tuning['analyze_threshold']},\n"
        f"  autovacuum_vacuum_scale_factor = {tuning['vacuum_scale_factor']},\n"
        f"  autovacuum_vacuum_threshold = {tuning['vacuum_threshold']}\n"
        f");"
    )


def build_table_calculation_help(
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    issue_types: list[str] | None = None,
    priority: str = "MEDIUM",
    recommendation_note: str = "",
    never_analyzed: bool = False,
    stale_analyze: bool = False,
    never_vacuumed: bool = False,
    stale_vacuum: bool = False,
    needs_vacuum_pressure: bool = False,
    stats_age_days: Any = None,
    vacuum_age_days: Any = None,
    n_live_tup: int = 0,
    n_dead_tup: int = 0,
    n_mod_since_analyze: int = 0,
    modified_since_analyze_pct: Any = None,
    dead_tuple_pct: Any = None,
    table_size_pretty: str = "",
    vacuum_urgency: float = 0.0,
    alter_available: bool = False,
    alter_skip_reason: str = "",
    tuning: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Structured per-table explanation for the UI help modal."""
    stale_days = normalize_stale_days(stale_days)
    issue_types = issue_types or []
    flagging_points: list[str] = []

    if never_analyzed:
        flagging_points.append("No manual or auto analyze recorded.")
    elif stale_analyze:
        flagging_points.append(
            f"Last analyze/autoanalyze is older than {stale_days} day(s)"
            + (f" ({stats_age_days} d)." if stats_age_days is not None else ".")
        )
    else:
        flagging_points.append(
            f"Statistics are fresh (within {stale_days} day(s)"
            + (f", age {stats_age_days} d)." if stats_age_days is not None else ").")
        )

    if never_vacuumed or stale_vacuum:
        if needs_vacuum_pressure:
            flagging_points.append(
                f"Vacuum pressure detected: {n_dead_tup:,} dead tuples"
                + (f", {dead_tuple_pct}% dead/live." if dead_tuple_pct is not None else ".")
            )
            if never_vacuumed:
                flagging_points.append("Vacuum/autovacuum has never run on this table.")
            elif stale_vacuum:
                flagging_points.append(
                    f"Last vacuum/autovacuum is older than {stale_days} day(s)"
                    + (f" ({vacuum_age_days} d)." if vacuum_age_days is not None else ".")
                )
        else:
            flagging_points.append("Vacuum age alone is not enough without dead tuple pressure.")
    else:
        flagging_points.append("No vacuum issue under current dead-tuple criteria.")

    metrics = [
        f"Priority: {priority}",
        f"Size: {table_size_pretty or '—'}",
        f"Live rows: {n_live_tup:,}",
        f"Dead tuples: {n_dead_tup:,}",
    ]
    if modified_since_analyze_pct is not None:
        metrics.append(f"Modified since analyze: {modified_since_analyze_pct}% ({n_mod_since_analyze:,} rows)")
    if vacuum_urgency:
        metrics.append(f"Vacuum urgency (YAML rule): {vacuum_urgency}")

    alter_section: dict[str, Any] = {
        "available": alter_available,
        "skip_reason": alter_skip_reason,
        "preview_only": not alter_available and bool(tuning),
    }
    if tuning:
        alter_section.update(
            {
                "parameters": {
                    "autovacuum_analyze_scale_factor": tuning["analyze_scale_factor"],
                    "autovacuum_analyze_threshold": tuning["analyze_threshold"],
                    "autovacuum_vacuum_scale_factor": tuning["vacuum_scale_factor"],
                    "autovacuum_vacuum_threshold": tuning["vacuum_threshold"],
                },
                "adjustments": tuning.get("adjustments") or [],
                "vacuum_formula": tuning.get("vacuum_formula"),
                "analyze_formula": tuning.get("analyze_formula"),
                "vacuum_trigger_at": tuning.get("vacuum_trigger_at"),
                "analyze_trigger_at": tuning.get("analyze_trigger_at"),
            }
        )

    return {
        "stale_days": stale_days,
        "issue_types": issue_types,
        "recommendation_note": recommendation_note,
        "flagging": flagging_points,
        "metrics": metrics,
        "alter": alter_section,
    }


def build_tuning_rationale(
    *,
    n_live_tup: int = 0,
    n_dead_tup: int = 0,
    table_size_bytes: int = 0,
    modified_since_analyze_ratio: float = 0.0,
    vacuum_urgency: float = 0.0,
    never_analyzed: bool = False,
) -> str:
    notes: list[str] = []
    dead_ratio = n_dead_tup / max(n_live_tup, 1)
    is_huge = n_live_tup >= 10_000_000 or table_size_bytes >= 10 * 1024**3
    is_large = n_live_tup >= 1_000_000 or table_size_bytes >= 1024**3

    if never_analyzed:
        notes.append("Statistics were never collected; analyze thresholds are tightened.")
    elif modified_since_analyze_ratio >= 0.30:
        notes.append("Many rows changed since the last analyze; analyze thresholds are tightened.")

    if is_huge:
        notes.append("Very large table: default autovacuum scale factors would trigger too late.")
    elif is_large:
        notes.append("Large table: per-table thresholds are reduced versus PostgreSQL defaults.")

    if n_dead_tup >= 1_000_000 or (is_huge and dead_ratio >= 0.02) or (is_large and dead_ratio >= 0.05):
        notes.append(
            f"High dead tuple pressure ({n_dead_tup:,} dead, {dead_ratio * 100:.1f}% of live rows): "
            "vacuum/analyze scale factors are lowered so maintenance can still run."
        )

    if vacuum_urgency >= 2:
        notes.append("Vacuum urgency is critical; vacuum thresholds are aggressively reduced.")
    elif vacuum_urgency >= 1:
        notes.append("Elevated vacuum urgency; vacuum scale factor is reduced.")

    if not notes:
        notes.append("Starting-point per-table autovacuum settings based on table size and activity.")
    return " ".join(notes)


def build_criteria_help(stale_days: int = DEFAULT_STALE_DAYS) -> dict[str, Any]:
    """Structured help text describing flagging and tuning algorithms."""
    stale_days = normalize_stale_days(stale_days)
    min_table_mb = DEFAULT_MIN_TABLE_BYTES // (1024 * 1024)
    dead_ratio_pct = int(DEAD_TUPLE_MIN_RATIO * 100)
    large_dead_ratio_pct = int(LARGE_TABLE_DEAD_RATIO * 100)

    return {
        "stale_days": stale_days,
        "sections": [
            {
                "id": "selection",
                "title": "Which tables appear in the report",
                "points": [
                    f"Only user tables ≥ {min_table_mb} MiB are considered.",
                    "A table is listed when it needs ANALYZE (missing or stale statistics) "
                    f"and/or VACUUM (dead tuple pressure with an overdue vacuum).",
                    "An old vacuum timestamp alone is not enough: low dead tuple pressure "
                    "does not flag a table for vacuum.",
                ],
            },
            {
                "id": "analyze",
                "title": "ANALYZE criteria",
                "points": [
                    f"Reference date: GREATEST(last_analyze, last_autoanalyze) — manual ANALYZE "
                    "counts the same as autovacuum.",
                    f"never_analyzed: no manual or auto analyze recorded.",
                    f"stale_analyze: last analyze/autoanalyze is older than {stale_days} day(s).",
                    "If statistics were refreshed within the threshold, the table is not flagged "
                    "for analyze even when autovacuum never ran.",
                ],
            },
            {
                "id": "vacuum",
                "title": "VACUUM criteria (dead tuple pressure required)",
                "points": [
                    f"Reference date: GREATEST(last_vacuum, last_autovacuum).",
                    "needs_vacuum_pressure is true when any of:",
                    f"  • n_dead_tup ≥ {DEAD_TUPLE_MIN_ABSOLUTE:,} AND dead/live ratio ≥ {dead_ratio_pct}%",
                    f"  • n_dead_tup ≥ {DEAD_TUPLE_HIGH_ABSOLUTE:,}",
                    f"  • table ≥ 1 GiB AND dead/live ratio ≥ {large_dead_ratio_pct}%",
                    "never_vacuumed: no vacuum/autovacuum recorded AND needs_vacuum_pressure.",
                    f"stale_vacuum: last vacuum/autovacuum older than {stale_days} day(s) "
                    "AND needs_vacuum_pressure.",
                    "dead/live ratio = n_dead_tup / (n_live_tup + n_dead_tup).",
                ],
            },
            {
                "id": "priority",
                "title": "Priority levels (pga_recommendation_level)",
                "points": [
                    "HIGH: never analyzed; or stale analyze with > 20% rows modified since analyze; "
                    "or vacuum issue with ≥ 10k dead tuples and ≥ 5% dead/live ratio; "
                    "or never vacuumed with ≥ 100k dead tuples.",
                    "MEDIUM: other analyze or vacuum issues.",
                ],
            },
            {
                "id": "alter",
                "title": "Per-table ALTER TABLE autovacuum tuning",
                "points": [
                    "Available only after at least one ANALYZE (manual or auto).",
                    "Base thresholds: analyze_scale_factor=0.05, analyze_threshold=1000, "
                    "vacuum_scale_factor=0.05, vacuum_threshold=1000.",
                    "Large table (≥ 1M rows or ≥ 1 GiB): scale factors → 0.02–0.03, thresholds → 2000.",
                    "Very large (≥ 10M rows or ≥ 10 GiB): scale factors → 0.01–0.02, thresholds → 5000.",
                    "Stale stats (≥ 30% modified since analyze): analyze thresholds tightened.",
                    "High dead pressure or vacuum_urgency ≥ 1: vacuum scale factor reduced.",
                    "vacuum_urgency ≥ 2: vacuum_scale_factor=0.01, threshold ≥ 2000.",
                    "≥ 1M dead tuples: vacuum_scale_factor ≤ 0.005, threshold ≤ 200.",
                    "PostgreSQL triggers autovacuum when: "
                    "dead_tuples ≥ vacuum_threshold + vacuum_scale_factor × n_live_tup "
                    "(same pattern for analyze).",
                ],
            },
            {
                "id": "global",
                "title": "Global ALTER SYSTEM recommendations",
                "points": [
                    "Cluster load score from: total dead tuples, high-dead-pressure tables, "
                    "critical vacuum_urgency count, flagged table count, saturated autovacuum workers.",
                    "Score ≥ 7 → critical, ≥ 4 → high, ≥ 2 → medium, else low.",
                    "Targets adjust autovacuum_max_workers, naptime, scale factors, cost delay/limit, "
                    "and log_autovacuum_min_duration by load level.",
                    "autovacuum_max_workers requires a PostgreSQL restart; other parameters reload "
                    "with SELECT pg_reload_conf();.",
                ],
            },
        ],
    }


def build_restore_script(analyze_sql: str, vacuum_sql: str, autovacuum_sql: str = "") -> str:
    lines = [
        "-- 1) Reclaim dead tuples (manual vacuum if autovacuum is lagging)",
        vacuum_sql,
        "",
        "-- 2) Refresh planner statistics",
        analyze_sql,
    ]
    if autovacuum_sql:
        lines.extend(
            [
                "",
                "-- 3) Tune per-table autovacuum thresholds",
                autovacuum_sql,
            ]
        )
    return "\n".join(lines)


def get_execution_risks(
    action: str,
    *,
    parameter: str = "",
    context: str = "",
    table_name: str = "",
) -> list[str]:
    risks: list[str] = []

    if action == "analyze":
        risks.extend(
            [
                "Acquires a SHARE UPDATE EXCLUSIVE lock: concurrent DDL on this table may wait.",
                "Can increase I/O and CPU while statistics are collected; large tables may run for a long time.",
                "Does not reclaim dead tuples by itself.",
            ]
        )
    elif action == "vacuum":
        risks.extend(
            [
                "Increases I/O and may compete with application traffic during cleanup.",
                "Uses a SHARE UPDATE EXCLUSIVE lock (reads/writes continue, but some DDL can be blocked).",
                "On very large tables, runtime can be significant — prefer a low-traffic window.",
                "This is a plain VACUUM, not VACUUM FULL: it does not return disk space to the OS.",
            ]
        )
    elif action == "alter_table":
        risks.extend(
            [
                "Changes per-table autovacuum thresholds until RESET or overridden again.",
                "More aggressive settings can increase background autovacuum I/O on this table.",
                "Does not run ANALYZE or VACUUM immediately; it only affects future autovacuum behavior.",
            ]
        )
    elif action == "alter_system":
        risks.extend(
            [
                "Changes cluster-wide PostgreSQL configuration via ALTER SYSTEM.",
                "Requires sufficient privileges (typically superuser or pg_write_all_settings).",
                "Incorrect values can increase I/O pressure or trigger excessive autovacuum activity.",
            ]
        )
        if context == "postmaster" or parameter == "autovacuum_max_workers":
            risks.append(
                f"{parameter or 'This parameter'} requires a PostgreSQL restart before it takes effect."
            )
        else:
            risks.append("Most parameters require SELECT pg_reload_conf(); after applying.")
    elif action == "reload_conf":
        risks.extend(
            [
                "Reloads PostgreSQL configuration for parameters that support SIGHUP reload.",
                "Postmaster-level parameters (for example autovacuum_max_workers) still need a restart.",
            ]
        )
    elif action == "alter_system_batch":
        risks.extend(
            [
                "Applies several cluster-wide configuration changes in sequence.",
                "Requires sufficient privileges and a maintenance review before production use.",
                "Some parameters only take effect after pg_reload_conf() or a full restart.",
            ]
        )

    if table_name:
        risks.append(f"Target table: {table_name}.")

    return risks


def get_batch_execution_risks(action: str, table_count: int = 0) -> list[str]:
    table_count = max(0, int(table_count or 0))
    risks: list[str] = []

    if action in {"vacuum", "all"}:
        risks.extend(get_execution_risks("vacuum"))
    if action in {"analyze", "all"}:
        risks.extend(get_execution_risks("analyze"))
    if action in {"alter", "all"}:
        risks.extend(get_execution_risks("alter_table"))

    labels = {
        "vacuum": "VACUUM",
        "analyze": "ANALYZE",
        "alter": "ALTER TABLE",
        "all": "VACUUM, ANALYZE and ALTER TABLE",
    }
    risks.append(
        f"Batch run: executes {labels.get(action, action)} sequentially on "
        f"{table_count} flagged table(s)."
    )
    risks.append("Total runtime and I/O can be significant; prefer a maintenance window.")
    if action == "all":
        risks.append("Order is VACUUM → ANALYZE → ALTER TABLE for each flagged table group.")
    if action == "alter":
        risks.append("ALTER TABLE changes persist until RESET or manual override.")

    deduped: list[str] = []
    for risk in risks:
        if risk not in deduped:
            deduped.append(risk)
    return deduped


def build_batch_actions(tables: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(tables)
    alter_eligible = sum(1 for table in tables if table.get("alter_available"))
    return {
        "table_count": count,
        "alter_eligible_count": alter_eligible,
        "vacuum": {"risks": get_batch_execution_risks("vacuum", count)},
        "analyze": {"risks": get_batch_execution_risks("analyze", count)},
        "alter": {"risks": get_batch_execution_risks("alter", alter_eligible)},
        "all": {"risks": get_batch_execution_risks("all", count)},
    }


def _table_key(schema_name: str, table_name: str) -> str:
    return f"{schema_name}.{table_name}"


GLOBAL_AUTOVACUUM_SETTINGS = (
    "autovacuum",
    "autovacuum_max_workers",
    "autovacuum_naptime",
    "autovacuum_vacuum_scale_factor",
    "autovacuum_vacuum_threshold",
    "autovacuum_analyze_scale_factor",
    "autovacuum_analyze_threshold",
    "autovacuum_vacuum_cost_delay",
    "autovacuum_vacuum_cost_limit",
    "log_autovacuum_min_duration",
    "max_worker_processes",
)


def _parse_setting_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_setting_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _format_alter_system_value(parameter: str, value: Any) -> str:
    if parameter == "autovacuum_naptime":
        seconds = _parse_setting_int(value, 60)
        return f"'{seconds}s'"
    if parameter == "log_autovacuum_min_duration":
        text = str(value)
        if text.isdigit():
            return f"'{text}ms'" if int(text) >= 0 else "'-1'"
        return f"'{text.replace(chr(39), chr(39) + chr(39))}'"
    if parameter == "autovacuum":
        return "on" if str(value).lower() in {"1", "true", "on"} else str(value)
    if isinstance(value, float):
        text = f"{value:g}"
        return text if "." in text else f"{text}.0" if parameter.endswith("_factor") else text
    return str(value)


def _build_alter_system_sql(parameter: str, value: Any) -> str:
    literal = _format_alter_system_value(parameter, value)
    if literal.startswith("'") or literal in {"on", "off"}:
        if literal in {"on", "off"}:
            return f"ALTER SYSTEM SET {parameter} = {literal};"
        return f"ALTER SYSTEM SET {parameter} = {literal};"
    return f"ALTER SYSTEM SET {parameter} = {literal};"


def assess_cluster_load(
    *,
    total_dead_tuples: int = 0,
    high_dead_pressure_tables: int = 0,
    never_vacuumed_tables: int = 0,
    flagged_table_count: int = 0,
    critical_vacuum_count: int = 0,
    elevated_vacuum_count: int = 0,
    active_autovacuum_workers: int = 0,
    current_max_workers: int = 3,
    user_table_count: int = 0,
) -> str:
    score = 0

    if total_dead_tuples >= 10_000_000:
        score += 3
    elif total_dead_tuples >= 1_000_000:
        score += 2
    elif total_dead_tuples >= 100_000:
        score += 1

    if high_dead_pressure_tables >= 20:
        score += 3
    elif high_dead_pressure_tables >= 10:
        score += 2
    elif high_dead_pressure_tables >= 3:
        score += 1

    if critical_vacuum_count >= 5:
        score += 2
    elif critical_vacuum_count >= 1:
        score += 1

    if elevated_vacuum_count >= 10:
        score += 1

    if flagged_table_count >= 50:
        score += 2
    elif flagged_table_count >= 15:
        score += 1

    if never_vacuumed_tables >= 10:
        score += 1

    if (
        current_max_workers > 0
        and active_autovacuum_workers >= current_max_workers
        and (high_dead_pressure_tables >= 3 or total_dead_tuples >= 500_000)
    ):
        score += 2

    if user_table_count >= 500 and total_dead_tuples >= 250_000:
        score += 1

    if score >= 7:
        return "critical"
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _recommended_max_workers(
    load_level: str,
    user_table_count: int,
    max_worker_processes: int,
    current_max_workers: int,
) -> int:
    if load_level == "low":
        return current_max_workers
    if load_level == "medium":
        target = max(3, min(6, (user_table_count // 250) + 3))
    elif load_level == "high":
        target = max(4, min(8, (user_table_count // 120) + 4))
    else:
        target = max(5, min(10, (user_table_count // 80) + 5))

    ceiling = max(3, max_worker_processes - 2)
    return max(current_max_workers, min(target, ceiling))


def build_global_autovacuum_targets(
    load_level: str,
    *,
    user_table_count: int = 0,
    max_worker_processes: int = 8,
    current_max_workers: int = 3,
) -> dict[str, Any]:
    targets: dict[str, Any] = {}

    if load_level == "low":
        targets.update(
            {
                "autovacuum_vacuum_scale_factor": 0.1,
                "autovacuum_analyze_scale_factor": 0.05,
                "autovacuum_vacuum_threshold": 50,
                "autovacuum_analyze_threshold": 50,
                "autovacuum_naptime": 60,
                "autovacuum_vacuum_cost_delay": 2,
                "autovacuum_vacuum_cost_limit": -1,
                "log_autovacuum_min_duration": "10min",
            }
        )
    elif load_level == "medium":
        targets.update(
            {
                "autovacuum_vacuum_scale_factor": 0.1,
                "autovacuum_analyze_scale_factor": 0.05,
                "autovacuum_vacuum_threshold": 50,
                "autovacuum_analyze_threshold": 50,
                "autovacuum_naptime": 30,
                "autovacuum_vacuum_cost_delay": 2,
                "autovacuum_vacuum_cost_limit": 1000,
                "log_autovacuum_min_duration": "5min",
            }
        )
    elif load_level == "high":
        targets.update(
            {
                "autovacuum_vacuum_scale_factor": 0.05,
                "autovacuum_analyze_scale_factor": 0.02,
                "autovacuum_vacuum_threshold": 50,
                "autovacuum_analyze_threshold": 50,
                "autovacuum_naptime": 15,
                "autovacuum_vacuum_cost_delay": 1,
                "autovacuum_vacuum_cost_limit": 2000,
                "log_autovacuum_min_duration": "1min",
            }
        )
    else:
        targets.update(
            {
                "autovacuum_vacuum_scale_factor": 0.02,
                "autovacuum_analyze_scale_factor": 0.01,
                "autovacuum_vacuum_threshold": 25,
                "autovacuum_analyze_threshold": 25,
                "autovacuum_naptime": 10,
                "autovacuum_vacuum_cost_delay": 0,
                "autovacuum_vacuum_cost_limit": 3000,
                "log_autovacuum_min_duration": "0",
            }
        )

    targets["autovacuum_max_workers"] = _recommended_max_workers(
        load_level,
        user_table_count,
        max_worker_processes,
        current_max_workers,
    )
    targets["autovacuum"] = "on"
    return targets


def _global_parameter_rationale(parameter: str, load_level: str) -> str:
    rationales = {
        "autovacuum": "Autovacuum must stay enabled for cluster-wide dead tuple cleanup.",
        "autovacuum_max_workers": (
            f"Cluster load is {load_level}: increase worker capacity so autovacuum can keep up."
        ),
        "autovacuum_naptime": (
            f"Cluster load is {load_level}: run the autovacuum launcher more frequently."
        ),
        "autovacuum_vacuum_scale_factor": (
            "Lower the global vacuum trigger ratio so large tables do not accumulate excessive dead tuples."
        ),
        "autovacuum_analyze_scale_factor": (
            "Lower the global analyze trigger ratio to refresh planner statistics sooner under write pressure."
        ),
        "autovacuum_vacuum_threshold": (
            "Lower the base vacuum threshold so autovacuum starts sooner under cluster-wide dead tuple pressure."
        ),
        "autovacuum_analyze_threshold": (
            "Lower the base analyze threshold so statistics refresh sooner under write-heavy load."
        ),
        "autovacuum_vacuum_cost_delay": (
            "Reduce throttling so autovacuum workers reclaim dead tuples faster during high load."
        ),
        "autovacuum_vacuum_cost_limit": (
            "Increase the autovacuum I/O budget so workers make progress when many tables are lagging."
        ),
        "log_autovacuum_min_duration": (
            "Improve observability of long or frequent autovacuum runs while investigating backlog."
        ),
    }
    return rationales.get(parameter, "Adjust this global autovacuum parameter for the current workload.")


def _should_recommend_global_change(
    parameter: str,
    current_value: Any,
    recommended_value: Any,
) -> bool:
    if parameter == "autovacuum":
        return str(current_value).lower() not in {"on", "true", "1"}

    if parameter == "autovacuum_max_workers":
        return _parse_setting_int(recommended_value) > _parse_setting_int(current_value, 3)

    if parameter == "autovacuum_naptime":
        return _parse_setting_int(recommended_value, 60) < _parse_setting_int(current_value, 60)

    if parameter.endswith("_scale_factor"):
        return _parse_setting_float(recommended_value) < _parse_setting_float(current_value, 1.0) - 1e-9

    if parameter.endswith("_threshold"):
        current = _parse_setting_int(current_value, 50)
        recommended = _parse_setting_int(recommended_value, 50)
        return recommended < current

    if parameter == "autovacuum_vacuum_cost_delay":
        return _parse_setting_float(recommended_value) < _parse_setting_float(current_value, 2.0) - 1e-9

    if parameter == "autovacuum_vacuum_cost_limit":
        current = _parse_setting_int(current_value, -1)
        recommended = _parse_setting_int(recommended_value, -1)
        if recommended < 0:
            return False
        return current < 0 or recommended > current

    if parameter == "log_autovacuum_min_duration":
        current = str(current_value or "-1")
        return current in {"-1", ""}

    return str(current_value) != str(recommended_value)


def build_global_autovacuum_recommendations(
    *,
    load_level: str,
    metrics: dict[str, Any],
    current_settings: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    user_table_count = int(metrics.get("user_table_count") or 0)
    max_worker_processes = _parse_setting_int(
        (current_settings.get("max_worker_processes") or {}).get("setting"),
        8,
    )
    current_max_workers = _parse_setting_int(
        (current_settings.get("autovacuum_max_workers") or {}).get("setting"),
        3,
    )

    targets = build_global_autovacuum_targets(
        load_level,
        user_table_count=user_table_count,
        max_worker_processes=max_worker_processes,
        current_max_workers=current_max_workers,
    )

    recommendations: list[dict[str, Any]] = []
    restart_required = False

    for parameter, recommended_value in targets.items():
        if parameter == "max_worker_processes":
            continue

        meta = current_settings.get(parameter) or {}
        current_value = meta.get("setting", "")
        if not _should_recommend_global_change(parameter, current_value, recommended_value):
            continue

        context = meta.get("context") or "sighup"
        if context == "postmaster":
            restart_required = True

        recommendations.append(
            {
                "parameter": parameter,
                "current_value": current_value,
                "recommended_value": recommended_value,
                "unit": meta.get("unit") or "",
                "context": context,
                "rationale": _global_parameter_rationale(parameter, load_level),
                "sql": _build_alter_system_sql(parameter, recommended_value),
                "risks": get_execution_risks(
                    "alter_system",
                    parameter=parameter,
                    context=context,
                ),
            }
        )

    script_lines = [
        "-- Global autovacuum tuning proposal based on current cluster load",
        f"-- Detected load level: {load_level}",
        "",
    ]
    script_lines.extend(item["sql"] for item in recommendations)
    if recommendations:
        script_lines.extend(
            [
                "",
                "SELECT pg_reload_conf();",
            ]
        )
        if restart_required:
            script_lines.append("-- Restart required for postmaster-level parameters (autovacuum_max_workers).")

    load_labels = {
        "low": "Low maintenance pressure",
        "medium": "Moderate maintenance pressure",
        "high": "High maintenance pressure",
        "critical": "Critical maintenance backlog",
    }

    return {
        "load_level": load_level,
        "load_label": load_labels.get(load_level, load_level.title()),
        "metrics": metrics,
        "recommendations": recommendations,
        "restart_required": restart_required,
        "script_sql": "\n".join(script_lines).strip(),
        "reload_sql": "SELECT pg_reload_conf();",
        "batch_risks": get_execution_risks("alter_system_batch"),
        "reload_risks": get_execution_risks("reload_conf"),
        "summary": (
            f"{load_labels.get(load_level, load_level.title())}: "
            f"{len(recommendations)} global parameter(s) proposed."
        ),
    }


def run_global_autovacuum_tuning(
    conn,
    *,
    catalog: list[dict[str, Any]] | None = None,
    yaml_path: str | None = None,
    flagged_table_count: int = 0,
    critical_vacuum_count: int = 0,
    elevated_vacuum_count: int = 0,
) -> dict[str, Any]:
    if catalog is None:
        yaml_path = yaml_path or str(Path(__file__).resolve().parents[2] / "advisor_enriched.yml")
        catalog = load_recommendation_catalog(yaml_path)

    rows = json.loads(
        db_fetch_json(conn, get_rule_sql(catalog, CLUSTER_LOAD_RULE_ID))
    )
    if not rows:
        return {"ok": False, "error": "Unable to read cluster autovacuum metrics."}

    row = rows[0]
    settings_raw = row.get("autovacuum_settings") or {}
    if isinstance(settings_raw, str):
        settings_raw = json.loads(settings_raw)

    current_settings = {
        key: value for key, value in settings_raw.items() if isinstance(value, dict)
    }

    current_max_workers = _parse_setting_int(
        (current_settings.get("autovacuum_max_workers") or {}).get("setting"),
        3,
    )
    metrics = {
        "user_table_count": int(row.get("user_table_count") or 0),
        "total_dead_tuples": int(row.get("total_dead_tuples") or 0),
        "total_live_tuples": int(row.get("total_live_tuples") or 0),
        "high_dead_pressure_tables": int(row.get("high_dead_pressure_tables") or 0),
        "never_vacuumed_tables": int(row.get("never_vacuumed_tables") or 0),
        "never_analyzed_tables": int(row.get("never_analyzed_tables") or 0),
        "active_autovacuum_workers": int(row.get("active_autovacuum_workers") or 0),
        "flagged_table_count": flagged_table_count,
        "critical_vacuum_count": critical_vacuum_count,
        "elevated_vacuum_count": elevated_vacuum_count,
    }

    load_level = assess_cluster_load(
        total_dead_tuples=metrics["total_dead_tuples"],
        high_dead_pressure_tables=metrics["high_dead_pressure_tables"],
        never_vacuumed_tables=metrics["never_vacuumed_tables"],
        flagged_table_count=flagged_table_count,
        critical_vacuum_count=critical_vacuum_count,
        elevated_vacuum_count=elevated_vacuum_count,
        active_autovacuum_workers=metrics["active_autovacuum_workers"],
        current_max_workers=current_max_workers,
        user_table_count=metrics["user_table_count"],
    )

    tuning = build_global_autovacuum_recommendations(
        load_level=load_level,
        metrics=metrics,
        current_settings=current_settings,
    )
    tuning["ok"] = True
    tuning["current_settings"] = {
        name: meta.get("setting", "")
        for name, meta in current_settings.items()
        if name in GLOBAL_AUTOVACUUM_SETTINGS
    }
    return tuning


def run_table_autovacuum_advisor(
    db_config,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    yaml_path: str | None = None,
) -> dict[str, Any]:
    stale_days = normalize_stale_days(stale_days)
    yaml_path = yaml_path or str(Path(__file__).resolve().parents[2] / "advisor_enriched.yml")

    conn, message = connectdb(db_config)
    if conn is None:
        return {"ok": False, "error": message or "Unable to connect.", "tables": []}

    try:
        catalog = load_recommendation_catalog(yaml_path)
        vacuum_rule = _find_rule(catalog, VACUUM_URGENCY_RULE_ID)

        flagged_sql = get_rule_sql(
            catalog,
            MAINTENANCE_FLAGGED_RULE_ID,
            stale_days=stale_days,
            min_table_size_bytes=DEFAULT_MIN_TABLE_BYTES,
        )
        stale_rows = json.loads(db_fetch_json(conn, flagged_sql))
        vacuum_rows: list[dict[str, Any]] = []
        if vacuum_rule and vacuum_rule.get("sql"):
            vacuum_rows = json.loads(db_fetch_json(conn, vacuum_rule["sql"]))

        vacuum_by_table = {
            _table_key(row.get("schema_name", ""), row.get("table_name", "")): row
            for row in vacuum_rows
            if row.get("schema_name") and row.get("table_name")
        }

        tables: list[dict[str, Any]] = []
        for row in stale_rows:
            schema_name = str(row.get("schema_name") or "")
            table_name = str(row.get("table_name") or row.get("object_name") or "")
            if not schema_name or not table_name:
                continue

            key = _table_key(schema_name, table_name)
            vacuum_info = vacuum_by_table.get(key, {})
            never_analyzed = bool(row.get("never_analyzed"))
            never_vacuumed = bool(row.get("never_vacuumed"))
            issue_types = _normalize_issue_types(row.get("issue_types"))
            if not issue_types:
                issue_types = []
                if never_analyzed:
                    issue_types.append("never_analyzed")
                elif row.get("stale_analyze"):
                    issue_types.append("stale_analyze")
                if never_vacuumed:
                    issue_types.append("never_vacuumed")
                elif row.get("stale_vacuum"):
                    issue_types.append("stale_vacuum")
            stats_age_days = row.get("stats_age_days")
            vacuum_age_days = row.get("vacuum_age_days")
            mod_ratio = float(row.get("modified_since_analyze_ratio") or 0)
            vacuum_urgency = float(vacuum_info.get("vacuum_urgency") or 0)

            analyze_sql = build_analyze_sql(schema_name, table_name)
            vacuum_sql = build_vacuum_sql(schema_name, table_name)
            alter_available = not never_analyzed
            tuning = compute_autovacuum_tuning(
                n_live_tup=int(row.get("n_live_tup") or 0),
                n_dead_tup=int(row.get("n_dead_tup") or 0),
                table_size_bytes=int(row.get("table_size_bytes") or 0),
                modified_since_analyze_ratio=mod_ratio,
                vacuum_urgency=vacuum_urgency,
                never_analyzed=False,
            )
            if alter_available:
                autovacuum_sql = build_autovacuum_tuning_sql(
                    schema_name,
                    table_name,
                    n_live_tup=int(row.get("n_live_tup") or 0),
                    n_dead_tup=int(row.get("n_dead_tup") or 0),
                    table_size_bytes=int(row.get("table_size_bytes") or 0),
                    modified_since_analyze_ratio=mod_ratio,
                    vacuum_urgency=vacuum_urgency,
                    never_analyzed=False,
                )
                tuning_rationale = build_tuning_rationale(
                    n_live_tup=int(row.get("n_live_tup") or 0),
                    n_dead_tup=int(row.get("n_dead_tup") or 0),
                    table_size_bytes=int(row.get("table_size_bytes") or 0),
                    modified_since_analyze_ratio=mod_ratio,
                    vacuum_urgency=vacuum_urgency,
                    never_analyzed=False,
                )
                alter_risks = get_execution_risks("alter_table", table_name=key)
                alter_skip_reason = ""
            else:
                autovacuum_sql = ""
                tuning_rationale = ""
                alter_risks = []
                alter_skip_reason = "Run ANALYZE first; autovacuum tuning requires planner statistics."

            calculation_help = build_table_calculation_help(
                stale_days=stale_days,
                issue_types=issue_types,
                priority=row.get("pga_recommendation_level") or "MEDIUM",
                recommendation_note=row.get("recommendation_note") or "",
                never_analyzed=never_analyzed,
                stale_analyze=bool(row.get("stale_analyze")),
                never_vacuumed=never_vacuumed,
                stale_vacuum=bool(row.get("stale_vacuum")),
                needs_vacuum_pressure=bool(row.get("needs_vacuum_pressure")),
                stats_age_days=stats_age_days,
                vacuum_age_days=vacuum_age_days,
                n_live_tup=int(row.get("n_live_tup") or 0),
                n_dead_tup=int(row.get("n_dead_tup") or 0),
                n_mod_since_analyze=int(row.get("n_mod_since_analyze") or 0),
                modified_since_analyze_pct=row.get("modified_since_analyze_pct"),
                dead_tuple_pct=row.get("dead_tuple_pct"),
                table_size_pretty=row.get("table_size_pretty") or "",
                vacuum_urgency=vacuum_urgency,
                alter_available=alter_available,
                alter_skip_reason=alter_skip_reason,
                tuning=tuning,
            )

            issue_type = _primary_issue_type(issue_types)
            tables.append(
                {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "object_name": key,
                    "issue_type": issue_type,
                    "issue_types": issue_types,
                    "priority": row.get("pga_recommendation_level") or "MEDIUM",
                    "recommendation_note": row.get("recommendation_note") or "",
                    "last_analyze": row.get("last_analyze"),
                    "last_autoanalyze": row.get("last_autoanalyze"),
                    "last_vacuum": row.get("last_vacuum"),
                    "last_autovacuum": row.get("last_autovacuum"),
                    "last_any_analyze": row.get("last_any_analyze"),
                    "last_any_vacuum": row.get("last_any_vacuum"),
                    "stats_age_days": stats_age_days,
                    "vacuum_age_days": vacuum_age_days,
                    "never_analyzed": never_analyzed,
                    "never_vacuumed": never_vacuumed,
                    "stale_analyze": bool(row.get("stale_analyze")),
                    "stale_vacuum": bool(row.get("stale_vacuum")),
                    "alter_available": alter_available,
                    "alter_skip_reason": alter_skip_reason,
                    "n_live_tup": row.get("n_live_tup"),
                    "n_dead_tup": row.get("n_dead_tup"),
                    "n_mod_since_analyze": row.get("n_mod_since_analyze"),
                    "modified_since_analyze_pct": row.get("modified_since_analyze_pct"),
                    "dead_tuple_pct": row.get("dead_tuple_pct"),
                    "needs_vacuum_pressure": bool(row.get("needs_vacuum_pressure")),
                    "table_size_pretty": row.get("table_size_pretty"),
                    "table_size_bytes": row.get("table_size_bytes"),
                    "vacuum_urgency": vacuum_info.get("vacuum_urgency"),
                    "analyze_sql": analyze_sql,
                    "vacuum_sql": vacuum_sql,
                    "autovacuum_sql": autovacuum_sql,
                    "tuning_rationale": tuning_rationale,
                    "calculation_help": calculation_help,
                    "script_sql": build_restore_script(analyze_sql, vacuum_sql, autovacuum_sql),
                    "analyze_risks": get_execution_risks("analyze", table_name=key),
                    "vacuum_risks": get_execution_risks("vacuum", table_name=key),
                    "alter_risks": alter_risks,
                }
            )

        tables.sort(
            key=lambda item: (
                ISSUE_SORT_ORDER.get(item["issue_type"], 99),
                0 if item["priority"] == "HIGH" else 1 if item["priority"] == "MEDIUM" else 2,
                -(item.get("n_live_tup") or 0),
            )
        )

        critical_vacuum_count = sum(
            1 for item in tables if float(item.get("vacuum_urgency") or 0) >= 2
        )
        elevated_vacuum_count = sum(
            1 for item in tables if float(item.get("vacuum_urgency") or 0) >= 1
        )
        global_tuning = run_global_autovacuum_tuning(
            conn,
            catalog=catalog,
            flagged_table_count=len(tables),
            critical_vacuum_count=critical_vacuum_count,
            elevated_vacuum_count=elevated_vacuum_count,
        )

        return {
            "ok": True,
            "error": "",
            "stale_days": stale_days,
            "criteria_help": build_criteria_help(stale_days),
            "tables": tables,
            "global_tuning": global_tuning,
            "batch_actions": build_batch_actions(tables),
            "summary": {
                "total": len(tables),
                "never_analyzed": sum(1 for t in tables if t.get("never_analyzed")),
                "stale_analyze": sum(1 for t in tables if t.get("stale_analyze")),
                "never_vacuumed": sum(1 for t in tables if t.get("never_vacuumed")),
                "stale_vacuum": sum(1 for t in tables if t.get("stale_vacuum")),
            },
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "tables": []}
    finally:
        try:
            conn.close()
        except Exception:
            pass
