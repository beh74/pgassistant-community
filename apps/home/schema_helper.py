# -*- encoding: utf-8 -*-
"""Database schema digest helpers for LLM prompts."""
from __future__ import annotations

import datetime
import decimal
from typing import Any, Dict, List


USER_SCHEMA_FILTER = """
    n.nspname <> 'information_schema'
    AND n.nspname !~ '^pg_'
"""


TABLES_SQL = f"""
WITH RECURSIVE user_relations AS (
    SELECT
        c.oid,
        n.nspname,
        c.relname,
        c.relkind,
        c.relnamespace
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND {USER_SCHEMA_FILTER}
),
table_roots AS (
    SELECT ur.*
    FROM user_relations ur
    WHERE ur.relkind = 'p'
       OR NOT EXISTS (
           SELECT 1
           FROM pg_inherits inh
           WHERE inh.inhrelid = ur.oid
       )
),
relation_members(root_oid, member_oid) AS (
    SELECT
        tr.oid AS root_oid,
        tr.oid AS member_oid
    FROM table_roots tr

    UNION ALL

    SELECT
        rm.root_oid,
        child.oid AS member_oid
    FROM relation_members rm
    JOIN pg_inherits inh ON inh.inhparent = rm.member_oid
    JOIN user_relations child ON child.oid = inh.inhrelid
)
SELECT
    n.nspname AS schemaname,
    c.relname AS table_name,
    c.relkind,
    CASE c.relkind
        WHEN 'p' THEN 'partitioned table'
        ELSE 'table'
    END AS table_kind,
    GREATEST(COUNT(DISTINCT rm.member_oid) - 1, 0) AS partition_count,
    COALESCE(SUM(pg_total_relation_size(member.oid)), 0) AS total_size_bytes,
    pg_size_pretty(COALESCE(SUM(pg_total_relation_size(member.oid)), 0)) AS total_size_pretty,
    COALESCE(SUM(pg_relation_size(member.oid)), 0) AS table_size_bytes,
    pg_size_pretty(COALESCE(SUM(pg_relation_size(member.oid)), 0)) AS table_size_pretty,
    COALESCE(SUM(pg_indexes_size(member.oid)), 0) AS indexes_size_bytes,
    pg_size_pretty(COALESCE(SUM(pg_indexes_size(member.oid)), 0)) AS indexes_size_pretty,
    COALESCE(SUM(st.n_live_tup), 0) AS n_live_tup,
    COALESCE(SUM(st.n_dead_tup), 0) AS n_dead_tup,
    COALESCE(SUM(st.seq_scan), 0) AS seq_scan,
    COALESCE(SUM(st.seq_tup_read), 0) AS seq_tup_read,
    COALESCE(SUM(st.idx_scan), 0) AS idx_scan,
    COALESCE(SUM(st.idx_tup_fetch), 0) AS idx_tup_fetch,
    MAX(st.last_vacuum) AS last_vacuum,
    MAX(st.last_autovacuum) AS last_autovacuum,
    MAX(st.last_analyze) AS last_analyze,
    MAX(st.last_autoanalyze) AS last_autoanalyze,
    ROUND(
        100.0 * SUM(sio.heap_blks_hit)
        / NULLIF(SUM(sio.heap_blks_hit + sio.heap_blks_read), 0),
        2
    ) AS table_cache_hit_pct,
    ROUND(
        100.0 * SUM(sio.idx_blks_hit)
        / NULLIF(SUM(sio.idx_blks_hit + sio.idx_blks_read), 0),
        2
    ) AS index_cache_hit_pct
FROM table_roots c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN relation_members rm ON rm.root_oid = c.oid
JOIN pg_class member ON member.oid = rm.member_oid
LEFT JOIN pg_stat_all_tables st ON st.relid = member.oid
LEFT JOIN pg_statio_all_tables sio ON sio.relid = member.oid
GROUP BY n.nspname, c.relname, c.relkind
ORDER BY n.nspname, c.relname
"""


CONSTRAINTS_SQL = f"""
SELECT
    n.nspname AS schemaname,
    tbl.relname AS table_name,
    con.conname AS constraint_name,
    con.contype,
    CASE con.contype
        WHEN 'p' THEN 'primary_key'
        WHEN 'u' THEN 'unique'
    END AS constraint_type,
    ARRAY_AGG(att.attname ORDER BY k.ord) AS columns
FROM pg_constraint con
JOIN pg_class tbl ON tbl.oid = con.conrelid
JOIN pg_namespace n ON n.oid = tbl.relnamespace
JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = k.attnum
WHERE con.contype IN ('p', 'u')
  AND tbl.relkind IN ('r', 'p')
  AND {USER_SCHEMA_FILTER}
  AND (
      tbl.relkind = 'p'
      OR NOT EXISTS (
          SELECT 1
          FROM pg_inherits inh
          WHERE inh.inhrelid = tbl.oid
      )
  )
GROUP BY n.nspname, tbl.relname, con.conname, con.contype
ORDER BY n.nspname, tbl.relname, con.contype, con.conname
"""


FOREIGN_KEYS_SQL = f"""
SELECT
    con.conname AS constraint_name,
    nsrc.nspname AS from_schema,
    src.relname AS from_table,
    ARRAY_AGG(src_att.attname ORDER BY k.ord) AS from_columns,
    ntgt.nspname AS to_schema,
    tgt.relname AS to_table,
    ARRAY_AGG(tgt_att.attname ORDER BY k.ord) AS to_columns,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update,
    EXISTS (
        SELECT 1
        FROM pg_index idx
        WHERE idx.indrelid = con.conrelid
          AND idx.indisvalid
          AND idx.indisready
          AND idx.indpred IS NULL
          AND idx.indexprs IS NULL
          AND (
              string_to_array(idx.indkey::text, ' ')::smallint[]
          )[1:array_length(con.conkey, 1)] = con.conkey
    ) AS fk_index_covered
FROM pg_constraint con
JOIN pg_class src ON src.oid = con.conrelid
JOIN pg_namespace nsrc ON nsrc.oid = src.relnamespace
JOIN pg_class tgt ON tgt.oid = con.confrelid
JOIN pg_namespace ntgt ON ntgt.oid = tgt.relnamespace
JOIN LATERAL (
    SELECT u.ord, u.src_attnum, v.tgt_attnum
    FROM unnest(con.conkey) WITH ORDINALITY u(src_attnum, ord)
    JOIN unnest(con.confkey) WITH ORDINALITY v(tgt_attnum, ord)
      USING (ord)
) k ON true
JOIN pg_attribute src_att ON src_att.attrelid = src.oid AND src_att.attnum = k.src_attnum
JOIN pg_attribute tgt_att ON tgt_att.attrelid = tgt.oid AND tgt_att.attnum = k.tgt_attnum
WHERE con.contype = 'f'
  AND src.relkind IN ('r', 'p')
  AND tgt.relkind IN ('r', 'p')
  AND (
      src.relkind = 'p'
      OR NOT EXISTS (
          SELECT 1
          FROM pg_inherits inh
          WHERE inh.inhrelid = src.oid
      )
  )
  AND (
      tgt.relkind = 'p'
      OR NOT EXISTS (
          SELECT 1
          FROM pg_inherits inh
          WHERE inh.inhrelid = tgt.oid
      )
  )
  AND nsrc.nspname <> 'information_schema'
  AND nsrc.nspname !~ '^pg_'
  AND ntgt.nspname <> 'information_schema'
  AND ntgt.nspname !~ '^pg_'
GROUP BY
    con.conname,
    con.conrelid,
    con.conkey,
    con.confdeltype,
    con.confupdtype,
    nsrc.nspname,
    src.relname,
    ntgt.nspname,
    tgt.relname
ORDER BY nsrc.nspname, src.relname, con.conname
"""


STATS_RESET_SQL = """
SELECT stats_reset
FROM pg_stat_database
WHERE datname = current_database()
"""


ARCHITECTURE_SQL = """
WITH settings AS (
    SELECT
        MAX(setting) FILTER (WHERE name = 'archive_mode') AS archive_mode,
        MAX(setting) FILTER (WHERE name = 'archive_command') AS archive_command,
        MAX(setting) FILTER (WHERE name = 'archive_library') AS archive_library,
        MAX(setting) FILTER (WHERE name = 'restore_command') AS restore_command,
        MAX(setting) FILTER (WHERE name = 'primary_conninfo') AS primary_conninfo,
        MAX(setting) FILTER (WHERE name = 'primary_slot_name') AS primary_slot_name,
        MAX(setting) FILTER (WHERE name = 'wal_level') AS wal_level,
        MAX(setting) FILTER (WHERE name = 'max_wal_senders') AS max_wal_senders,
        MAX(setting) FILTER (WHERE name = 'hot_standby') AS hot_standby,
        MAX(setting) FILTER (WHERE name = 'cluster_name') AS cluster_name
    FROM pg_settings
    WHERE name IN (
        'archive_mode', 'archive_command', 'archive_library', 'restore_command',
        'primary_conninfo', 'primary_slot_name', 'wal_level', 'max_wal_senders',
        'hot_standby', 'cluster_name'
    )
)
SELECT
    pg_is_in_recovery() AS is_in_recovery,
    (SELECT COUNT(*) FROM pg_stat_replication) AS connected_replicas,
    settings.*
FROM settings
"""


COLUMN_STATS_SQL = """
SELECT
    s.schemaname,
    s.tablename AS table_name,
    s.attname AS column_name,
    s.null_frac,
    s.n_distinct,
    s.avg_width,
    array_length(s.most_common_freqs, 1) AS most_common_values_count,
    s.histogram_bounds IS NOT NULL AS histogram_available,
    s.correlation
FROM pg_stats s
JOIN pg_namespace n ON n.nspname = s.schemaname
JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = s.tablename
WHERE s.schemaname <> 'information_schema'
  AND s.schemaname !~ '^pg_'
  AND c.relkind IN ('r', 'p')
  AND (
      c.relkind = 'p'
      OR NOT EXISTS (
          SELECT 1
          FROM pg_inherits inh
          WHERE inh.inhrelid = c.oid
      )
  )
ORDER BY s.schemaname, s.tablename, s.attname
"""


def _fetch_all_dicts(conn, sql: str) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return [_normalize_row(dict(zip(columns, row))) for row in rows]


def _normalize_value(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, tuple):
        return list(value)
    return value


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: _normalize_value(value) for key, value in row.items()}


def _table_key(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _format_columns(columns: List[str]) -> str:
    return ", ".join(columns or []) if columns else "-"


def _format_stat(value: Any, suffix: str = "") -> str:
    if value is None:
        return "-"
    return f"{value}{suffix}"


def _detect_wal_archive_tool(*values: Any) -> str:
    """Classify WAL archive tooling without exposing command contents."""
    command = " ".join(str(value or "") for value in values).casefold()
    detectors = (
        ("pgBackRest", ("pgbackrest",)),
        ("Barman", ("barman-wal-archive", "barman-cloud-wal-archive", "barman")),
        ("WAL-G", ("wal-g", "walg")),
        ("WAL-E", ("wal-e", "wale")),
        ("pg_probackup", ("pg_probackup", "pg-probackup")),
        ("AWS S3 command", ("aws s3", "s3cmd")),
        ("Google Cloud Storage command", ("gsutil", "gcloud storage")),
        ("Azure storage command", ("azcopy", "az storage")),
        ("rsync", ("rsync",)),
    )
    for label, patterns in detectors:
        if any(pattern in command for pattern in patterns):
            return label
    return "Custom command or library" if command.strip() else "Not configured"


def _build_database_architecture(row: Dict[str, Any]) -> Dict[str, Any]:
    """Build a safe, evidence-based architecture summary from server settings."""
    is_in_recovery = bool(row.get("is_in_recovery"))
    connected_replicas = int(row.get("connected_replicas") or 0)
    archive_mode = str(row.get("archive_mode") or "off").casefold()
    wal_archiving = archive_mode in {"on", "always"}
    archive_tool = (
        _detect_wal_archive_tool(
            row.get("archive_command"),
            row.get("archive_library"),
            row.get("restore_command"),
        )
        if wal_archiving
        else "Not enabled"
    )

    if is_in_recovery or connected_replicas > 0:
        architecture_type = "Replicated PostgreSQL cluster"
    elif wal_archiving:
        architecture_type = "Standalone PostgreSQL with WAL archiving"
    else:
        architecture_type = "Standalone PostgreSQL"

    if is_in_recovery:
        server_role = "Standby"
    else:
        server_role = "Primary"

    try:
        max_wal_senders = int(row.get("max_wal_senders") or 0)
    except (TypeError, ValueError):
        max_wal_senders = 0
    replication_capable = (
        str(row.get("wal_level") or "").casefold() in {"replica", "logical"}
        and max_wal_senders > 0
    )

    return {
        "type": architecture_type,
        "server_role": server_role,
        "is_in_recovery": is_in_recovery,
        "connected_replicas": connected_replicas,
        "replication_capable": replication_capable,
        "standby_source_configured": bool(str(row.get("primary_conninfo") or "").strip()),
        "replication_slot_configured": bool(str(row.get("primary_slot_name") or "").strip()),
        "wal_level": row.get("wal_level") or "unknown",
        "wal_archiving": wal_archiving,
        "archive_mode": archive_mode,
        "archive_tool": archive_tool,
        "cluster_name": row.get("cluster_name") or "",
        "inference_note": (
            "Cluster membership is confirmed only when this server is a standby or "
            "has connected replicas. Replication-capable settings alone do not prove "
            "that a cluster is currently active."
        ),
    }


def get_database_architecture(conn) -> Dict[str, Any]:
    """Read and safely classify the connected PostgreSQL architecture."""
    try:
        rows = _fetch_all_dicts(conn, ARCHITECTURE_SQL)
        if rows:
            return _build_database_architecture(rows[0])
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "type": "Unknown",
            "server_role": "Unknown",
            "connected_replicas": 0,
            "wal_archiving": False,
            "archive_mode": "unknown",
            "archive_tool": "Unknown",
            "error": str(exc),
        }

    return {
        "type": "Unknown",
        "server_role": "Unknown",
        "connected_replicas": 0,
        "wal_archiving": False,
        "archive_mode": "unknown",
        "archive_tool": "Unknown",
    }


def _column_distinct_estimate(n_distinct: Any, live_rows: int) -> Any:
    if n_distinct is None:
        return None

    try:
        value = float(n_distinct)
    except (TypeError, ValueError):
        return None

    if value < 0 and live_rows > 0:
        return round(abs(value) * live_rows)
    return round(value)


def _build_column_roles(
    table_map: Dict[str, Dict[str, Any]],
    foreign_keys: List[Dict[str, Any]],
) -> Dict[str, Dict[str, set[str]]]:
    roles_by_table: Dict[str, Dict[str, set[str]]] = {}

    for table_name, table in table_map.items():
        roles_by_table[table_name] = {}
        for column in table.get("primary_key") or []:
            roles_by_table[table_name].setdefault(column, set()).add("PK")

    for fk in foreign_keys:
        from_table = fk["from_table"]
        roles_by_table.setdefault(from_table, {})

        for column in fk.get("from_columns") or []:
            roles_by_table[from_table].setdefault(column, set()).add("FK")

    return roles_by_table


def _attach_column_statistics(
    table_map: Dict[str, Dict[str, Any]],
    column_stats: List[Dict[str, Any]],
    foreign_keys: List[Dict[str, Any]],
) -> int:
    roles_by_table = _build_column_roles(table_map, foreign_keys)
    attached_count = 0

    for row in column_stats:
        table_name = _table_key(row["schemaname"], row["table_name"])
        table = table_map.get(table_name)
        if not table:
            continue

        column_name = row["column_name"]
        roles = roles_by_table.get(table_name, {}).get(column_name, set())
        if not roles:
            continue

        live_rows = _table_live_rows(table)
        table.setdefault("column_statistics", []).append(
            {
                "column_name": column_name,
                "roles": sorted(roles),
                "null_frac": row.get("null_frac"),
                "n_distinct": row.get("n_distinct"),
                "estimated_distinct": _column_distinct_estimate(
                    row.get("n_distinct"),
                    live_rows,
                ),
                "avg_width": row.get("avg_width"),
                "most_common_values_count": row.get("most_common_values_count") or 0,
                "histogram_available": row.get("histogram_available"),
                "correlation": row.get("correlation"),
            }
        )
        attached_count += 1

    return attached_count


def _build_llm_context(digest: Dict[str, Any]) -> str:
    lines = [
        "# Database schema relationship digest",
        "",
        "This digest is intentionally compact. It includes user tables, PK/FK/UNIQUE constraints,",
        "foreign-key index coverage, and PostgreSQL cumulative table statistics from pg_stat views.",
        "It excludes PostgreSQL internal schemas.",
        "",
        "## Scope",
        f"- Tables: {digest['summary']['table_count']}",
        f"- Primary keys: {digest['summary']['primary_key_count']}",
        f"- Unique constraints: {digest['summary']['unique_constraint_count']}",
        f"- Foreign keys: {digest['summary']['foreign_key_count']}",
        f"- PK/FK column statistics: {digest['summary'].get('column_stat_count', 0)}",
        f"- Foreign keys without covering index: {digest['summary']['foreign_keys_without_covering_index']}",
        f"- pg_stat_database.stats_reset: {digest.get('stats_reset') or '-'}",
    ]

    architecture = digest.get("architecture") or {}
    lines.extend(
        [
            "",
            "## Database architecture",
            f"- Inferred architecture: {architecture.get('type', 'Unknown')}",
            f"- Server role: {architecture.get('server_role', 'Unknown')}",
            f"- Cluster name: {architecture.get('cluster_name') or 'not configured'}",
            f"- Connected downstream replicas: {architecture.get('connected_replicas', 0)}",
            f"- WAL level: {architecture.get('wal_level', 'unknown')}",
            f"- WAL archiving: {'enabled' if architecture.get('wal_archiving') else 'disabled'}",
            f"- Archive mode: {architecture.get('archive_mode', 'unknown')}",
            f"- Archive or restore tool detected: {architecture.get('archive_tool', 'Unknown')}",
            f"- Standby source configured: {'yes' if architecture.get('standby_source_configured') else 'no'}",
            f"- Replication slot configured: {'yes' if architecture.get('replication_slot_configured') else 'no'}",
            f"- Inference limitation: {architecture.get('inference_note', 'None provided')}",
            "",
            "## Tables",
        ]
    )

    for table in digest["tables"]:
        pk = table.get("primary_key") or []
        uniques = table.get("unique_constraints") or []
        stats = table.get("statistics") or {}
        lines.extend(
            [
                f"- {table['qualified_name']} ({table['table_kind']}, partitions={table.get('partition_count', 0)})",
                f"  - PK: {_format_columns(pk)}",
                "  - UNIQUE: "
                + (
                    "; ".join(
                        f"{item['constraint_name']}({_format_columns(item['columns'])})"
                        for item in uniques
                    )
                    if uniques
                    else "-"
                ),
                "  - pg_stat: "
                + ", ".join(
                    [
                        f"live={_format_stat(stats.get('n_live_tup'))}",
                        f"dead={_format_stat(stats.get('n_dead_tup'))}",
                        f"seq_scan={_format_stat(stats.get('seq_scan'))}",
                        f"idx_scan={_format_stat(stats.get('idx_scan'))}",
                        f"table_cache_hit={_format_stat(stats.get('table_cache_hit_pct'), '%')}",
                        f"index_cache_hit={_format_stat(stats.get('index_cache_hit_pct'), '%')}",
                    ]
                ),
                "  - sizes: "
                + ", ".join(
                    [
                        f"total={stats.get('total_size_pretty') or '-'}",
                        f"table={stats.get('table_size_pretty') or '-'}",
                        f"indexes={stats.get('indexes_size_pretty') or '-'}",
                    ]
                ),
                "  - maintenance: "
                + ", ".join(
                    [
                        f"last_vacuum={stats.get('last_vacuum') or '-'}",
                        f"last_autovacuum={stats.get('last_autovacuum') or '-'}",
                        f"last_analyze={stats.get('last_analyze') or '-'}",
                        f"last_autoanalyze={stats.get('last_autoanalyze') or '-'}",
                    ]
                ),
            ]
        )
        column_statistics = table.get("column_statistics") or []
        if column_statistics:
            lines.append("  - PK/FK column stats:")
            for column in column_statistics:
                roles = ",".join(column.get("roles") or [])
                lines.append(
                    "    - "
                    f"{column['column_name']} [{roles}]: "
                    + ", ".join(
                        [
                            f"null_frac={_format_stat(column.get('null_frac'))}",
                            f"n_distinct={_format_stat(column.get('n_distinct'))}",
                            f"estimated_distinct={_format_stat(column.get('estimated_distinct'))}",
                            f"avg_width={_format_stat(column.get('avg_width'))}",
                            f"mcv_count={_format_stat(column.get('most_common_values_count'))}",
                            f"histogram={'yes' if column.get('histogram_available') else 'no'}",
                            f"correlation={_format_stat(column.get('correlation'))}",
                        ]
                    )
                )

    lines.extend(["", "## Relationships"])

    if not digest["foreign_keys"]:
        lines.append("- No foreign keys found.")
    else:
        for fk in digest["foreign_keys"]:
            coverage = "covered" if fk["fk_index_covered"] else "missing covering index"
            lines.append(
                "- "
                f"{fk['from_table']}({_format_columns(fk['from_columns'])}) "
                f"-> {fk['to_table']}({_format_columns(fk['to_columns'])}) "
                f"[constraint={fk['constraint_name']}, on_delete={fk['on_delete']}, "
                f"on_update={fk['on_update']}, fk_index={coverage}]"
            )

    lines.extend(["", "## Top table workload from pg_stat_statements"])
    table_workload = digest.get("table_workload") or []
    if not table_workload:
        lines.append("- No parsed pg_stat_statements table workload was available.")
    else:
        lines.append(
            "- Attribution note: a statement referencing multiple tables contributes its counters to each table."
        )
        for table in table_workload:
            lines.append(
                f"- {table['table']}: statements={table['query_count']}, "
                f"calls={table['calls']}, rows={table['rows']}, "
                f"total_exec_ms={table['total_exec_time']:.2f}, "
                f"mean_per_call_ms={table['mean_exec_time']:.3f}"
            )
            for operation in table.get("operations") or []:
                lines.append(
                    f"  - {operation['operation'].upper()}: "
                    f"statements={operation['query_count']}, calls={operation['calls']}, "
                    f"rows={operation['rows']}, "
                    f"total_exec_ms={operation['total_exec_time']:.2f}, "
                    f"mean_per_call_ms={operation['mean_exec_time']:.3f}"
                )

    return "\n".join(lines)


def _mermaid_entity_name(value: str) -> str:
    out = "".join(ch if ch.isalnum() else "_" for ch in value)
    out = "_".join(part for part in out.split("_") if part)
    return (out or "table").upper()


def _table_live_rows(table: Dict[str, Any]) -> int:
    stats = table.get("statistics") or {}
    try:
        return max(int(stats.get("n_live_tup") or 0), 0)
    except (TypeError, ValueError):
        return 0


def _mermaid_size_class(table: Dict[str, Any], average_live_rows: float) -> str:
    live_rows = _table_live_rows(table)
    if live_rows <= 0 or average_live_rows <= 0:
        return "tableSize0"

    ratio = live_rows / average_live_rows
    if ratio < 0.25:
        return "tableSize1"
    if ratio < 0.75:
        return "tableSize2"
    if ratio < 1.5:
        return "tableSize3"
    if ratio < 3:
        return "tableSize4"
    return "tableSize5"


def _average_live_rows(tables: List[Dict[str, Any]]) -> float:
    positive_live_rows = [
        _table_live_rows(table)
        for table in tables
        if _table_live_rows(table) > 0
    ]
    if not positive_live_rows:
        return 0
    return sum(positive_live_rows) / len(positive_live_rows)


def _resolve_and_merge_table_workload(table_workload, table_map, limit=20):
    """Resolve schema names, merge duplicates, rank, then limit workload."""
    operation_order = ("select", "insert", "update", "delete", "other")
    merged = {}

    for raw_stats in table_workload or ():
        table_name = str(raw_stats.get("table") or "")
        resolved_name = table_name if table_name in table_map else None

        if not resolved_name and "." not in table_name:
            candidates = [
                qualified_name
                for qualified_name in table_map
                if qualified_name.rsplit(".", 1)[-1] == table_name
            ]
            if len(candidates) == 1:
                resolved_name = candidates[0]

        # DB Design should only receive workload for relations in its digest.
        if not resolved_name:
            continue

        table_stats = merged.setdefault(
            resolved_name,
            {"table": resolved_name, "operation_stats": {}},
        )
        operations = raw_stats.get("operations") or [
            {
                "operation": "other",
                "query_count": raw_stats.get("query_count", 0),
                "calls": raw_stats.get("calls", 0),
                "rows": raw_stats.get("rows", 0),
                "total_exec_time": raw_stats.get("total_exec_time", 0),
            }
        ]

        for operation in operations:
            operation_name = str(operation.get("operation") or "other").casefold()
            if operation_name not in operation_order:
                operation_name = "other"
            target = table_stats["operation_stats"].setdefault(
                operation_name,
                {
                    "operation": operation_name,
                    "query_count": 0,
                    "calls": 0,
                    "rows": 0,
                    "total_exec_time": 0.0,
                },
            )
            target["query_count"] += int(operation.get("query_count") or 0)
            target["calls"] += int(operation.get("calls") or 0)
            target["rows"] += int(operation.get("rows") or 0)
            target["total_exec_time"] += float(operation.get("total_exec_time") or 0)

    result = []
    for table_stats in merged.values():
        operations = []
        for operation_name in operation_order:
            operation = table_stats["operation_stats"].get(operation_name)
            if not operation:
                continue
            operation["mean_exec_time"] = (
                operation["total_exec_time"] / operation["calls"]
                if operation["calls"]
                else 0.0
            )
            operations.append(operation)

        table_stats["operations"] = operations
        table_stats["query_count"] = sum(item["query_count"] for item in operations)
        table_stats["calls"] = sum(item["calls"] for item in operations)
        table_stats["rows"] = sum(item["rows"] for item in operations)
        table_stats["total_exec_time"] = sum(
            item["total_exec_time"] for item in operations
        )
        table_stats["mean_exec_time"] = (
            table_stats["total_exec_time"] / table_stats["calls"]
            if table_stats["calls"]
            else 0.0
        )
        del table_stats["operation_stats"]
        result.append(table_stats)

    result.sort(
        key=lambda stats: (-stats["calls"], -stats["total_exec_time"], stats["table"])
    )
    return result[:limit]


def _build_mermaid_code(digest: Dict[str, Any]) -> str:
    table_lookup = {table["qualified_name"]: table for table in digest["tables"]}
    fk_columns_by_table: Dict[str, set[str]] = {
        table["qualified_name"]: set() for table in digest["tables"]
    }

    for fk in digest["foreign_keys"]:
        fk_columns_by_table.setdefault(fk["from_table"], set()).update(fk["from_columns"])

    average_live_rows = _average_live_rows(digest["tables"])

    lines = [
        "erDiagram",
        f"    %% Table colors are relative to average n_live_tup: {average_live_rows:.2f}",
    ]
    class_lines = []

    for table in digest["tables"]:
        entity = _mermaid_entity_name(table["qualified_name"])
        pk_columns = set(table.get("primary_key") or [])
        fk_columns = fk_columns_by_table.get(table["qualified_name"], set())
        columns = list(dict.fromkeys(list(pk_columns) + list(fk_columns)))
        class_lines.append(f"    class {entity} {_mermaid_size_class(table, average_live_rows)}")

        lines.append(f"    {entity} {{")
        if columns:
            for column in columns:
                flags = []
                if column in pk_columns:
                    flags.append("PK")
                if column in fk_columns:
                    flags.append("FK")
                lines.append(f"        text {column} {', '.join(flags)}")
        else:
            lines.append("        text no_pk_or_fk")
        lines.append("    }")
        lines.append("")

    for fk in digest["foreign_keys"]:
        if fk["from_table"] not in table_lookup or fk["to_table"] not in table_lookup:
            continue
        from_entity = _mermaid_entity_name(fk["from_table"])
        to_entity = _mermaid_entity_name(fk["to_table"])
        lines.append(f"    {to_entity} ||--o{{ {from_entity} : {fk['constraint_name']}")

    lines.extend(
        [
            "",
            "    classDef tableSize0 fill:#f8fafc,stroke:#cbd5e1,color:#0f172a;",
            "    classDef tableSize1 fill:#eff6ff,stroke:#93c5fd,color:#0f172a;",
            "    classDef tableSize2 fill:#dbeafe,stroke:#60a5fa,color:#0f172a;",
            "    classDef tableSize3 fill:#bfdbfe,stroke:#3b82f6,color:#0f172a;",
            "    classDef tableSize4 fill:#93c5fd,stroke:#2563eb,color:#0f172a;",
            "    classDef tableSize5 fill:#60a5fa,stroke:#1d4ed8,color:#0f172a;",
        ]
    )
    lines.extend(class_lines)

    return "\n".join(lines)


def _build_llm_prompt(llm_context: str) -> str:
    return f"""You are a senior PostgreSQL data model reviewer.

Analyze the following compact database schema relationship digest.

Goals:
1. Describe the observed database architecture: standalone, standalone with WAL archiving, or replicated cluster. State the server role, WAL archive status, detected archive/restore tool, and the evidence and limitations behind the classification.
2. Explain how the database appears to be used in practice, based on the observed pg_stat_statements workload:
   - characterize it as read-heavy, write-heavy, or mixed
   - identify the busiest tables by call volume and by total execution time
   - distinguish frequent inexpensive operations from less frequent expensive operations
   - describe the observed SELECT, INSERT, UPDATE, and DELETE mix
   - identify likely transactional, reporting, batch, ingestion, or maintenance patterns only when supported by evidence
3. Explain the main functional areas suggested by table names and relationships, and connect them to the observed workload.
4. Identify central tables and high-impact relationships. Distinguish structural centrality (PK/FK relationships) from workload centrality (calls and execution time).
5. Detect possible schema design risks:
   - missing primary keys
   - isolated tables
   - suspicious missing foreign keys
   - foreign keys without a covering index
   - many-to-many bridge tables
   - one-to-one relationships
   - circular dependencies
   - tables with high sequential scan activity compared to index usage
   - stale statistics or maintenance concerns
6. Use pg_stat and pg_stats values as context, not as absolute truth. Mention the stats reset timestamp.
7. Interpret workload counters carefully:
   - they are cumulative for the available statistics window, not a live trace
   - a statement referencing multiple tables contributes its counters to every referenced table
   - query_count is the number of distinct captured statements, while calls is their execution count
   - total_exec_ms measures cumulative impact, while mean_per_call_ms helps identify individually expensive operations
   - rows does not by itself prove business volume or rows physically modified
8. If no parsed workload is available, say that database usage cannot be inferred from pg_stat_statements and rely only on structural observations.
9. Do not expose or reconstruct archive commands, restore commands, credentials, connection strings, or other secrets. Use only the safe architecture classification supplied in the digest.
10. Do not invent tables, columns, queries, users, applications, business processes, or time-of-day patterns that are not present in the digest.
11. Clearly label every inferred usage pattern as an inference and cite the supporting table, operation, and metric values.
12. Provide actionable PostgreSQL SQL only when it is safe and directly supported by the digest.

Begin the answer with one concise paragraph, before any heading, that directly answers:
"What is the role of this database?" Infer its likely business or technical purpose from the schema relationships and observed workload. Clearly state that this is an inference and cite the strongest supporting evidence. If the available evidence is insufficient, say so explicitly instead of inventing a role.

Then return the answer in Markdown with these sections:
- Executive summary
- Database architecture
- Observed database usage
- Relationship map
- Central tables
- Risks and anomalies
- Missing or weak relationships to investigate
- Foreign-key index coverage
- pg_stat observations
- Workload evidence by table and statement type
- Column statistics observations
- Recommended next actions
- SQL suggestions, if any

Schema digest:

{llm_context}
"""


def get_database_schema_llm_context(conn, table_workload=None) -> Dict[str, Any]:
    """Return a compact relationship digest and LLM-ready text for a database."""
    tables = _fetch_all_dicts(conn, TABLES_SQL)
    constraints = _fetch_all_dicts(conn, CONSTRAINTS_SQL)
    foreign_keys = _fetch_all_dicts(conn, FOREIGN_KEYS_SQL)
    column_stats = _fetch_all_dicts(conn, COLUMN_STATS_SQL)
    stats_reset_rows = _fetch_all_dicts(conn, STATS_RESET_SQL)
    stats_reset = stats_reset_rows[0].get("stats_reset") if stats_reset_rows else None
    architecture = get_database_architecture(conn)

    table_map: Dict[str, Dict[str, Any]] = {}
    for row in tables:
        key = _table_key(row["schemaname"], row["table_name"])
        table_map[key] = {
            "schemaname": row["schemaname"],
            "table_name": row["table_name"],
            "qualified_name": key,
            "table_kind": row["table_kind"],
            "partition_count": row.get("partition_count") or 0,
            "primary_key": [],
            "unique_constraints": [],
            "statistics": {
                "total_size_bytes": row["total_size_bytes"],
                "total_size_pretty": row["total_size_pretty"],
                "table_size_bytes": row["table_size_bytes"],
                "table_size_pretty": row["table_size_pretty"],
                "indexes_size_bytes": row["indexes_size_bytes"],
                "indexes_size_pretty": row["indexes_size_pretty"],
                "n_live_tup": row["n_live_tup"],
                "n_dead_tup": row["n_dead_tup"],
                "seq_scan": row["seq_scan"],
                "seq_tup_read": row["seq_tup_read"],
                "idx_scan": row["idx_scan"],
                "idx_tup_fetch": row["idx_tup_fetch"],
                "table_cache_hit_pct": row["table_cache_hit_pct"],
                "index_cache_hit_pct": row["index_cache_hit_pct"],
                "last_vacuum": row["last_vacuum"],
                "last_autovacuum": row["last_autovacuum"],
                "last_analyze": row["last_analyze"],
                "last_autoanalyze": row["last_autoanalyze"],
            },
        }

    primary_key_count = 0
    unique_constraint_count = 0
    for constraint in constraints:
        key = _table_key(constraint["schemaname"], constraint["table_name"])
        table = table_map.get(key)
        if not table:
            continue

        if constraint["contype"] == "p":
            primary_key_count += 1
            table["primary_key"] = constraint["columns"]
        elif constraint["contype"] == "u":
            unique_constraint_count += 1
            table["unique_constraints"].append(
                {
                    "constraint_name": constraint["constraint_name"],
                    "columns": constraint["columns"],
                }
            )

    normalized_fks = []
    for fk in foreign_keys:
        normalized_fks.append(
            {
                "constraint_name": fk["constraint_name"],
                "from_schema": fk["from_schema"],
                "from_table_name": fk["from_table"],
                "from_table": _table_key(fk["from_schema"], fk["from_table"]),
                "from_columns": fk["from_columns"],
                "to_schema": fk["to_schema"],
                "to_table_name": fk["to_table"],
                "to_table": _table_key(fk["to_schema"], fk["to_table"]),
                "to_columns": fk["to_columns"],
                "on_delete": fk["on_delete"],
                "on_update": fk["on_update"],
                "fk_index_covered": fk["fk_index_covered"],
            }
        )

    column_stat_count = _attach_column_statistics(
        table_map,
        column_stats,
        normalized_fks,
    )

    digest = {
        "summary": {
            "table_count": len(table_map),
            "primary_key_count": primary_key_count,
            "unique_constraint_count": unique_constraint_count,
            "foreign_key_count": len(normalized_fks),
            "column_stat_count": column_stat_count,
            "average_live_rows": round(_average_live_rows(list(table_map.values())), 2),
            "foreign_keys_without_covering_index": len(
                [fk for fk in normalized_fks if not fk["fk_index_covered"]]
            ),
        },
        "stats_reset": stats_reset,
        "architecture": architecture,
        "tables": list(table_map.values()),
        "foreign_keys": normalized_fks,
        "table_workload": _resolve_and_merge_table_workload(
            table_workload,
            table_map,
            limit=20,
        ),
    }

    return {
        "success": True,
        "query_type": "database_schema_llm_context",
        "digest": digest,
        "llm_context": _build_llm_context(digest),
        "mermaid_code": _build_mermaid_code(digest),
        "llm_prompt": _build_llm_prompt(_build_llm_context(digest)),
    }
