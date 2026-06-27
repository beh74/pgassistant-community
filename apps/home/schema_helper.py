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
SELECT
    n.nspname AS schemaname,
    c.relname AS table_name,
    c.relkind,
    CASE c.relkind
        WHEN 'p' THEN 'partitioned table'
        ELSE 'table'
    END AS table_kind,
    pg_total_relation_size(c.oid) AS total_size_bytes,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size_pretty,
    pg_relation_size(c.oid) AS table_size_bytes,
    pg_size_pretty(pg_relation_size(c.oid)) AS table_size_pretty,
    pg_indexes_size(c.oid) AS indexes_size_bytes,
    pg_size_pretty(pg_indexes_size(c.oid)) AS indexes_size_pretty,
    COALESCE(st.n_live_tup, 0) AS n_live_tup,
    COALESCE(st.n_dead_tup, 0) AS n_dead_tup,
    COALESCE(st.seq_scan, 0) AS seq_scan,
    COALESCE(st.seq_tup_read, 0) AS seq_tup_read,
    COALESCE(st.idx_scan, 0) AS idx_scan,
    COALESCE(st.idx_tup_fetch, 0) AS idx_tup_fetch,
    st.last_vacuum,
    st.last_autovacuum,
    st.last_analyze,
    st.last_autoanalyze,
    ROUND(
        100.0 * sio.heap_blks_hit
        / NULLIF(sio.heap_blks_hit + sio.heap_blks_read, 0),
        2
    ) AS table_cache_hit_pct,
    ROUND(
        100.0 * sio.idx_blks_hit
        / NULLIF(sio.idx_blks_hit + sio.idx_blks_read, 0),
        2
    ) AS index_cache_hit_pct
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
LEFT JOIN pg_statio_user_tables sio ON sio.relid = c.oid
WHERE c.relkind IN ('r', 'p')
  AND {USER_SCHEMA_FILTER}
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


COLUMN_STATS_SQL = """
SELECT
    schemaname,
    tablename AS table_name,
    attname AS column_name,
    null_frac,
    n_distinct,
    avg_width,
    array_length(most_common_freqs, 1) AS most_common_values_count,
    histogram_bounds IS NOT NULL AS histogram_available,
    correlation
FROM pg_stats
WHERE schemaname <> 'information_schema'
  AND schemaname !~ '^pg_'
ORDER BY schemaname, tablename, attname
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
        "",
        "## Tables",
    ]

    for table in digest["tables"]:
        pk = table.get("primary_key") or []
        uniques = table.get("unique_constraints") or []
        stats = table.get("statistics") or {}
        lines.extend(
            [
                f"- {table['qualified_name']} ({table['table_kind']})",
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
1. Explain the main functional areas you infer from table relationships.
2. Identify central tables and high-impact relationships.
3. Detect possible schema design risks:
   - missing primary keys
   - isolated tables
   - suspicious missing foreign keys
   - foreign keys without a covering index
   - many-to-many bridge tables
   - one-to-one relationships
   - circular dependencies
   - tables with high sequential scan activity compared to index usage
   - stale statistics or maintenance concerns
4. Use pg_stat and pg_stats values as context, not as absolute truth. Mention the stats reset timestamp.
5. Do not invent tables or columns that are not present in the digest.
6. If a recommendation is speculative, clearly mark it as an assumption.
7. Provide actionable PostgreSQL SQL only when it is safe and directly supported by the digest.

Return the answer in Markdown with these sections:
- Executive summary
- Relationship map
- Central tables
- Risks and anomalies
- Missing or weak relationships to investigate
- Foreign-key index coverage
- pg_stat observations
- Column statistics observations
- Recommended next actions
- SQL suggestions, if any

Schema digest:

{llm_context}
"""


def get_database_schema_llm_context(conn) -> Dict[str, Any]:
    """Return a compact relationship digest and LLM-ready text for a database."""
    tables = _fetch_all_dicts(conn, TABLES_SQL)
    constraints = _fetch_all_dicts(conn, CONSTRAINTS_SQL)
    foreign_keys = _fetch_all_dicts(conn, FOREIGN_KEYS_SQL)
    column_stats = _fetch_all_dicts(conn, COLUMN_STATS_SQL)
    stats_reset_rows = _fetch_all_dicts(conn, STATS_RESET_SQL)
    stats_reset = stats_reset_rows[0].get("stats_reset") if stats_reset_rows else None

    table_map: Dict[str, Dict[str, Any]] = {}
    for row in tables:
        key = _table_key(row["schemaname"], row["table_name"])
        table_map[key] = {
            "schemaname": row["schemaname"],
            "table_name": row["table_name"],
            "qualified_name": key,
            "table_kind": row["table_kind"],
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
        "tables": list(table_map.values()),
        "foreign_keys": normalized_fks,
    }

    return {
        "success": True,
        "query_type": "database_schema_llm_context",
        "digest": digest,
        "llm_context": _build_llm_context(digest),
        "mermaid_code": _build_mermaid_code(digest),
        "llm_prompt": _build_llm_prompt(_build_llm_context(digest)),
    }
