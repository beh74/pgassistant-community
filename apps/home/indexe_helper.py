# -*- encoding: utf-8 -*-
"""
Index statistics helper functions for pgAssistant.

The functions in this module expect an already opened PostgreSQL connection.
They do not open or close connections themselves; route handlers are responsible
for connection lifecycle management.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


INTERNAL_SCHEMA_FILTER = """
    ns.nspname <> 'information_schema'
    AND ns.nspname !~ '^pg_'
"""


INDEX_STATS_SQL = f"""
WITH index_stats AS (
    SELECT
        s.schemaname,
        s.relname AS table_name,
        s.indexrelname AS index_name,
        s.idx_scan,
        s.idx_tup_read,
        s.idx_tup_fetch
    FROM pg_stat_user_indexes s
    WHERE s.schemaname <> 'information_schema'
      AND s.schemaname !~ '^pg_'
),
index_io AS (
    SELECT
        io.schemaname,
        io.relname AS table_name,
        io.indexrelname AS index_name,
        io.idx_blks_read,
        io.idx_blks_hit,
        (io.idx_blks_read + io.idx_blks_hit) AS total_index_block_access,
        ROUND(
            100.0 * io.idx_blks_hit
            / NULLIF(io.idx_blks_hit + io.idx_blks_read, 0),
            2
        ) AS cache_hit_pct
    FROM pg_statio_user_indexes io
    WHERE io.schemaname <> 'information_schema'
      AND io.schemaname !~ '^pg_'
),
table_stats AS (
    SELECT
        t.schemaname,
        t.relname AS table_name,
        t.seq_scan,
        t.seq_tup_read,
        t.idx_scan AS table_idx_scan,
        t.n_live_tup,
        t.n_dead_tup,
        t.last_vacuum,
        t.last_autovacuum,
        t.last_analyze,
        t.last_autoanalyze
    FROM pg_stat_user_tables t
    WHERE t.schemaname <> 'information_schema'
      AND t.schemaname !~ '^pg_'
),
index_catalog AS (
    SELECT
        ns.nspname AS schemaname,
        tbl.relname AS table_name,
        idx.relname AS index_name,
        idx.oid AS index_oid,
        tbl.oid AS table_oid,
        am.amname AS index_type,
        pg_get_indexdef(idx.oid) AS index_definition,
        pg_relation_size(idx.oid) AS index_size_bytes,
        pg_size_pretty(pg_relation_size(idx.oid)) AS index_size_pretty,
        pg_relation_size(tbl.oid) AS table_size_bytes,
        pg_size_pretty(pg_relation_size(tbl.oid)) AS table_size_pretty,
        i.indisprimary,
        i.indisunique,
        i.indisvalid,
        i.indisready,
        i.indislive,
        i.indisreplident,
        array_to_string(
            ARRAY(
                SELECT pg_get_indexdef(idx.oid, k + 1, true)
                FROM generate_subscripts(i.indkey, 1) AS k
                ORDER BY k
            ),
            ', '
        ) AS indexed_columns
    FROM pg_index i
    JOIN pg_class idx ON idx.oid = i.indexrelid
    JOIN pg_class tbl ON tbl.oid = i.indrelid
    JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
    JOIN pg_am am ON am.oid = idx.relam
    WHERE {INTERNAL_SCHEMA_FILTER}
),
stats_reset AS (
    SELECT stats_reset
    FROM pg_stat_database
    WHERE datname = current_database()
)
SELECT
    c.schemaname,
    c.table_name,
    c.index_name,
    c.schemaname || '.' || c.index_name AS qualified_index_name,
    c.schemaname || '.' || c.table_name AS qualified_table_name,
    c.index_type,
    c.indexed_columns,
    c.index_definition,

    c.index_size_bytes,
    c.index_size_pretty,
    c.table_size_bytes,
    c.table_size_pretty,

    c.indisprimary,
    c.indisunique,
    c.indisvalid,
    c.indisready,
    c.indislive,
    c.indisreplident,

    COALESCE(s.idx_scan, 0) AS idx_scan,
    COALESCE(s.idx_tup_read, 0) AS idx_tup_read,
    COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch,
    ROUND(
        COALESCE(s.idx_tup_read, 0)::numeric
        / NULLIF(COALESCE(s.idx_scan, 0), 0),
        2
    ) AS avg_tuples_read_per_index_scan,

    COALESCE(io.idx_blks_read, 0) AS idx_blks_read,
    COALESCE(io.idx_blks_hit, 0) AS idx_blks_hit,
    COALESCE(io.total_index_block_access, 0) AS total_index_block_access,
    io.cache_hit_pct,

    COALESCE(t.seq_scan, 0) AS table_seq_scan,
    COALESCE(t.seq_tup_read, 0) AS table_seq_tup_read,
    COALESCE(t.table_idx_scan, 0) AS table_idx_scan,
    ROUND(
        COALESCE(t.seq_tup_read, 0)::numeric
        / NULLIF(COALESCE(t.seq_scan, 0), 0),
        2
    ) AS avg_tuples_read_per_seq_scan,

    t.n_live_tup,
    t.n_dead_tup,
    t.last_vacuum,
    t.last_autovacuum,
    t.last_analyze,
    t.last_autoanalyze,
    r.stats_reset,

    'UNKNOWN'::text AS bloat_status,
    'Bloat is not computed by this endpoint. Use a dedicated pgstattuple/pgstatindex check when available.'::text AS bloat_note,

    CASE
        WHEN NOT c.indisvalid THEN 'INVALID_INDEX'
        WHEN NOT c.indisready THEN 'NOT_READY'
        WHEN NOT c.indislive THEN 'NOT_LIVE'
        WHEN COALESCE(s.idx_scan, 0) = 0
             AND COALESCE(t.seq_scan, 0) > 100
             AND COALESCE(t.seq_tup_read, 0) > 100000
          THEN 'POSSIBLY_UNUSED_WHILE_TABLE_SEQ_SCANNED'
        WHEN COALESCE(s.idx_scan, 0) = 0
             AND c.index_size_bytes >= 100 * 1024 * 1024
          THEN 'UNUSED_LARGE_INDEX'
        WHEN COALESCE(s.idx_scan, 0) = 0
          THEN 'NO_INDEX_ACTIVITY'
        WHEN COALESCE(io.total_index_block_access, 0) = 0
          THEN 'NO_IO_ACTIVITY'
        WHEN io.idx_blks_read = 0
             AND COALESCE(io.total_index_block_access, 0) >= 1000
          THEN 'OK_FULLY_CACHED'
        WHEN COALESCE(io.total_index_block_access, 0) < 1000
          THEN 'MONITOR_LOW_VOLUME'
        WHEN io.cache_hit_pct >= 99
          THEN 'OK_EXCELLENT_CACHE_HIT'
        WHEN io.cache_hit_pct >= 95
          THEN 'MONITOR_GOOD_CACHE_HIT'
        WHEN io.idx_blks_read >= 100000
             AND io.cache_hit_pct < 95
          THEN 'TUNE_MEMORY_OR_REVIEW_INDEX'
        WHEN io.cache_hit_pct < 90
          THEN 'INVESTIGATE_LOW_CACHE_HIT'
        ELSE 'REVIEW'
    END AS severity,

    CASE
        WHEN NOT c.indisvalid
          THEN 'Index is invalid. Queries cannot reliably use it. Consider REINDEX INDEX CONCURRENTLY or DROP/CREATE depending on context.'
        WHEN NOT c.indisready
          THEN 'Index is not ready. Check whether a failed CREATE INDEX CONCURRENTLY left an unusable artifact.'
        WHEN NOT c.indislive
          THEN 'Index is not live. It may be related to an interrupted or failed index operation.'
        WHEN COALESCE(s.idx_scan, 0) = 0
             AND COALESCE(t.seq_scan, 0) > 100
             AND COALESCE(t.seq_tup_read, 0) > 100000
          THEN 'The index has not been used since stats reset, while the table has significant sequential scan activity. Check whether predicates match this index or whether a better index is needed.'
        WHEN COALESCE(s.idx_scan, 0) = 0
             AND c.index_size_bytes >= 100 * 1024 * 1024
          THEN 'Large index with no recorded usage since stats reset. Review observation window, constraints, uniqueness and business cycles before considering removal.'
        WHEN COALESCE(s.idx_scan, 0) = 0
          THEN 'No recorded usage since stats reset. Monitor before taking action.'
        WHEN COALESCE(io.total_index_block_access, 0) < 1000
          THEN 'Low I/O volume. There is not enough activity to conclude.'
        WHEN io.idx_blks_read = 0
          THEN 'Index appears fully served from cache. No action required.'
        WHEN io.cache_hit_pct >= 99
          THEN 'Excellent cache hit ratio. No action required.'
        WHEN io.cache_hit_pct >= 95
          THEN 'Good cache behavior. Monitor if physical reads increase.'
        WHEN io.idx_blks_read >= 100000
             AND io.cache_hit_pct < 95
          THEN 'Index is hot on disk. Review shared_buffers, RAM pressure, index size, query patterns, and possible bloat.'
        WHEN io.cache_hit_pct < 90
          THEN 'Low cache hit ratio. Investigate memory pressure, poor locality, bloated index, or inefficient access pattern.'
        ELSE 'Review usage, size, cache behavior, and table scan activity.'
    END AS recommendation_action
FROM index_catalog c
LEFT JOIN index_stats s
       ON s.schemaname = c.schemaname
      AND s.table_name = c.table_name
      AND s.index_name = c.index_name
LEFT JOIN index_io io
       ON io.schemaname = c.schemaname
      AND io.table_name = c.table_name
      AND io.index_name = c.index_name
LEFT JOIN table_stats t
       ON t.schemaname = c.schemaname
      AND t.table_name = c.table_name
CROSS JOIN stats_reset r
WHERE
    (
        %(index_name)s IS NULL
        OR lower(c.index_name) = lower(%(index_name)s)
        OR lower(c.schemaname || '.' || c.index_name) = lower(%(index_name)s)
    )
    AND (%(schemaname)s IS NULL OR lower(c.schemaname) = lower(%(schemaname)s))
    AND (%(table_name)s IS NULL OR lower(c.table_name) = lower(%(table_name)s))
ORDER BY
    CASE
        WHEN NOT c.indisvalid THEN 1
        WHEN COALESCE(s.idx_scan, 0) = 0 AND c.index_size_bytes >= 100 * 1024 * 1024 THEN 2
        WHEN io.idx_blks_read >= 100000 AND io.cache_hit_pct < 95 THEN 3
        WHEN io.cache_hit_pct < 90 THEN 4
        ELSE 9
    END,
    c.index_size_bytes DESC,
    io.idx_blks_read DESC NULLS LAST,
    c.schemaname,
    c.table_name,
    c.index_name
"""


def _fetch_all_dicts(conn, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute SQL and return rows as dictionaries."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    return [dict(zip(columns, row)) for row in rows]


def _split_qualified_name(name: str) -> tuple[Optional[str], str]:
    """
    Split an optionally schema-qualified SQL object name.

    This intentionally supports the common `schema.index` case. It does not try
    to fully parse quoted SQL identifiers; route parameters are used only as
    filter values, not interpolated in SQL.
    """
    value = (name or "").strip()
    if "." not in value:
        return None, value
    schema, object_name = value.split(".", 1)
    return schema.strip() or None, object_name.strip()


def get_index_stats_by_name(conn, index_name: str) -> Dict[str, Any]:
    """Return index statistics for an index name or schema-qualified index name."""
    schema_from_name, bare_index_name = _split_qualified_name(index_name)

    rows = _fetch_all_dicts(
        conn,
        INDEX_STATS_SQL,
        {
            "index_name": bare_index_name,
            "schemaname": schema_from_name,
            "table_name": None,
        },
    )

    return {
        "success": True,
        "query_type": "index",
        "index_name": index_name,
        "count": len(rows),
        "indexes": rows,
    }


def get_table_indexes_stats(conn, schemaname: str, table_name: str) -> Dict[str, Any]:
    """Return statistics for every index attached to a given schema.table."""
    rows = _fetch_all_dicts(
        conn,
        INDEX_STATS_SQL,
        {
            "index_name": None,
            "schemaname": schemaname,
            "table_name": table_name,
        },
    )

    return {
        "success": True,
        "query_type": "table",
        "schemaname": schemaname,
        "table_name": table_name,
        "count": len(rows),
        "indexes": rows,
    }
