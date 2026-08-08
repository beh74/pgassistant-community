"""Read-only checks used to qualify fillfactor recommendations."""

from __future__ import annotations

from typing import Any

from pglast import parse_sql
from pglast.ast import UpdateStmt


def _fetchone_dict(conn, sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            return {}
        columns = [item[0] for item in cursor.description]
        return dict(zip(columns, row))
    finally:
        cursor.close()


def _fetchall_dict(conn, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


def _updated_columns_for_table(
    query: str,
    *,
    schema_name: str,
    table_name: str,
) -> set[str]:
    """Return UPDATE target columns when the parsed target is the requested table."""
    try:
        statements = parse_sql(query)
    except Exception:
        return set()

    columns: set[str] = set()
    for raw_statement in statements:
        statement = raw_statement.stmt
        if not isinstance(statement, UpdateStmt):
            continue

        relation = statement.relation
        if relation.relname != table_name:
            continue
        if relation.schemaname and relation.schemaname != schema_name:
            continue

        columns.update(
            target.name
            for target in statement.targetList or ()
            if target.name
        )
    return columns


def run_fillfactor_checks(conn, schema_name: str, table_name: str) -> dict[str, Any]:
    indexes = _fetchone_dict(
        conn,
        """
        SELECT count(DISTINCT i.indexrelid)::int AS index_count,
               COALESCE(array_agg(DISTINCT a.attname) FILTER (WHERE a.attname IS NOT NULL), ARRAY[]::name[]) AS indexed_columns
        FROM pg_class AS t
        JOIN pg_namespace AS n ON n.oid = t.relnamespace
        LEFT JOIN pg_index AS i ON i.indrelid = t.oid
        LEFT JOIN LATERAL unnest(i.indkey) AS key(attnum) ON key.attnum > 0
        LEFT JOIN pg_attribute AS a ON a.attrelid = t.oid AND a.attnum = key.attnum
        WHERE n.nspname = %s AND t.relname = %s AND t.relkind IN ('r', 'p')
        """,
        (schema_name, table_name),
    )
    indexed_columns = {str(value) for value in indexes.get("indexed_columns") or []}
    observed_queries: list[str] = []
    try:
        rows = _fetchall_dict(
            conn,
            """
            SELECT query
            FROM pg_stat_statements
            WHERE lower(query) LIKE '%%update%%'
              AND lower(query) LIKE %s
            ORDER BY total_exec_time DESC
            LIMIT 20
            """,
            (f"%{table_name.lower()}%",),
        )
        observed_queries = [str(row.get("query") or "") for row in rows]
    except Exception:
        conn.rollback()

    parsed_updates = [
        columns
        for query in observed_queries
        if (columns := _updated_columns_for_table(
            query,
            schema_name=schema_name,
            table_name=table_name,
        ))
    ]
    observed_updated = set().union(*parsed_updates) if parsed_updates else set()
    matching_update_count = len(parsed_updates)
    indexed_updated = sorted(indexed_columns & observed_updated)
    if indexed_updated:
        indexed_check = {
            "status": "warning",
            "label": "Indexed columns updated",
            "summary": "Observed UPDATE statements modify indexed columns: " + ", ".join(indexed_updated) + ".",
        }
    elif matching_update_count:
        indexed_check = {
            "status": "ok",
            "label": "No indexed update observed",
            "summary": f"No indexed column was found in {matching_update_count} parsed UPDATE statement(s).",
        }
    else:
        indexed_check = {
            "status": "unknown",
            "label": "Workload unavailable",
            "summary": "No usable UPDATE statement was found in pg_stat_statements; this check is inconclusive.",
        }

    maintenance = _fetchone_dict(
        conn,
        """
        SELECT n_live_tup, n_dead_tup, last_autovacuum,
               round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct
        FROM pg_stat_user_tables
        WHERE schemaname = %s AND relname = %s
        """,
        (schema_name, table_name),
    )
    dead_tuples = int(maintenance.get("n_dead_tup") or 0)
    dead_pct = float(maintenance.get("dead_pct") or 0)
    maintenance_warning = dead_tuples >= 100_000 or dead_pct >= 20
    autovacuum_check = {
        "status": "warning" if maintenance_warning else "ok",
        "label": "Vacuum pressure" if maintenance_warning else "No strong vacuum pressure",
        "summary": (
            f"{dead_tuples:,} dead tuples ({dead_pct:.1f}%); last autovacuum: "
            f"{maintenance.get('last_autovacuum') or 'never recorded'}."
        ),
    }

    transactions = _fetchone_dict(
        conn,
        """
        SELECT count(*)::int AS transaction_count,
               COALESCE(max(extract(epoch FROM now() - xact_start))::bigint, 0) AS oldest_seconds
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND xact_start IS NOT NULL
          AND state <> 'idle'
          AND xact_start < now() - interval '5 minutes'
        """,
        (),
    )
    transaction_count = int(transactions.get("transaction_count") or 0)
    oldest_seconds = int(transactions.get("oldest_seconds") or 0)
    transaction_check = {
        "status": "warning" if transaction_count else "ok",
        "label": "Long transactions detected" if transaction_count else "No long transaction detected",
        "summary": (
            f"{transaction_count} active transaction(s) older than 5 minutes; "
            f"oldest age: {oldest_seconds // 60} minute(s)."
        ),
    }

    return {
        "success": True,
        "checks": {
            "indexed_updates": indexed_check,
            "autovacuum": autovacuum_check,
            "long_transactions": transaction_check,
        },
        "metadata": {
            "index_count": int(indexes.get("index_count") or 0),
            "indexed_columns": sorted(indexed_columns),
            "observed_update_statements": matching_update_count,
        },
    }
