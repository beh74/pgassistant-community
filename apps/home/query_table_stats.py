"""Aggregate pg_stat_statements workload metrics by parsed table reference."""


KNOWN_OPERATIONS = ("select", "insert", "update", "delete")
OPERATION_ORDER = (*KNOWN_OPERATIONS, "other")

USER_RELATIONS_SQL = """
    SELECT n.nspname AS schemaname, c.relname
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND n.nspname NOT LIKE 'pg_temp%'
      AND c.relname NOT LIKE 'pg_%'
"""


def _number(value, converter, default=0):
    try:
        if value is None or value == "":
            return default
        return converter(value)
    except (TypeError, ValueError):
        return default


def aggregate_by_table(rows):
    """Return workload totals for every table referenced by the supplied rows.

    A statement referencing several tables contributes its counters to each
    table. This describes the workload involving a table and is intentionally
    not suitable for summing across tables into a database-wide total.
    """
    aggregated = {}

    for row in rows or ():
        operation = str(row.get("operation_type") or "unknown").casefold()
        operation_key = operation if operation in KNOWN_OPERATIONS else "other"
        tables = dict.fromkeys(row.get("tables") or ())
        calls = _number(row.get("calls"), int)
        returned_rows = _number(row.get("rows"), int)
        total_exec_time = _number(row.get("total_exec_time"), float, 0.0)

        for table in tables:
            if not table:
                continue
            stats = aggregated.setdefault(
                table,
                {
                    "table": table,
                    "query_count": 0,
                    "select_count": 0,
                    "insert_count": 0,
                    "update_count": 0,
                    "delete_count": 0,
                    "other_count": 0,
                    "calls": 0,
                    "rows": 0,
                    "total_exec_time": 0.0,
                    "operation_stats": {},
                },
            )
            operation_stats = stats["operation_stats"].setdefault(
                operation_key,
                {
                    "operation": operation_key,
                    "query_count": 0,
                    "calls": 0,
                    "rows": 0,
                    "total_exec_time": 0.0,
                },
            )
            stats["query_count"] += 1
            stats[f"{operation_key}_count"] += 1
            stats["calls"] += calls
            stats["rows"] += returned_rows
            stats["total_exec_time"] += total_exec_time
            operation_stats["query_count"] += 1
            operation_stats["calls"] += calls
            operation_stats["rows"] += returned_rows
            operation_stats["total_exec_time"] += total_exec_time

    result = []
    for stats in aggregated.values():
        stats["mean_exec_time"] = (
            stats["total_exec_time"] / stats["calls"]
            if stats["calls"]
            else 0.0
        )
        stats["operations"] = []
        for operation in OPERATION_ORDER:
            operation_stats = stats["operation_stats"].get(operation)
            if not operation_stats:
                continue
            operation_stats["mean_exec_time"] = (
                operation_stats["total_exec_time"] / operation_stats["calls"]
                if operation_stats["calls"]
                else 0.0
            )
            stats["operations"].append(operation_stats)
        del stats["operation_stats"]
        result.append(stats)

    return sorted(
        result,
        key=lambda stats: (-stats["calls"], -stats["total_exec_time"], stats["table"]),
    )


def _is_postgres_internal_table(table_name):
    parts = str(table_name or "").casefold().split(".")
    relation = parts[-1]
    schema = parts[-2] if len(parts) > 1 else ""
    return (
        relation.startswith("pg_")
        or schema in {"pg_catalog", "information_schema"}
        or schema.startswith("pg_toast")
        or schema.startswith("pg_temp")
    )


def normalize_table_references(table_names, relation_names=None, excluded_names=None):
    """Filter internal relations and qualify unambiguous user table names."""
    known_relations = {
        str(name)
        for name in (relation_names or ())
        if name and not _is_postgres_internal_table(name)
    }
    excluded = {str(name).casefold() for name in (excluded_names or ())}
    by_unqualified_name = {}
    for qualified_name in known_relations:
        relation_name = qualified_name.rsplit(".", 1)[-1].casefold()
        by_unqualified_name.setdefault(relation_name, []).append(qualified_name)

    normalized = []
    for raw_name in table_names or ():
        table_name = str(raw_name or "").strip()
        relation_name = table_name.rsplit(".", 1)[-1]
        if (
            not table_name
            or _is_postgres_internal_table(table_name)
            or relation_name.casefold() in excluded
        ):
            continue

        resolved_name = table_name
        if "." not in table_name:
            candidates = by_unqualified_name.get(table_name.casefold(), [])
            if len(candidates) == 1:
                resolved_name = candidates[0]
        elif table_name not in known_relations:
            casefold_matches = [
                name for name in known_relations if name.casefold() == table_name.casefold()
            ]
            if len(casefold_matches) == 1:
                resolved_name = casefold_matches[0]

        if resolved_name not in normalized:
            normalized.append(resolved_name)
    return normalized


def load_user_relation_names(db_config):
    """Return qualified user relations used to resolve parsed pgss references."""
    from . import database

    conn, status = database.connectdb(db_config)
    if conn is None or status != "OK":
        return []
    try:
        with conn.cursor() as cursor:
            cursor.execute(USER_RELATIONS_SQL)
            return [f"{schema}.{relation}" for schema, relation in cursor.fetchall()]
    except Exception as exc:
        print(f"Warning: unable to resolve Query Activity relations: {exc}")
        return []
    finally:
        conn.close()


def aggregate_pgss_rows(
    rows,
    limit=None,
    exclude_internal=False,
    relation_names=None,
):
    """Parse raw pg_stat_statements rows and aggregate their table workload."""
    from . import analyze_param, sqlhelper

    enriched_rows = []
    for raw_row in rows or ():
        row = dict(raw_row)
        query = row.get("query") or ""
        row["tables"] = analyze_param.extract_referenced_tables_safe(query)
        if not row["tables"]:
            row["tables"] = sqlhelper.get_tables(query)
        if exclude_internal:
            row["tables"] = normalize_table_references(
                row["tables"],
                relation_names=relation_names,
            )
        row["operation_type"] = sqlhelper.get_sql_type(query)
        enriched_rows.append(row)

    result = aggregate_by_table(enriched_rows)
    return result[:limit] if limit else result


def load_top_table_workload(db_config, limit=None):
    """Load table workload without making schema analysis depend on pgss."""
    from . import database

    try:
        return aggregate_pgss_rows(
            database.get_top_queries(db_config),
            limit=limit,
            exclude_internal=True,
        )
    except Exception as exc:
        print(f"Warning: table workload unavailable for DB Design: {exc}")
        return []
