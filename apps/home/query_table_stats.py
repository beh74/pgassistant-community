"""Aggregate pg_stat_statements workload metrics by parsed table reference."""


KNOWN_OPERATIONS = ("select", "insert", "update", "delete")
OPERATION_ORDER = (*KNOWN_OPERATIONS, "other")

QUERY_ACTIVITY_GRAPH_SQL = """
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        a.attname AS column_name,
        a.attnum AS column_number,
        COALESCE(pk.is_primary_key, false) AS is_primary_key,
        COALESCE(fk.is_foreign_key, false) AS is_foreign_key
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute AS a
      ON a.attrelid = c.oid
     AND a.attnum > 0
     AND NOT a.attisdropped
    LEFT JOIN LATERAL (
        SELECT true AS is_primary_key
        FROM pg_catalog.pg_constraint AS con
        WHERE con.conrelid = c.oid
          AND con.contype = 'p'
          AND a.attnum = ANY(con.conkey)
        LIMIT 1
    ) AS pk ON true
    LEFT JOIN LATERAL (
        SELECT true AS is_foreign_key
        FROM pg_catalog.pg_constraint AS con
        WHERE con.conrelid = c.oid
          AND con.contype = 'f'
          AND a.attnum = ANY(con.conkey)
        LIMIT 1
    ) AS fk ON true
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_toast%'
      AND n.nspname NOT LIKE 'pg_temp%'
    ORDER BY n.nspname, c.relname, a.attnum
"""

QUERY_ACTIVITY_GRAPH_FKS_SQL = """
    SELECT
        child_ns.nspname AS from_schema,
        child.relname AS from_table,
        parent_ns.nspname AS to_schema,
        parent.relname AS to_table,
        con.conname AS constraint_name
    FROM pg_catalog.pg_constraint AS con
    JOIN pg_catalog.pg_class AS child ON child.oid = con.conrelid
    JOIN pg_catalog.pg_namespace AS child_ns ON child_ns.oid = child.relnamespace
    JOIN pg_catalog.pg_class AS parent ON parent.oid = con.confrelid
    JOIN pg_catalog.pg_namespace AS parent_ns ON parent_ns.oid = parent.relnamespace
    WHERE con.contype = 'f'
      AND child_ns.nspname NOT IN ('pg_catalog', 'information_schema')
      AND parent_ns.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY child_ns.nspname, child.relname, con.conname
"""

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


def _mermaid_identifier(value):
    import re

    normalized = re.sub(r"[^A-Za-z0-9_]", "_", str(value or ""))
    if not normalized or normalized[0].isdigit():
        normalized = f"t_{normalized}"
    return normalized


def _mermaid_entity_ids(table_names):
    """Return readable Mermaid IDs, adding a hash only for real collisions."""
    import hashlib

    grouped = {}
    for table_name in sorted(table_names):
        grouped.setdefault(_mermaid_identifier(table_name), []).append(table_name)

    result = {}
    for base_identifier, names in grouped.items():
        for table_name in names:
            if len(names) == 1:
                result[table_name] = base_identifier
                continue
            suffix = hashlib.sha1(table_name.encode("utf-8")).hexdigest()[:7]
            result[table_name] = f"{base_identifier}_{suffix}"
    return result


def _mermaid_column(value):
    import re

    normalized = re.sub(r"[^A-Za-z0-9_]", "_", str(value or "column"))
    return normalized if normalized and not normalized[0].isdigit() else f"c_{normalized}"


def _workload_color_level(total_exec_time, maximum_total):
    if maximum_total <= 0 or total_exec_time <= 0:
        return 0
    ratio = total_exec_time / maximum_total
    if ratio <= 0.10:
        return 1
    if ratio <= 0.25:
        return 2
    if ratio <= 0.50:
        return 3
    if ratio <= 0.75:
        return 4
    return 5


def _display_column_role(role):
    """Translate parser role names into labels used by the DBA-facing UI."""
    return "SELECT" if role == "projection" else role


def build_query_activity_graph(conn, rows, table_stats):
    """Build an ER graph colored by total pg_stat_statements time per table."""
    from . import query_column_usage

    with conn.cursor() as cursor:
        cursor.execute(QUERY_ACTIVITY_GRAPH_SQL)
        column_rows = cursor.fetchall()
        cursor.execute(QUERY_ACTIVITY_GRAPH_FKS_SQL)
        foreign_keys = cursor.fetchall()

    table_columns = {}
    column_flags = {}
    for schema, table, column, _position, is_pk, is_fk in column_rows:
        qualified = f"{schema}.{table}"
        table_columns.setdefault(qualified, []).append(column)
        column_flags[(qualified, column)] = {"pk": bool(is_pk), "fk": bool(is_fk)}

    stats_by_table = {item["table"]: item for item in table_stats or ()}
    graph_tables = set(stats_by_table)
    for from_schema, from_table, to_schema, to_table, _name in foreign_keys:
        source = f"{from_schema}.{from_table}"
        target = f"{to_schema}.{to_table}"
        if source in graph_tables or target in graph_tables:
            graph_tables.update((source, target))
    graph_tables &= set(table_columns)

    if not graph_tables:
        return "", "No user table referenced by Query Activity could be resolved."

    entity_ids = _mermaid_entity_ids(graph_tables)
    usage_by_table = {}
    for qualified in graph_tables:
        schema, table = qualified.split(".", 1)
        usage_by_table[qualified] = query_column_usage.build_table_usage(
            rows, schema, table, table_columns[qualified]
        )

    maximum_total = max(
        (
            float((stats_by_table.get(table) or {}).get("total_exec_time") or 0)
            for table in graph_tables
        ),
        default=0.0,
    )
    lines = [
        "erDiagram",
        "    %% Colors represent relative total execution time from pg_stat_statements.",
    ]
    classes = []
    for qualified in sorted(graph_tables):
        entity = entity_ids[qualified]
        stats = stats_by_table.get(qualified) or {}
        total_time = float(stats.get("total_exec_time") or 0)
        level = _workload_color_level(total_time, maximum_total)
        classes.append(f"    class {entity} queryTime{level}")
        used = {
            item["column"]: [
                _display_column_role(role["role"])
                for role in item["roles"]
            ]
            for item in usage_by_table[qualified]["columns"]
        }
        visible_columns = [
            column for column in table_columns[qualified]
            if column in used
            or column_flags[(qualified, column)]["pk"]
            or column_flags[(qualified, column)]["fk"]
        ]
        lines.append(f"    {entity} {{")
        if not visible_columns:
            lines.append("        text no_query_column_detected")
        for column in visible_columns:
            flags = column_flags[(qualified, column)]
            keys = []
            if flags["pk"]:
                keys.append("PK")
            if flags["fk"]:
                keys.append("FK")
            key_text = f" {', '.join(keys)}" if keys else ""
            roles = ", ".join(used.get(column) or [])
            comment = f' "used: {roles}"' if roles else ""
            lines.append(f"        text {_mermaid_column(column)}{key_text}{comment}")
        lines.append("    }")

    for from_schema, from_table, to_schema, to_table, constraint_name in foreign_keys:
        source = f"{from_schema}.{from_table}"
        target = f"{to_schema}.{to_table}"
        if source not in graph_tables or target not in graph_tables:
            continue
        label = _mermaid_column(constraint_name)
        lines.append(
            f"    {entity_ids[target]} ||--o{{ {entity_ids[source]} : {label}"
        )

    lines.extend([
        "",
        "    classDef queryTime0 fill:#f8fafc,stroke:#cbd5e1,color:#0f172a;",
        "    classDef queryTime1 fill:#e0f2fe,stroke:#7dd3fc,color:#0f172a;",
        "    classDef queryTime2 fill:#bae6fd,stroke:#38bdf8,color:#0f172a;",
        "    classDef queryTime3 fill:#7dd3fc,stroke:#0284c7,color:#0f172a;",
        "    classDef queryTime4 fill:#38bdf8,stroke:#0369a1,color:#0f172a;",
        "    classDef queryTime5 fill:#0ea5e9,stroke:#075985,color:#0f172a;",
    ])
    lines.extend(classes)
    return "\n".join(lines), ""
