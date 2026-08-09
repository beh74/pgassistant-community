"""Derive table and column workload observations from pg_stat_statements SQL."""

from collections import defaultdict

from pglast import ast, parse_sql
from pglast.parser import ParseError as PglastParseError

from . import analyze_param, sqlhelper


ROLE_ORDER = ("join", "filter", "projection", "group_sort", "write", "expression")


def _walk(node):
    if node is None:
        return
    if isinstance(node, (list, tuple)):
        for item in node:
            yield from _walk(item)
        return
    if not isinstance(node, ast.Node):
        return
    yield node
    for attribute in type(node).__slots__:
        yield from _walk(getattr(node, attribute, None))


def _column_parts(column):
    return [item.sval for item in column.fields or () if isinstance(item, ast.String)]


def _target_aliases(statement, schema_name, table_name):
    aliases = {table_name.casefold(), f"{schema_name}.{table_name}".casefold()}
    target_ranges = []
    for item in _walk(statement):
        if not isinstance(item, ast.RangeVar) or not item.relname or item.relname.casefold() != table_name.casefold():
            continue
        if item.schemaname and item.schemaname.casefold() != schema_name.casefold():
            continue
        target_ranges.append(item)
        if item.alias and item.alias.aliasname:
            aliases.add(item.alias.aliasname.casefold())
    return aliases, target_ranges


def _columns_in(node, aliases, target_columns, allow_unqualified):
    result = set()
    for item in _walk(node):
        if not isinstance(item, ast.ColumnRef):
            continue
        parts = _column_parts(item)
        if not parts:
            continue
        column_name = parts[-1]
        qualifier = parts[-2].casefold() if len(parts) > 1 else ""
        if qualifier:
            if qualifier in aliases and column_name in target_columns:
                result.add(column_name)
        elif allow_unqualified and column_name in target_columns:
            result.add(column_name)
    return result


def extract_table_column_roles(query, schema_name, table_name, table_columns):
    """Return column roles for one table, skipping ambiguous unqualified columns."""
    target_columns = set(table_columns or ())
    roles = defaultdict(set)
    ambiguous = set()
    try:
        statements = parse_sql(query)
    except (PglastParseError, ValueError, TypeError):
        return {"roles": {}, "ambiguous_columns": [], "parse_error": True}

    for raw_statement in statements:
        statement = raw_statement.stmt
        aliases, target_ranges = _target_aliases(statement, schema_name, table_name)
        if not target_ranges:
            continue
        physical_ranges = [item for item in _walk(statement) if isinstance(item, ast.RangeVar)]
        allow_unqualified = len(physical_ranges) == 1

        contextual_nodes = {
            "filter": [getattr(statement, "whereClause", None), getattr(statement, "havingClause", None)],
            "projection": [getattr(statement, "targetList", None)],
            "group_sort": [getattr(statement, "groupClause", None), getattr(statement, "sortClause", None)],
        }
        for join in (item for item in _walk(statement) if isinstance(item, ast.JoinExpr)):
            contextual_nodes.setdefault("join", []).append(join.quals)
            for using_column in join.usingClause or ():
                if isinstance(using_column, ast.String) and using_column.sval in target_columns:
                    roles[using_column.sval].add("join")

        if isinstance(statement, ast.UpdateStmt):
            for target in statement.targetList or ():
                if isinstance(target, ast.ResTarget) and target.name in target_columns:
                    roles[target.name].add("write")
        if isinstance(statement, ast.InsertStmt):
            for target in statement.cols or ():
                if target.name in target_columns:
                    roles[target.name].add("write")

        classified = set()
        for role, nodes in contextual_nodes.items():
            for node in nodes:
                columns = _columns_in(node, aliases, target_columns, allow_unqualified)
                classified.update(columns)
                for column in columns:
                    roles[column].add(role)

        all_columns = _columns_in(statement, aliases, target_columns, allow_unqualified)
        for column in all_columns - classified - set(roles):
            roles[column].add("expression")
        if not allow_unqualified:
            for item in _walk(statement):
                if isinstance(item, ast.ColumnRef):
                    parts = _column_parts(item)
                    if len(parts) == 1 and parts[0] in target_columns:
                        ambiguous.add(parts[0])

    return {
        "roles": {column: sorted(values, key=ROLE_ORDER.index) for column, values in roles.items()},
        "ambiguous_columns": sorted(ambiguous - set(roles)),
        "parse_error": False,
    }


def build_table_usage(rows, schema_name, table_name, table_columns):
    """Aggregate pgss counters for statements referencing a selected table."""
    column_stats = {
        column: {"column": column, "statements": 0, "calls": 0, "roles": defaultdict(int)}
        for column in table_columns
    }
    related_queries = []
    ambiguous = set()
    parse_errors = 0

    for row in rows or ():
        query = row.get("query") or ""
        if not analyze_param.query_references_table(query, schema_name, table_name):
            continue
        usage = extract_table_column_roles(query, schema_name, table_name, table_columns)
        calls = int(row.get("calls") or 0)
        columns = sorted(usage["roles"])
        ambiguous.update(usage["ambiguous_columns"])
        parse_errors += int(usage["parse_error"])
        for column, roles in usage["roles"].items():
            target = column_stats[column]
            target["statements"] += 1
            target["calls"] += calls
            for role in roles:
                target["roles"][role] += 1
        related_queries.append({
            "queryid": str(row.get("queryid") or ""),
            "operation": sqlhelper.get_sql_type(query),
            "calls": calls,
            "total_exec_time": float(row.get("total_exec_time") or 0),
            "columns": columns,
        })

    columns = []
    for stats in column_stats.values():
        stats["roles"] = [
            {"role": role, "statements": stats["roles"][role]}
            for role in ROLE_ORDER if stats["roles"].get(role)
        ]
        if stats["statements"]:
            columns.append(stats)
    columns.sort(key=lambda item: (-item["calls"], -item["statements"], item["column"]))
    related_queries.sort(key=lambda item: (-item["calls"], -item["total_exec_time"]))
    return {
        "success": True,
        "table": f"{schema_name}.{table_name}",
        "summary": {
            "statements": len(related_queries),
            "calls": sum(item["calls"] for item in related_queries),
            "total_exec_time": sum(item["total_exec_time"] for item in related_queries),
            "columns_used": len(columns),
        },
        "columns": columns,
        "queries": related_queries[:20],
        "ambiguous_columns": sorted(ambiguous),
        "parse_errors": parse_errors,
    }
