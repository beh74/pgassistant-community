"""Map PostgreSQL query parameters to the columns they constrain."""

import re
import sqlglot
from pglast import ast, parse_sql
from pglast.parser import ParseError as PglastParseError
from sqlglot.expressions import (
    Column, Literal, And, Or, EQ, GT, LT, Like, Parameter, Table,
    Select, Subquery, In, Paren, GTE, LTE, NEQ, Alias, Not, Where, Between
)

PARAM_PATTERN = re.compile(r"^\$?\d+$")


def extract_binary_conditions(expression):
    """
    Walk a SQLGlot expression tree and collect comparison predicates.

    Recursive function to extract all comparison conditions (=, >, <, IN, LIKE, etc.),
    including those wrapped in parentheses, NOT, and logical combinations.
    """
    conditions = []

    if expression is None:
        return conditions

    # Base case: it's a binary or IN-like condition
    if isinstance(expression, (EQ, GT, LT, GTE, LTE, NEQ, In, Like, Between)):
        conditions.append(expression)

    # Parentheses
    elif isinstance(expression, Paren):
        conditions.extend(extract_binary_conditions(expression.this))

    # Logical NOT
    elif isinstance(expression, Not):
        # NOT (...) → on descend dans le contenu
        conditions.extend(extract_binary_conditions(expression.this))

    # Logical combinations and subqueries
    elif isinstance(expression, (And, Or, Subquery, Select)):
        left_expr = expression.args.get("this")
        right_expr = expression.args.get("expression")

        if left_expr:
            conditions.extend(extract_binary_conditions(left_expr))
        if right_expr:
            conditions.extend(extract_binary_conditions(right_expr))

    return conditions


def extract_table_aliases(expression):
    """
    Retrieves table aliases defined in the FROM clause and JOINs.

    :param expression: AST expression from SQLGlot
    :return: Dictionary {alias: actual_table_name}
    """
    aliases = {}

    for table in expression.find_all(Table):
        table_name = table.name
        alias = table.alias_or_name
        if alias:
            aliases[alias] = table_name

    return aliases


def find_table_for_column(column, table_aliases, default_table):
    """
    Finds the table associated with a column by replacing aliases.

    :param column: SQLGlot Column object
    :param table_aliases: Dictionary of table aliases
    :param default_table: Default table name
    :return: Associated table name
    """
    if column.table:
        return table_aliases.get(column.table, column.table)
    return default_table


def extract_param_keys_from_expr(expr):
    """
    Return a set of parameter numbers (as strings) found inside an expression.

    It looks for:
    - Parameter nodes of the form $1, $2...
    - Literal nodes whose value looks like '$1', '$2', etc. (e.g. inside CAST)
    """
    keys = set()

    if expr is None:
        return keys

    # 1) Real Parameter nodes: Parameter(this=Literal('1')) for $1
    for p in expr.find_all(Parameter):
        inner = p.this
        if isinstance(inner, Literal):
            val = str(inner.this).strip()
        else:
            val = str(inner).strip()

        # For PostgreSQL-style parameters, this will be just digits: "1", "2", ...
        if val.isdigit():
            keys.add(val)

    # 2) Literals that look like "$1", "$2", ... (e.g. inside CAST: $1::date)
    for lit in expr.find_all(Literal):
        raw = str(lit.this).strip()
        if raw.startswith("$") and raw[1:].isdigit():
            keys.add(raw[1:])

    return keys


def _extract_parameter_columns_sqlglot(sql_query):
    """
    Main helper used by query analysis to understand what each bind parameter targets.

    Parse an SQL query and return a mapping of parameters ($1, $2, etc.)
    to the used columns (table.column) based on WHERE clauses and SELECT lists.

    :param sql_query: SQL query as a string
    :return: Dictionary {parameter_number_as_str: "table.column"}
    """
    try:
        expression = sqlglot.parse_one(sql_query, dialect="postgres")
    except sqlglot.errors.ParseError:
        return {}
    
    param_columns = {}

    # Global table aliases (top-level)
    global_aliases = extract_table_aliases(expression)

    # ---------- PASS 1: WHERE clauses ----------
    where_clauses = expression.find_all(Where)

    for where_clause in where_clauses:
        # Parent SELECT for this WHERE (handles subqueries / UNION branches)
        parent_select = where_clause.find_ancestor(Select)
        if parent_select:
            local_aliases = extract_table_aliases(parent_select)
            default_table = next(iter(local_aliases.values()), None)
        else:
            local_aliases = {}
            default_table = next(iter(global_aliases.values()), None)

        # Merge global + local aliases, local taking precedence
        table_aliases = {**global_aliases, **local_aliases}

        # Extract all conditions under this WHERE
        conditions = extract_binary_conditions(where_clause.this)

        for condition in conditions:
            # IN / NOT IN
            if isinstance(condition, In):
                col = condition.this
                if not isinstance(col, Column):
                    continue

                for expr in condition.expressions or []:
                    for param_key in extract_param_keys_from_expr(expr):
                        column_table = find_table_for_column(col, table_aliases, default_table)
                        column_name = col.name
                        full_column_name = (
                            f"{column_table}.{column_name}" if column_table else column_name
                        )
                        param_columns[param_key] = full_column_name

            # BETWEEN / NOT BETWEEN
            elif isinstance(condition, Between):
                col = condition.this
                if not isinstance(col, Column):
                    continue

                low_expr = condition.args.get("low")
                high_expr = condition.args.get("high")

                for expr in (low_expr, high_expr):
                    for param_key in extract_param_keys_from_expr(expr):
                        column_table = find_table_for_column(col, table_aliases, default_table)
                        column_name = col.name
                        full_column_name = (
                            f"{column_table}.{column_name}" if column_table else column_name
                        )
                        param_columns[param_key] = full_column_name

            else:
                # Generic binary condition (=, !=, <, >, <=, >=, LIKE)
                left = condition.args.get("this")
                right = condition.args.get("expression")

                if left is None or right is None:
                    continue

                if isinstance(left, Column):
                    for param_key in extract_param_keys_from_expr(right):
                        column_table = find_table_for_column(left, table_aliases, default_table)
                        column_name = left.name
                        full_column_name = (
                            f"{column_table}.{column_name}" if column_table else column_name
                        )
                        param_columns[param_key] = full_column_name

                if isinstance(right, Column):
                    for param_key in extract_param_keys_from_expr(left):
                        column_table = find_table_for_column(right, table_aliases, default_table)
                        column_name = right.name
                        full_column_name = (
                            f"{column_table}.{column_name}" if column_table else column_name
                        )
                        param_columns[param_key] = full_column_name

    # ---------- PASS 2: SELECT lists (projection) ----------
    # This is where we catch things like: SELECT $3::regclass AS classid
    for select in expression.find_all(Select):
        local_aliases = extract_table_aliases(select)
        table_aliases = {**global_aliases, **local_aliases}
        default_table = next(iter(local_aliases.values()), None) or \
                        next(iter(global_aliases.values()), None)

        for proj in select.expressions:
            # Handle possible alias: expression AS alias
            if isinstance(proj, Alias):
                alias_name = proj.alias
                expr = proj.this
            else:
                alias_name = None
                expr = proj

            param_keys = extract_param_keys_from_expr(expr)
            if not param_keys:
                continue

            # Try to find an underlying column, if any
            col = expr.find(Column) if hasattr(expr, "find") else None

            if col is not None:
                column_table = find_table_for_column(col, table_aliases, default_table)
                column_name = col.name
            else:
                column_table = default_table
                column_name = alias_name or "expr"

            full_column_name = (
                f"{column_table}.{column_name}" if column_table else column_name
            )

            for param_key in param_keys:
                # On laisse la projection compléter ou écraser les infos WHERE si besoin
                param_columns[param_key] = full_column_name

    return param_columns


_STATEMENT_NODES = (
    ast.SelectStmt,
    ast.UpdateStmt,
    ast.InsertStmt,
    ast.DeleteStmt,
)


def _iter_ast_children(node):
    """Yield the child values of a pglast AST node or collection."""
    if isinstance(node, ast.Node):
        for attribute in type(node).__slots__:
            value = getattr(node, attribute, None)
            if value is not None:
                yield value
    elif isinstance(node, (list, tuple)):
        yield from node


def _walk_ast(node, stop_at_statements=False):
    """Walk a pglast tree, optionally leaving nested statements to their scope."""
    if node is None:
        return

    if isinstance(node, (list, tuple)):
        for item in node:
            yield from _walk_ast(item, stop_at_statements=stop_at_statements)
        return

    if not isinstance(node, ast.Node):
        return

    yield node
    if stop_at_statements and isinstance(node, _STATEMENT_NODES):
        return

    for child in _iter_ast_children(node):
        yield from _walk_ast(child, stop_at_statements=stop_at_statements)


def _parameter_numbers(node):
    return {
        str(item.number)
        for item in _walk_ast(node)
        if isinstance(item, ast.ParamRef) and item.number
    }


def _column_parts(column):
    return [
        field.sval
        for field in column.fields or ()
        if isinstance(field, ast.String)
    ]


def _range_vars(node):
    """Return physical table references contained in a FROM/USING clause."""
    return [item for item in _walk_ast(node) if isinstance(item, ast.RangeVar)]


def _scope_tables(statement):
    """Build alias resolution and choose the statement's default table."""
    ranges = []

    relation = getattr(statement, "relation", None)
    if isinstance(relation, ast.RangeVar):
        ranges.append(relation)

    for attribute in ("fromClause", "usingClause"):
        ranges.extend(_range_vars(getattr(statement, attribute, None)))

    aliases = {}
    tables = []
    for range_var in ranges:
        table = range_var.relname
        if not table:
            continue
        tables.append(table)
        aliases[table] = table
        if range_var.alias and range_var.alias.aliasname:
            aliases[range_var.alias.aliasname] = table

    return aliases, (tables[0] if tables else None)


def _qualified_column(column, aliases, default_table):
    parts = _column_parts(column)
    if not parts:
        return None

    column_name = parts[-1]
    qualifier = parts[-2] if len(parts) > 1 else None
    table = aliases.get(qualifier, qualifier) if qualifier else default_table
    return f"{table}.{column_name}" if table else column_name


def _map_predicates(node, aliases, default_table, result):
    """Associate parameters and columns found on opposite sides of predicates."""
    for expression in _walk_ast(node, stop_at_statements=True):
        if not isinstance(expression, ast.A_Expr):
            continue

        left_columns = [
            item for item in _walk_ast(expression.lexpr)
            if isinstance(item, ast.ColumnRef)
        ]
        right_columns = [
            item for item in _walk_ast(expression.rexpr)
            if isinstance(item, ast.ColumnRef)
        ]
        left_parameters = _parameter_numbers(expression.lexpr)
        right_parameters = _parameter_numbers(expression.rexpr)

        pairs = (
            (left_columns, right_parameters),
            (right_columns, left_parameters),
        )
        for columns, parameters in pairs:
            if not columns or not parameters:
                continue
            full_column = _qualified_column(columns[0], aliases, default_table)
            if full_column:
                for number in parameters:
                    result[number] = full_column


def _map_select_projections(statement, aliases, default_table, result):
    for target in statement.targetList or ():
        if not isinstance(target, ast.ResTarget):
            continue

        parameters = _parameter_numbers(target.val)
        if not parameters:
            continue

        column = next(
            (item for item in _walk_ast(target.val) if isinstance(item, ast.ColumnRef)),
            None,
        )
        if column:
            full_column = _qualified_column(column, aliases, default_table)
        else:
            column_name = target.name or "expr"
            full_column = (
                f"{default_table}.{column_name}" if default_table else column_name
            )

        for number in parameters:
            result[number] = full_column


def _map_update_targets(statement, aliases, default_table, result):
    for target in statement.targetList or ():
        if not isinstance(target, ast.ResTarget) or not target.name:
            continue
        full_column = (
            f"{default_table}.{target.name}" if default_table else target.name
        )
        for number in _parameter_numbers(target.val):
            result[number] = full_column


def _map_insert_targets(statement, result):
    table = statement.relation.relname if statement.relation else None
    columns = [target.name for target in statement.cols or () if target.name]
    select = statement.selectStmt
    if not columns or not isinstance(select, ast.SelectStmt):
        return

    rows = select.valuesLists or ()
    if not rows and select.targetList:
        rows = (tuple(target.val for target in select.targetList),)

    for row in rows:
        for column, value in zip(columns, row):
            full_column = f"{table}.{column}" if table else column
            for number in _parameter_numbers(value):
                result[number] = full_column


def _nested_statements(statement):
    for child in _iter_ast_children(statement):
        for item in _walk_ast(child, stop_at_statements=True):
            if isinstance(item, _STATEMENT_NODES):
                yield item


def _analyze_pglast_statement(statement, result):
    aliases, default_table = _scope_tables(statement)

    for attribute in ("whereClause", "havingClause", "fromClause", "usingClause"):
        _map_predicates(
            getattr(statement, attribute, None), aliases, default_table, result
        )

    if isinstance(statement, ast.SelectStmt):
        _map_select_projections(statement, aliases, default_table, result)
    elif isinstance(statement, ast.UpdateStmt):
        _map_update_targets(statement, aliases, default_table, result)
    elif isinstance(statement, ast.InsertStmt):
        _map_insert_targets(statement, result)

    for nested in _nested_statements(statement):
        _analyze_pglast_statement(nested, result)


def _extract_parameter_columns_pglast(sql_query):
    """Map bind parameters with PostgreSQL's own grammar through pglast."""
    result = {}
    for raw_statement in parse_sql(sql_query):
        if isinstance(raw_statement.stmt, _STATEMENT_NODES):
            _analyze_pglast_statement(raw_statement.stmt, result)
    return result


def extract_referenced_tables(sql_query):
    """Return table references found by PostgreSQL's parser.

    Qualified names are returned as ``schema.table`` and unqualified names as
    ``table``. CTE names are excluded so callers can match physical relations.
    """
    references = []
    seen = set()

    for raw_statement in parse_sql(sql_query):
        statement = raw_statement.stmt
        cte_names = {
            item.ctename
            for item in _walk_ast(statement)
            if isinstance(item, ast.CommonTableExpr) and item.ctename
        }

        for range_var in _range_vars(statement):
            if not range_var.relname or (
                not range_var.schemaname and range_var.relname in cte_names
            ):
                continue
            relation = (
                f"{range_var.schemaname}.{range_var.relname}"
                if range_var.schemaname
                else range_var.relname
            )
            if relation not in seen:
                references.append(relation)
                seen.add(relation)

    return references


def extract_referenced_tables_safe(sql_query):
    """Return pglast table references, or an empty list for invalid SQL."""
    try:
        return extract_referenced_tables(sql_query)
    except (PglastParseError, ValueError, TypeError):
        return []


def query_references_table(sql_query, schema_name, table_name):
    """Return whether a parsed query references the selected physical table."""
    expected_schema = (schema_name or "").casefold()
    expected_table = (table_name or "").casefold()

    try:
        references = extract_referenced_tables(sql_query)
    except (PglastParseError, ValueError, TypeError):
        return False

    for reference in references:
        parts = reference.casefold().split(".")
        if parts[-1] != expected_table:
            continue
        if len(parts) == 1 or not expected_schema or parts[-2] == expected_schema:
            return True
    return False


def extract_parameter_columns(sql_query):
    """
    Return ``{parameter_number: table.column}`` for PostgreSQL bind parameters.

    pglast is the primary parser because it follows PostgreSQL's native grammar.
    SQLGlot remains a compatibility fallback for incomplete or non-PostgreSQL SQL.
    """
    try:
        result = _extract_parameter_columns_pglast(sql_query)
    except (PglastParseError, ValueError, TypeError):
        return _extract_parameter_columns_sqlglot(sql_query)

    return result or _extract_parameter_columns_sqlglot(sql_query)
