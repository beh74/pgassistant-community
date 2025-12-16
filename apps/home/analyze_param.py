import re
import sqlglot
from sqlglot.expressions import (
    Column, Literal, And, Or, EQ, GT, LT, Like, Parameter, Table,
    Select, Subquery, In, Paren, GTE, LTE, NEQ, Alias, Not, Where, Between
)

PARAM_PATTERN = re.compile(r"^\$?\d+$")


def extract_binary_conditions(expression):
    """
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


def extract_parameter_columns(sql_query):
    """
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