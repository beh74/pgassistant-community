import re
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from sql_metadata import Parser
from sql_formatter.core import format_sql

from . import database
from . import analyze_param


def get_tables(query):
    """
    Extract table names from an SQL query.
    """
    try:
        tables = Parser(query.lower()).tables
        return tables
    except Exception:
        return []


def get_sql_type(sql_query):
    """
    Return query type: select, insert, update, delete, etc.
    """
    try:
        parser = Parser(sql_query)
        sqltype = parser.query_type.replace("QueryType.", "")
        return sqltype.lower()
    except Exception:
        return "unknown"


def get_formated_sql(sql_query):
    """
    Format SQL query for better readability.
    """
    try:
        sqlf = format_sql(sql_query)
        return sqlf
    except Exception:
        return sql_query


def replace_query_parameters(query, params):
    """
    Replace parameters ($1, $2, etc.) in an SQL string with their provided values.

    The values in `params` are assumed to be already SQL-formatted
    (including quotes, NULL, etc.). This function does not modify or escape them.
    """
    def replace_match(match):
        param_index = int(match.group(1))

        if param_index not in params:
            return match.group(0)

        value = params[param_index]
        return "" if value is None else str(value)

    return re.sub(r"\$(\d+)", replace_match, query)


def parse_most_common_vals(value, limit=10):
    """
    Parse PostgreSQL's most_common_vals field into a Python list.
    Limit the number of returned values (default: 10).
    """
    if not value or value == "{}":
        return []

    if isinstance(value, tuple):
        value = value[0]

    if value is None:
        return []

    value = value.strip("{}")
    pattern = r'"([^"]+)"|([^,]+)'
    matches = re.findall(pattern, value)

    parsed_values = []

    for i, match in enumerate(matches):
        if i >= limit:
            break

        raw_value = match[0] if match[0] else match[1]

        try:
            parsed_value = str(datetime.strptime(raw_value, "%Y-%m-%d %H:%M:%S.%f"))
        except ValueError:
            try:
                parsed_value = str(datetime.strptime(raw_value, "%Y-%m-%d"))
            except ValueError:
                try:
                    parsed_value = int(raw_value)
                except ValueError:
                    parsed_value = raw_value

        parsed_values.append(parsed_value)

    return parsed_values


def extract_schema_table(full_name):
    """
    Split vars 'schema.table' or 'table'.
    """
    parts = full_name.split(".")
    if len(parts) == 2:
        schema, table = parts
    else:
        schema, table = None, parts[0]
    return schema, table


def fetch_column_data(table, column, data_type, session):
    """
    Fetch up to 10 rows from a specific column of a table and return the result as a typed JSON array.
    """
    try:
        conn, msg = database.connectdb(session)
        if "OK" in msg:
            with conn.cursor() as cursor:
                schema, tablename = extract_schema_table(table)
                if schema is None:
                    query = (
                        f"select most_common_vals from pg_stats "
                        f"where tablename='{table}' and attname='{column}' limit 1"
                    )
                else:
                    query = (
                        f"select most_common_vals from pg_stats "
                        f"where schemaname='{schema}' and tablename='{tablename}' "
                        f"and attname='{column}' limit 1"
                    )

                cursor.execute(query)
                row = cursor.fetchall()

                if row and row[0]:
                    values_common = parse_most_common_vals(row[0])
                    if len(values_common) > 0:
                        return values_common

                query = f"SELECT {column} FROM {table} LIMIT 10;"
                cursor.execute(query)
                rows = cursor.fetchall()

                def convert_value(value):
                    if value is None:
                        return None
                    if data_type in ("integer", "bigint", "smallint"):
                        return int(value)
                    elif data_type in ("real", "double precision", "numeric"):
                        return float(value)
                    elif data_type in ("boolean",):
                        return bool(value)
                    else:
                        return str(value)

                result = []
                seen = set()

                for row in rows:
                    v = convert_value(row[0])
                    if v not in seen:
                        seen.add(v)
                        result.append(v)

                return result

    except Exception as e:
        print(f"Error: {e}")
        return []


# --------------------------------------------------------------------
# Parameter analysis helpers
# --------------------------------------------------------------------

def normalize_query_for_parameter_analysis(query: str) -> str:
    """
    Normalize a few SQL forms so parser and PREPARE have better chances to work.

    Examples:
      DATE $3       -> $3::date
      TIMESTAMP $4  -> $4::timestamp
      TEXT $5       -> $5::text
    """
    if not query:
        return query

    normalized = query

    replacements = {
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "INTEGER": "integer",
        "INT": "integer",
        "BIGINT": "bigint",
        "SMALLINT": "smallint",
        "TEXT": "text",
        "VARCHAR": "varchar",
        "BOOLEAN": "boolean",
        "NUMERIC": "numeric",
        "REAL": "real",
        "DOUBLE PRECISION": "double precision",
    }

    # longest first
    for sql_type, pg_type in sorted(replacements.items(), key=lambda x: -len(x[0])):
        pattern = rf"\b{re.escape(sql_type)}\s+\$(\d+)\b"
        normalized = re.sub(
            pattern,
            rf"$\1::{pg_type}",
            normalized,
            flags=re.IGNORECASE,
        )

    return normalized


def normalize_parameter_name(param: str) -> str:
    """
    Normalize parameter name to numeric format: '1', '2', '3'
    Accepts '1', '$1', ' 2 ' etc.
    """
    if param is None:
        return param

    param = str(param).strip()
    m = re.fullmatch(r"\$?(\d+)", param)
    if not m:
        return param

    return m.group(1)


def extract_ordered_parameters(query: str) -> List[str]:
    """
    Return parameters found in query ordered numerically.

    Example:
        SELECT * WHERE a=$1 AND b=$3

    returns:
        ['1', '3']
    """
    params = sorted({int(x) for x in re.findall(r"\$(\d+)", query)})
    return [str(p) for p in params]


def fallback_extract_parameter_columns(query: str) -> Dict[str, str]:
    """
    Fallback extractor for simple predicates like:
      col = $1
      alias.col = $2
      col >= $3::date

    Returns:
      {'1': 'orders.customer_id', '2': 'orders.employee_id'}
      or alias-based names when table resolution is not available.
    """
    result: Dict[str, str] = {}

    pattern = re.compile(
        r'(?P<lhs>(?:[A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*)\s*'
        r'(=|>=|<=|>|<)\s*'
        r'\$(?P<param>\d+)(?:::[A-Za-z_][A-Za-z0-9_\[\]"]*(?:\s+[A-Za-z_][A-Za-z0-9_\[\]"]*)?)?',
        flags=re.IGNORECASE,
    )

    for match in pattern.finditer(query):
        lhs = match.group("lhs")
        param = match.group("param")
        result[param] = lhs

    return result


def get_postgres_parameter_types(query: str, connection: Any) -> List[str]:
    """
    Return PostgreSQL inferred parameter types for a SQL query.

    Works with psycopg2 / psycopg3 connections.

    Behavior:
    - If already inside a transaction, use a SAVEPOINT so errors don't poison
      the caller transaction.
    - If not inside a transaction (e.g. autocommit), don't use SAVEPOINT.
    - Always try to DEALLOCATE the prepared statement.
    """
    stmt_name = f"pgassistant_{uuid.uuid4().hex}"
    savepoint_name = f"sp_{uuid.uuid4().hex}"

    normalized_query = normalize_query_for_parameter_analysis(query)

    prepare_sql = f"PREPARE {stmt_name} AS {normalized_query}"
    deallocate_sql = f"DEALLOCATE {stmt_name}"
    savepoint_sql = f"SAVEPOINT {savepoint_name}"
    rollback_to_savepoint_sql = f"ROLLBACK TO SAVEPOINT {savepoint_name}"
    release_savepoint_sql = f"RELEASE SAVEPOINT {savepoint_name}"

    def _in_transaction(conn: Any) -> bool:
        info = getattr(conn, "info", None)
        if info is not None and hasattr(info, "transaction_status"):
            return info.transaction_status != 0

        status = getattr(conn, "status", None)
        return status == 2

    with connection.cursor() as cur:
        prepared = False
        use_savepoint = _in_transaction(connection)

        try:
            if use_savepoint:
                cur.execute(savepoint_sql)

            cur.execute(prepare_sql)
            prepared = True

            cur.execute(
                """
                SELECT parameter_types::text[]
                FROM pg_prepared_statements
                WHERE name = %s
                """,
                (stmt_name,),
            )
            row = cur.fetchone()
            param_types = row[0] if row and row[0] is not None else []

            cur.execute(deallocate_sql)
            prepared = False

            if use_savepoint:
                cur.execute(release_savepoint_sql)

            return list(param_types)

        except Exception as e:
            if prepared:
                try:
                    cur.execute(deallocate_sql)
                except Exception:
                    pass

            if use_savepoint:
                try:
                    cur.execute(rollback_to_savepoint_sql)
                    cur.execute(release_savepoint_sql)
                except Exception:
                    pass

            print(f"Warning: could not infer PostgreSQL parameter types with PREPARE: {e}")
            return []


def get_column_data_types(connection, table_column_pairs):
    """
    Query PostgreSQL to get the data types of specific columns in tables.

    Args:
        connection: psycopg connection object.
        table_column_pairs: A list of tuples (table_name, column_name).

    Returns:
        A dictionary { (schema.table_name, column_name): column_type }.
    """
    column_types = {}
    query = """
        SELECT
            table_schema,
            table_name,
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            (table_name, column_name) IN %s;
    """
    try:
        formatted_pairs = [(table, column) for table, column in table_column_pairs]
        if not formatted_pairs:
            print("No table-column pairs provided.")
            return {}

        with connection.cursor() as cursor:
            cursor.execute(query, (tuple(formatted_pairs),))
            for row in cursor.fetchall():
                schema_name = row[0]
                table_name = row[1]
                column_name = row[2]
                column_type = row[3]
                column_types[(f"{schema_name}.{table_name}", column_name)] = column_type

    except Exception as e:
        print(f"Error querying column data types: {e}")

    return column_types


def _resolve_param_column_to_table_and_column(
    raw_column_ref: str,
    query: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Try to resolve parser/fallback output to (table_name, column_name).

    Supported forms:
    - schema.table.column  -> table=table, column=column
    - table.column         -> table=table, column=column
    - alias.column         -> table=alias, column=column (best effort)
    - column only          -> (None, column)
    """
    if not raw_column_ref:
        return None, None

    parts = raw_column_ref.split(".")

    if len(parts) >= 3:
        return parts[-2], parts[-1]

    if len(parts) == 2:
        return parts[0], parts[1]

    return None, parts[0]


def map_query_parameters(query, connection):
    """
    Extract SQL parameters and retrieve their corresponding data types from PostgreSQL.

    Returns:
        A dictionary { parameter: (table_name, column_name, data_type) }.
    """
    normalized_query = normalize_query_for_parameter_analysis(query)

    try:
        param_columns = analyze_param.extract_parameter_columns(normalized_query)
    except Exception as e:
        print(f"Warning: parameter extraction failed: {e}")
        param_columns = fallback_extract_parameter_columns(normalized_query)

    if not param_columns:
        print("No SQL parameters found by SQL parser.")
        return {}

    table_column_pairs = []
    normalized_param_columns: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    for param, raw_column in param_columns.items():
        normalized_param = normalize_parameter_name(param)
        table_name, column_name = _resolve_param_column_to_table_and_column(raw_column, normalized_query)
        normalized_param_columns[normalized_param] = (table_name, column_name)

        if table_name and column_name:
            table_column_pairs.append((table_name, column_name))

    column_types = get_column_data_types(connection, table_column_pairs)

    param_mapping = {}
    for param, (table_name, column_name) in normalized_param_columns.items():
        if not column_name:
            param_mapping[param] = (None, None, None)
            continue

        matching_key = None
        if table_name:
            matching_key = next(
                (
                    key
                    for key in column_types.keys()
                    if key[1] == column_name and key[0].endswith(f".{table_name}")
                ),
                None,
            )
        else:
            matching_key = next(
                (key for key in column_types.keys() if key[1] == column_name),
                None,
            )

        if matching_key:
            column_type = column_types[matching_key]
            resolved_table_name = matching_key[0]
        else:
            column_type = None
            resolved_table_name = table_name

        param_mapping[param] = (resolved_table_name, column_name, column_type)

    return param_mapping


def split_query_by_parameters(query, parameters):
    """
    Split the query into smaller parts based on the presence of multiple parameters in a single line.
    """
    fragments = []
    if not parameters:
        return fragments

    for line in query.splitlines():
        parts = re.split(rf"({'|'.join(re.escape(p) for p in parameters)})", line)
        for part in parts:
            if part.strip():
                fragments.append(part.strip())
    return fragments


def merge_parameter_mappings(sql_query, parser_mapping, postgres_types):
    """
    Merge parser-based mapping with PostgreSQL inferred types.

    Rule:
    - PostgreSQL inferred type always wins when present.
    - Parser/catalog type is only a fallback.
    - Keeps parser-resolved table/column when available.
    - Parameters not resolved by parser are still returned.

    Output format:
        {
            '1': ('public.orders', 'customer_id', 'integer'),
            '2': ('UNKNOWN', 'UNKNOWN', 'bigint')
        }
    """
    result = {}

    ordered_params = extract_ordered_parameters(sql_query)

    normalized_parser_mapping = {}
    for param, value in (parser_mapping or {}).items():
        normalized_param = normalize_parameter_name(param)
        normalized_parser_mapping[normalized_param] = value

    for param, value in normalized_parser_mapping.items():
        result[param] = value

    for idx, param in enumerate(ordered_params):
        pg_type = postgres_types[idx] if idx < len(postgres_types) else None

        if param in result:
            table_name, column_name, parser_type = result[param]

            if pg_type and str(pg_type).upper() != "UNKNOWN":
                result[param] = (table_name, column_name, pg_type)
            else:
                result[param] = (table_name, column_name, parser_type or None)
        else:
            if pg_type and str(pg_type).upper() != "UNKNOWN":
                result[param] = (None, None, pg_type)
            else:
                result[param] = (None, None, None)

    return dict(sorted(result.items(), key=lambda x: int(x[0])))


def get_genius_parameters(sql_query, session):
    """
    Return parameter mapping enriched with PostgreSQL inferred parameter types.

    Output format:
        {
            '1': ('public.orders', 'customer_id', 'integer'),
            '2': ('public.orders', 'created_at', 'timestamp without time zone'),
            '3': (None, None, 'bigint')
        }
    """
    conn, msg = database.connectdb(session)
    if "OK" not in msg:
        return None

    normalized_query = normalize_query_for_parameter_analysis(sql_query)

    parser_mapping = {}
    postgres_types = []

    try:
        parser_mapping = map_query_parameters(normalized_query, conn) or {}
    except Exception as e:
        print(f"Warning in map_query_parameters: {e}")
        parser_mapping = {}

    try:
        postgres_types = get_postgres_parameter_types(normalized_query, conn) or []
    except Exception as e:
        print(f"Warning in get_postgres_parameter_types: {e}")
        postgres_types = []

    try:
        final_mapping = merge_parameter_mappings(
            sql_query=normalized_query,
            parser_mapping=parser_mapping,
            postgres_types=postgres_types,
        )
        return final_mapping if final_mapping else None
    except Exception as e:
        print(f"Error in get_genius_parameters: {e}")
        return parser_mapping or None


def analyze_explain_row(row):
    """
    Generate a comment based on the content of an EXPLAIN ANALYZE row.
    The input row is a dictionary with a 'QUERY PLAN' key.
    """
    query_plan = row.get("QUERY PLAN", "")

    if "Seq Scan" in query_plan:
        return "⚠️ Sequential scan detected. Consider adding an index."
    elif "Bitmap Heap Scan" in query_plan:
        return "🟡 Bitmap heap scan used. Consider an index scan if performance is slow."
    elif "Bitmap Index Scan" in query_plan:
        return "🟡 Bitmap index scan used. Works well if not scanning too many pages."
    elif "Index Scan" in query_plan:
        return "✅ Efficient index scan detected."
    elif "Index Only Scan" in query_plan:
        return "🚀 Very efficient index-only scan. No need to access the table directly."
    elif "Nested Loop" in query_plan:
        return "⚠️ Nested loop detected. Ensure indexes exist on join conditions."
    elif "Hash Join" in query_plan:
        return "🟢 Hash join used. Efficient for large datasets."
    elif "Merge Join" in query_plan:
        return "🟡 Merge join detected. Ensure both tables are sorted for efficiency."
    elif "Sort" in query_plan:
        return "⚠️ Sorting operation detected. Increase work_mem if sorting large datasets."
    elif "HashAggregate" in query_plan:
        return "⚠️ Hash aggregate used. May be slow if memory is insufficient."
    elif "Materialize" in query_plan:
        return "🟡 Materialize used. Can increase memory usage."
    elif "CTE Scan" in query_plan:
        return "⚠️ Common Table Expression (CTE) Scan. Consider inlining if performance is slow."
    elif "Gather" in query_plan:
        return "🔄 Parallel execution detected. Improves performance on large datasets."
    elif "Disk Spill" in query_plan:
        return "❌ Disk spill detected. Increase work_mem to avoid slow disk operations."
    elif "External Merge Disk" in query_plan:
        return "❌ External disk merge detected. PostgreSQL is using disk instead of memory."
    else:
        return ""