import re
import psycopg2
from datetime import datetime
from sql_metadata import Parser
from sql_formatter.core import format_sql
from . import database
from . import analyze_param
import json
import uuid
from typing import List, Any
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple




def get_tables(query):
    """
    Extracts table names from an SQL query.

    :param query: The SQL query as a string.
    :return: A list of table names used in the query. Returns an empty list if extraction fails.
    """    
    try:
        tables = Parser(query.lower()).tables
        return tables
    except:
        return []

def get_sql_type(sql_query):
    """
    which query type  : SELECT, INSERT, UPDATE, DELETE, etc.
    
    :param sql_query: the SQL query.
    :return: query type (str).
    """
    try:
        parser = Parser(sql_query)
        sqltype = parser.query_type.replace ('QueryType.', '')
        return sqltype.lower()
    except:
        return "unknown"

def get_formated_sql(sql_query):
    """
    Formats an SQL query for better readability.

    :param sql_query: The raw SQL query as a string.
    :return: The formatted SQL query if formatting succeeds, otherwise returns the original query.
    """    
    try:
        sqlf = format_sql(sql_query)
        return sqlf
    except:
        return sql_query
    

def replace_query_parameters(query, params):
    """
    Replace parameters ($1, $2, etc.) in an SQL string with their provided values.

    The values in `params` are assumed to be already SQL-formatted
    (including quotes, NULL, etc.). This function does not modify or escape them.
    """
    def replace_match(match):
        param_index = int(match.group(1))

        # If the parameter is not provided, keep the original $n placeholder
        if param_index not in params:
            return match.group(0)

        value = params[param_index]
        return '' if value is None else str(value)

    # Replace all occurrences of $<number>
    return re.sub(r"\$(\d+)", replace_match, query)

def parse_most_common_vals(value):
    """Parse PostgreSQL's most_common_vals field into a Python list."""
    
    if not value or value == "{}":
        return []  # returns an empty list if NULL or empty

        # Check if the value is indeed a string
    if isinstance(value, tuple):  
        value = value[0]  # Extract the first valute

    if value is None:
        return []

    # Remove the surrounding braces `{}` from the string
    value = value.strip("{}")

    # regular expression to capture:
    # - Values in quotes (e.g., "John Doe", "2024-12-23 08:31:35.616712")
    # - Unquoted values (e.g., F, M, 2020-07
    pattern = r'"([^"]+)"|([^,]+)'

    matches = re.findall(pattern, value)
    
    parsed_values = []
    for match in matches:
        raw_value = match[0] if match[0] else match[1]  # priority to quoted value

        # try to parse as timestamp or date
        try:
            parsed_value = str(datetime.strptime(raw_value, "%Y-%m-%d %H:%M:%S.%f"))  # Format timestamp
        except ValueError:
            try:
                parsed_value = str(datetime.strptime(raw_value, "%Y-%m-%d"))  # Format date 
            except ValueError:
                # try to parse as integer
                try:
                    parsed_value = int(raw_value)
                except ValueError:
                    parsed_value = raw_value  # keep as string if all else fails
        
        parsed_values.append(parsed_value)

    return parsed_values

def extract_schema_table(full_name):
    """Split vars 'schema.table' or 'table'."""
    parts = full_name.split('.')
    if len(parts) == 2:
        schema, table = parts
    else:
        schema, table = None, parts[0]  # No schema=None
    return schema, table

def fetch_column_data(table, column, data_type, session):
    """
    Fetch up to 10 rows from a specific column of a table and return the result as a typed JSON array.

    Args:
        table (str): The name of the table.
        column (str): The name of the column.
        data_type (str): The PostgreSQL data type of the column.
        connection_params (dict): Connection parameters for psycopg2.

    Returns:
        list: A JSON array of the results with correctly typed data.
    """
    try:
        conn, msg = database.connectdb(session)
        if "OK" in msg:
        # Connect to the database
            with conn.cursor() as cursor:

                # Try to select pg_stats most_columns_vals
                schema, tablename = extract_schema_table(table)
                if schema is None:
                    query = f"select most_common_vals from pg_stats where tablename='{table}' and attname='{column}' limit 1"
                else:
                    query = f"select most_common_vals from pg_stats where schemaname='{schema}' and tablename='{tablename}' and attname='{column}' limit 1"

                cursor.execute (query)
                row = cursor.fetchall()
               

                if row and row[0]:  # check null values
                    values_common=parse_most_common_vals(row[0])
                    if len(values_common)>0:
                        return values_common

                # Generate the SQL query
                query = f"SELECT {column} FROM {table} LIMIT 10;"
                              
                # Execute the query
                cursor.execute(query)
                rows = cursor.fetchall()
                
                # Map PostgreSQL types to Python types
                def convert_value(value):
                    if value is None:
                        return None
                    if data_type in ('integer', 'bigint', 'smallint'):
                        return int(value)
                    elif data_type in ('real', 'double precision', 'numeric'):
                        return float(value)
                    elif data_type in ('boolean',):
                        return bool(value)
                    else:
                        return str(value)  # Default: Treat as string

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

    prepare_sql = f"PREPARE {stmt_name} AS {query}"
    deallocate_sql = f"DEALLOCATE {stmt_name}"
    savepoint_sql = f"SAVEPOINT {savepoint_name}"
    rollback_to_savepoint_sql = f"ROLLBACK TO SAVEPOINT {savepoint_name}"
    release_savepoint_sql = f"RELEASE SAVEPOINT {savepoint_name}"

    def _in_transaction(conn: Any) -> bool:
        """
        Best-effort detection for psycopg2 / psycopg3.
        """
        # psycopg3
        info = getattr(conn, "info", None)
        if info is not None and hasattr(info, "transaction_status"):
            # 0 = idle / no transaction
            return info.transaction_status != 0

        # psycopg2
        status = getattr(conn, "status", None)
        # STATUS_IN_TRANSACTION is usually 2 in psycopg2
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

def extract_ordered_parameters(query: str) -> List[str]:
    """
    Return parameters found in query ordered by parameter number.
    Example: ['$1', '$2', '$4']
    """
    params = sorted(
        {int(x) for x in re.findall(r"\$(\d+)", query)}
    )
    return [f"${p}" for p in params]


def get_column_data_types(connection, table_column_pairs):
    """
    Query PostgreSQL to get the data types of specific columns in tables.

    Args:
        connection: psycopg2 connection object.
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


def map_query_parameters(query, connection):
    """
    Extract SQL parameters and retrieve their corresponding data types from PostgreSQL.

    Args:
        query: The SQL query string.
        connection: psycopg2 connection object.

    Returns:
        A dictionary { parameter: (table_name, column_name, data_type) }.
    """
    param_columns = analyze_param.extract_parameter_columns(query)

    if not param_columns:
        print("No SQL parameters found by SQL parser.")
        return {}

    table_column_pairs = []
    for table_column in param_columns.values():
        parts = table_column.split(".")
        if len(parts) >= 2:
            table_name = parts[0]
            column_name = parts[1]
            table_column_pairs.append((table_name, column_name))

    column_types = get_column_data_types(connection, table_column_pairs)

    param_mapping = {}
    for param, column in param_columns.items():
        parts = column.split(".")
        if len(parts) < 2:
            param_mapping[param] = ("UNKNOWN", "UNKNOWN", "UNKNOWN")
            continue

        table_name, column_name = parts[0], parts[1]
        column_key = (table_name, column_name)

        matching_key = next(
            (
                key
                for key in column_types.keys()
                if key[1] == column_name and key[0].endswith(f".{table_name}")
            ),
            None,
        )

        if matching_key:
            column_type = column_types[matching_key]
            resolved_table_name = matching_key[0]
        else:
            column_type = "UNKNOWN"
            resolved_table_name = table_name
            print(f"⚠️ Warning: Column {column_key} not found in column_types. Returning UNKNOWN.")

        param_mapping[param] = (resolved_table_name, column_name, column_type)

    return param_mapping


def split_query_by_parameters(query, parameters):
    """
    Split the query into smaller parts based on the presence of multiple parameters in a single line.

    Args:
        query: The SQL query string.
        parameters: List of parameters ($1, $2, etc.).

    Returns:
        A list of query fragments, each containing at most one parameter.
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

    # Normalize parser mapping keys to '1', '2', ...
    normalized_parser_mapping = {}
    for param, value in (parser_mapping or {}).items():
        normalized_param = normalize_parameter_name(param)
        normalized_parser_mapping[normalized_param] = value

    # Start from parser mapping so we keep table/column info
    for param, value in normalized_parser_mapping.items():
        result[param] = value

    # PostgreSQL inferred types have priority
    for idx, param in enumerate(ordered_params):
        pg_type = postgres_types[idx] if idx < len(postgres_types) else None

        if param in result:
            table_name, column_name, parser_type = result[param]

            if pg_type and str(pg_type).upper() != "UNKNOWN":
                # PostgreSQL is the source of truth
                result[param] = (table_name, column_name, pg_type)
            else:
                # Fallback to parser/catalog type
                result[param] = (table_name, column_name, parser_type or None)
        else:
            # Parameter not found by parser
            if pg_type and str(pg_type).upper() != "UNKNOWN":
                result[param] = (None, None, pg_type)
            else:
                result[param] = (None, None, None)

    return dict(sorted(result.items(), key=lambda x: int(x[0])))
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

def extract_ordered_parameters(query):
    """
    Return parameters found in query ordered numerically.

    Example:
        SELECT * WHERE a=$1 AND b=$3

    returns:
        ['1','3']
    """
    params = sorted({int(x) for x in re.findall(r"\$(\d+)", query)})
    return [str(p) for p in params]

def get_genius_parameters(sql_query, session):

    """
    Return parameter mapping enriched with PostgreSQL inferred parameter types.

    Output format:
        {
            '$1': ('public.orders', 'customer_id', 'integer'),
            '$2': ('public.orders', 'created_at', 'timestamp without time zone'),
            '$3': ('UNKNOWN', 'UNKNOWN', 'bigint')
        }
    """
    conn, msg = database.connectdb(session)
    if "OK" not in msg:
        return None

    try:
        parser_mapping = map_query_parameters(sql_query, conn)
        postgres_types = get_postgres_parameter_types(sql_query, conn)

        final_mapping = merge_parameter_mappings(
            sql_query=sql_query,
            parser_mapping=parser_mapping or {},
            postgres_types=postgres_types or [],
        )

        return final_mapping

    except Exception as e:
        print(f"Error in get_genius_parameters: {e}")
        # fallback to current behavior
        try:
            return map_query_parameters(sql_query, conn)
        except Exception:
            return None
        
def analyze_explain_row(row):
    """
    Generates a comment based on the content of an EXPLAIN ANALYZE row.
    The input row is a dictionary with a 'QUERY PLAN' key.
    """
    query_plan = row.get('QUERY PLAN', '')

    if 'Seq Scan' in query_plan:
        return "⚠️ Sequential scan detected. Consider adding an index."
    elif 'Bitmap Heap Scan' in query_plan:
        return "🟡 Bitmap heap scan used. Consider an index scan if performance is slow."
    elif 'Bitmap Index Scan' in query_plan:
        return "🟡 Bitmap index scan used. Works well if not scanning too many pages."    
    elif 'Index Scan' in query_plan:
        return "✅ Efficient index scan detected."
    elif 'Index Only Scan' in query_plan:
        return "🚀 Very efficient index-only scan. No need to access the table directly."
    elif 'Nested Loop' in query_plan:
        return "⚠️ Nested loop detected. Ensure indexes exist on join conditions."
    elif 'Hash Join' in query_plan:
        return "🟢 Hash join used. Efficient for large datasets."
    elif 'Merge Join' in query_plan:
        return "🟡 Merge join detected. Ensure both tables are sorted for efficiency."
    elif 'Sort' in query_plan:
        return "⚠️ Sorting operation detected. Increase work_mem if sorting large datasets."
    elif 'HashAggregate' in query_plan:
        return "⚠️ Hash aggregate used. May be slow if memory is insufficient."
    elif 'Materialize' in query_plan:
        return "🟡 Materialize used. Can increase memory usage."
    elif 'CTE Scan' in query_plan:
        return "⚠️ Common Table Expression (CTE) Scan. Consider inlining if performance is slow."
    elif 'Gather' in query_plan:
        return "🔄 Parallel execution detected. Improves performance on large datasets."
    elif 'Disk Spill' in query_plan:
        return "❌ Disk spill detected. Increase work_mem to avoid slow disk operations."
    elif 'External Merge Disk' in query_plan:
        return "❌ External disk merge detected. PostgreSQL is using disk instead of memory."
    else:
        return ""