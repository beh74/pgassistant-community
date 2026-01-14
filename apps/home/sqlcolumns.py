from sql_metadata import Parser

def extract_where_columns(sql_query, table_name):
    """
    Analyzes an SQL query and extracts the columns of the specified table
    that are used in the WHERE clause.
    Uses sql-metadata to identify the columns present in the WHERE clause.

    :param sql_query: The SQL query to analyze.
    :param table_name: The name of the target table (e.g. 'authors').
    :return: List of columns from the table used in the WHERE clause.
    """
    # Parse the SQL query
    parser = Parser(sql_query)

    # Extract columns used in the WHERE clause
    where_columns = parser.columns_dict.get("where", [])

    # Keep only columns from the target table
    filtered_columns = []
    for col in where_columns:
        # Check if the column is in the form "table.column"
        if "." in col:
            table, column = col.split(".", 1)
            if table == table_name:
                filtered_columns.append(col)  # Keep the table.column format
        else:
            # Case where the column is used without an explicit alias
            filtered_columns.append(f"{table_name}.{col}")

    return list(set(filtered_columns))  # Remove duplicates