import subprocess
import re
from pygments import highlight
from pygments.lexers import PostgresLexer
from pygments.formatters import HtmlFormatter
from typing import Iterable, Optional

def remove_restrict_lines(html):
    return "\n".join(
        line for line in html.splitlines()
        if not line.strip().startswith("\\restrict") and not line.strip().startswith("\\unrestrict")
    )

def sql_to_html(sql_query):
    """
    Convert a SQL query into syntax-highlighted HTML.

    Args:
        sql_query (str): The SQL query to format.

    Returns:
        str: HTML code with syntax highlighting.
    """
    # Use the SQL lexer and an HTML formatter
    #formatter = HtmlFormatter(style="colorful", full=True, linenos=False)
    sql_query = remove_pg_catalog_lines(sql_query)
    formatter = HtmlFormatter(style="colorful", full=True, linenos=False, cssclass="sql-highlight")
    highlighted_sql = highlight(sql_query, PostgresLexer(), formatter)
    return remove_restrict_lines(highlighted_sql)

def remove_pg_catalog_lines(sql_script):
    """
    Remove all lines starting with 'SELECT pg_catalog.' from the SQL script.

    Args:
        sql_script (str): The input SQL script.

    Returns:
        str: The cleaned SQL script.
    """
    # Use a regular expression to remove lines starting with 'SELECT pg_catalog.'
    cleaned_script = re.sub(r'^SELECT pg_catalog\..*$', '', sql_script, flags=re.MULTILINE)
    return cleaned_script.strip()

def quote_pg_identifier(identifier: str) -> str:
    """
    Quote a PostgreSQL identifier if needed, and escape embedded double quotes.

    Examples:
        public          -> public
        MyTable         -> "MyTable"
        my table        -> "my table"
        weird"name      -> "weird""name"
    """
    if identifier is None:
        raise ValueError("Identifier cannot be None")

    escaped = identifier.replace('"', '""')

    # PostgreSQL unquoted identifiers are safe only if they match this style
    # and are effectively lowercase/simple.
    if escaped.isidentifier() and escaped == escaped.lower():
        return escaped

    return f'"{escaped}"'


def quote_table_for_pg_dump(table_name: str) -> str:
    """
    Quote a table name for pg_dump --table.

    Supports:
      - table
      - schema.table

    If schema/table contains special chars, each part is quoted separately.
    """
    if not table_name or not table_name.strip():
        raise ValueError("Table name cannot be empty")

    table_name = table_name.strip()

    if "." in table_name:
        schema, table = table_name.split(".", 1)
        return f"{quote_pg_identifier(schema)}.{quote_pg_identifier(table)}"

    return quote_pg_identifier(table_name)

def generate_tables_ddl(host, port, database, user, password, tables) -> Optional[str]:
    """
    Generate a cleaned DDL for the given tables using pg_dump, excluding:
    - comments
    - SET statements
    - GRANT/REVOKE statements
    - pg_catalog references
    - ALTER SEQUENCE ... OWNER TO
    - ALTER SEQUENCE ... OWNED BY

    Handles schema/table names requiring PostgreSQL identifier quoting.
    """
    try:
        if not tables:
            return ""

        quoted_tables = [quote_table_for_pg_dump(table) for table in tables]

        pg_dump_cmd = [
            "pg_dump",
            "-h", str(host),
            "-p", str(port),
            "-U", str(user),
            "-d", str(database),
            "--schema-only",
            "--format", "plain",
        ]

        for table in quoted_tables:
            pg_dump_cmd.extend(["--table", table])

        env = dict()
        env.update({"PGPASSWORD": str(password)})

        result = subprocess.run(
            pg_dump_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=env,
        )

        ddl_s = result.stdout

        filtered_lines = []
        for line in ddl_s.splitlines():
            stripped = line.strip()

            if stripped.startswith("--"):
                continue
            if stripped.startswith("SET"):
                continue
            if stripped.startswith("GRANT"):
                continue
            if stripped.startswith("REVOKE"):
                continue
            if stripped.startswith("\\restrict"):
                continue
            if stripped.startswith("\\unrestrict"):
                continue
            if "pg_catalog" in line:
                continue
            if stripped.startswith("ALTER SEQUENCE") and " OWNER TO" in stripped:
                continue
            if stripped.startswith("ALTER TABLE") and " OWNER TO" in stripped:
                continue
            if stripped.startswith("ALTER SEQUENCE") and " OWNED BY" in stripped:
                continue

            filtered_lines.append(line)

        ddl_cleaned = "\n".join(filtered_lines)

        while "\n\n" in ddl_cleaned:
            ddl_cleaned = ddl_cleaned.replace("\n\n", "\n")

        return ddl_cleaned.strip()

    except subprocess.CalledProcessError as e:
        print(f"Error generating DDL: {e.stderr or e}")
        return None
    """
    Generate a cleaned DDL for the given tables using pg_dump, excluding:
    - comments
    - SET statements
    - GRANT/REVOKE statements
    - pg_catalog references
    - ALTER SEQUENCE ... OWNER TO
    - ALTER SEQUENCE ... OWNED BY
    """    
    try:
        # Build the `--table` arguments for pg_dump
        table_args = " ".join([f"--table {table}" for table in tables])

        # Combine the command with piping and filtering
        command = f"""
        PGPASSWORD="{password}" pg_dump -h {host} -p {port} -U {user} -d {database} --schema-only {table_args} --format plain |
        sed -e '/^--/d' \
            -e '/^SET/d' \
            -e '/^GRANT/d' \
            -e '/^REVOKE/d' \
            -e '/pg_catalog/d' \
            -e '/ALTER SEQUENCE .* OWNER TO/d' \
            -e '/ALTER TABLE .* OWNER TO/d' \
            -e '/ALTER SEQUENCE .* OWNED BY/d'
        """

        # Run the command in a shell
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            text=True,
            shell=True,
            check=True
        )

        ddl_s = result.stdout

        ddl_cleaned = "\n".join(
            line for line in ddl_s.splitlines()
            if not line.strip().startswith("\\restrict") and not line.strip().startswith("\\unrestrict")
        )

        while '\n\n' in ddl_cleaned:
            ddl_cleaned = ddl_cleaned.replace('\n\n', '\n')

        return ddl_cleaned

    except subprocess.CalledProcessError as e:
        print(f"Error generating DDL: {e}")
        return None
