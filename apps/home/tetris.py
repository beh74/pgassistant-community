import re
from typing import List


USEFUL_STATEMENT_PREFIXES = (
    "COMMENT ON TABLE",
    "COMMENT ON COLUMN",
    "ALTER TABLE",
    "CREATE INDEX",
    "CREATE UNIQUE INDEX",
)


def split_sql_statements(sql: str) -> List[str]:
    """
    Split SQL script into statements terminated by ';'
    while ignoring semicolons inside single-quoted strings
    and dollar-quoted blocks.

    Good enough for pg_dump-style DDL.
    """
    statements = []
    current = []
    i = 0
    n = len(sql)

    in_single_quote = False
    dollar_tag = None

    while i < n:
        ch = sql[i]

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, i):
                current.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(ch)
            i += 1
            continue

        if in_single_quote:
            current.append(ch)
            if ch == "'":
                if i + 1 < n and sql[i + 1] == "'":
                    current.append("'")
                    i += 2
                    continue
                in_single_quote = False
            i += 1
            continue

        if ch == "'":
            in_single_quote = True
            current.append(ch)
            i += 1
            continue

        if ch == "$":
            m = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
            if m:
                dollar_tag = m.group(0)
                current.append(dollar_tag)
                i += len(dollar_tag)
                continue

        if ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt + ";")
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)

    return statements


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def rename_fq_table_refs(sql: str, schema: str, table: str, target_table: str) -> str:
    """
    Rename references to schema.table and quoted variants.
    """
    patterns = [
        (rf'\b{re.escape(schema)}\.{re.escape(table)}\b', f"{schema}.{target_table}"),
        (
            rf'"{re.escape(schema)}"\."{re.escape(table)}"',
            f'{quote_ident(schema)}.{quote_ident(target_table)}'
        ),
    ]

    for pattern, repl in patterns:
        sql = re.sub(pattern, repl, sql)

    return sql


def rename_sequence_names(sql: str, schema: str, table: str, target_table: str) -> str:
    """
    Rename obvious sequence names containing the table name.
    Example:
      bookings.flights_flight_id_seq -> bookings.flights_tetris_flight_id_seq
    """
    sql = re.sub(
        rf'\b{re.escape(schema)}\.{re.escape(table)}_([A-Za-z0-9_]+)_seq\b',
        rf'{schema}.{target_table}_\1_seq',
        sql,
    )
    sql = re.sub(
        rf'"{re.escape(schema)}"\."{re.escape(table)}_([A-Za-z0-9_]+)_seq"',
        lambda m: f'{quote_ident(schema)}.{quote_ident(target_table + "_" + m.group(1) + "_seq")}',
        sql,
    )
    return sql


def rename_constraint_names(sql: str, table: str, target_table: str) -> str:
    """
    Rename explicit constraint names that start with the source table name.
    Examples:
      flights_pkey -> flights_tetris_pkey
      flights_route_no_key -> flights_tetris_route_no_key
    """
    def repl(match: re.Match) -> str:
        old_name = match.group(1)
        new_name = old_name.replace(table, target_table, 1)
        return f"CONSTRAINT {new_name}"

    sql = re.sub(
        rf'\bCONSTRAINT\s+({re.escape(table)}[A-Za-z0-9_]*)\b',
        repl,
        sql,
        flags=re.IGNORECASE,
    )

    sql = re.sub(
        rf'\bADD\s+CONSTRAINT\s+({re.escape(table)}[A-Za-z0-9_]*)\b',
        lambda m: f"ADD CONSTRAINT {m.group(1).replace(table, target_table, 1)}",
        sql,
        flags=re.IGNORECASE,
    )

    return sql


def rename_index_names(sql: str, table: str, target_table: str) -> str:
    """
    Rename obvious index names that start with the source table name.
    """
    def repl(match: re.Match) -> str:
        old_name = match.group(2)
        new_name = old_name.replace(table, target_table, 1)
        return f"{match.group(1)} {new_name}"

    sql = re.sub(
        rf'\b(CREATE\s+(?:UNIQUE\s+)?INDEX)\s+({re.escape(table)}[A-Za-z0-9_]*)\b',
        repl,
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def is_create_table_for_source(stmt: str, schema: str, table: str) -> bool:
    patterns = [
        rf'^\s*CREATE\s+TABLE\s+{re.escape(schema)}\.{re.escape(table)}\b',
        rf'^\s*CREATE\s+TABLE\s+"{re.escape(schema)}"\."{re.escape(table)}"\b',
    ]
    return any(re.search(p, stmt, flags=re.IGNORECASE | re.DOTALL) for p in patterns)


def is_useful_post_create_statement(stmt: str, schema: str, table: str) -> bool:
    normalized = " ".join(stmt.strip().split()).upper()

    if not normalized.startswith(USEFUL_STATEMENT_PREFIXES):
        return False

    table_refs = [
        f"{schema}.{table}",
        f'"{schema}"."{table}"',
    ]

    if not any(ref in stmt for ref in table_refs):
        return False

    if re.search(r'\bOWNER\s+TO\b', stmt, flags=re.IGNORECASE):
        return False
    if re.search(r'\bGRANT\b|\bREVOKE\b', stmt, flags=re.IGNORECASE):
        return False
    if re.search(r'\bSET\b', stmt, flags=re.IGNORECASE):
        return False
    if re.search(r'\bsetval\s*\(', stmt, flags=re.IGNORECASE):
        return False

    return True


def extract_post_create_ddl(
    pg_dump_sql: str,
    schema: str,
    table: str,
    suffix: str = "_tetris",
) -> str:
    """
    Extract statements after CREATE TABLE that are useful to recreate
    comments, constraints, identity, and indexes for the tetris table.
    """
    target_table = f"{table}{suffix}"
    statements = split_sql_statements(pg_dump_sql)

    found_create = False
    kept = []

    for stmt in statements:
        if not found_create:
            if is_create_table_for_source(stmt, schema, table):
                found_create = True
            continue

        if not is_useful_post_create_statement(stmt, schema, table):
            continue

        rewritten = stmt
        rewritten = rename_fq_table_refs(rewritten, schema, table, target_table)
        rewritten = rename_sequence_names(rewritten, schema, table, target_table)
        rewritten = rename_constraint_names(rewritten, table, target_table)
        rewritten = rename_index_names(rewritten, table, target_table)

        kept.append(rewritten.strip())

    return "\n".join(kept)
