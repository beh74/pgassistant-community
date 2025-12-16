#!/usr/bin/env python3
"""
Helper script to manage PostgreSQL tables:
- target_database (upsert by unique_name)
- action_run (insert linked to target_database)
"""
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
import datetime
from typing import List, Tuple
from . import database


# --- DDL statements ---------------------------------------------------------

DDL_TARGET_DATABASE = """
CREATE TABLE IF NOT EXISTS target_database (
  id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  unique_name   VARCHAR(255) NOT NULL,
  host          VARCHAR(255) NOT NULL,
  dbname        VARCHAR(63)  NOT NULL,
  port          INTEGER      NOT NULL DEFAULT 5432 CHECK (port BETWEEN 1 AND 65535),
  username      VARCHAR(63)  NOT NULL,
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  CONSTRAINT target_database_unique_name_key UNIQUE (unique_name)
);
"""

DDL_ACTION_RUN = """
CREATE TABLE IF NOT EXISTS action_run (
  id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  target_database_id  BIGINT NOT NULL REFERENCES target_database(id) ON DELETE RESTRICT,
  started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at         TIMESTAMPTZ,
  success             BOOLEAN NOT NULL DEFAULT FALSE,
  executed_sql        TEXT NOT NULL,
  issue_type          VARCHAR(128) NOT NULL,
  dry_mode            BOOLEAN NOT NULL DEFAULT TRUE
);
"""

def merge_unique(list1: List[str], list2: List[str]) -> List[str]:
    """
    Merge two lists of strings and return unique values,
    preserving order.
    """
    merged = list1 + list2
    return list(dict.fromkeys(merged))

# --- Connection helpers -----------------------------------------------------

def get_connection(dsn: str):
    """
    Open a PostgreSQL connection from a DSN, e.g.:
        postgresql://user:password@host:port/dbname
    """
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)


def init_schema(conn) -> None:
    """
    Create tables target_database and action_run if they do not exist.
    """
    with conn.cursor() as cur:
        cur.execute(DDL_TARGET_DATABASE)
        cur.execute(DDL_ACTION_RUN)
    conn.commit()


# --- CRUD operations --------------------------------------------------------

def upsert_target_database(
    conn,
    *,
    unique_name: str,
    host: str,
    dbname: str,
    port: int,
    username: str,
) -> int:
    """
    Insert or update target_database based on unique_name.
    Returns the target_database.id (BIGINT).
    """
    sql = """
    INSERT INTO target_database (unique_name, host, dbname, port, username)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (unique_name) DO UPDATE
      SET host     = EXCLUDED.host,
          dbname   = EXCLUDED.dbname,
          port     = EXCLUDED.port,
          username = EXCLUDED.username
    RETURNING id;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (unique_name, host, dbname, port, username))
        row = cur.fetchone()
    conn.commit()
    return row["id"]


def insert_action_run(
    conn,
    *,
    target_database_id: int,
    executed_sql: str,
    issue_type: str,
    success: bool = False,
    dry_mode: bool = True,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
) -> int:
    """
    Insert an entry into action_run, optionally providing custom started_at and finished_at.
    """

    if started_at is None and finished_at is None:
        sql = """
        INSERT INTO action_run (
            target_database_id, executed_sql, issue_type, success, dry_mode
        )
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id;
        """
        params = (target_database_id, executed_sql, issue_type, success, dry_mode)

    elif started_at is not None and finished_at is None:
        sql = """
        INSERT INTO action_run (
            target_database_id, executed_sql, issue_type, success, dry_mode, started_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        params = (target_database_id, executed_sql, issue_type, success, dry_mode, started_at)

    elif started_at is None and finished_at is not None:
        sql = """
        INSERT INTO action_run (
            target_database_id, executed_sql, issue_type, success, dry_mode, finished_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        params = (target_database_id, executed_sql, issue_type, success, dry_mode, finished_at)

    else:
        sql = """
        INSERT INTO action_run (
            target_database_id, executed_sql, issue_type, success, dry_mode, started_at, finished_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """
        params = (target_database_id, executed_sql, issue_type, success, dry_mode, started_at, finished_at)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    conn.commit()
    return row["id"]


def upsert_target_and_add_action(
    conn,
    *,
    unique_name: str,
    host: str,
    dbname: str,
    port: int,
    username: str,
    executed_sql: str,
    issue_type: str,
    success: bool = False,
    dry_mode: bool = True,
    finished_at: Optional[str] = None,
) -> tuple[int, int]:
    """
    Helper that:
      1. Upserts into target_database by unique_name
      2. Inserts a new action_run linked to the target row

    Returns (target_database_id, action_run_id)
    """
    target_id = upsert_target_database(
        conn,
        unique_name=unique_name,
        host=host,
        dbname=dbname,
        port=port,
        username=username,
    )

    action_id = insert_action_run(
        conn,
        target_database_id=target_id,
        executed_sql=executed_sql,
        issue_type=issue_type,
        success=success,
        dry_mode=dry_mode,
        finished_at=finished_at,
    )

    return target_id, action_id


def action_analyze_table(dbcon, pgassistant_con, target_database_id, table_list = [], dry_mode: bool = True) -> Tuple[List[str], List[str]]:
    """
    Analyze the given list of tables.
    Returns a list of unique 'schema.table' names that were analyzed,
    so that the caller can later run ANALYZE on them.
    """
    unique_tables = set()  # will store (schemaname, tablename) tuples

    executed_queries = []

    try:
        for qualified_table in table_list:     
            sql = f'ANALYZE "{qualified_table}";'   

            # Measure start time
            started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            success = False

            if not dry_mode:
                try:
                    database.db_exec(dbcon, sql)
                    executed_queries.append(sql)
                    success = True
                except Exception as e:
                    print(f"action_analyze_table - Error analyzing table {qualified_table}: {e}")
            else:
                print(f"DRY MODE - Skipping execution of: {sql}")
                executed_queries.append(sql)
                # In dry mode we consider it as "success" for logging purposes
                success = True

            # Measure end time
            finished_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Log the action run
            action_id = insert_action_run(
                pgassistant_con,
                target_database_id=target_database_id,
                executed_sql=sql,
                issue_type="analyze table",
                success=success,
                dry_mode=dry_mode,
                started_at=started_at,
                finished_at=finished_at,
            )
            print (f"action_analyze_table - Logged action_run.id={action_id} for analyzing table {qualified_table}")

    except Exception as e:
        print(f"action_analyze_table - Error executing analyze on tables: {e}")
        return [], []

    # Return a sorted list of 'schema.table' strings (unique)
    
    return table_list, executed_queries


def action_alter_column_datatype_fk(dbcon, pgassistant_con, target_database_id, dry_mode: bool = True) -> Tuple[List[str], List[str]]:
    """
    Alter column datatype for foreign key constraints when necessary.
    Returns a list of unique 'schema.table' names that were inspected / targeted,
    so that the caller can later run ANALYZE on them.
    """
    query_id = "issue_idx_fk_datatype"
    unique_tables = set()  # will store (schemaname, tablename) tuples

    executed_queries = []

    try:
        rows, _ = database.db_query(dbcon, query_id)

        for row in rows:
            sql = row["pga_suggestion"]
            schemaname = row["schemaname"]
            tablename = row["foreign_key_table"]

            # Track the table for later ANALYZE
            unique_tables.add((schemaname, tablename))

            # Measure start time
            started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            success = False

            if not dry_mode:
                try:
                    database.db_exec(dbcon, sql)
                    executed_queries.append(sql)
                    success = True
                except Exception as e:
                    print(f"action_alter_column_datatype_fk - Error altering column datatype on {schemaname}.{tablename}: {e}")
            else:
                print(f"DRY MODE - Skipping execution of: {sql}")
                executed_queries.append(sql)
                # In dry mode we consider it as "success" for logging purposes
                success = True

            # Measure end time
            finished_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Log the action run
            action_id = insert_action_run(
                pgassistant_con,
                target_database_id=target_database_id,
                executed_sql=sql,
                issue_type="datatype on foreign keys are different",
                success=success,
                dry_mode=dry_mode,
                started_at=started_at,
                finished_at=finished_at,
            )
            print (f"action_alter_column_datatype_fk - Logged action_run.id={action_id} for altering column datatype on {schemaname}.{tablename}")

    except Exception as e:
        print(f"action_alter_column_datatype_fk - Error executing query_id {query_id}: {e}")
        return [], []

    # Return a sorted list of 'schema.table' strings (unique)
    qualified_tables = [f"{schema}.{table}" for schema, table in sorted(unique_tables)]
    return qualified_tables, executed_queries

def action_remove_dup_indexes(dbcon, pgassistant_con, target_database_id, dry_mode: bool = True) -> Tuple[List[str], List[str]]:
    """
    Remove strict duplicate indexes.
    Returns a list of unique 'schema.table' names that were inspected / targeted,
    so that the caller can later run ANALYZE on them.
    """
    query_id = "action_idx_duplicate"
    unique_tables = set()  # will store (schemaname, tablename) tuples

    executed_queries = []

    try:
        rows, _ = database.db_query(dbcon, query_id)

        for row in rows:
            sql = row["pga_action"]
            schemaname = row["schemaname"]
            tablename = row["table_name"]

            # Track the table for later ANALYZE
            unique_tables.add((schemaname, tablename))

            # Measure start time
            started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            success = False

            if not dry_mode:
                try:
                    database.db_exec(dbcon, sql)
                    executed_queries.append(sql)
                    success = True
                except Exception as e:
                    print(f"action_remove_dup_indexes - Error creating index on {schemaname}.{tablename}: {e}")
            else:
                print(f"DRY MODE - Skipping execution of: {sql}")
                executed_queries.append(sql)
                # In dry mode we consider it as "success" for logging purposes
                success = True

            # Measure end time
            finished_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Log the action run
            action_id = insert_action_run(
                pgassistant_con,
                target_database_id=target_database_id,
                executed_sql=sql,
                issue_type="remove duplicate indexes",
                success=success,
                dry_mode=dry_mode,
                started_at=started_at,
                finished_at=finished_at,
            )
            print (f"action_remove_dup_indexes - Logged action_run.id={action_id} for removing duplicate indexes on {schemaname}.{tablename}")

    except Exception as e:
        print(f"action_remove_dup_indexes - Error executing query_id {query_id}: {e}")
        return [], []

    # Return a sorted list of 'schema.table' strings (unique)
    qualified_tables = [f"{schema}.{table}" for schema, table in sorted(unique_tables)]
    return qualified_tables, executed_queries

def action_create_fk(dbcon, pgassistant_con, target_database_id, dry_mode: bool = True) -> Tuple[List[str], List[str]]:
    """
    Create missing indexes on foreign keys only if referenced table is heavy enough.
    Returns a list of unique 'schema.table' names that were inspected / targeted,
    so that the caller can later run ANALYZE on them.
    """
    query_id = "action_idx_fk_missing"
    unique_tables = set()  # will store (schemaname, tablename) tuples

    executed_queries = []

    try:
        rows, _ = database.db_query(dbcon, query_id)

        for row in rows:
            sql = row["pga_action"]
            schemaname = row["schemaname"]
            tablename = row["tablename"]

            # Track the table for later ANALYZE
            unique_tables.add((schemaname, tablename))

            # Measure start time
            started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            success = False

            if not dry_mode:
                try:
                    database.db_exec(dbcon, sql)
                    executed_queries.append(sql)
                    success = True
                except Exception as e:
                    print(f"action_create_fk - Error creating index on {schemaname}.{tablename}: {e}")
            else:
                print(f"DRY MODE - Skipping execution of: {sql}")
                executed_queries.append(sql)
                # In dry mode we consider it as "success" for logging purposes
                success = True

            # Measure end time
            finished_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

            # Log the action run
            action_id = insert_action_run(
                pgassistant_con,
                target_database_id=target_database_id,
                executed_sql=sql,
                issue_type="missing indexes on foreign keys",
                success=success,
                dry_mode=dry_mode,
                started_at=started_at,
                finished_at=finished_at,
            )
            print (f"action_create_fk - Logged action_run.id={action_id} for creating index on {schemaname}.{tablename}")

    except Exception as e:
        print(f"action_create_fk - Error executing query_id {query_id}: {e}")
        return [], []

    # Return a sorted list of 'schema.table' strings (unique)
    qualified_tables = [f"{schema}.{table}" for schema, table in sorted(unique_tables)]
    return qualified_tables, executed_queries

def run_actions(db_config, unique_name: str, dry_mode: bool = True) -> Tuple[List[str], List[str]]:
    """
    Run all available actions and return the executed SQL statements and any errors.
    """
    errors = []
    # Step 1: Connect to the pgAssistant database, init the schema and insert target_database
    try:
        pgassistant_dsn = os.getenv("PG_ASSISTANT_DSN")
        pgassistant_con = get_connection(pgassistant_dsn)
        init_schema(pgassistant_con)
        target_database_id = upsert_target_database(
            pgassistant_con,
            unique_name=unique_name,
            host=db_config['db_host'],
            dbname=db_config['db_name'],
            port=db_config['db_port'],
            username=db_config['db_user'],
        )
        print(f"run_actions - target_database.id = {target_database_id}")
    except Exception as e:
        print(f"run_actions - Error connecting to pgAssistant database or initializing schema: {e}")
        errors.append(str(e))
        return [], errors

    # Step 2: Connect to the target database
    try:
        dbcon, _ = database.connectdb(db_config)
        if not dbcon:
            print("run_actions - Error: Unable to connect to the target database.")
            errors.append("Unable to connect to the target database.")
            return [], errors
    except Exception as e:
        print(f"run_actions - Error connecting to target database: {e}")
        errors.append(str(e))
        return [], errors

    # Step 3: Run actions
    
    # Action: create missing indexes on foreign keys
    all_tables, executed_queries = action_create_fk(dbcon, pgassistant_con, target_database_id, dry_mode=dry_mode)

    # Action: alter column datatype for foreign keys when necessary
    tables2, queries2 = action_alter_column_datatype_fk(dbcon, pgassistant_con, target_database_id, dry_mode=dry_mode)
    
    all_tables = merge_unique(all_tables, tables2)
    executed_queries.extend(queries2)

    # Action: remove strict duplicate indexes
    tables3, queries3 = action_remove_dup_indexes(dbcon, pgassistant_con, target_database_id, dry_mode=dry_mode)
    all_tables = merge_unique(all_tables, tables3)
    executed_queries.extend(queries3)

    # Action: analyze tables that were modified
    _, queries4 = action_analyze_table(dbcon, pgassistant_con, target_database_id, table_list=all_tables, dry_mode=dry_mode)
    executed_queries.extend(queries4)

    return executed_queries, errors
