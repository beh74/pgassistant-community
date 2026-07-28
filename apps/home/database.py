import datetime
import decimal
import json
import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import parse_dsn
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


from flask import g
from . import sqlhelper

from typing import Dict, Any, Iterable, Tuple, List, Optional

PGA_QUERIES={}
PGA_TABLES=[]


def get_pg_major_version(server_version: str) -> int:
    match = re.match(r'^(\d+)', server_version.strip())
    if not match:
        raise ValueError(f"Invalid PostgreSQL version string: {server_version}")
    return int(match.group(1))

def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], dict)):  #noqa
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]




def _dsn_has_param(dsn: str, param_name: str) -> bool:
    """
    Returns True if the PostgreSQL URI already contains the given query parameter.
    """
    parsed = urlparse(dsn)
    query_params = parse_qs(parsed.query, keep_blank_values=True)
    return param_name in query_params


def _add_default_uri_param(dsn: str, param_name: str, param_value: str) -> str:
    """
    Adds a query parameter to a PostgreSQL URI only if it is not already present.
    """
    if _dsn_has_param(dsn, param_name):
        return dsn

    parsed = urlparse(dsn)
    query_params = parse_qs(parsed.query, keep_blank_values=True)
    query_params[param_name] = [param_value]

    new_query = urlencode(query_params, doseq=True)

    return urlunparse(parsed._replace(query=new_query))




def is_multi_db(db_config) -> bool:
    value = db_config.get("multi_db")
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "on", "yes"}


def normalize_db_config(db_config) -> dict:
    """Return a plain dict from Flask session or mapping objects."""
    if db_config is None:
        return {}
    if hasattr(db_config, "items"):
        return {key: db_config.get(key) for key in db_config}
    return dict(db_config)


def get_resolved_database_name(db_config) -> str:
    """Database name after multi-db active_db resolution."""
    cfg = resolve_db_config(normalize_db_config(db_config))
    return (cfg.get("db_name") or "").strip()


def _quote_sql_literal(value: str) -> str:
    return "'" + (value or "").replace("'", "''") + "'"


def resolve_db_config(db_config):
    """Return a connection config, honoring active_db in multi-database mode."""
    if db_config is None:
        return db_config
    cfg = normalize_db_config(db_config)
    if not is_multi_db(cfg):
        return cfg
    active_db = (cfg.get("active_db") or cfg.get("db_name") or "").strip()
    if not active_db:
        return cfg
    cfg["db_name"] = active_db
    uri = (cfg.get("db_uri") or "").strip()
    if uri:
        cfg["db_uri"] = _uri_with_database(uri, active_db)
    return cfg


def _uri_with_database(uri: str, database_name: str) -> str:
    database_name = (database_name or "").strip()
    uri = (uri or "").strip()
    if not uri or not database_name:
        return uri
    if not re.match(r"^postgres(ql)?://", uri, flags=re.IGNORECASE):
        return uri
    parsed = urlparse(uri)
    query_params = parse_qs(parsed.query, keep_blank_values=True)
    query_params.pop("dbname", None)
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(parsed._replace(path=f"/{database_name}", query=new_query))


def connectdb(db_config):
    """Establishes a connection to a PostgreSQL database using psycopg2."""
    db_config = resolve_db_config(normalize_db_config(db_config))
    try:
        db_uri = (db_config.get("db_uri") or "").strip()

        if db_uri:
            parsed_dsn = parse_dsn(db_uri)
            connect_kwargs = {}
            if "connect_timeout" not in parsed_dsn:
                connect_kwargs["connect_timeout"] = 5
            if "application_name" not in parsed_dsn:
                connect_kwargs["application_name"] = "pgAssistant"
            con = psycopg2.connect(db_uri, **connect_kwargs)
        else:
            con = psycopg2.connect(
                database=db_config["db_name"],
                host=db_config["db_host"],
                user=db_config["db_user"],
                password=db_config["db_password"],
                port=db_config["db_port"],
                connect_timeout=5,
                application_name="pgAssistant",
            )

        con.autocommit = True
    except psycopg2.Error as err:
        return None, format(err).rstrip()

    return con, "OK"


def connectdb_to(db_config, database_name: str):
    """Connect to a specific database on the same cluster."""
    database_name = (database_name or "").strip()
    if not database_name:
        return connectdb(db_config)

    cfg = dict(db_config)
    cfg.pop("active_db", None)
    uri = (cfg.get("db_uri") or "").strip()

    if uri:
        cfg["db_uri"] = _uri_with_database(uri, database_name)
        cfg["db_name"] = database_name
        return connectdb(cfg)

    cfg["db_name"] = database_name
    cfg.pop("db_uri", None)
    return connectdb(cfg)


def list_cluster_databases(db_config, con=None):
    """List non-template databases with sizes (psql \\l+ equivalent)."""
    close_connection = False
    if con is None:
        con, message = connectdb(db_config)
        if not con:
            return [], message
        close_connection = True

    cursor = None
    try:
        cursor = con.cursor()
        cursor.execute(
            """
            SELECT
                d.datname,
                pg_catalog.pg_get_userbyid(d.datdba) AS owner,
                pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
                pg_catalog.pg_database_size(d.datname) AS size_bytes,
                pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) AS size_pretty,
                (d.datname = current_database()) AS is_connected
            FROM pg_catalog.pg_database d
            WHERE NOT d.datistemplate
            ORDER BY pg_catalog.pg_database_size(d.datname) DESC, d.datname ASC
            """
        )
        rows = cursor.fetchall()
        databases = [
            {
                "name": str(row[0]),
                "owner": str(row[1] or ""),
                "encoding": str(row[2] or ""),
                "size_bytes": int(row[3] or 0),
                "size_pretty": str(row[4] or "?"),
                "is_connected": bool(row[5]),
            }
            for row in rows
        ]
        return databases, "OK"
    except Exception as exc:
        return [], str(exc)
    finally:
        if cursor is not None:
            cursor.close()
        if close_connection and con is not None:
            con.close()


def _format_cluster_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    value = float(size_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f} {unit}"
    return f"{value:.1f} PB"


def get_db_info_cluster(db_config):
    info = {"multi_db": True, "error": None}
    con, message = connectdb(db_config)
    if not con:
        info["error"] = message
        return info

    try:
        version, _ = db_query(con, "db_version")
        info["version"] = version[0]["server_version"]

        databases, db_message = list_cluster_databases(db_config, con=con)
        if not databases and db_message != "OK":
            info["error"] = db_message
            return info

        info["databases"] = databases
        total_bytes = sum(db["size_bytes"] for db in databases)
        info["size"] = _format_cluster_size(total_bytes)
        info["size_bytes"] = total_bytes

        cache_rows = json.loads(
            db_fetch_json(
                con,
                """
                SELECT
                    ROUND(
                        100.0 * SUM(blks_hit) / NULLIF(SUM(blks_hit) + SUM(blks_read), 0),
                        2
                    ) AS ratio
                FROM pg_stat_database
                WHERE datname NOT IN ('template0', 'template1')
                """,
            )
        )
        info["cache"] = float(cache_rows[0].get("ratio") or 0) if cache_rows else 0

        try:
            uptime, _ = db_query(con, "reporting_pguptime")
            info["uptime"] = uptime[0]["uptime_pretty"]
        except Exception:
            info["uptime"] = "?"

        try:
            shared_buffers, _ = db_query(con, "shared_buffers_setting")
            info["shared_buffers"] = shared_buffers[0]["current_setting"]
        except Exception:
            info["shared_buffers"] = "?"

        info["profile"], _ = db_query(con, "reporting_db_profile")

        connexions, _ = db_query(con, "database_count_connexions")
        info["connexions"] = connexions[0]["nb"]

        max_connexions, _ = db_query(con, "database_max_connexions")
        info["max_connexions"] = max_connexions[0]["setting"]

        database_top_clients, _ = db_query(con, "database_top_clients")
        info["top_clients"] = database_top_clients

        largest_tables = []
        for db in databases[:20]:
            db_con, db_status = connectdb_to(db_config, db["name"])
            if not db_con:
                continue
            try:
                rows, _ = db_query(db_con, "table_size_top_5")
                for row in rows:
                    item = dict(row)
                    item["database"] = db["name"]
                    largest_tables.append(item)
            finally:
                db_con.close()

        largest_tables.sort(
            key=lambda row: row.get("total_size_bytes") or row.get("size_bytes") or 0,
            reverse=True,
        )
        info["table_size"] = largest_tables[:5]
    finally:
        con.close()

    return info


def db_exec(conn, sql):
    """
    Executes a SQL statement that does not return a result (e.g., INSERT, UPDATE).
    
    :param conn: The active database connection.
    :param sql: The SQL statement to execute.
    """    
    sql = '/* launched by pgAssistant */ ' + sql
    conn.set_session(autocommit=True)
    cursor = conn.cursor()
    cursor.execute(sql)

def format_sql_execution_output(cursor, notices: list[str] | None = None) -> str:
    """Build a human-readable command output from result rows and PG notices."""
    parts: list[str] = []

    if cursor is not None and cursor.description:
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        if columns:
            parts.append(" | ".join(str(column) for column in columns))
            parts.append("-|-".join("-" * max(len(str(column)), 1) for column in columns))
        for row in rows:
            parts.append(" | ".join("" if value is None else str(value) for value in row))

    for notice in notices or []:
        cleaned = str(notice or "").strip()
        if cleaned:
            parts.append(cleaned)

    if cursor is not None and cursor.rowcount >= 0 and not cursor.description:
        parts.append(f"Rows affected: {cursor.rowcount}")

    return "\n".join(parts).strip()


def db_exec_recommandation(conn, sql):
    """
    Execute a SQL clause on the given PostgreSQL connection.

    Returns success metadata plus any server notices (for example VACUUM VERBOSE)
    or result rows (for example SELECT pg_reload_conf()).
    """
    sql = '/* launched by pgAssistant */ ' + sql
    cursor = None
    try:
        conn.set_session(autocommit=True)
        if hasattr(conn, "notices"):
            conn.notices.clear()

        cursor = conn.cursor()
        cursor.execute(sql)

        notices = list(getattr(conn, "notices", []) or [])
        output = format_sql_execution_output(cursor, notices)
        message = output or "Command completed successfully."

        return {
            "success": True,
            "message": message,
            "output": output,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        if cursor is not None:
            cursor.close()

def get_json_cursor(conn):
    """
    Returns a database cursor that fetches results as a dictionary (JSON-like).
    """    
    return conn.cursor(cursor_factory=RealDictCursor)

def execute_and_fetch(cursor, query):
    """
    Executes a SQL query and fetches all results.
    
    :param cursor: The active database cursor.
    :param query: The SQL query to execute.
    :return: Fetched query results.
    """    
    query = '/* launched by pgAssistant */ ' + query
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    return res

def get_json_response(conn, query):
    """
    Executes a SQL query and returns the result as a JSON string.
    
    :param conn: The active database connection.
    :param query: The SQL query to execute.
    :return: JSON string of the query results.
    """    
    cursor = get_json_cursor(conn)
    response = execute_and_fetch(cursor, query)
    return json.dumps(response)

def defaultconverter(o):
    """
    Converts datetime and decimal objects to strings for JSON serialization.
    """    
    if isinstance(o, datetime.datetime):
        return o.__str__()
    elif isinstance(o, decimal.Decimal):
        return str(o)
    
def db_fetch_json(conn,sql):
    """
    Executes a SQL query and returns results as a JSON string with type conversion.
    
    :param conn: The active database connection.
    :param sql: The SQL query to execute.
    :return: JSON string containing the query results.
    """
    cursor = get_json_cursor(conn)
    response = execute_and_fetch(cursor, sql)
    return json.dumps(response, default = defaultconverter)


_PGSS_DB_FILTER_TAIL = (
    "dbid = (SELECT oid FROM pg_database WHERE datname = {db_literal})"
)


def pg_stat_statements_has_dbid(con) -> bool:
    """Return True when pg_stat_statements exposes cluster-wide dbid (PG 14+)."""
    cursor = None
    try:
        cursor = con.cursor()
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class AS c
                JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
                WHERE c.relname = 'pg_stat_statements'
                  AND c.relkind IN ('v', 'm')
                  AND NOT a.attisdropped
                  AND a.attnum > 0
                  AND a.attname = 'dbid'
            )
            """
        )
        row = cursor.fetchone()
        return bool(row[0]) if row else False
    except Exception:
        return False
    finally:
        if cursor is not None:
            cursor.close()


def apply_pgss_database_filter(sql: str, database_name: str) -> str:
    """Restrict pg_stat_statements rows to one database."""
    if not sql or not database_name or re.search(r"\bdbid\b", sql, flags=re.IGNORECASE):
        return sql
    if "pg_stat_statements" not in sql.lower():
        return sql

    db_literal = _quote_sql_literal(database_name.strip())
    clause = " AND " + _PGSS_DB_FILTER_TAIL.format(db_literal=db_literal) + " "
    for pattern in (r"\bORDER\s+BY\b", r"\bLIMIT\b"):
        match = re.search(pattern, sql, flags=re.IGNORECASE)
        if match:
            return sql[: match.start()] + clause + sql[match.start() :]

    trimmed = sql.rstrip().rstrip(";")
    if re.search(r"\bWHERE\b", trimmed, flags=re.IGNORECASE):
        return trimmed + clause + ";"
    return trimmed + f" WHERE {_PGSS_DB_FILTER_TAIL.format(db_literal=db_literal)};"


def prepare_pgss_sql(sql: str, con, database_name: str) -> str:
    """Apply a database filter to pg_stat_statements SQL when dbid is available."""
    if not database_name:
        return sql
    if pg_stat_statements_has_dbid(con):
        return apply_pgss_database_filter(sql, database_name)
    return sql


def inject_pgss_current_db_filter(sql: str) -> str:
    """Backward-compatible alias using current_database()."""
    db_literal = "current_database()"
    if not sql or re.search(r"\bdbid\b", sql, flags=re.IGNORECASE):
        return sql
    if "pg_stat_statements" not in sql.lower():
        return sql

    clause = f" AND dbid = (SELECT oid FROM pg_database WHERE datname = {db_literal}) "
    match = re.search(r"\bORDER\s+BY\b", sql, flags=re.IGNORECASE)
    if match:
        return sql[: match.start()] + clause + sql[match.start() :]

    trimmed = sql.rstrip().rstrip(";")
    if re.search(r"\bWHERE\b", trimmed, flags=re.IGNORECASE):
        return trimmed + clause + ";"
    return trimmed + f" WHERE dbid = (SELECT oid FROM pg_database WHERE datname = {db_literal});"


def _db_query_pgss(cnx, query_id, db_config, db_name=None):
    """Run a catalog query, scoping pg_stat_statements to the active database."""
    get_queries()
    database_name = get_resolved_database_name(db_config)

    for query in PGA_QUERIES["sql"]:
        if query["id"] != query_id:
            continue
        sql = query["sql"]
        if db_name:
            sql = sql.replace("$1", db_name)
        sql = prepare_pgss_sql(sql, cnx, database_name)
        if query["type"] == "select" or query["type"] == "param_query":
            return json.loads(db_fetch_json(cnx, sql)), query
        db_exec(cnx, sql)
        return [], query
    return [], None


def get_top_queries(db_config):
    """
    Retrieves the top queries executed in the database.
    """
    rows = []
    cfg = normalize_db_config(db_config)
    con, message = connectdb(cfg)
    if con:
       try:
            if cfg.get('version')==18:
               rows, description = _db_query_pgss(con, 'top_queries_18', cfg)
            else:
               rows, description = _db_query_pgss(con, 'top_queries', cfg)
       except:
           rows=[]
       con.close()
    return rows


def get_rank_queries(db_config):
    """
    Retrieves the ranked queries from the database.
    """
    rows = []
    cfg = normalize_db_config(db_config)
    con, message = connectdb(cfg)
    if con:
       try:
            if cfg.get('version')==18:
               rows, description = _db_query_pgss(con, 'top_ranking_18', cfg)
            else:
               rows, description = _db_query_pgss(con, 'top_ranking', cfg)
       except:
           rows=[]
       con.close()
    return rows

def exec_cmd(db_config,query_id):
    """
    Executes a predefined query by its ID.
    """    
    con, message = connectdb(db_config)
    if con:
       db_query(con,query_id)
       con.close()

def generic_select(db_config, query_id):
    con, message = connectdb(db_config)
    if not con:
        return [], {"connection_error": message or "Unable to connect to database."}
    try:
        result = db_query(con, query_id)
        if not result:
            return [], None
        return result
    except Exception as exc:
        return [], {"connection_error": str(exc)}
    finally:
        con.close()

def generic_select_with_sql(db_config,sql):
    rows = []
    con, message = connectdb(db_config)
    if con:
       rows=json.loads(db_fetch_json(con,sql))
       con.close()
    return rows


def ensure_pg_stat_statements(con):
    """Ensure pg_stat_statements exists without issuing DDL unnecessarily.

    CREATE EXTENSION is rejected in a read-only transaction even when used with
    IF NOT EXISTS. Check pg_extension first so read-only monitoring roles can
    connect when the extension was installed by an administrator.
    """
    cursor = None

    try:
        cursor = con.cursor()
        cursor.execute(
            """
            /* launched by pgAssistant */
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_extension
                WHERE extname = 'pg_stat_statements'
            );
            """
        )
        extension_installed = bool(cursor.fetchone()[0])

        if extension_installed:
            return True, None

        cursor.execute(
            "/* launched by pgAssistant */ SHOW transaction_read_only;"
        )
        transaction_read_only = str(cursor.fetchone()[0]).lower() in {
            "on",
            "true",
            "1",
        }

        if transaction_read_only:
            return False, (
                "pg_stat_statements is not installed in this database. "
                "The connected role is read-only, so a PostgreSQL "
                "administrator must run: "
                "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
            )

        cursor.execute(
            "/* launched by pgAssistant */ "
            "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
        )
        return True, None

    except psycopg2.Error as exc:
        return False, (
            "Error while checking or enabling pg_stat_statements: "
            f"{exc.pgcode or 'Unknown Code'} - "
            f"{exc.pgerror or str(exc)}"
        )
    except Exception as exc:
        return False, (
            "Unexpected error while checking or enabling "
            f"pg_stat_statements: {exc}"
        )
    finally:
        if cursor is not None:
            cursor.close()


def get_db_info(db_config,con=None):
    if is_multi_db(db_config):
        return get_db_info_cluster(db_config)

    info = {}

    if not con:
        con, message = connectdb(db_config)

    if con:
        if db_config:
            pgss_available, pgss_error = ensure_pg_stat_statements(con)
            if not pgss_available:
                info["error"] = pgss_error
                return info

            version, _= db_query(con,'db_version')
            info['version']=version[0]['server_version']

            size, _= db_query(con,'db_size',db_config['db_name'])
            info["size"]=size[0]['pg_size_pretty']

            try:
                cache, _= db_query(con,'db_cache')
                info["cache"]=float(cache[0]['ratio'])
                if not info["cache"]:
                    info["cache"]=0 
            except:
                info["cache"]=0

            try:
                uptime, _= db_query(con,'reporting_pguptime')
                info["uptime"]=uptime[0]['uptime_pretty']
            except:
                info["uptime"]="?"

            try: 
                shared_buffers, _= db_query(con,'shared_buffers_setting')    
                info["shared_buffers"]=shared_buffers[0]['current_setting']
            except:
                info["shared_buffers"]="?"

            table_size, _= db_query(con,'table_size_top_5')
            info["table_size"]=table_size

            info['profile'], _= db_query(con,'reporting_db_profile')

            connexions, _= db_query(con,'database_count_connexions')
            info['connexions']=connexions[0]['nb']

            max_connexions, _= db_query(con,'database_max_connexions')
            info['max_connexions']=max_connexions[0]['setting']

            database_top_clients, _= db_query(con,'database_top_clients')
            info['top_clients']=database_top_clients

            try:
                conflicts, _=  db_query(con,'database_count_conlicts')
                info['conflicts']=conflicts[0]['nb']
            except:
                info['conflicts']="???"
        con.close()
    else:
        info["error"]=message
    return info

def get_query_by_id(query_id):
    get_queries()

    for query in PGA_QUERIES['sql']:
        if query_id == query['id']:
            return query
    return None

def get_pgstat_query_by_id(db_config, query_id):
    query = get_query_by_id('pgstat_get_sqlquery_by_id')
    sql=query['sql'].replace ('$1', query_id)
    cfg = normalize_db_config(db_config)
    db_name = get_resolved_database_name(cfg)
    con, _ = connectdb(cfg)
    sql_text=''
    try:
        if con:
            sql = prepare_pgss_sql(sql, con, db_name)
            sql_rows=json.loads(db_fetch_json(con,sql))
            if sql_rows:
                sql_text=sql_rows[0].get('query') or ''
        return sql_text
    finally:
        if con:
            con.close()



def search(key):
    get_queries()

    rows=[]
    searchkey=key.lower().strip()
    for query in PGA_QUERIES['sql']:
        if searchkey in query['description'].lower() or searchkey in query['category'].lower() or searchkey in query['id'].lower():
            rows.append(query)
    return rows
    
def db_query(cnx, query_id, db_name=None):
    get_queries()

    for query in PGA_QUERIES['sql']:
        if query['id']==query_id:
            sql=query['sql']
            #sql = '/* launched by pgAssistant */ ' + sql
            if db_name:
                sql = sql.replace ('$1', db_name)
            if query['type']=='select' or query['type']=='param_query':
                return json.loads(db_fetch_json(cnx,sql)),query
            else:
                db_exec(cnx,sql)
                return [], query
    return [], None

def get_query_by_id_reporing(query_id):
    get_queries()

    for query in PGA_QUERIES['sql']:
        if query_id == query['id']:
            return '/* launched by pgAssistant */ ' + query['sql']
    return None


def get_my_queries():
    if os.path.isfile("myqueries.json"):
        rows=[]
        with open("myqueries.json", encoding="utf-8") as f_in:
            userqueries=json.load(f_in)
        for query in userqueries['sql']:
            rows.append(query)
        return rows
    return []

def get_pga_tables():
    return PGA_TABLES

def get_queries():
    global PGA_QUERIES
    global PGA_TABLES

    # get the standard json queries
    if PGA_QUERIES=={}:
    #if not PGA_QUERIES.get('sql'):
        with open("queries.json", encoding="utf-8") as f_in:
            PGA_QUERIES=json.load(f_in)
            print("Loaded", len(PGA_QUERIES.get('sql')), "pgAssistant queries.")

            # get tables names from each pgassistant queries
            PGA_TABLES=[]
            for query in PGA_QUERIES.get('sql'):
                tables=sqlhelper.get_tables(query['sql'])
                for table in tables:
                    if table not in PGA_TABLES:
                        PGA_TABLES.append(table)

        #get the user defined queries
        if os.path.isfile("myqueries.json"):
            with open("myqueries.json", encoding="utf-8") as f_in:
                userqueries=json.load(f_in)
            PGA_QUERIES['sql']=PGA_QUERIES['sql']+userqueries['sql']
            
def get_pg_tune_parameter(db_config):
    """
    Retrieves PostgreSQL tuning parameters and version.
    
    :param db_config: Database configuration dictionary.
    :return: Dictionary of tuning parameters and major version.
    """    
    con, message = connectdb(db_config)
    if con:
        # get the the current setting of key pgtune run-time parameters.
        running_values={}
        params=['max_connections','shared_buffers','effective_cache_size',
                'maintenance_work_mem','checkpoint_completion_target','wal_buffers',
                'default_statistics_target','random_page_cost','effective_io_concurrency',
                'work_mem','huge_pages','min_wal_size','max_wal_size',
                'max_worker_processes','max_parallel_workers_per_gather',
                'max_parallel_workers','max_parallel_maintenance_workers']
        for aparam in params:
            sql = 'SHOW ' + aparam + ';'
            value=json.loads(db_fetch_json(con,sql))
            running_values[aparam]=value[0][aparam]

        # get the database version
        version_raw, _= db_query(con,'db_version')
        version=version_raw[0]['server_version']

        # alter system command supported by versions >= 12
        if '.' in version:
            major=version.split('.',1)[0]
        else:
            major=version
        
        con.close()
        return running_values, major


def get_existing_indexes(db_config):
    """
    Retrieves existing indexes from PostgreSQL.
    
    :param db_config: Database configuration dictionary.
    :return: Dictionary {table_name: set(frozenset(columns))} of existing indexes.
    """
    existing_indexes = {}
    try:
        conn, message = connectdb(db_config)
        if (conn):
            cur = conn.cursor()

            query = """
            SELECT tablename, indexdef FROM pg_indexes where schemaname !='pg_catalog';
            """

            cur.execute(query)
            indexes = cur.fetchall()

            for table, index_def in indexes:
                
                # Extract column names
                match = re.search(r"ON\s+(?:ONLY\s+)?\S+\s+USING\s+\w+\s*\((.*?)\)", index_def, re.IGNORECASE)
                if not match:
                    print ("**** ERROR WHILE PARSING INDEXE DEFINITION > ", index_def, table)
                if match:
                    indexed_columns = tuple(col.strip() for col in match.group(1).split(","))  # Convertir en tuple (ordre important)
                    
                    if table not in existing_indexes:
                        existing_indexes[table] = set()
                    existing_indexes[table].add(indexed_columns)  # Stocker sous forme de tuple immuable

            cur.close()
            conn.close()
    except Exception as e:
        print(f"⚠️ Error while getting indexes definition : {e}")

    return existing_indexes


def fetch_tables_pgstat(db_config, tables: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """
    Returns pg_stats for each table and its columns.
    """

    if not tables:
        return {}

    rows = {}
    conn = None 
    try:
        conn, message = connectdb(db_config)
    except Exception as e:
        return None
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT schemaname, tablename, attname,
                   null_frac, avg_width, n_distinct,
                   most_common_vals, most_common_freqs,
                   histogram_bounds, correlation
            FROM pg_stats
            WHERE (tablename) = ANY(%s)
            """,
            (tables,),
        )
        for r in cur.fetchall():
            key = f"{r['schemaname']}.{r['tablename']}"
            rows.setdefault(key, {})
            rows[key][r["attname"]] = {
                "null_frac": float(r["null_frac"]),
                "avg_width": int(r["avg_width"]),
                "n_distinct": float(r["n_distinct"]),
                "most_common_vals": r["most_common_vals"],
                "most_common_freqs": r["most_common_freqs"],
                "histogram_bounds": r["histogram_bounds"],
                "correlation": float(r["correlation"]) if r["correlation"] is not None else None,
            }

    return rows

def fetch_table_stats(db_config, tables):
    if not tables:
        return {}

    rows = {}

    try:
        conn, message = connectdb(db_config)
    except Exception:
        return None

    qualified = [t for t in tables if "." in t]
    unqualified = [t for t in tables if "." not in t]

    rel_filters = []
    rel_params = []

    if qualified:
        rel_filters.append("(n.nspname || '.' || c.relname) = ANY(%s)")
        rel_params.append(qualified)

    if unqualified:
        rel_filters.append("c.relname = ANY(%s)")
        rel_params.append(unqualified)

    rel_where = " OR ".join(rel_filters)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:

        cur.execute(
            f"""
            WITH RECURSIVE input_matches AS (
                SELECT c.oid
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r','p')
                  AND ({rel_where})
            ),
            ancestor_chain(relid, ancestor_oid) AS (
                SELECT oid AS relid, oid AS ancestor_oid
                FROM input_matches

                UNION ALL

                SELECT ac.relid, inh.inhparent AS ancestor_oid
                FROM ancestor_chain ac
                JOIN pg_inherits inh ON inh.inhrelid = ac.ancestor_oid
            ),
            roots AS (
                SELECT DISTINCT ac.ancestor_oid AS oid
                FROM ancestor_chain ac
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM pg_inherits parent_link
                    WHERE parent_link.inhrelid = ac.ancestor_oid
                )
            ),
            relation_members(root_oid, member_oid) AS (
                SELECT oid AS root_oid, oid AS member_oid
                FROM roots

                UNION ALL

                SELECT rm.root_oid, child.oid AS member_oid
                FROM relation_members rm
                JOIN pg_inherits inh ON inh.inhparent = rm.member_oid
                JOIN pg_class child ON child.oid = inh.inhrelid
                WHERE child.relkind IN ('r','p')
            )
            SELECT
                n.nspname AS schemaname,
                root.relname AS tablename,
                GREATEST(COUNT(DISTINCT rm.member_oid) - 1, 0) AS partition_count,
                COALESCE(SUM(member.reltuples), 0)::bigint AS estimated_rows,
                COALESCE(SUM(pg_total_relation_size(member.oid)), 0) AS total_bytes,
                COALESCE(SUM(st.n_live_tup), 0) AS n_live_tup,
                COALESCE(SUM(st.n_dead_tup), 0) AS n_dead_tup,
                MAX(st.last_vacuum) AS last_vacuum,
                MAX(st.last_autovacuum) AS last_autovacuum,
                MAX(st.last_analyze) AS last_analyze,
                MAX(st.last_autoanalyze) AS last_autoanalyze
            FROM roots r
            JOIN pg_class root ON root.oid = r.oid
            JOIN pg_namespace n ON n.oid = root.relnamespace
            JOIN relation_members rm ON rm.root_oid = root.oid
            JOIN pg_class member ON member.oid = rm.member_oid
            LEFT JOIN pg_stat_all_tables st ON st.relid = member.oid
            GROUP BY n.nspname, root.relname
            """,
            tuple(rel_params),
        )

        for r in cur.fetchall():
            key = f"{r['schemaname']}.{r['tablename']}"
            rows[key] = {
                "partition_count": int(r["partition_count"] or 0),
                "estimated_rows": int(r["estimated_rows"] or 0),
                "total_bytes": int(r["total_bytes"] or 0),
                "n_live_tup": int(r["n_live_tup"] or 0),
                "n_dead_tup": int(r["n_dead_tup"] or 0),
                "last_vacuum": str(r["last_vacuum"]) if r["last_vacuum"] else None,
                "last_autovacuum": str(r["last_autovacuum"]) if r["last_autovacuum"] else None,
                "last_analyze": str(r["last_analyze"]) if r["last_analyze"] else None,
                "last_autoanalyze": str(r["last_autoanalyze"]) if r["last_autoanalyze"] else None,
            }

    return rows


def fetch_foreign_key_index_coverage(db_config, tables):
    """
    Returns child-side foreign-key index coverage for the provided tables.

    A FK is considered covered when a valid, ready, non-partial, non-expression
    index starts with the FK column list in the same order.
    """
    if not tables:
        return []

    try:
        conn, message = connectdb(db_config)
    except Exception:
        return []

    qualified = [t for t in tables if "." in t]
    unqualified = [t for t in tables if "." not in t]

    filters = []
    params = []

    if qualified:
        filters.append("(nsrc.nspname || '.' || src.relname) = ANY(%s)")
        params.append(qualified)

    if unqualified:
        filters.append("src.relname = ANY(%s)")
        params.append(unqualified)

    if not filters:
        conn.close()
        return []

    where_filter = " OR ".join(filters)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                    con.conname AS constraint_name,
                    nsrc.nspname AS from_schema,
                    src.relname AS from_table,
                    ARRAY_AGG(src_att.attname ORDER BY k.ord) AS from_columns,
                    ntgt.nspname AS to_schema,
                    tgt.relname AS to_table,
                    ARRAY_AGG(tgt_att.attname ORDER BY k.ord) AS to_columns,
                    EXISTS (
                        SELECT 1
                        FROM pg_index idx
                        WHERE idx.indrelid = con.conrelid
                          AND idx.indisvalid
                          AND idx.indisready
                          AND idx.indpred IS NULL
                          AND idx.indexprs IS NULL
                          AND (
                              string_to_array(idx.indkey::text, ' ')::smallint[]
                          )[1:array_length(con.conkey, 1)] = con.conkey
                    ) AS fk_index_covered
                FROM pg_constraint con
                JOIN pg_class src ON src.oid = con.conrelid
                JOIN pg_namespace nsrc ON nsrc.oid = src.relnamespace
                JOIN pg_class tgt ON tgt.oid = con.confrelid
                JOIN pg_namespace ntgt ON ntgt.oid = tgt.relnamespace
                JOIN LATERAL (
                    SELECT u.ord, u.src_attnum, v.tgt_attnum
                    FROM unnest(con.conkey) WITH ORDINALITY u(src_attnum, ord)
                    JOIN unnest(con.confkey) WITH ORDINALITY v(tgt_attnum, ord)
                      USING (ord)
                ) k ON true
                JOIN pg_attribute src_att
                  ON src_att.attrelid = src.oid
                 AND src_att.attnum = k.src_attnum
                JOIN pg_attribute tgt_att
                  ON tgt_att.attrelid = tgt.oid
                 AND tgt_att.attnum = k.tgt_attnum
                WHERE con.contype = 'f'
                  AND src.relkind IN ('r', 'p')
                  AND tgt.relkind IN ('r', 'p')
                  AND nsrc.nspname <> 'information_schema'
                  AND nsrc.nspname !~ '^pg_'
                  AND ntgt.nspname <> 'information_schema'
                  AND ntgt.nspname !~ '^pg_'
                  AND ({where_filter})
                GROUP BY
                    con.conname,
                    con.conrelid,
                    con.conkey,
                    nsrc.nspname,
                    src.relname,
                    ntgt.nspname,
                    tgt.relname
                ORDER BY nsrc.nspname, src.relname, con.conname
                """,
                tuple(params),
            )

            rows = []
            for row in cur.fetchall():
                rows.append(
                    {
                        "constraint_name": row["constraint_name"],
                        "from_table": f"{row['from_schema']}.{row['from_table']}",
                        "from_columns": list(row["from_columns"] or []),
                        "to_table": f"{row['to_schema']}.{row['to_table']}",
                        "to_columns": list(row["to_columns"] or []),
                        "fk_index_covered": bool(row["fk_index_covered"]),
                    }
                )
            return rows
    finally:
        conn.close()
