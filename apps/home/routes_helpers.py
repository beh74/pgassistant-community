# -*- encoding: utf-8 -*-
"""Helper functions for HTML routes."""

import re

from flask import render_template, request, session, redirect

from . import config
from . import analyze_param
from . import database
from . import llm
from . import pgstat_helper
from . import pgtune
from . import ranking
from . import schema_helper
from . import sqlcolumns
from . import sqlhelper
from . import stats
from . import table_autovacuum_advisor


def is_db_connected(session_obj) -> bool:
    return bool(session_obj.get("db_connected"))


def is_multi_db_session(session_obj) -> bool:
    return is_db_connected(session_obj) and database.is_multi_db(session_obj)


def get_active_db(session_obj) -> str:
    if is_multi_db_session(session_obj):
        return (session_obj.get("active_db") or session_obj.get("db_name") or "").strip()
    return (session_obj.get("db_name") or "").strip()


def get_cluster_database_names(session_obj) -> list[str]:
    cached = session_obj.get("cluster_databases")
    if cached:
        return sorted(cached, key=str.casefold)

    databases, _ = database.list_cluster_databases(session_obj)
    names = sorted((db["name"] for db in databases), key=str.casefold)
    if names:
        session_obj["cluster_databases"] = names
        session_obj.modified = True
    return names


def set_active_db(session_obj, db_name: str) -> bool:
    db_name = (db_name or "").strip()
    if not db_name or not is_multi_db_session(session_obj):
        return False

    allowed = get_cluster_database_names(session_obj)
    if db_name not in allowed:
        session_obj.pop("cluster_databases", None)
        allowed = get_cluster_database_names(session_obj)
        if db_name not in allowed:
            return False

    session_obj["active_db"] = db_name
    session_obj.modified = True
    return True


def sync_cluster_databases(session_obj):
    if not database.is_multi_db(session_obj):
        session_obj.pop("cluster_databases", None)
        session_obj.pop("active_db", None)
        return

    names = get_cluster_database_names(session_obj)
    active_db = (session_obj.get("db_name") or session_obj.get("active_db") or "").strip()
    if active_db not in names and names:
        active_db = names[0]
    elif not active_db and names:
        active_db = names[0]
    session_obj["active_db"] = active_db
    session_obj.modified = True


def apply_active_db_from_request():
    if not is_multi_db_session(session):
        return
    db_name = (request.args.get("db") or request.form.get("db") or "").strip()
    if db_name:
        set_active_db(session, db_name)


def multi_db_template_context(session_obj):
    if not is_multi_db_session(session_obj):
        return {
            "multi_db_filter": False,
            "cluster_databases": [],
            "active_db": get_active_db(session_obj),
        }
    return {
        "multi_db_filter": True,
        "cluster_databases": get_cluster_database_names(session_obj),
        "active_db": get_active_db(session_obj),
    }


def require_db_connection():
    if not is_db_connected(session):
        return redirect("/database.html")
    return None


_CONNECTION_KEYS = (
    "db_uri",
    "db_host",
    "db_port",
    "db_name",
    "db_user",
    "db_password",
)


def _db_config_from_form(form, session_obj):
    """Build a fresh connection config from the form, without stale multi-db session state."""
    merged = {
        key: session_obj[key]
        for key in _CONNECTION_KEYS
        if session_obj.get(key)
    }
    for key in _CONNECTION_KEYS:
        if key not in form:
            continue
        val = form.get(key, "")
        if key == "db_uri":
            uri = str(val or "").strip()
            if uri:
                merged["db_uri"] = uri
            else:
                merged.pop("db_uri", None)
        elif key == "db_password":
            if str(val or "").strip():
                merged["db_password"] = val
        elif str(val or "").strip():
            merged[key] = val.strip() if isinstance(val, str) else val
        else:
            merged.pop(key, None)
    merged["multi_db"] = form.get("multi_db") == "on"
    merged.pop("active_db", None)
    merged.pop("cluster_databases", None)
    return merged


def _connection_error_response(error_message: str, segment: str = "database.html"):
    session["db_connected"] = False
    session.modified = True
    connection_form = {
        key: session.get(key, "")
        for key in ("db_uri", "db_host", "db_port", "db_name", "db_user", "db_password", "multi_db")
    }
    return render_template(
        "home/database.html",
        segment=segment,
        dbinfo={"error": error_message},
        connection_form=connection_form,
    )


def generic_select_session(query_id: str):
    """Run generic_select with session and return a connection-error response when needed."""
    rows, description = database.generic_select(session, query_id)
    if isinstance(description, dict) and description.get("connection_error"):
        return None, _connection_error_response(description["connection_error"])
    return (rows, description), None


def handle_database_post(segment: str):
    dbinfo = {}
    merged = _db_config_from_form(request.form, session)
    con, message = database.connectdb(merged)

    if con is None:
        return render_template(
            f"home/{segment}",
            segment=segment,
            dbinfo={"error": message},
            connection_form=merged,
        )

    session.permanent = True
    for key, val in request.form.items():
        if key == "multi_db":
            continue
        session[key] = val
    session["multi_db"] = request.form.get("multi_db") == "on"
    session["db_connected"] = True
    session.pop("cluster_databases", None)
    session.pop("active_db", None)
    sync_cluster_databases(session)

    try:
        dbinfo = database.get_db_info(session)
        if dbinfo.get("error"):
            session["db_connected"] = False
            return render_template(
                f"home/{segment}",
                segment=segment,
                dbinfo=dbinfo,
                connection_form=merged,
            )
    finally:
        con.close()

    session["version"] = database.get_pg_major_version(str(dbinfo["version"]))
    session.modified = True
    return redirect("/dashboard.html")

def handle_database_get(segment: str):
    return render_template(f"home/{segment}", segment=segment, dbinfo={})

def handle_dashboard_get(segment: str):
    if not is_db_connected(session):
        return redirect("/database.html")
    dbinfo = database.get_db_info(session)
    return render_template("home/dashboard.html", segment=segment, dbinfo=dbinfo)

def handle_topqueries_get(template: str, segment: str, tablename: str = None):


    if session.get("db_name"):
        
        # get optional tablename parameter from URL
        if tablename is None:
            tablename = request.args.get('tablename')
        schema_name = (request.args.get('schema') or '').strip()

        # get top queries
        rows = database.get_top_queries(session)

        # add additional information on queries
        for row in rows:
            query = row.get('query') or ''
            row['tables'] = analyze_param.extract_referenced_tables_safe(query)
            if not row['tables']:
                # Keep non-PostgreSQL or truncated pg_stat_statements entries visible.
                row['tables'] = sqlhelper.get_tables(query)
            row['operation_type'] = sqlhelper.get_sql_type(row['query'])

        # Get PostgreSQL internal tables
        pga_tables = database.get_pga_tables()

        # Filter queries to ignore system tables
        rows_filtered = [
            row for row in rows
            if not any(table.split(".")[-1] in pga_tables for table in row['tables'])
        ]

        # Filter even more if 'tablename' is provided
        if tablename:
            rows_filtered = [
                row for row in rows_filtered
                if analyze_param.query_references_table(
                    row.get('query') or '', schema_name, tablename
                )
            ]

        # Render the template with the filtered data
        return render_template(
            f"home/{template}",
            segment=segment,
            rows=rows_filtered,
            tablename=tablename,
            schema_name=schema_name,
            related_mode=bool(tablename),
            column_descriptions=pgstat_helper.PGSS_COLUMN_DOCS,
        )

    else:
        return redirect("/database.html")
    
def handle_rank_queries_get(template: str, segment: str):
    if session.get("db_name"):
        rows = database.get_rank_queries(session)
        ranked_queries = ranking.rank_queries(rows)
        return render_template(f"home/{template}", segment=segment, ranked_queries=ranked_queries)
    else:
        return redirect("/database.html")    

def handle_topstatistics_get(template: str, segment: str):
    if session.get("db_name"):
        rows = database.get_top_queries(session)
        
        i=0
        table_stats=[]
        for query in rows:            
            tables = sqlhelper.get_tables(query['query'])
            for table  in tables:
                columns = []
                
                try:
                    columns = sqlcolumns.extract_where_columns(query['query'], table)
                except:
                    columns = []
                
                stats.add_or_update_table_info(table_stats,
                                               table, 
                                               query['calls'], 
                                               float(query['mean_exec_time']),
                                               query['rows'],
                                               sqlhelper.get_sql_type(query['query']),
                                               columns
                                               )
            rows[i]['tables']=tables
            i = i + 1
        table_stats.sort(reverse=True, key=lambda x: (x['avg_execution_time'], x['operation_type']))
        pga_tables=database.get_pga_tables()
        table_stats_filtered=[]
        for table in table_stats:
            if table['table_name'] not in pga_tables and "$" not in table['table_name']:
                table_stats_filtered.append(table)
        
        return render_template(f"home/{template}", segment=segment, table_stats=table_stats_filtered)
    else:
        return redirect("/database.html")

def handle_primarykey_get(template: str, segment: str):
    redirect_response = require_db_connection()
    if redirect_response:
        return redirect_response

    result, error_response = generic_select_session("issue_no_pk")
    if error_response:
        return error_response
    query_rows, description = result
    return render_template("home/primary_key.html", rows=query_rows, segment=segment, description=description)

def handle_table_rfc_get(template: str, segment: str):
    redirect_response = require_db_connection()
    if redirect_response:
        return redirect_response

    result, error_response = generic_select_session("table_size")
    if error_response:
        return error_response
    query_rows, _description = result
    return render_template("home/tables_cards.html", tables=query_rows, segment="tables_cards.html")

def handle_indexes_get(template: str, segment: str):
    if session.get("db_name"):
        return render_template("home/indexes.html", segment=segment)
    else:
        return redirect("/database.html")

def handle_query_index_advisor_get(template: str, segment: str):
    if session.get("db_name"):
        return render_template(
            "home/query_index_advisor.html",
            segment=segment,
            postgres_major_version=session.get("version"),
        )
    else:
        return redirect("/database.html")

def handle_query_parameter_advisor_get(template: str, segment: str):
    if session.get("db_name"):
        return render_template(
            "home/query_parameter_advisor.html",
            segment=segment,
            postgres_major_version=session.get("version"),
        )
    else:
        return redirect("/database.html")

def handle_database_analyze_llm_get(template: str, segment: str):
    if session.get("db_name"):
        return render_template("home/database_analyze_llm.html", segment=segment)
    else:
        return redirect("/database.html")

def handle_database_analyze_llm_post(template: str, segment: str):
    if not session.get("db_name"):
        return redirect("/database.html")

    llm_prompt = request.form.get("llm_prompt", "").strip()

    if not llm_prompt:
        conn, status = database.connectdb(session)
        if conn is None or status != "OK":
            return render_template("home/page-500.html", err=status, traceback_text=status), 500
        try:
            result = schema_helper.get_database_schema_llm_context(conn)
            llm_prompt = result.get("llm_prompt", "")
        finally:
            conn.close()

    try:
        chatgpt_response = llm.query_chatgpt(llm_prompt)
    except Exception as e1:
        return render_template("home/page-500.html", err=e1, traceback_text=str(e1)), 500

    return render_template(
        "home/chatgpt.html",
        chatgpt_response=chatgpt_response,
        chatgpt_query=llm.render_markdown(llm_prompt),
        title="Database schema analysis",
    )

def handle_cache_table_get(template: str, segment: str):
    redirect_response = require_db_connection()
    if redirect_response:
        return redirect_response

    result, error_response = generic_select_session("hit_cache_by_table")
    if error_response:
        return error_response
    query_rows, description = result
    for row in query_rows:
        try:
            row['table_cache_hit_ratio'] = float(row['table_cache_hit_ratio'])
        except (ValueError, TypeError):
            row['table_cache_hit_ratio'] = 0

        try:
            row['index_cache_hit_ratio'] = float(row['index_cache_hit_ratio'])
        except (ValueError, TypeError):
            row['index_cache_hit_ratio'] = 0
    return render_template("home/cache_table.html", rows=query_rows, segment=segment, description=description)

def handle_reset_pg_stat():
    database.exec_cmd(session, "pg_stat_reset")
    query_rows,description=database.generic_select(session,"hit_cache_by_table")
    return render_template("home/cache_table.html", segment="cache_table.html", rows=query_rows, description=description)


def handle_myqueries_get():
    queries=database.get_my_queries()
    return render_template(f"home/search.html", segment='search.html', rows=queries, searchkey='My queries')

def handle_tools_get():
    return render_template(f"home/tools.html", segment='tools.html')

def handle_reset_pg_statistics():
    database.exec_cmd(session, "pg_stat_statements_reset")
    rows = database.get_top_queries(session)
    return render_template("home/topqueries.html", segment="topqueries.html", rows=rows, column_descriptions=pgstat_helper.PGSS_COLUMN_DOCS)

def handle_enable_pg_statistics():
    database.exec_cmd(session, "pg_stat_statements_enable")
    rows = database.get_top_queries(session)
    return render_template("home/topqueries.html", segment="topqueries.html", rows=rows, column_descriptions=pgstat_helper.PGSS_COLUMN_DOCS)

def handle_lint_post():
    original_sql = request.form.get('sqlo')
    
    return render_template("home/lint.html", segment="lint.html",
                           sqlo=original_sql, linted=sqlhelper.get_formated_sql(original_sql)) 

def handle_search_post():
    searchkey = request.form.get('searchkey')
    rows = database.search(searchkey)
    return render_template("home/search.html", segment="search.html", rows=rows, searchkey=searchkey)

def handle_pgtune_post():
    db_cpu = request.form.get('db_cpu')
    db_type = request.form.get('db_type')
    db_memory = request.form.get('db_memory')
    db_memory_unity = request.form.get('db_memory_unity')
    db_maxconn = request.form.get('db_maxconn')
    db_storage = request.form.get('db_storage')

    running_values,major_version=database.get_pg_tune_parameter(session)
    a_pgtune = pgtune.pgTune (major_version,db_cpu,db_memory+db_memory_unity,db_storage,db_type,db_maxconn)
    
    tuned_values = a_pgtune.get_pg_tune()
    sqlalter = a_pgtune.get_alter_system(running_values)
    docker_cmd = a_pgtune.get_docker_cmd(session, major_version)
    kubernetes_cmd = a_pgtune.get_kube_cmd(session, major_version)

    return render_template("home/pgtune_result.html", segment="pgtune_result.html", 
                           major_version=int(major_version),
                           running_values=running_values, 
                           tuned_values=tuned_values,
                           sqlalter=sqlalter,
                           docker_cmd=docker_cmd,
                           kubernetes_cmd=kubernetes_cmd
                           )

def handle_table_autovacuum_tune_get(template: str, segment: str):
    redirect_response = require_db_connection()
    if redirect_response:
        return redirect_response

    stale_days = table_autovacuum_advisor.normalize_stale_days(
        request.args.get("stale_days")
    )
    result = table_autovacuum_advisor.run_table_autovacuum_advisor(
        session,
        stale_days=stale_days,
    )
    return render_template(
        f"home/{template}",
        segment=segment,
        advisor_result=result,
        stale_days=stale_days,
        criteria_help=table_autovacuum_advisor.build_criteria_help(stale_days),
    )


def handle_table_autovacuum_tune_post(template: str, segment: str):
    redirect_response = require_db_connection()
    if redirect_response:
        return redirect_response

    stale_days = table_autovacuum_advisor.normalize_stale_days(
        request.form.get("stale_days") or session.get("autovacuum_stale_days")
    )
    session["autovacuum_stale_days"] = stale_days
    session.modified = True

    result = table_autovacuum_advisor.run_table_autovacuum_advisor(
        session,
        stale_days=stale_days,
    )
    return render_template(
        f"home/{template}",
        segment=segment,
        advisor_result=result,
        stale_days=stale_days,
        criteria_help=table_autovacuum_advisor.build_criteria_help(stale_days),
    )


def get_segment(request):
    try:
        segment = request.path.split('/')[-1]
        if segment == '':
            segment = 'database'
        return segment
    except Exception:
        return None
