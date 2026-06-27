# -*- encoding: utf-8 -*-
"""Helper functions for HTML routes."""

import re

from flask import render_template, request, session, redirect

from . import config
from . import database
from . import llm
from . import pgstat_helper
from . import pgtune
from . import ranking
from . import schema_helper
from . import sqlcolumns
from . import sqlhelper
from . import stats


def handle_database_post(segment: str):
    dbinfo = {}
    session.permanent = True
    for key, val in request.form.items():
        session[key] = val

    dbinfo = database.get_db_info(session)
    

    if "error" in dbinfo:
        return render_template(f"home/{segment}", segment=segment, dbinfo=dbinfo)  

    session['version']=database.get_pg_major_version(str(dbinfo['version']))
    session.modified = True

    return redirect("/dashboard.html")

def handle_database_get(segment: str):
    return render_template(f"home/{segment}", segment=segment, dbinfo={})

def handle_dashboard_get(segment: str):
    if session.get("db_name"):
        dbinfo = database.get_db_info(session)
        return render_template("home/dashboard.html", segment=segment, dbinfo=dbinfo)
    else:
        return redirect("/database.html")

def handle_topqueries_get(template: str, segment: str, tablename: str = None):


    if session.get("db_name"):
        
        # get optional tablename parameter from URL
        if tablename is None:
            tablename = request.args.get('tablename')  

        # get top queries
        rows = database.get_top_queries(session)

        # add additional information on queries
        for row in rows:            
            row['tables'] = sqlhelper.get_tables(row['query'])
            row['operation_type'] = sqlhelper.get_sql_type(row['query'])

        # Get PostgreSQL internal tables
        pga_tables = database.get_pga_tables()

        # Filter queries to ignore system tables
        rows_filtered = [row for row in rows if not any(table in pga_tables for table in row['tables'])]

        # Filter even more if 'tablename' is provided
        if tablename:
            rows_filtered = [row for row in rows_filtered if tablename in row['tables']]

        # Render the template with the filtered data
        return render_template(f"home/{template}", segment=segment, rows=rows_filtered, tablename=tablename,column_descriptions=pgstat_helper.PGSS_COLUMN_DOCS)

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
    if session.get("db_name"):
            query_rows,description=database.generic_select(session,"issue_no_pk")
            return render_template("home/primary_key.html", rows=query_rows, segment=segment, description=description )
    else:
        return redirect("/database.html")

def handle_table_rfc_get(template: str, segment: str):
    if session.get("db_name"):
            query_rows,description=database.generic_select(session,"table_size")
            return render_template("home/tables_cards.html", tables=query_rows, segment="tables_cards.html" )
    else:
        return redirect("/database.html")

def handle_indexes_get(template: str, segment: str):
    if session.get("db_name"):
        return render_template("home/indexes.html", segment=segment)
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
    if session.get("db_name"):
            query_rows,description=database.generic_select(session,"hit_cache_by_table")
            for row in query_rows:
                # check and convert 'table_cache_hit_ratio'
                try:
                    row['table_cache_hit_ratio'] = float(row['table_cache_hit_ratio'])
                except (ValueError, TypeError):
                    row['table_cache_hit_ratio'] = 0  # Valeur invalide remplacée par None


                # check and convert 'index_cache_hit_ratio'
                try:
                    row['index_cache_hit_ratio'] = float(row['index_cache_hit_ratio'])
                except (ValueError, TypeError):
                    row['index_cache_hit_ratio'] = 0  # Invalid value replaced by 0          
            return render_template("home/cache_table.html", rows=query_rows, segment=segment, description=description )
    else:
        return redirect("/database.html")

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

def get_segment(request):
    try:
        segment = request.path.split('/')[-1]
        if segment == '':
            segment = 'database'
        return segment
    except:
        return None
