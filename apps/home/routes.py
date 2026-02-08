# -*- encoding: utf-8 -*-
"""
Main routes
"""
import traceback
import os
import math 
from apps.home import blueprint
from flask import render_template, request, session,redirect, jsonify, Response
from jinja2 import TemplateNotFound
from . import database
from . import dbanalyze
from . import llm
from . import pgtune
from . import sqlhelper
from . import stats
from . import ddl
from . import sqlcolumns
from . import analyze_aquery
from . import config
from . import reporting
from . import action
import re
import requests
import json

config.init_or_load_env()

def handle_database_post(segment: str):
    dbinfo = {}
    session.permanent = True
    for key, val in request.form.items():
        session[key] = val

    dbinfo = database.get_db_info(session)

    if "error" in dbinfo:
        return render_template(f"home/{segment}", segment=segment, dbinfo=dbinfo)  

    session['version']=database.get_pg_major_version(dbinfo['version'])
    session.modified = True

    return render_template("home/dashboard.html", segment="dashboard.html", dbinfo=dbinfo)

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
        return render_template(f"home/{template}", segment=segment, rows=rows_filtered, tablename=tablename)

    else:
        return redirect("/database.html")
    
def handle_rank_queries_get(template: str, segment: str):
    if session.get("db_name"):
        rows = database.get_rank_queries(session)
        
        i=0
        table_stats=[]
        for query in rows:            
            tables = sqlhelper.get_tables(query['query'])
            for table  in tables:
                stats.add_or_update_table_info(table_stats,
                                               table, 
                                               query['calls'], 
                                               query['mean_exec_time'],
                                               query['rows'],
                                               'select',
                                               []
                                               )
            rows[i]['tables']=tables
            i = i + 1   
        pga_tables=database.get_pga_tables()
        rows_filtered=[]
        for row in rows:
            filtered=False
            for table in row ['tables']:
                if table in pga_tables:
                    filtered=True
            if not row ['tables']:
                filtered=True    
            if not filtered:
                rows_filtered.append(row)
        
        return render_template(f"home/{template}", segment=segment, rows=rows_filtered)
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
            query_rows,description=database.generic_select(session,"table_list")
            return render_template("home/table_rfc.html", rows=query_rows, segment=segment, description=description )
    else:
        return redirect("/database.html")

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

def handle_reset_pg_statistics():
    database.exec_cmd(session, "pg_stat_statements_reset")
    rows = database.get_top_queries(session)
    return render_template("home/topqueries.html", segment="topqueries.html", rows=rows)

def handle_enable_pg_statistics():
    database.exec_cmd(session, "pg_stat_statements_enable")
    rows = database.get_top_queries(session)
    return render_template("home/topqueries.html", segment="topqueries.html", rows=rows)

def handle_lint_post():
    original_sql = request.form.get('sqlo')
    
    return render_template("home/lint.html", segment="lint.html",
                           sqlo=original_sql, linted=sqlhelper.format_sql(original_sql))

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


@blueprint.route('/index')
def index():
    return redirect("/database.html")

@blueprint.route('/generic/<genericid>', methods=['GET', 'POST'])
def generic(genericid):
    try:
        if session.get("db_name"):
            segment=genericid
            query_rows,description=database.generic_select(session,genericid)
            return render_template("home/generic.html", rows=query_rows, segment=segment, genericid=genericid, description=description )
        else:
            return redirect("/database.html")
    except Exception as e1:
        traceback.print_exc()
        return render_template('home/page-500.html', err=e1), 500

@blueprint.route('/generic_param/<genericid>', methods=['GET', 'POST'])
def generic_param(genericid):
    try:
        if session.get("db_name"):
            rows = []
            query = database.get_query_by_id(genericid)
            sql_query = query['sql']
            # extract parameters list
            pattern = r'\$[0-9]+' 
            parameters = re.findall(pattern, query['sql'])

            if request.method == 'POST':
                for key, val in request.form.items():
                    sql_query = sql_query.replace (key,val)
                rows = database.generic_select_with_sql(session,sql_query)

            return render_template('home/generic_param.html', parameters=parameters, query=sql_query, rows=rows, description=query['description'] )
        else:
            redirect("/database.html")
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e1:
        traceback.print_exc()
        return render_template('home/page-500.html', err=e1), 500

@blueprint.route('/analyze/<querid>', methods=['GET', 'POST'])
def analyze_query(querid):
    try:
        chatgpt = ""
        genius_parameters = {}
        if session.get("db_name"):
            rows = []
            tables_and_columns = {}
            statistics = {}
            sql_query = database.get_pgstat_query_by_id(session,querid)
            # format SQL
            ##sql_query = sqlhelper.get_formated_sql(sql_query)
            tables = sqlhelper.get_tables(sql_query)
            
            # extract parameters list
            pattern = r'\$[0-9]+' 
            parameters = re.findall(pattern, sql_query)
            parameters = sorted(set(parameters), key=lambda p: int(p[1:]))

            if request.method == 'POST':

                params = {}
                for key, val in request.form.items():
                # Verify parameters ($1, $2, ...)
                    if key.startswith('$'):
                        param_index = int(key[1:])  # Convert '$1' to 1
                        if val is None or val.strip()=='':
                            val='NULL'
                        params[param_index] = val  # Add to dictionnary

                sql_query=sqlhelper.replace_query_parameters(sql_query,params)
                sql_query_analyze = f"EXPLAIN (ANALYZE, BUFFERS, WAL, VERBOSE, SETTINGS, FORMAT JSON) {sql_query}"

                if request.form.get('action')=='chatgpt':
                    rows = database.generic_select_with_sql(session,sql_query_analyze)
                    chatgpt = llm.get_llm_query_for_query_analyze(db_config=session, sql_query=sql_query_analyze, rows=rows, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
                    try:
                        chatgpt_response=llm.query_chatgpt(chatgpt)
                        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(chatgpt))
                    except Exception as e1:
                        traceback.print_exc()
                        return render_template('home/page-500.html', err=e1), 500
                elif request.form.get('action')=='analyze':                   
                    parameters = {}
                    rows = database.generic_select_with_sql(session,sql_query_analyze)
                    chatgpt = llm.get_llm_query_for_query_analyze(db_config=session,sql_query=sql_query_analyze, rows=rows, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])

                    statistics = dbanalyze.decode_explain_json_with_buffers(rows[0]["QUERY PLAN"], include_top_nodes=True, top_n=20)

                    # Get more informations on query
                    #existing_indexes = database.get_existing_indexes(session)
                    #analyzed_query = analyze_aquery.analyze_table_conditions(sql_query)
                    #tables_and_columns =  analyze_aquery.check_index_coverage(existing_indexes,analyzed_query)
                elif request.form.get('action')=='optimize':
                    question_optimize=llm.get_llm_query_for_query_optimize(sql_query)
                    try:
                        chatgpt_response=llm.query_chatgpt(question_optimize)
                        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response)
                    except Exception as e1:
                        traceback.print_exc()
                        return render_template('home/page-500.html', err=e1), 500
                elif request.form.get('action')=='ddl':
                    tables=sqlhelper.get_tables(sql_query)
                    sql_text=ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
                    sql_text=ddl.sql_to_html(sql_text)
                    return render_template('home/ddl.html', sql_text=sql_text, tables=tables, query=sql_query)
            else:
                # try to extract parameters from query
                genius_parameters=sqlhelper.get_genius_parameters(sql_query,session)

            def fmt_ms(x):
                if x is None:
                    return "—"
                return f"{x:.3f} ms" if x < 1000 else f"{x/1000:.3f} s"

            def fmt_pct(x):
                if x is None:
                    return "—"
                return f"{x:.2f}%"

            def fmt_int(x):
                if x is None:
                    return "—"
                # self_rows can be float in EXPLAIN ANALYZE
                if isinstance(x, (int, float)) and not math.isnan(x):
                    return str(int(round(x)))
                return str(x)

            return render_template('home/analyze.html', parameters=parameters, query=sql_query, rows=rows, 
                                   description='Analyze query',chatgpt=chatgpt, tables=tables, 
                                   genius_parameters=genius_parameters, analyze_explain_row=sqlhelper.analyze_explain_row, 
                                   result=statistics, fmt_ms=fmt_ms, fmt_pct=fmt_pct, fmt_int=fmt_int)
        else:
            dbinfo= {}
            return redirect("/database.html")
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e1:
        traceback.print_exc()
        return render_template('home/page-500.html', err=e1), 500

@blueprint.route("/execute", methods=["POST"])
def execute_sql():
    try:
        sql = request.json.get("sql")
        if not sql:
            return jsonify({"error": "No SQL clause provided", "success": False})
        
        con, _ = database.connectdb(session)
        result = database.db_exec_recommandation(con,sql)

        return jsonify(result)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        con.close()   

@blueprint.route('/api/v1/fetch_column_data', methods=['POST'])
def fetch_column_data_route():
    """
    Flask route to fetch data from a column in a table.
    Expects a JSON payload with 'table', 'column', and 'data_type'.
    """
    try:
        # Parse the request payload
        payload = request.json
        table = payload.get('table')
        column = payload.get('column')
        data_type = payload.get('data_type')

        if not table or not column or not data_type:
            return jsonify({"error": "Missing required parameters (table, column, data_type)."}), 400

        # Call the fetch_column_data function
        result = sqlhelper.fetch_column_data(table, column, data_type, session)

        # Return the result as JSON
        return jsonify({"data": result})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint.route("/api/v1/llm_get_models", methods=["POST"])
def llm_get_models():
    data = request.get_json()
    llm_uri = data.get("llm_uri", "").rstrip("/")
    api_key = data.get("llm_api_key", "").strip()

    if not llm_uri:
        return jsonify({"error": "Missing LLM URI"}), 400

    models_url = f"{llm_uri}/models"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        response = requests.get(models_url, headers=headers, timeout=10)
        response.raise_for_status()
        models_data = response.json()

        model_names = [m.get("id") for m in models_data.get("data", []) if "id" in m]

        return jsonify({"models": model_names})

    except Exception as e:
        return jsonify({"error": "Could not fetch models"}), 500

@blueprint.route("/api/v1/report", methods=["POST"])
def api_database_report():
    try:
        # read JSON POST
        data = request.get_json(force=True)

        # check db_config
        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]

        # Check keys in db_config
        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        # Generate report
        database_reports = reporting.get_database_report(
            db_config,
            report_yaml_definition_file="./reporting.yml",
            template_folder="db_report_templates"
        )
        if not database_reports:
            return jsonify({"error": "No report generated"}), 500
        return Response(database_reports, mimetype="text/markdown; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@blueprint.route("/dba_report", methods=["GET"])
def dba_database_report():
    try:
        # Generate report
        database_reports = reporting.get_database_report(
            session,
            report_yaml_definition_file="./reporting.yml",
            template_folder="db_report_templates"
        )
        if not database_reports:
            raise Exception("No report generated")
        html_report = llm.render_markdown(database_reports)
        return render_template('home/report.html', report=html_report, segment='dba_report')

    except Exception as e1:
        return render_template('home/page-500.html', err=e1), 500
    
@blueprint.route("/api/v1/pg_stat_statements_reset", methods=["POST"])
def api_reset_stats():
    try:
        # read JSON POST
        data = request.get_json(force=True)

        # check db_config
        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400
        db_config = data["db_config"]

        # Check keys in db_config
        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        # Reset pg_stat_statements statistics
        database.exec_cmd(db_config, "pg_stat_statements_reset")

        return Response("pg_stat_statements statistics are reset", mimetype="text/markdown; charset=utf-8")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@blueprint.route("/api/v1/apply_recommandations", methods=["POST"])
def api_apply_recommendations():
    try:
        # read JSON POST
        data = request.get_json(force=True)

        # 1) Validate db_config
        if not data or "db_config" not in data:
            return jsonify({"error": "Missing 'db_config' in request body"}), 400

        db_config = data["db_config"]

        # --- Validate unique_name ---
        if "unique_name" not in data or not data["unique_name"]:
            return jsonify({"error": "Missing or empty 'unique_name'"}), 400
        unique_name = data["unique_name"]

        # --------------------------
        required_keys = ["db_host", "db_port", "db_name", "db_user", "db_password"]
        missing = [k for k in required_keys if k not in db_config]
        if missing:
            return jsonify({"error": f"Missing keys in db_config: {', '.join(missing)}"}), 400

        # 2) Validate dryrun (optional but must be boolean if provided)
        dryrun = data.get("dryrun", True)
        if not isinstance(dryrun, bool):
            return jsonify({"error": "'dryrun' must be a boolean"}), 400

        # 3) Validate run_recommandations
        allowed_reco = {
            "create_missing_fk_indexes",
            "drop_duplicate_indexes",
            "alter_table_columns_on_fk",
            "drop_unused_indexes",
            "all",
        }
        run_recommandations = data.get("run_recommandations", None)

        if run_recommandations is None:
            return jsonify({"error": "Missing 'run_recommandations'"}), 400

        if not isinstance(run_recommandations, list):
            return jsonify({"error": "'run_recommandations' must be a list"}), 400

        unknown = [x for x in run_recommandations if x not in allowed_reco]
        if unknown:
            return jsonify({"error": f"Unknown recommendation(s): {', '.join(unknown)}"}), 400

        # if "all" is present → expand to all
        if "all" in run_recommandations:
            run_recommandations = list(allowed_reco - {"all"})

        # 4) Run actions
        executed_sql, errors = action.run_actions(db_config, unique_name=unique_name, dry_mode=dryrun)
        result = {
            "dryrun": dryrun,
            "run_recommandations": run_recommandations,
            "message": "Recommendations executed." if not errors else "Errors occurred while executing recommendations.",
            "executed_sql": executed_sql,
            "errors": errors
        }

        # If errors exist → return same payload but as an error
        if errors:
            return Response(
                json.dumps(result, indent=2),
                mimetype="application/json; charset=utf-8",
                status=400   # or 500 depending on what you prefer
            )

        # Otherwise → success
        return Response(
            json.dumps(result, indent=2),
            mimetype="application/json; charset=utf-8",
            status=200
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@blueprint.route('/<template>', methods=['GET', 'POST'])
def route_template(template: str):
    try:
        if not template.endswith('.html'):
            template += '.html'
        
        segment = get_segment(request)
        tablename = request.args.get('tablename')  # None if no parameter defined
        
        if segment == "database.html" and request.method == 'POST':
            return handle_database_post(segment)
        elif segment == "database.html":
            return handle_database_get(segment)
        elif segment == "dashboard.html" and request.method == 'GET':
            return handle_dashboard_get(segment)
        elif segment == "topqueries.html" and request.method == 'GET':
            return handle_topqueries_get(template, segment,tablename)
        elif segment == "rankqueries.html" and request.method == 'GET':
            return handle_rank_queries_get(template, segment)
        elif segment == "stats.html" and request.method == 'GET':
            return handle_topstatistics_get(template, segment)
        elif segment == "reset_pg_statistics.html":
            return handle_reset_pg_statistics()
        elif segment == "reset_pg_stat.html":
            return handle_reset_pg_stat()
        elif segment == "enable_pg_statistics.html":
            return handle_enable_pg_statistics()
        elif segment == "lint.html" and request.method == 'POST':
            return handle_lint_post()
        elif segment == "search.html" and request.method == 'POST':
            return handle_search_post()
        elif segment == "pgtune.html" and request.method == 'POST':
            return handle_pgtune_post()
        elif segment == "myqueries.html":
            return handle_myqueries_get()
        elif segment == "primary_key.html" and request.method == 'GET':
            return handle_primarykey_get(template, segment)
        elif segment == "table_rfc.html"  and request.method == 'GET':
            return handle_table_rfc_get(template, segment)
        elif segment == "cache_table.html" and request.method == 'GET':
            return handle_cache_table_get(template, segment)
        elif segment == "llm.html" and request.method == 'GET':
            return render_template(
                f"home/{template}",
                segment=segment,
                llm_uri=config.get_config_value("LOCAL_LLM_URI"),
                llm_api_key=config.get_config_value("OPENAI_API_KEY"),
                llm_model=config.get_config_value("OPENAI_API_MODEL"),
                llm_sql_guidelines=config.get_config_value("LLM_SQL_GUIDELINES")
)
        elif segment == "llm.html" and request.method == 'POST':
            llm_uri = request.form.get("llm_uri")
            llm_api_key = request.form.get("llm_api_key")
            llm_model = request.form.get("llm_model")
            llm_sql_guidelines = request.form.get("llm_sql_guidelines", "")

            config.update_llm_config(llm_uri=llm_uri, llm_api_key=llm_api_key, llm_model=llm_model, llm_sql_guidelines=llm_sql_guidelines)

            return render_template(f"home/{template}", segment=segment, llm_uri=llm_uri, llm_api_key=llm_api_key, llm_model=llm_model,llm_sql_guidelines=llm_sql_guidelines)
        return render_template(f"home/{template}", segment=segment, dbinfo={})
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e:
        traceback.print_exc()
        return render_template('home/page-500.html', err=str(e)), 500

@blueprint.route('/primary_key_llm/<schema>/<tablename>', methods=['GET','POST'])
def llm_primary_key(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    llm_prompt = llm.generate_primary_key_prompt(table_name=f"{schema}.{tablename}",ddl=ddl_str)
    if request.method == 'GET':
        return render_template('home/primary_key_llm.html', segment='primary_key_llm.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", llm_prompt=llm_prompt, title=f"Find a primary key for {schema}.{tablename}")
    else:
        try:
            chatgpt_response=llm.query_chatgpt(llm_prompt)
        except Exception as e:
            traceback.print_exc()
            return render_template('home/page-500.html', err=e), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt))        

@blueprint.route('/table_llm/<schema>/<tablename>', methods=['GET','POST'])
def llm_table(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    llm_prompt = llm.analyze_table_format(ddl=ddl_str)
    if request.method == 'GET':
        return render_template('home/primary_key_llm.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", llm_prompt=llm_prompt, title=f"Analyze table definition for {schema}.{tablename}")
    else:
        try:
            chatgpt_response=llm.query_chatgpt(llm_prompt)
        except Exception as e:
            traceback.print_exc()
            return render_template('home/page-500.html', err=e), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt))

@blueprint.route('/table_llm_guidelines/<schema>/<tablename>', methods=['GET','POST'])
def llm_table_guidelines(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    llm_prompt = llm.analyze_with_sql_quide(ddl=ddl_str, guidelines=config.get_config_value("LLM_SQL_GUIDELINES"))
    if request.method == 'GET':
        return render_template('home/primary_key_llm.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", llm_prompt=llm_prompt, title=f"Analyze SQL conventions for {schema}.{tablename}")
    else:
        try:
            chatgpt_response=llm.query_chatgpt(llm_prompt)
        except Exception as e:
            traceback.print_exc()
            return render_template('home/page-500.html', err=e), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt))        

@blueprint.route('/table_tetris/<schema>/<tablename>', methods=['GET'])
def tetris_table(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    
    tetris_sql = database.get_query_by_id('tetris_play')
    tetris_sql = tetris_sql['sql'].replace('$1', schema).replace('$2', tablename)
    tetris_result = database.generic_select_with_sql(session, tetris_sql)
    tetris_result_sql = tetris_result[0]['create_table_tetris_ddl']
    
    tetris_result_sql = tetris_result_sql.replace("\\n", "\n")
    tetris_result_sql=ddl.sql_to_html(tetris_result_sql)
    
    return render_template('home/tetris.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", tetris=tetris_result_sql, title=f"Postgres column Tetris for {schema}.{tablename}")
      


# Helper - Extract current page name from request
def get_segment(request):
    try:
        segment = request.path.split('/')[-1]
        if segment == '':
            segment = 'database'
        return segment
    except:
        return None
