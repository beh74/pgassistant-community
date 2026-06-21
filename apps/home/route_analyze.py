# -*- encoding: utf-8 -*-
"""Routes related to query analysis."""

import json
import math
import re
import traceback

from apps.home import blueprint
from flask import render_template, request, session, redirect
from jinja2 import TemplateNotFound

from . import analyze_advisor
from . import database
from . import dbanalyze
from . import ddl
from . import graph
from . import llm
from . import sqlhelper

@blueprint.route('/analyze/<querid>', methods=['GET', 'POST'])
def analyze_query(querid):
    try:
        chatgpt = ""
        genius_parameters = {}
        if session.get("db_name"):
            rows = []
            tables_and_columns = {}
            statistics = {}
            mermaid_code = None
            queryplan = None
            plan_text = None
            advisor_result = None
            column_statistics = None

            # Clear any previous analyze-derived table list when opening a new query page (optional but recommended)
            prev_qid = session.get("analyze_querid")
            if prev_qid != querid:
                session.pop("analyze_tables", None)
                session.pop("full_query", None)
                session["analyze_querid"] = querid

            # get informations from pg_stat_statements by queryid
            sql_query = database.get_pgstat_query_by_id(session, querid)

            # format SQL
            # sql_query = sqlhelper.get_formated_sql(sql_query)
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
                        if val is None or val.strip() == '':
                            val = 'NULL'
                        params[param_index] = val  # Add to dictionary

                sql_query = sqlhelper.replace_query_parameters(sql_query, params)
                
                sql_query_analyze = f"EXPLAIN (ANALYZE, BUFFERS, WAL, VERBOSE, SETTINGS, FORMAT JSON) {sql_query}"
                
                # generic plan
                #sql_query = database.get_pgstat_query_by_id(session, querid)
                #sql_query_analyze = f" EXPLAIN (GENERIC_PLAN, VERBOSE,  SETTINGS, FORMAT JSON)  {sql_query}"
                #print("SQL for GENERIC PLAN:", sql_query_analyze)

                if request.form.get('action') == 'analyze':
                    parameters = {}
                    rows = database.generic_select_with_sql(session, sql_query_analyze)

                    session["full_query"] = sql_query  # Store the full query with parameters in session

                    # get statistics from EXPLAIN ANALYZE result
                    queryplan = rows[0]["QUERY PLAN"]

                    try:
                        advisor_result = analyze_advisor.analyze_plan_for_safe_indexes(
                            queryplan, session, querid
                        )

                        # ------------------------------------------------------------
                        # SORT ONLY BY CONFIDENCE (safe > review > none)
                        # ------------------------------------------------------------
                        if advisor_result and advisor_result.get("recommendations"):

                            priority = {
                                "safe": 0,
                                "review": 1,
                                "none": 2,
                            }
                            advisor_result["recommendations"] = sorted(
                                advisor_result["recommendations"],
                                key=lambda r: priority.get(r.get("confidence"), 99)
                            )
                        #analyze_advisor.pretty_print_analysis(advisor_result)
                        column_statistics=analyze_advisor.get_columns_statistics(advisor_result)

                    except Exception as e:
                        advisor_result = None
                        print("Error during advisor analysis:", e)

                    if isinstance(queryplan, str):
                        plan_text = queryplan
                    else:
                        plan_text = json.dumps(queryplan, indent=2, ensure_ascii=False)

                    statistics = dbanalyze.decode_explain_json_with_buffers(
                        rows[0]["QUERY PLAN"],
                        include_top_nodes=True,
                        top_n=20
                    )

                    # ✅ Store the table list from decode_explain_json_with_buffers into session
                    # dbanalyze.tables_from_decode_stats(stats) returns ["schema.table", ...]
                    tables_from_analyze = dbanalyze.tables_from_decode_stats(statistics)
                    tables_from_sql = sqlhelper.get_tables(sql_query)
                    tables = dbanalyze.union_tables(tables_from_analyze, tables_from_sql)
                    session["analyze_tables"] = tables  # Store the table list in session for later use (e.g., LLM, DDL)

                    chatgpt = llm.get_llm_query_for_query_analyze(
                        db_config=session,
                        sql_query=sql_query_analyze,
                        rows=rows,
                        database=session['db_name'],
                        host=session["db_host"],
                        user=session["db_user"],
                        port=session["db_port"],
                        password=session["db_password"],
                        table_genius=tables,
                        column_statistics=column_statistics
                    )

                    # generate mermaid code
                    mermaid_code, err = graph.build_mermaid_erd_from_explain_stats(statistics, session)
                elif request.form.get('action') == 'chatgpt':
                    posted_prompt = (request.form.get("chatgpt") or "").strip()
                    

                    try:
                        chatgpt_response = llm.query_chatgpt(posted_prompt)
                        return render_template(
                            'home/chatgpt.html',
                            chatgpt_response=chatgpt_response,
                            chatgpt_query=llm.render_markdown(posted_prompt)
                        )
                    except Exception as e1:
                        tb = traceback.format_exc()
                        print(tb)
                        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500

                elif request.form.get('action') == 'ddl':
                    # ✅ Use the same table list as the LLM (derived from ANALYZE if available)
                    tables_from_analyze = session.get("analyze_tables")
                    tables_from_sql = sqlhelper.get_tables(sql_query)
                    tables = dbanalyze.union_tables(tables_from_analyze, tables_from_sql)

                    sql_text = ddl.generate_tables_ddl(
                        tables=tables,
                        database=session['db_name'],
                        host=session["db_host"],
                        user=session["db_user"],
                        port=session["db_port"],
                        password=session["db_password"]
                    )
                    sql_text = ddl.sql_to_html(sql_text)
                    return render_template('home/ddl.html', sql_text=sql_text, tables=tables, query=sql_query)

            else:
                # try to extract parameters from query
                try:
                    genius_parameters = sqlhelper.get_genius_parameters(sql_query, session)
                    #print("Genius parameters extracted:", genius_parameters)
                except Exception:
                    genius_parameters = []

            def fmt_ms(x):
                if x is None:
                    return "—"
                return f"{x:.2f} ms" if x < 1000 else f"{x/1000:.2f} s"

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

            return render_template(
                'home/analyze.html',
                parameters=parameters,
                query=sql_query,
                rows=rows,
                description='Analyze query',
                chatgpt=chatgpt,
                tables=tables,
                genius_parameters=genius_parameters,
                analyze_explain_row=sqlhelper.analyze_explain_row,
                result=statistics,
                fmt_ms=fmt_ms,
                fmt_pct=fmt_pct,
                fmt_int=fmt_int,
                mermaid_code=mermaid_code,
                advisor_result=advisor_result,
                queryplan=plan_text
            )
        else:
            dbinfo = {}
            return redirect("/database.html")
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500
