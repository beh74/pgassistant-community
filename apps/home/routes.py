# -*- encoding: utf-8 -*-
"""Main HTML route dispatcher.

Domain-specific routes are registered by importing their modules below.
"""

import re
import traceback

from apps.home import blueprint
from flask import render_template, request, redirect, session
from jinja2 import TemplateNotFound

from . import config
from . import database
from . import llm
from . import pgstat_helper
from .routes_helpers import (
    get_segment,
    handle_cache_table_get,
    handle_database_analyze_llm_get,
    handle_database_analyze_llm_post,
    handle_dashboard_get,
    handle_database_get,
    handle_database_post,
    handle_enable_pg_statistics,
    handle_lint_post,
    handle_indexes_get,
    handle_myqueries_get,
    handle_pgtune_post,
    handle_primarykey_get,
    handle_query_index_advisor_get,
    handle_rank_queries_get,
    handle_reset_pg_stat,
    handle_reset_pg_statistics,
    handle_search_post,
    handle_table_rfc_get,
    handle_topqueries_get,
    handle_topstatistics_get,
)

config.init_or_load_env()

# Import route modules so they are registered on the shared blueprint.
from . import route_api  # noqa: F401,E402
from . import route_analyze  # noqa: F401,E402
from . import route_llm_tables  # noqa: F401,E402
from . import route_reports  # noqa: F401,E402


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
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500

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
            return redirect("/database.html")
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500

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
        elif segment == "query_index_advisor.html" and request.method == 'GET':
            return handle_query_index_advisor_get(template, segment)
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
        elif segment == "tables_cards.html"  and request.method == 'GET':
            return handle_table_rfc_get(template, segment)
        elif segment == "indexes.html" and request.method == 'GET':
            return handle_indexes_get(template, segment)
        elif segment == "database_analyze_llm.html" and request.method == 'GET':
            return handle_database_analyze_llm_get(template, segment)
        elif segment == "database_analyze_llm.html" and request.method == 'POST':
            return handle_database_analyze_llm_post(template, segment)
        elif segment == "cache_table.html" and request.method == 'GET':
            return handle_cache_table_get(template, segment)
        elif segment == "llm.html" and request.method == 'GET':
            return render_template(
                f"home/{template}",
                segment=segment,
                llm_uri=config.get_config_value("LOCAL_LLM_URI"),
                llm_api_key=config.get_config_value("OPENAI_API_KEY"),
                llm_model=config.get_config_value("OPENAI_API_MODEL"),
                llm_sql_guidelines=config.get_config_value("LLM_SQL_GUIDELINES"),
                llm_table_rfc_prompt_template=llm.get_configured_table_prompt_template(
                    "LLM_TABLE_RFC_PROMPT_TEMPLATE",
                    llm.get_default_table_rfc_prompt_template(),
                ),
                llm_table_naming_prompt_template=llm.get_configured_table_prompt_template(
                    "LLM_TABLE_NAMING_PROMPT_TEMPLATE",
                    llm.get_default_table_naming_prompt_template(),
                ),
                default_table_rfc_prompt_template=llm.get_default_table_rfc_prompt_template(),
                default_table_naming_prompt_template=llm.get_default_table_naming_prompt_template(),
            )
        elif segment == "llm.html" and request.method == 'POST':
            llm_uri = request.form.get("llm_uri")
            llm_api_key = request.form.get("llm_api_key")
            llm_model = request.form.get("llm_model")
            llm_sql_guidelines = request.form.get("llm_sql_guidelines", "")
            llm_table_rfc_prompt_template = request.form.get(
                "llm_table_rfc_prompt_template", ""
            )
            llm_table_naming_prompt_template = request.form.get(
                "llm_table_naming_prompt_template", ""
            )

            if not llm_table_rfc_prompt_template.strip():
                llm_table_rfc_prompt_template = llm.get_default_table_rfc_prompt_template()
            if not llm_table_naming_prompt_template.strip():
                llm_table_naming_prompt_template = llm.get_default_table_naming_prompt_template()

            try:
                llm.validate_table_prompt_template(llm_table_rfc_prompt_template)
                llm.validate_table_prompt_template(llm_table_naming_prompt_template)
            except ValueError as prompt_error:
                return render_template(
                    f"home/{template}",
                    segment=segment,
                    llm_uri=llm_uri,
                    llm_api_key=llm_api_key,
                    llm_model=llm_model,
                    llm_sql_guidelines=llm_sql_guidelines,
                    llm_table_rfc_prompt_template=llm_table_rfc_prompt_template,
                    llm_table_naming_prompt_template=llm_table_naming_prompt_template,
                    default_table_rfc_prompt_template=llm.get_default_table_rfc_prompt_template(),
                    default_table_naming_prompt_template=llm.get_default_table_naming_prompt_template(),
                    llm_settings_error=str(prompt_error),
                )

            config.update_llm_config(
                llm_uri=llm_uri,
                llm_api_key=llm_api_key,
                llm_model=llm_model,
                llm_sql_guidelines=llm_sql_guidelines,
                llm_table_rfc_prompt_template=llm_table_rfc_prompt_template,
                llm_table_naming_prompt_template=llm_table_naming_prompt_template,
            )

            return render_template(
                f"home/{template}",
                segment=segment,
                llm_uri=llm_uri,
                llm_api_key=llm_api_key,
                llm_model=llm_model,
                llm_sql_guidelines=llm_sql_guidelines,
                llm_table_rfc_prompt_template=llm_table_rfc_prompt_template,
                llm_table_naming_prompt_template=llm_table_naming_prompt_template,
                default_table_rfc_prompt_template=llm.get_default_table_rfc_prompt_template(),
                default_table_naming_prompt_template=llm.get_default_table_naming_prompt_template(),
            )
        return render_template(f"home/{template}", segment=segment, dbinfo={})
    except TemplateNotFound:
        return render_template('home/page-404.html'), 404
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500

@blueprint.route("/tools")
def tools():
    selected_category = request.args.get("cat")  # e.g. /tools?cat=database
    return render_template("home/tools.html", selected_category=selected_category)
