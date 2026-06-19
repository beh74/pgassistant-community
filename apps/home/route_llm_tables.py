# -*- encoding: utf-8 -*-
"""Routes for table-level LLM helpers and table rewrite tools."""

import traceback

from apps.home import blueprint
from flask import render_template, request, session

from . import config
from . import database
from . import ddl
from . import graph_table
from . import llm
from . import tetris


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
        except Exception as e1:
            tb = traceback.format_exc()
            print(tb)
            return render_template('home/page-500.html', err=e1, traceback_text=tb), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt))        

@blueprint.route('/table_llm/<schema>/<tablename>', methods=['GET','POST'])
def llm_table(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    llm_prompt = llm.analyze_table_format(ddl=ddl_str)
    if request.method == 'GET':
        mermaid_graph=graph_table.generate_mermaid_table_dependencies_erdiagram(session, f"{schema}.{tablename}")
        return render_template('home/primary_key_llm.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", llm_prompt=llm_prompt, mermaid_code=mermaid_graph, title=f"Analyze table definition for {schema}.{tablename}")
    else:
        try:
            chatgpt_response=llm.query_chatgpt(llm_prompt)
        except Exception as e1:
            tb = traceback.format_exc()
            print(tb)
            return render_template('home/page-500.html', err=e1, traceback_text=tb), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt), title=f"Analyze table definition for {schema}.{tablename}") 

@blueprint.route('/table_llm_guidelines/<schema>/<tablename>', methods=['GET','POST'])
def llm_table_guidelines(schema: str, tablename:str):
    tables = []
    tables.append (f"{schema}.{tablename}")
    ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
    llm_prompt = llm.analyze_with_sql_quide(ddl=ddl_str, guidelines=config.get_config_value("LLM_SQL_GUIDELINES"))
    if request.method == 'GET':
        mermaid_graph=graph_table.generate_mermaid_table_dependencies_erdiagram(session, f"{schema}.{tablename}")
        return render_template('home/primary_key_llm.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", mermaid_code=mermaid_graph, llm_prompt=llm_prompt, title=f"Analyze SQL conventions for {schema}.{tablename}")
    else:
        try:
            chatgpt_response=llm.query_chatgpt(llm_prompt)
        except Exception as e1:
            tb = traceback.format_exc()
            print(tb)
            return render_template('home/page-500.html', err=e1, traceback_text=tb), 500
        return render_template('home/chatgpt.html', chatgpt_response=chatgpt_response, chatgpt_query=llm.render_markdown(llm_prompt), title=f"Analyze SQL conventions for {schema}.{tablename}")        

@blueprint.route('/table_tetris/<schema>/<tablename>', methods=['GET'])
def tetris_table(schema: str, tablename:str):
    try:
        tables = []
        tables.append (f"{schema}.{tablename}")
        ddl_str = ddl.generate_tables_ddl(tables=tables, database=session['db_name'], host=session["db_host"], user=session["db_user"],port=session["db_port"],password=session["db_password"])
        
        tetris_sql = database.get_query_by_id('tetris_play')
        tetris_sql = tetris_sql['sql'].replace('$1', schema).replace('$2', tablename)
        tetris_result = database.generic_select_with_sql(session, tetris_sql)
        tetris_result_sql = "-- Create Tetris table DDL and copy source data\n" +tetris_result[0]['create_table_tetris_ddl']
        tetris_result_sql = tetris_result_sql.replace("\\n", "\n")
        tetris_result_sql += "\n\n" + "-- Alter table with constraints and indexes\n" + tetris.extract_post_create_ddl(ddl_str, schema, tablename)
        tetris_result_sql += "\n\n" + """
    -- pgAssistant notice:
    -- The final table swap (dropping the original table and renaming the _tetris table)
    -- is NOT automatically generated.
    --
    -- Renaming a table may NOT have the expected effect:
    -- dependencies such as foreign keys, views, or application references
    -- may still point to the original table (renamed), not the new one.
    --
    -- You may need to:
    --   - drop and recreate foreign keys
    --   - drop and recreate dependent views
    --   - validate application dependencies
    --
    -- Please review and execute the final migration steps manually.
    """
        tetris_result_sql=ddl.sql_to_html(tetris_result_sql)
        
        return render_template('home/tetris.html', sql_text=ddl.sql_to_html(ddl_str), table_name=f"{schema}.{tablename}", tetris=tetris_result_sql, title=f"Postgres column Tetris for {schema}.{tablename}")
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500
