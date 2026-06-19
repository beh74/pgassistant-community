# -*- encoding: utf-8 -*-
"""Routes for reports and global advisor pages."""

import traceback

from apps.home import blueprint
from flask import jsonify, render_template, session

from . import database
from . import global_advisor
from . import llm
from . import reporting

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
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=e1, traceback_text=tb), 500

@blueprint.route('/global/advisor', methods=['GET'])
def global_advisor_route():
    try:
        
        result = global_advisor.run_global_advisor(session, yaml_path="advisor_enriched.yml")
        return render_template('home/advisor_summary_tabs.html', segment='global_advisor.html', recommendations=result["recommendations"])
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return jsonify({"error": str(e1), "traceback": tb}), 500

@blueprint.route('/global/table_health', methods=['GET'])
def global_table_health_route():
    try:
        if session.get("db_name"):
            rows,description=database.generic_select(session,"table_health")
            return render_template('home/table_health.html', segment='table_health', table_health=rows)
       
    except Exception as e1:
        tb = traceback.format_exc()
        print(tb)
        return jsonify({"error": str(e1), "traceback": tb}), 500
