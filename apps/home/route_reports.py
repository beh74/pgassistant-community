# -*- encoding: utf-8 -*-
"""Routes for reports and global advisor pages."""


import traceback

from apps.home import blueprint
from flask import jsonify, redirect, render_template, request, send_file, session

from . import database
from . import database_design
from . import collector_history
from . import executive_plan
from . import executive_plan_report
from . import global_advisor
from . import llm
from . import report_pdf
from . import reporting

@blueprint.route('/executive-plan.html', methods=['GET'])
def executive_plan_route():
    if not session.get("db_connected"):
        return redirect("/database.html")

    if request.args.get("run") != "1":
        return render_template(
            "home/executive_plan.html",
            segment="executive_plan.html",
            plan=None,
            collector_history_enabled=collector_history.is_configured(),
        )

    try:
        plan = executive_plan.build_executive_plan(session)
        return render_template(
            "home/executive_plan.html",
            segment="executive_plan.html",
            plan=plan,
            collector_history_enabled=collector_history.is_configured(),
        )
    except Exception as exc:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=exc, traceback_text=tb), 500


@blueprint.route('/workload-correlation.html', methods=['GET'])
def workload_correlation_route():
    """Display Collector-backed workload and Executive Plan correlations."""
    if not collector_history.is_configured() or not str(session.get("target_id") or "").strip():
        return redirect("/database.html")
    return render_template(
        "home/workload_correlation.html",
        segment="workload_correlation.html",
        target_id=session.get("target_id"),
    )


@blueprint.route('/executive-plan/report.pdf', methods=['POST'])
def executive_plan_report_route():
    if not session.get("db_connected"):
        return redirect("/database.html")

    teams = request.form.getlist("teams")
    if not ({"DEV", "OPS"} & set(teams)):
        return jsonify({"error": "Select at least one team."}), 400

    try:
        pdf = executive_plan_report.build_report(
            session, teams, include_db_design=request.form.get("include_db_design") == "1",
        )
        return _download_pdf(pdf, "executive-plan")
    except Exception as exc:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=exc, traceback_text=tb), 500


def _download_pdf(pdf, report_name):
    database_name = database.get_resolved_database_name(session) or "database"
    safe_database_name = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in database_name
    )
    return send_file(
        pdf, mimetype="application/pdf", as_attachment=True,
        download_name=f"pgassistant-{report_name}-{safe_database_name}.pdf",
    )


@blueprint.route('/database-analyze/report.pdf', methods=['POST'])
def database_design_report_route():
    if not session.get("db_connected"):
        return redirect("/database.html")

    try:
        prompt = database_design.get_analysis_prompt(
            session, request.form.get("llm_prompt", ""),
        )
        analysis = llm.query_chatgpt(prompt, render_html=False)
        pdf = report_pdf.build_database_design_pdf(
            database.get_resolved_database_name(session), analysis,
        )
        return _download_pdf(pdf, "database-design")
    except Exception as exc:
        tb = traceback.format_exc()
        print(tb)
        return render_template('home/page-500.html', err=exc, traceback_text=tb), 500

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
