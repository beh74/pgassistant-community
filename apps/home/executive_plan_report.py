"""Executive Plan report generation shared by the web form and integration API."""

from . import database_design, executive_plan, executive_plan_pdf, llm


def build_report(db_config, teams, include_db_design=False):
    plan = executive_plan.build_executive_plan(db_config)
    analysis = None
    if include_db_design:
        analysis = llm.query_chatgpt(
            database_design.get_analysis_prompt(db_config), render_html=False,
        )
        if not str(analysis or "").strip():
            raise ValueError("The LLM returned an empty database design analysis.")
    return executive_plan_pdf.build_executive_plan_pdf(
        plan, teams, db_design_markdown=analysis,
    )
