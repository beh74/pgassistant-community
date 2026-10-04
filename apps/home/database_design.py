"""Shared context preparation for HTML and PDF database design analyses."""

from . import database, query_table_stats, schema_helper


def get_analysis_prompt(db_config, prompt=""):
    """Honor an edited prompt, or collect the current schema and workload."""
    if prompt and prompt.strip():
        return prompt.strip()

    conn, status = database.connectdb(db_config)
    try:
        if conn is None or status != "OK":
            raise RuntimeError(status or "Unable to connect to the database.")
        table_workload = query_table_stats.load_top_table_workload(db_config, limit=None)
        context = schema_helper.get_database_schema_llm_context(
            conn, table_workload=table_workload,
        )
        prompt = str(context.get("llm_prompt") or "").strip()
        if not prompt:
            raise ValueError("Unable to generate the database design prompt.")
        return prompt
    finally:
        if conn is not None:
            conn.close()
