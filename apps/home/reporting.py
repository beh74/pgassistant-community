import yaml
from pathlib import Path
import sys
import datetime
from flask import render_template
from . import database


def get_database_report(db_config, report_yaml_definition_file="./reporting.yml", template_folder="db_report_templates"):

    # Step 1: Connect to the database
    dbcon, msql = database.connectdb(db_config)
    if not dbcon:
        print("Error: Unable to connect to the database.")
        return None
    
    # Step 2: Load and validate the reporting YAML definition file
    report_yaml_definition_path = Path(report_yaml_definition_file)
    if not report_yaml_definition_path.exists():
        print(f"Error: Reporting definition file '{report_yaml_definition_file}' not found.")
        return None

    with open(report_yaml_definition_path, "r") as file:
        report_definitions = yaml.safe_load(file)

    required_keys = ["chapter_name", "query_id", "description", "enabled", "template"]

    # Step 3: Validate each entry in the YAML file and generate the report
    database_reports = ""
    for idx, entry in enumerate(report_definitions, start=1):
        
        # Check that all required keys are present
        missing_keys = [k for k in required_keys if k not in entry]
        if missing_keys:
            print(f"Entry {idx} is missing the following keys: {', '.join(missing_keys)}.")
            return None
        
        chapter_name = entry.get("chapter_name", None)
        query_id     = entry.get("query_id",     None)
        description  = entry.get("description",  None)
        enabled      = entry.get("enabled",      None)
        template     = entry.get("template",     None)

        if idx==1:
            chapter_render = render_template(f"{template_folder}/main.md", db_config=db_config, now=datetime.datetime.now())
            database_reports = chapter_render

        if enabled:
            try:
                rows, _ = database.db_query(dbcon, query_id)
                sql = database.get_query_by_id(query_id)
                if len(rows) > 0:
                    chapter_render = render_template(f"{template_folder}/{template}", rows=rows, chapter_name=chapter_name, sql=sql)
                    database_reports = database_reports+chapter_render
                
            except Exception as e:
                print(f"get_database_report - Error executing query for entry {idx} (query_id: {query_id}): {e}")


    return database_reports

