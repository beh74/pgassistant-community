"""Database report orchestration."""

import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml
from flask import render_template

from . import api_helper
from . import database
from . import global_advisor
from . import query_index_advisor


QUERY_SOURCE = "query"
QUERY_RANKING_SOURCE = "query_ranking"
GLOBAL_ADVISOR_SOURCE = "global_advisor"
INDEX_ADVISOR_SOURCE = "index_advisor"
SUPPORTED_SOURCES = {
    QUERY_SOURCE,
    QUERY_RANKING_SOURCE,
    GLOBAL_ADVISOR_SOURCE,
    INDEX_ADVISOR_SOURCE,
}


def _validate_report_entry(entry: Dict[str, Any], index: int) -> str:
    """Validate one report definition and return its normalized source."""
    required_keys = ["chapter_name", "description", "enabled", "template"]
    missing_keys = [key for key in required_keys if key not in entry]
    if missing_keys:
        raise ValueError(
            f"Entry {index} is missing the following keys: "
            f"{', '.join(missing_keys)}."
        )

    source = str(entry.get("source") or QUERY_SOURCE).strip().lower()
    if source not in SUPPORTED_SOURCES:
        raise ValueError(
            f"Entry {index} uses unsupported report source '{source}'."
        )

    if source == QUERY_SOURCE and not entry.get("query_id"):
        raise ValueError(
            f"Entry {index} is missing query_id for source '{QUERY_SOURCE}'."
        )

    return source


def _recommendation_to_dict(recommendation: Any) -> Dict[str, Any]:
    """Normalize Global Advisor models for stable Jinja rendering."""
    if hasattr(recommendation, "to_dict"):
        return recommendation.to_dict()
    if isinstance(recommendation, dict):
        return recommendation
    raise TypeError(
        "Global Advisor returned an unsupported recommendation type: "
        f"{type(recommendation).__name__}."
    )


def _render_query_chapter(
    connection: Any,
    entry: Dict[str, Any],
    template_folder: str,
) -> str:
    query_id = entry["query_id"]
    rows, _description = database.db_query(connection, query_id)
    if not rows:
        return ""

    return render_template(
        f"{template_folder}/{entry['template']}",
        rows=rows,
        chapter_name=entry["chapter_name"],
        sql=database.get_query_by_id_reporing(query_id),
    )


def _render_global_advisor_chapter(
    db_config: Dict[str, Any],
    entry: Dict[str, Any],
    template_folder: str,
) -> str:
    try:
        limit = max(1, min(int(entry.get("limit", 20)), 100))
    except (TypeError, ValueError):
        limit = 20

    result = global_advisor.run_global_advisor(
        db_config,
        yaml_path=entry.get("advisor_yaml_path", "advisor_enriched.yml"),
    )
    all_recommendations = [
        _recommendation_to_dict(recommendation)
        for recommendation in result.get("recommendations", [])
    ]
    recommendations = all_recommendations[:limit]

    return render_template(
        f"{template_folder}/{entry['template']}",
        chapter_name=entry["chapter_name"],
        result=result,
        recommendations=recommendations,
        recommendations_available=len(all_recommendations),
        recommendation_limit=limit,
        summary=result.get("summary") or {},
        errors=result.get("errors") or [],
    )


def _render_query_ranking_chapter(
    db_config: Dict[str, Any],
    entry: Dict[str, Any],
    template_folder: str,
) -> str:
    try:
        limit = max(1, min(int(entry.get("limit", 10)), 10))
    except (TypeError, ValueError):
        limit = 10

    ranked_queries = api_helper.get_rank_top_10_queries(db_config)[:limit]

    return render_template(
        f"{template_folder}/{entry['template']}",
        chapter_name=entry["chapter_name"],
        ranked_queries=ranked_queries,
        query_limit=limit,
    )


def _render_index_advisor_chapter(
    db_config: Dict[str, Any],
    entry: Dict[str, Any],
    template_folder: str,
) -> str:
    try:
        limit = max(1, min(int(entry.get("limit", 10)), 50))
    except (TypeError, ValueError):
        limit = 10

    result = query_index_advisor.analyze_top_ranked_query_indexes(
        db_config,
        limit=limit,
    )

    return render_template(
        f"{template_folder}/{entry['template']}",
        chapter_name=entry["chapter_name"],
        result=result,
        summary=result.get("summary") or {},
        query_results=result.get("results") or [],
    )


def get_database_report(
    db_config: Dict[str, Any],
    report_yaml_definition_file: str = "./reporting.yml",
    template_folder: str = "db_report_templates",
) -> str | None:
    """Build the Markdown database report from query and advisor sources."""
    report_definition_path = Path(report_yaml_definition_file)
    if not report_definition_path.exists():
        print(
            f"Error: Reporting definition file "
            f"'{report_yaml_definition_file}' not found."
        )
        return None

    with report_definition_path.open("r", encoding="utf-8") as report_file:
        report_definitions = yaml.safe_load(report_file) or []

    if not isinstance(report_definitions, list):
        raise ValueError("The reporting definition root must be a list.")

    validated_entries: List[tuple[Dict[str, Any], str]] = []
    for index, entry in enumerate(report_definitions, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Entry {index} must be a mapping.")
        validated_entries.append(
            (entry, _validate_report_entry(entry, index))
        )

    connection = None
    report_parts = [
        render_template(
            f"{template_folder}/main.md",
            db_config=db_config,
            now=datetime.datetime.now(),
        )
    ]

    try:
        for entry, source in validated_entries:
            if not entry.get("enabled", False):
                continue

            try:
                if source == QUERY_SOURCE:
                    if connection is None:
                        connection, status_message = database.connectdb(db_config)
                        if connection is None:
                            raise RuntimeError(
                                status_message or "Unable to connect to the database."
                            )
                    chapter = _render_query_chapter(
                        connection,
                        entry,
                        template_folder,
                    )
                elif source == QUERY_RANKING_SOURCE:
                    chapter = _render_query_ranking_chapter(
                        db_config,
                        entry,
                        template_folder,
                    )
                elif source == GLOBAL_ADVISOR_SOURCE:
                    chapter = _render_global_advisor_chapter(
                        db_config,
                        entry,
                        template_folder,
                    )
                else:
                    chapter = _render_index_advisor_chapter(
                        db_config,
                        entry,
                        template_folder,
                    )

                if chapter:
                    report_parts.append(chapter)
            except Exception as exc:
                print(
                    "get_database_report - Error generating chapter "
                    f"'{entry['chapter_name']}' from source '{source}': {exc}"
                )
                report_parts.append(
                    render_template(
                        f"{template_folder}/report_error.md",
                        chapter_name=entry["chapter_name"],
                        error=str(exc),
                    )
                )
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    return "\n".join(report_parts)
