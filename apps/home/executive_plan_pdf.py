"""PDF rendering for the Executive Plan using the shared report layout."""

from __future__ import annotations

from html import escape
from io import BytesIO
from typing import Any, Iterable

from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import CondPageBreak, KeepTogether, PageBreak, Paragraph, Spacer, Table, TableStyle, XPreformatted
from reportlab.platypus.tableofcontents import TableOfContents

from .report_pdf import (
    BORDER, CYAN, SURFACE, _register_fonts, _wrap_sql, create_report_document,
    database_design_section, finish_report, report_cover, report_styles,
)


def _source_label(source: str) -> str:
    return {
        "global_advisor": "Global Advisor",
        "index_advisor": "Index Advisor",
        "parameter_advisor": "Parameter Advisor",
        "autovacuum": "Autovacuum Tuning",
    }.get(source, source.replace("_", " ").title())


def filter_plan_for_teams(plan: dict[str, Any], teams: Iterable[str]) -> dict[str, Any]:
    """Return plan phases containing tasks relevant to the selected audiences."""
    selected = {str(team).upper() for team in teams} & {"DEV", "OPS"}
    if not selected:
        raise ValueError("Select at least one team.")

    phases = []
    tasks = []
    for phase in plan.get("phases") or []:
        phase_tasks = [
            task
            for task in phase.get("tasks") or []
            if task.get("team") in selected or task.get("team") == "DEV_OPS"
        ]
        if phase_tasks:
            phases.append({**phase, "tasks": phase_tasks})
            tasks.extend(phase_tasks)
    return {**plan, "phases": phases, "tasks": tasks, "selected_teams": sorted(selected)}


def build_executive_plan_pdf(
    plan: dict[str, Any],
    teams: Iterable[str],
    db_design_markdown: str | None = None,
) -> BytesIO:
    """Render a filtered Executive Plan as a styled PDF stream."""
    filtered = filter_plan_for_teams(plan, teams)
    _, bold_font = _register_fonts()
    stream = BytesIO()
    document = create_report_document(stream, "Executive Plan")
    styles = report_styles()
    audience = " + ".join(filtered["selected_teams"])
    story = report_cover(
        styles, title="Executive Plan",
        subtitle="An ordered implementation roadmap for PostgreSQL recommendations",
        database_name=filtered.get("database"), detail_label="AUDIENCE", detail_value=audience,
    )

    toc = TableOfContents()
    toc.levelStyles = [styles["toc"]]
    toc.dotsMinLevel = 0
    story.extend([
        Paragraph("Table of contents", styles["phase"]),
        Paragraph("Navigate directly to each implementation chapter.", styles["body"]),
        Spacer(1, 7 * mm),
        toc,
        Spacer(1, 12 * mm),
    ])

    task_count = len(filtered["tasks"])
    recommendation_count = sum(task.get("recommendation_count", 0) for task in filtered["tasks"])
    summary = Table(
        [
            [Paragraph("PHASES", styles["small"]), Paragraph("WORK PACKAGES", styles["small"]), Paragraph("RECOMMENDATIONS", styles["small"])],
            [Paragraph(str(len(filtered["phases"])), styles["phase"]), Paragraph(str(task_count), styles["phase"]), Paragraph(str(recommendation_count), styles["phase"])],
        ],
        colWidths=[52 * mm] * 3,
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), SURFACE),
            ("BOX", (0, 0), (-1, -1), 0.8, BORDER),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, BORDER),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ]),
    )
    story.extend([Paragraph("Plan overview", styles["phase"]), summary, Spacer(1, 8 * mm)])

    for phase_index, phase in enumerate(filtered["phases"], start=1):
        badges = [str(phase.get("team") or "DEV_OPS").replace("_", "/")]
        if phase.get("requires_maintenance_window"):
            badges.append("MAINTENANCE WINDOW")
        if phase.get("requires_restart"):
            badges.append("DATABASE RESTART")
        phase_title = f"{phase_index}. {str(phase.get('name') or 'Implementation phase')}"
        phase_heading = Paragraph(escape(phase_title), styles["phase"])
        phase_heading._report_toc_entry = (0, phase_title, f"executive-phase-{phase_index}")
        story.extend([
            PageBreak(),
            phase_heading,
            Paragraph(escape(str(phase.get("rationale") or "")), styles["body"]),
            Paragraph(" | ".join(badges), styles["small"]),
            Spacer(1, 3 * mm),
        ])
        for task in phase.get("tasks") or []:
            source_labels = [_source_label(str(source)) for source in task.get("sources") or []]
            source_widths = [stringWidth(label, bold_font, 7.2) + 16 for label in source_labels]
            source_badges = [
                Table(
                    [[Paragraph(escape(label), styles["source_badge"])]],
                    colWidths=[width],
                    rowHeights=[16],
                    cornerRadii=[8, 8, 8, 8],
                    style=TableStyle([
                        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#ECFEFF")),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 5),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                        ("TOPPADDING", (0, 0), (-1, -1), 2),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                    ]),
                )
                for label, width in zip(source_labels, source_widths)
            ]
            task_header = Table(
                [[Paragraph(escape(str(task.get("title") or "Work package")), styles["task"]), Paragraph(escape(str(task.get("team") or "DEV_OPS")).replace("_", "/"), styles["small"])]],
                colWidths=[132 * mm, 24 * mm],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#EEF2FF")),
                    ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#C7D2FE")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 9),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ]),
            )
            story.extend([CondPageBreak(32 * mm), task_header, Spacer(1, 3 * mm)])
            if source_badges:
                story.extend([
                    Paragraph("Sources", styles["small"]),
                    Spacer(1, 1.2 * mm),
                    Table(
                        [source_badges],
                        colWidths=[width + 5 for width in source_widths],
                        hAlign="LEFT",
                        style=TableStyle([
                            ("LEFTPADDING", (0, 0), (-1, -1), 0),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                            ("TOPPADDING", (0, 0), (-1, -1), 0),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ]),
                    ),
                    Spacer(1, 3 * mm),
                ])
            for group in task.get("recommendation_groups") or [{"scope_name": task.get("scope_name"), "recommendations": task.get("recommendations") or []}]:
                if task.get("workstream") in {"SCHEMA_DESIGN", "INDEX_STRATEGY"}:
                    story.append(
                        Table(
                            [[
                                Paragraph("Affected table", styles["table_label"]),
                                Paragraph(escape(str(group.get("scope_name") or "Database")), styles["table_name"]),
                            ]],
                            colWidths=[28 * mm, 128 * mm],
                            style=TableStyle([
                                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F0FDFA")),
                                ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#A5F3FC")),
                                ("LINEBEFORE", (0, 0), (0, -1), 3, CYAN),
                                ("ALIGN", (0, 0), (0, -1), "LEFT"),
                                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                                ("LEFTPADDING", (0, 0), (0, -1), 5),
                                ("LEFTPADDING", (1, 0), (1, -1), 9),
                                ("RIGHTPADDING", (0, 0), (-1, -1), 9),
                                ("TOPPADDING", (0, 0), (-1, -1), 8),
                                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                            ]),
                        )
                    )
                    story.append(Spacer(1, 3 * mm))
                for advice_index, advice in enumerate(group.get("recommendations") or [], start=1):
                    blocks = [
                        Paragraph(f"{advice_index}. {escape(str(advice.get('title') or 'Recommendation'))}", styles["task"]),
                    ]
                    if advice.get("description"):
                        blocks.append(Paragraph(escape(str(advice["description"])), styles["body"]))
                    story.append(KeepTogether(blocks))
                    if advice.get("sql"):
                        story.append(XPreformatted(escape(_wrap_sql(str(advice["sql"]))), styles["sql"]))
                story.append(Spacer(1, 3 * mm))
            story.append(Spacer(1, 4 * mm))

    if not filtered["phases"]:
        story.append(Paragraph("No work package matches the selected audience.", styles["body"]))

    if db_design_markdown:
        story.append(PageBreak())
        story.extend(database_design_section(db_design_markdown, styles))

    return finish_report(document, stream, story, f"Executive Plan - {audience}")
