"""PDF rendering for the Executive Plan."""

from __future__ import annotations

from datetime import datetime
from html import escape
from io import BytesIO
from pathlib import Path
import re
import textwrap
from typing import Any, Iterable

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.platypus import (
    CondPageBreak,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    XPreformatted,
)
from reportlab.platypus.tableofcontents import TableOfContents

from apps.version import __version__


GITHUB_URL = "https://github.com/beh74/pgassistant-community"
DOCUMENTATION_URL = "https://beh74.github.io/pgassistant-blog/"
PRIMARY = colors.HexColor("#4F46E5")
CYAN = colors.HexColor("#0891B2")
INK = colors.HexColor("#172554")
MUTED = colors.HexColor("#64748B")
SURFACE = colors.HexColor("#F8FAFC")
BORDER = colors.HexColor("#DBE4EF")


class _ExecutivePlanDocument(SimpleDocTemplate):
    """Document template that registers phase headings in the PDF outline and TOC."""

    def afterFlowable(self, flowable):
        toc_entry = getattr(flowable, "_executive_toc_entry", None)
        if not toc_entry:
            return
        level, title, bookmark = toc_entry
        self.canv.bookmarkPage(bookmark)
        self.canv.addOutlineEntry(title, bookmark, level=level, closed=False)
        self.notify("TOCEntry", (level, title, self.page, bookmark))


def _source_label(source: str) -> str:
    return {
        "global_advisor": "Global Advisor",
        "index_advisor": "Index Advisor",
        "parameter_advisor": "Parameter Advisor",
        "autovacuum": "Autovacuum Tuning",
    }.get(source, source.replace("_", " ").title())


def _wrap_sql(sql: str, width: int = 96) -> str:
    """Wrap SQL for the fixed-width PDF code block without losing existing lines."""
    wrapped_lines: list[str] = []
    for line in str(sql).splitlines() or [""]:
        if not line.strip():
            wrapped_lines.append("")
            continue
        wrapped_lines.extend(
            textwrap.wrap(
                line,
                width=width,
                break_long_words=True,
                break_on_hyphens=False,
                replace_whitespace=False,
                drop_whitespace=True,
            )
            or [""]
        )
    return "\n".join(wrapped_lines)


def _register_fonts() -> tuple[str, str]:
    """Use ReportLab's bundled Unicode fonts when available."""
    reportlab_fonts = Path(__import__("reportlab").__file__).resolve().parent / "fonts"
    regular = reportlab_fonts / "Vera.ttf"
    bold = reportlab_fonts / "VeraBd.ttf"
    if regular.exists() and bold.exists():
        if "PGA-Vera" not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont("PGA-Vera", str(regular)))
            pdfmetrics.registerFont(TTFont("PGA-Vera-Bold", str(bold)))
        return "PGA-Vera", "PGA-Vera-Bold"
    return "Helvetica", "Helvetica-Bold"


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


def _markdown_inline(value: str) -> str:
    """Convert a small, safe Markdown inline subset to ReportLab markup."""
    rendered = escape(value)
    rendered = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)", r'<link href="\2" color="#4F46E5">\1</link>', rendered)
    rendered = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", rendered)
    rendered = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", rendered)
    rendered = re.sub(r"`([^`]+)`", r'<font name="Courier">\1</font>', rendered)
    return rendered


def _markdown_flowables(markdown_text: str, styles: dict[str, ParagraphStyle]) -> list[Any]:
    """Render common LLM Markdown constructs as native ReportLab flowables."""
    flowables: list[Any] = []
    paragraph_lines: list[str] = []
    code_lines: list[str] = []
    in_code = False

    def flush_paragraph():
        if paragraph_lines:
            text = " ".join(line.strip() for line in paragraph_lines)
            flowables.append(Paragraph(_markdown_inline(text), styles["ai_body"]))
            paragraph_lines.clear()

    def flush_code():
        if code_lines:
            flowables.append(
                XPreformatted(escape(_wrap_sql("\n".join(code_lines))), styles["sql"])
            )
            code_lines.clear()

    lines = str(markdown_text or "").splitlines()
    line_index = 0
    while line_index < len(lines):
        raw_line = lines[line_index]
        line = raw_line.rstrip()
        if line.lstrip().startswith("```"):
            if in_code:
                flush_code()
            else:
                flush_paragraph()
            in_code = not in_code
            line_index += 1
            continue
        if in_code:
            code_lines.append(line)
            line_index += 1
            continue
        if not line.strip():
            flush_paragraph()
            line_index += 1
            continue

        if (
            "|" in line
            and line_index + 1 < len(lines)
            and re.match(
                r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$",
                lines[line_index + 1],
            )
        ):
            flush_paragraph()
            table_lines = [line]
            line_index += 2
            while line_index < len(lines) and "|" in lines[line_index] and lines[line_index].strip():
                table_lines.append(lines[line_index].rstrip())
                line_index += 1

            rows = [
                [cell.strip() for cell in table_line.strip().strip("|").split("|")]
                for table_line in table_lines
            ]
            column_count = max(len(row) for row in rows)
            normalized_rows = [row + [""] * (column_count - len(row)) for row in rows]
            pdf_rows = [
                [
                    Paragraph(
                        _markdown_inline(cell),
                        styles["ai_table_header"] if row_index == 0 else styles["ai_table_cell"],
                    )
                    for cell in row
                ]
                for row_index, row in enumerate(normalized_rows)
            ]
            flowables.append(
                Table(
                    pdf_rows,
                    colWidths=[156 * mm / column_count] * column_count,
                    repeatRows=1,
                    hAlign="LEFT",
                    style=TableStyle([
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEF2FF")),
                        ("GRID", (0, 0), (-1, -1), 0.5, BORDER),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 5),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                        ("TOPPADDING", (0, 0), (-1, -1), 5),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ]),
                )
            )
            flowables.append(Spacer(1, 3 * mm))
            continue

        heading = re.match(r"^(#{1,6})\s+(.+)$", line)
        if heading:
            flush_paragraph()
            level = min(len(heading.group(1)), 3)
            flowables.append(
                Paragraph(_markdown_inline(heading.group(2)), styles[f"ai_heading_{level}"])
            )
            line_index += 1
            continue

        bullet = re.match(r"^\s*[-+*]\s+(.+)$", line)
        numbered = re.match(r"^\s*(\d+)[.)]\s+(.+)$", line)
        if bullet or numbered:
            flush_paragraph()
            marker = "&#8226;" if bullet else f"{numbered.group(1)}."
            content = bullet.group(1) if bullet else numbered.group(2)
            flowables.append(
                Paragraph(f"{marker}&nbsp;&nbsp;{_markdown_inline(content)}", styles["ai_list"])
            )
            line_index += 1
            continue

        paragraph_lines.append(line)
        line_index += 1

    flush_paragraph()
    flush_code()
    return flowables


def build_executive_plan_pdf(
    plan: dict[str, Any],
    teams: Iterable[str],
    db_design_markdown: str | None = None,
) -> BytesIO:
    """Render a filtered Executive Plan as a styled PDF stream."""
    filtered = filter_plan_for_teams(plan, teams)
    regular_font, bold_font = _register_fonts()
    stream = BytesIO()
    document = _ExecutivePlanDocument(
        stream,
        pagesize=A4,
        rightMargin=17 * mm,
        leftMargin=17 * mm,
        topMargin=22 * mm,
        bottomMargin=18 * mm,
        title="pgAssistant Executive Plan",
        author="pgAssistant Community",
    )
    base = getSampleStyleSheet()
    styles = {
        "cover_brand": ParagraphStyle("cover_brand", parent=base["Title"], fontName=bold_font, fontSize=13, textColor=CYAN, alignment=TA_CENTER, spaceAfter=10),
        "cover_title": ParagraphStyle("cover_title", parent=base["Title"], fontName=bold_font, fontSize=29, leading=34, textColor=INK, alignment=TA_CENTER, spaceAfter=12),
        "cover_text": ParagraphStyle("cover_text", parent=base["BodyText"], fontName=regular_font, fontSize=10, leading=16, textColor=MUTED, alignment=TA_CENTER),
        "phase": ParagraphStyle("phase", parent=base["Heading1"], fontName=bold_font, fontSize=17, leading=21, textColor=INK, spaceBefore=5, spaceAfter=5),
        "task": ParagraphStyle("task", parent=base["Heading2"], fontName=bold_font, fontSize=12, leading=16, textColor=INK, spaceAfter=4),
        "body": ParagraphStyle("body", parent=base["BodyText"], fontName=regular_font, fontSize=8.5, leading=12.5, textColor=colors.HexColor("#334155")),
        "small": ParagraphStyle("small", parent=base["BodyText"], fontName=regular_font, fontSize=7.5, leading=10.5, textColor=MUTED),
        "sql": ParagraphStyle("sql", parent=base["Code"], fontName="Courier", fontSize=6.8, leading=9, textColor=colors.HexColor("#E5E7EB"), backColor=colors.HexColor("#111827"), borderPadding=8, spaceBefore=13, spaceAfter=9),
        "table_label": ParagraphStyle("table_label", parent=base["BodyText"], fontName=regular_font, fontSize=7.2, leading=10, textColor=CYAN, alignment=0),
        "table_name": ParagraphStyle("table_name", parent=base["BodyText"], fontName=bold_font, fontSize=10.5, leading=14, textColor=INK),
        "source_badge": ParagraphStyle("source_badge", parent=base["BodyText"], fontName=bold_font, fontSize=7.2, leading=9, textColor=colors.HexColor("#0E7490"), alignment=TA_CENTER),
        "toc": ParagraphStyle("toc", parent=base["BodyText"], fontName=regular_font, fontSize=10, leading=16, leftIndent=4, firstLineIndent=0, textColor=INK),
        "ai_heading_1": ParagraphStyle("ai_heading_1", parent=base["Heading2"], fontName=bold_font, fontSize=14, leading=18, textColor=INK, spaceBefore=10, spaceAfter=5),
        "ai_heading_2": ParagraphStyle("ai_heading_2", parent=base["Heading3"], fontName=bold_font, fontSize=11.5, leading=15, textColor=colors.HexColor("#1E3A8A"), spaceBefore=8, spaceAfter=4),
        "ai_heading_3": ParagraphStyle("ai_heading_3", parent=base["Heading4"], fontName=bold_font, fontSize=9.5, leading=13, textColor=INK, spaceBefore=6, spaceAfter=3),
        "ai_body": ParagraphStyle("ai_body", parent=base["BodyText"], fontName=regular_font, fontSize=8.5, leading=13, textColor=colors.HexColor("#334155"), spaceAfter=5),
        "ai_list": ParagraphStyle("ai_list", parent=base["BodyText"], fontName=regular_font, fontSize=8.5, leading=13, leftIndent=10, firstLineIndent=-8, textColor=colors.HexColor("#334155"), spaceAfter=3),
        "ai_table_header": ParagraphStyle("ai_table_header", parent=base["BodyText"], fontName=bold_font, fontSize=7.2, leading=10, textColor=INK),
        "ai_table_cell": ParagraphStyle("ai_table_cell", parent=base["BodyText"], fontName=regular_font, fontSize=7.2, leading=10, textColor=colors.HexColor("#334155")),
    }

    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    audience = " + ".join(filtered["selected_teams"])
    story = [
        Spacer(1, 28 * mm),
        Paragraph("pgAssistant Community", styles["cover_brand"]),
        Paragraph("Executive Plan", styles["cover_title"]),
        Paragraph("An ordered implementation roadmap for PostgreSQL recommendations", styles["cover_text"]),
        Spacer(1, 12 * mm),
        Table(
            [
                [Paragraph("DATABASE", styles["small"]), Paragraph("AUDIENCE", styles["small"])],
                [Paragraph(escape(str(filtered.get("database") or "PostgreSQL")), styles["task"]), Paragraph(escape(audience), styles["task"])],
            ],
            colWidths=[78 * mm, 78 * mm],
            style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), SURFACE),
                ("BOX", (0, 0), (-1, -1), 0.8, BORDER),
                ("INNERGRID", (0, 0), (-1, -1), 0.5, BORDER),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ]),
        ),
        Spacer(1, 12 * mm),
        Paragraph(f"Generated {escape(generated_at)} with pgAssistant v{escape(__version__)}", styles["cover_text"]),
        Spacer(1, 3 * mm),
        Paragraph(f'<link href="{GITHUB_URL}" color="#4F46E5">GitHub project</link> &nbsp; | &nbsp; <link href="{DOCUMENTATION_URL}" color="#4F46E5">Documentation</link>', styles["cover_text"]),
        PageBreak(),
    ]

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
        phase_heading._executive_toc_entry = (0, phase_title, f"executive-phase-{phase_index}")
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
        db_design_title = "AI database design analysis"
        db_design_heading = Paragraph(db_design_title, styles["phase"])
        db_design_heading._executive_toc_entry = (
            0,
            db_design_title,
            "executive-db-design-analysis",
        )
        story.extend([
            PageBreak(),
            db_design_heading,
            Paragraph(
                "This section is generated by the configured LLM from the current schema digest and observed table workload. Review recommendations before applying changes.",
                styles["small"],
            ),
            Spacer(1, 4 * mm),
        ])
        story.extend(_markdown_flowables(db_design_markdown, styles))

    def decorate_page(canvas, doc):
        canvas.saveState()
        width, height = A4
        canvas.setFillColor(PRIMARY)
        canvas.rect(0, height - 7 * mm, width, 7 * mm, fill=1, stroke=0)
        canvas.setFont(regular_font, 7)
        canvas.setFillColor(MUTED)
        canvas.drawString(17 * mm, 9 * mm, f"pgAssistant v{__version__} - Executive Plan - {audience}")
        canvas.drawRightString(width - 17 * mm, 9 * mm, f"Page {doc.page}")
        canvas.restoreState()

    document.multiBuild(story, onFirstPage=decorate_page, onLaterPages=decorate_page)
    stream.seek(0)
    return stream
