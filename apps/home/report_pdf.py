"""Shared PDF layout and Markdown rendering for pgAssistant reports."""

from __future__ import annotations

from datetime import datetime
from html import escape
from io import BytesIO
from pathlib import Path
import re
import textwrap
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    XPreformatted,
)

from apps.version import __version__


GITHUB_URL = "https://github.com/beh74/pgassistant-community"
DOCUMENTATION_URL = "https://beh74.github.io/pgassistant-blog/"
PRIMARY = colors.HexColor("#4F46E5")
CYAN = colors.HexColor("#0891B2")
INK = colors.HexColor("#172554")
MUTED = colors.HexColor("#64748B")
SURFACE = colors.HexColor("#F8FAFC")
BORDER = colors.HexColor("#DBE4EF")


class ReportDocument(SimpleDocTemplate):
    """Document template that registers headings in the PDF outline and TOC."""

    def afterFlowable(self, flowable):
        toc_entry = getattr(flowable, "_report_toc_entry", None)
        if not toc_entry:
            return
        level, title, bookmark = toc_entry
        self.canv.bookmarkPage(bookmark)
        self.canv.addOutlineEntry(title, bookmark, level=level, closed=False)
        self.notify("TOCEntry", (level, title, self.page, bookmark))


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


def report_styles():
    """Shared typography for plan and LLM-only reports."""
    regular_font, bold_font = _register_fonts()
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

    for level in (1, 2, 3):
        styles[f"ai_heading_{level}"].keepWithNext = True
    return styles


def report_cover(styles, *, title, subtitle, database_name, detail_label, detail_value):
    """Create a consistent cover with report-specific metadata."""
    generated_at = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    return [
        Spacer(1, 28 * mm),
        Paragraph("pgAssistant Community", styles["cover_brand"]),
        Paragraph(escape(title), styles["cover_title"]),
        Paragraph(escape(subtitle), styles["cover_text"]),
        Spacer(1, 12 * mm),
        Table(
            [
                [Paragraph("DATABASE", styles["small"]), Paragraph(escape(detail_label), styles["small"])],
                [Paragraph(escape(str(database_name or "PostgreSQL")), styles["task"]), Paragraph(escape(detail_value), styles["task"])],
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

def create_report_document(stream, title):
    return ReportDocument(
        stream, pagesize=A4, rightMargin=17 * mm, leftMargin=17 * mm,
        topMargin=22 * mm, bottomMargin=18 * mm,
        title=f"pgAssistant {title}", author="pgAssistant Community",
    )


def finish_report(document, stream, story, footer):
    regular_font, _ = _register_fonts()
    def decorate_page(canvas, doc):
        canvas.saveState()
        width, height = A4
        canvas.setFillColor(PRIMARY)
        canvas.rect(0, height - 7 * mm, width, 7 * mm, fill=1, stroke=0)
        canvas.setFont(regular_font, 7)
        canvas.setFillColor(MUTED)
        canvas.drawString(17 * mm, 9 * mm, f"pgAssistant v{__version__} - {footer}")
        canvas.drawRightString(width - 17 * mm, 9 * mm, f"Page {doc.page}")
        canvas.restoreState()

    document.multiBuild(story, onFirstPage=decorate_page, onLaterPages=decorate_page)
    stream.seek(0)
    return stream


def database_design_section(markdown_text, styles):
    heading = Paragraph("AI database design analysis", styles["phase"])
    heading._report_toc_entry = (0, "AI database design analysis", "db-design-analysis")
    return [
        heading,
        Paragraph(
            "This section is generated by the configured LLM from the schema digest and observed table workload. Review recommendations before applying changes.",
            styles["small"],
        ),
        Spacer(1, 4 * mm),
        *_markdown_flowables(markdown_text, styles),
    ]


def build_database_design_pdf(database_name: str, markdown_text: str) -> BytesIO:
    """Render only the LLM analysis, with the same presentation as Executive Plan."""
    if not str(markdown_text or "").strip():
        raise ValueError("The LLM returned an empty database design analysis.")
    stream = BytesIO()
    title = "Database Design Analysis"
    document = create_report_document(stream, title)
    styles = report_styles()
    story = report_cover(
        styles, title=title,
        subtitle="LLM analysis of PostgreSQL schema design and workload",
        database_name=database_name, detail_label="REPORT", detail_value="LLM analysis",
    )
    story.extend(database_design_section(markdown_text, styles))
    return finish_report(document, stream, story, title)
