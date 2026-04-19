"""
reports/pdf_generator.py
-------------------------
PDF report generator for Scout Bot — Camelot Property Management Services Corp.

Generates two report types:
  1. ``generate_property_report(property_data)``  → individual property/lead PDF
  2. ``generate_lead_report(leads_df)``            → daily digest PDF with lead table

Branding:
  - Primary gold:  #C9A84C
  - Dark navy:     #1A2645
  - Light grey:    #F5F5F5
  - White:         #FFFFFF
  - Logo:          Gold "C" text placeholder (Camelot wordmark)

Dependencies: reportlab (pip install reportlab)
"""

import io
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Brand colours
# ---------------------------------------------------------------------------

CAMELOT_GOLD = colors.HexColor("#C9A84C")
CAMELOT_NAVY = colors.HexColor("#1A2645")
LIGHT_GREY = colors.HexColor("#F5F5F5")
MID_GREY = colors.HexColor("#CCCCCC")
WHITE = colors.white
BLACK = colors.black
TEXT_DARK = colors.HexColor("#222222")

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

_BASE_STYLES = getSampleStyleSheet()


def _make_styles() -> Dict[str, ParagraphStyle]:
    """Build and return all named paragraph styles."""
    return {
        "logo": ParagraphStyle(
            "logo",
            fontName="Helvetica-Bold",
            fontSize=28,
            textColor=CAMELOT_GOLD,
            alignment=TA_LEFT,
            spaceAfter=0,
        ),
        "tagline": ParagraphStyle(
            "tagline",
            fontName="Helvetica",
            fontSize=9,
            textColor=WHITE,
            alignment=TA_LEFT,
            spaceAfter=0,
        ),
        "report_title": ParagraphStyle(
            "report_title",
            fontName="Helvetica-Bold",
            fontSize=18,
            textColor=WHITE,
            alignment=TA_RIGHT,
            spaceAfter=2,
        ),
        "report_subtitle": ParagraphStyle(
            "report_subtitle",
            fontName="Helvetica",
            fontSize=10,
            textColor=CAMELOT_GOLD,
            alignment=TA_RIGHT,
            spaceAfter=0,
        ),
        "section_header": ParagraphStyle(
            "section_header",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=CAMELOT_NAVY,
            spaceBefore=12,
            spaceAfter=4,
        ),
        "body": ParagraphStyle(
            "body",
            fontName="Helvetica",
            fontSize=9,
            textColor=TEXT_DARK,
            spaceAfter=4,
            leading=13,
        ),
        "body_bold": ParagraphStyle(
            "body_bold",
            fontName="Helvetica-Bold",
            fontSize=9,
            textColor=TEXT_DARK,
            spaceAfter=4,
        ),
        "label": ParagraphStyle(
            "label",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=CAMELOT_NAVY,
            spaceAfter=1,
        ),
        "value": ParagraphStyle(
            "value",
            fontName="Helvetica",
            fontSize=9,
            textColor=TEXT_DARK,
            spaceAfter=6,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontName="Helvetica",
            fontSize=7,
            textColor=MID_GREY,
            alignment=TA_CENTER,
        ),
        "table_header": ParagraphStyle(
            "table_header",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=WHITE,
            alignment=TA_CENTER,
        ),
        "table_cell": ParagraphStyle(
            "table_cell",
            fontName="Helvetica",
            fontSize=8,
            textColor=TEXT_DARK,
            leading=11,
        ),
        "table_cell_bold": ParagraphStyle(
            "table_cell_bold",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=TEXT_DARK,
            leading=11,
        ),
        "score_high": ParagraphStyle(
            "score_high",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=colors.HexColor("#1A7A1A"),
            alignment=TA_CENTER,
        ),
        "score_medium": ParagraphStyle(
            "score_medium",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=CAMELOT_GOLD,
            alignment=TA_CENTER,
        ),
        "score_low": ParagraphStyle(
            "score_low",
            fontName="Helvetica-Bold",
            fontSize=8,
            textColor=colors.HexColor("#CC3300"),
            alignment=TA_CENTER,
        ),
    }


STYLES = _make_styles()

# ---------------------------------------------------------------------------
# Shared header / footer builders
# ---------------------------------------------------------------------------

def _build_header_table(report_title: str, subtitle: str) -> Table:
    """Build the Camelot branded page header table.

    Left cell: gold "C" logo + company name.
    Right cell: report title + subtitle.

    Args:
        report_title: Bold report type label.
        subtitle: Date or secondary label.

    Returns:
        A ReportLab Table element.
    """
    logo_block = [
        Paragraph('<font color="#C9A84C" size="28"><b>C</b></font>'
                  '<font color="#FFFFFF" size="14"><b>amelot</b></font>',
                  ParagraphStyle("logo_inline", fontName="Helvetica-Bold",
                                 fontSize=14, textColor=WHITE)),
        Paragraph("Property Management Services Corp.",
                  STYLES["tagline"]),
    ]

    title_block = [
        Paragraph(report_title, STYLES["report_title"]),
        Paragraph(subtitle, STYLES["report_subtitle"]),
    ]

    data = [[logo_block, title_block]]
    t = Table(data, colWidths=[3.5 * inch, 4.0 * inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), CAMELOT_NAVY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (0, 0), 14),
        ("RIGHTPADDING", (-1, 0), (-1, 0), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
    ]))
    return t


def _score_style(score: int) -> ParagraphStyle:
    """Return a colour-coded style for a lead score."""
    if score >= 70:
        return STYLES["score_high"]
    if score >= 40:
        return STYLES["score_medium"]
    return STYLES["score_low"]


# ---------------------------------------------------------------------------
# generate_property_report
# ---------------------------------------------------------------------------

def generate_property_report(property_data: Dict[str, Any]) -> bytes:
    """Generate a detailed single-property / single-lead PDF report.

    Args:
        property_data: Scout lead dict (or augmented property dict) containing
            any combination of the following keys:
            - title, company_name, raw_location
            - post_description, category, lead_type, source_site, region
            - score, tags
            - email (list), phone (list)
            - contacts (list of enriched contact dicts)
            - asking_price, revenue (optional financial fields)
            - open_violations, building_id, unit_count (optional HPD fields)
            - link

    Returns:
        PDF as raw bytes.
    """
    buf = io.BytesIO()

    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.65 * inch,
        rightMargin=0.65 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.75 * inch,
        title=f"Scout Report — {property_data.get('company_name', 'Property')}",
        author="Camelot OS — Scout Bot",
    )

    story = []
    s = STYLES

    # ---- Header ----
    title_text = property_data.get("company_name") or property_data.get("title", "Scout Report")
    run_date = date.today().strftime("%B %d, %Y")
    story.append(_build_header_table("Property Intelligence Report", run_date))
    story.append(Spacer(1, 0.15 * inch))

    # ---- Property title ----
    story.append(Paragraph(title_text, ParagraphStyle(
        "prop_title", fontName="Helvetica-Bold", fontSize=14,
        textColor=CAMELOT_NAVY, spaceAfter=2,
    )))
    loc = property_data.get("raw_location", "")
    if loc:
        story.append(Paragraph(loc, ParagraphStyle(
            "prop_loc", fontName="Helvetica", fontSize=10,
            textColor=TEXT_DARK, spaceAfter=6,
        )))
    story.append(HRFlowable(width="100%", thickness=1.5, color=CAMELOT_GOLD, spaceAfter=8))

    # ---- Lead metadata grid ----
    story.append(Paragraph("LEAD OVERVIEW", s["section_header"]))

    def _field_row(label: str, value: str) -> List:
        return [
            Paragraph(label, s["label"]),
            Paragraph(str(value) if value else "—", s["value"]),
        ]

    score = property_data.get("score", 0)
    score_style = _score_style(score)

    overview_data = [
        [Paragraph("FIELD", s["label"]), Paragraph("VALUE", s["label"])],
        _field_row("Source", property_data.get("source_site", "")),
        _field_row("Region", property_data.get("region", "")),
        _field_row("Category", property_data.get("category", "")),
        _field_row("Lead Type", property_data.get("lead_type", "")),
        _field_row("Score", ""),  # replaced below
        _field_row("Days Posted", str(property_data.get("days_posted", "N/A"))),
        _field_row("Tags", ", ".join(property_data.get("tags") or [])),
    ]
    # Replace score row with coloured paragraph
    overview_data[5][1] = Paragraph(str(score), score_style)

    ov_table = Table(overview_data, colWidths=[1.8 * inch, 5.7 * inch])
    ov_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
        ("GRID", (0, 0), (-1, -1), 0.4, MID_GREY),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(ov_table)
    story.append(Spacer(1, 0.12 * inch))

    # ---- Financials (if present) ----
    asking = property_data.get("asking_price", "")
    revenue = property_data.get("revenue", "")
    if asking or revenue:
        story.append(Paragraph("FINANCIALS", s["section_header"]))
        fin_data = [
            [Paragraph("METRIC", s["label"]), Paragraph("VALUE", s["label"])],
        ]
        if asking:
            fin_data.append([Paragraph("Asking Price", s["label"]),
                             Paragraph(asking, s["value"])])
        if revenue:
            fin_data.append([Paragraph("Revenue / Gross", s["label"]),
                             Paragraph(revenue, s["value"])])
        fin_table = Table(fin_data, colWidths=[2.0 * inch, 5.5 * inch])
        fin_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
            ("GRID", (0, 0), (-1, -1), 0.4, MID_GREY),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(fin_table)
        story.append(Spacer(1, 0.12 * inch))

    # ---- HPD Violations (if present) ----
    violations = property_data.get("open_violations")
    building_id = property_data.get("building_id", "")
    unit_count = property_data.get("unit_count", "")
    if violations is not None or building_id:
        story.append(Paragraph("HPD BUILDING DATA", s["section_header"]))
        hpd_rows = [
            [Paragraph("FIELD", s["label"]), Paragraph("VALUE", s["label"])],
        ]
        if building_id:
            hpd_rows.append([Paragraph("Building ID", s["label"]),
                             Paragraph(str(building_id), s["value"])])
        if unit_count:
            hpd_rows.append([Paragraph("Unit Count", s["label"]),
                             Paragraph(str(unit_count), s["value"])])
        if violations is not None:
            viol_text = str(violations)
            viol_style = s["value"] if int(violations) < 5 else ParagraphStyle(
                "viol_high", fontName="Helvetica-Bold", fontSize=9,
                textColor=colors.HexColor("#CC3300"))
            hpd_rows.append([Paragraph("Open Violations", s["label"]),
                             Paragraph(viol_text, viol_style)])
        hpd_table = Table(hpd_rows, colWidths=[2.0 * inch, 5.5 * inch])
        hpd_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
            ("GRID", (0, 0), (-1, -1), 0.4, MID_GREY),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(hpd_table)
        story.append(Spacer(1, 0.12 * inch))

    # ---- Description ----
    desc = property_data.get("post_description", "")
    if desc:
        story.append(Paragraph("DESCRIPTION", s["section_header"]))
        story.append(Paragraph(desc[:1200], s["body"]))
        story.append(Spacer(1, 0.1 * inch))

    # ---- Contact info from lead ----
    emails = property_data.get("email") or []
    phones = property_data.get("phone") or []
    if emails or phones:
        story.append(Paragraph("CONTACT INFORMATION", s["section_header"]))
        if emails:
            story.append(Paragraph(
                "<b>Email(s):</b> " + ", ".join(emails), s["body"]))
        if phones:
            story.append(Paragraph(
                "<b>Phone(s):</b> " + ", ".join(phones), s["body"]))
        story.append(Spacer(1, 0.08 * inch))

    # ---- Enriched contacts ----
    contacts = property_data.get("contacts") or []
    if contacts:
        story.append(Paragraph("ENRICHED CONTACTS", s["section_header"]))

        contact_header = [
            Paragraph("NAME", s["table_header"]),
            Paragraph("TITLE", s["table_header"]),
            Paragraph("EMAIL", s["table_header"]),
            Paragraph("PHONE", s["table_header"]),
            Paragraph("SOURCE", s["table_header"]),
        ]
        contact_rows = [contact_header]
        for c in contacts:
            phone_val = ", ".join(c.get("phone") or []) or "—"
            contact_rows.append([
                Paragraph(c.get("name") or "—", s["table_cell_bold"]),
                Paragraph(c.get("title") or "—", s["table_cell"]),
                Paragraph(c.get("email") or "—", s["table_cell"]),
                Paragraph(phone_val, s["table_cell"]),
                Paragraph(c.get("source") or "—", s["table_cell"]),
            ])

        ct = Table(
            contact_rows,
            colWidths=[1.5 * inch, 1.6 * inch, 2.0 * inch, 1.3 * inch, 1.1 * inch],
        )
        ct.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
            ("GRID", (0, 0), (-1, -1), 0.4, MID_GREY),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(ct)
        story.append(Spacer(1, 0.1 * inch))

    # ---- Source link ----
    link = property_data.get("link", "")
    if link:
        story.append(Paragraph(
            f'<b>Source URL:</b> <a href="{link}" color="#1A2645">{link[:100]}</a>',
            s["body"],
        ))

    # ---- Footer ----
    story.append(Spacer(1, 0.2 * inch))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MID_GREY))
    story.append(Paragraph(
        f"Generated by Camelot OS — Scout Bot  •  {datetime.now().strftime('%Y-%m-%d %H:%M')} EDT  "
        f"•  Confidential — For Internal Use Only",
        s["footer"],
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# generate_lead_report
# ---------------------------------------------------------------------------

def generate_lead_report(leads_df: Any) -> bytes:
    """Generate a daily digest PDF of top Scout leads.

    Args:
        leads_df: Either a pandas DataFrame or a list of Scout lead dicts.
                  Must contain the Scout schema fields.

    Returns:
        PDF as raw bytes.
    """
    # Normalise to list of dicts
    if hasattr(leads_df, "to_dict"):
        leads: List[Dict[str, Any]] = leads_df.to_dict(orient="records")
    else:
        leads = list(leads_df)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.6 * inch,
        title=f"Scout Daily Digest — {date.today()}",
        author="Camelot OS — Scout Bot",
    )

    story = []
    s = STYLES
    run_date = date.today().strftime("%A, %B %d, %Y")

    # ---- Header ----
    story.append(_build_header_table("Daily Lead Digest", run_date))
    story.append(Spacer(1, 0.15 * inch))

    # ---- Summary stats ----
    total = len(leads)
    acquisitions = sum(1 for l in leads if l.get("lead_type") == "Acquisition")
    mandates = sum(1 for l in leads if l.get("lead_type") == "Management mandate")
    rfps = sum(1 for l in leads if l.get("category") == "RFP")
    hiring = sum(1 for l in leads if l.get("lead_type") == "Hiring signal")
    unmanaged = sum(1 for l in leads if l.get("lead_type") == "Unmanaged building")
    avg_score = (sum(l.get("score", 0) for l in leads) / total) if total else 0

    summary_data = [
        [
            Paragraph("TOTAL LEADS", s["table_header"]),
            Paragraph("ACQUISITIONS", s["table_header"]),
            Paragraph("MANDATES", s["table_header"]),
            Paragraph("RFPs", s["table_header"]),
            Paragraph("HIRING", s["table_header"]),
            Paragraph("UNMANAGED", s["table_header"]),
            Paragraph("AVG SCORE", s["table_header"]),
        ],
        [
            Paragraph(str(total), ParagraphStyle("big_num", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_NAVY, alignment=TA_CENTER)),
            Paragraph(str(acquisitions), ParagraphStyle("big_num2", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_GOLD, alignment=TA_CENTER)),
            Paragraph(str(mandates), ParagraphStyle("big_num3", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_NAVY, alignment=TA_CENTER)),
            Paragraph(str(rfps), ParagraphStyle("big_num4", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_NAVY, alignment=TA_CENTER)),
            Paragraph(str(hiring), ParagraphStyle("big_num5", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_NAVY, alignment=TA_CENTER)),
            Paragraph(str(unmanaged), ParagraphStyle("big_num6", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_NAVY, alignment=TA_CENTER)),
            Paragraph(f"{avg_score:.0f}", ParagraphStyle("big_num7", fontName="Helvetica-Bold",
                fontSize=16, textColor=CAMELOT_GOLD, alignment=TA_CENTER)),
        ],
    ]

    stat_col = (7.5 * inch) / 7
    stats_table = Table(summary_data, colWidths=[stat_col] * 7)
    stats_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
        ("BACKGROUND", (0, 1), (-1, 1), LIGHT_GREY),
        ("BOX", (0, 0), (-1, -1), 1, CAMELOT_GOLD),
        ("INNERGRID", (0, 0), (-1, -1), 0.4, MID_GREY),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(stats_table)
    story.append(Spacer(1, 0.2 * inch))

    # ---- Leads table ----
    story.append(Paragraph("TOP LEADS", s["section_header"]))
    story.append(Spacer(1, 0.04 * inch))

    COL_WIDTHS = [
        0.45 * inch,  # #
        1.85 * inch,  # Company
        0.75 * inch,  # Region
        1.15 * inch,  # Lead Type
        1.05 * inch,  # Source
        0.55 * inch,  # Score
        1.65 * inch,  # Contact
    ]

    lead_header = [
        Paragraph("#", s["table_header"]),
        Paragraph("COMPANY / TITLE", s["table_header"]),
        Paragraph("REGION", s["table_header"]),
        Paragraph("TYPE", s["table_header"]),
        Paragraph("SOURCE", s["table_header"]),
        Paragraph("SCORE", s["table_header"]),
        Paragraph("CONTACT", s["table_header"]),
    ]

    table_rows = [lead_header]
    for i, lead in enumerate(leads[:50], start=1):  # cap at 50 rows per page
        score = lead.get("score", 0)
        sty = _score_style(score)

        # Best available contact line
        contacts = lead.get("contacts") or []
        if contacts:
            c = contacts[0]
            contact_text = f"{c.get('name', '')} {c.get('email', '')}".strip()
        else:
            emails = lead.get("email") or []
            phones = lead.get("phone") or []
            contact_text = emails[0] if emails else (phones[0] if phones else "—")

        company = (lead.get("company_name") or lead.get("title") or "")[:45]
        source = (lead.get("source_site") or "")[:18]
        region = (lead.get("region") or "")[:6]
        lead_type = (lead.get("lead_type") or "")[:20]

        row = [
            Paragraph(str(i), ParagraphStyle("num_cell", fontName="Helvetica",
                fontSize=8, textColor=MID_GREY, alignment=TA_CENTER)),
            Paragraph(company, s["table_cell_bold"]),
            Paragraph(region, s["table_cell"]),
            Paragraph(lead_type, s["table_cell"]),
            Paragraph(source, s["table_cell"]),
            Paragraph(str(score), sty),
            Paragraph(contact_text[:50], s["table_cell"]),
        ]
        table_rows.append(row)

    leads_table = Table(table_rows, colWidths=COL_WIDTHS, repeatRows=1)
    row_bgs = []
    for r in range(1, len(table_rows)):
        bg = WHITE if r % 2 == 0 else LIGHT_GREY
        row_bgs.append(("BACKGROUND", (0, r), (-1, r), bg))

    leads_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), CAMELOT_NAVY),
        ("BOX", (0, 0), (-1, -1), 0.5, CAMELOT_GOLD),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, MID_GREY),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        *row_bgs,
    ]))
    story.append(leads_table)

    # ---- Footer ----
    story.append(Spacer(1, 0.25 * inch))
    story.append(HRFlowable(width="100%", thickness=0.5, color=MID_GREY))
    story.append(Paragraph(
        f"Camelot OS — Scout Bot Daily Digest  •  "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} EDT  •  "
        f"Confidential — For Internal Use Only",
        s["footer"],
    ))

    doc.build(story)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)

    # --- Property report smoke test ---
    sample_lead = {
        "company_name": "Acme Property Management LLC",
        "title": "Established PM Company — 450 Units Under Management",
        "raw_location": "123 Main St, Brooklyn, NY 11201",
        "source_site": "BizBuySell",
        "region": "NY",
        "category": "Business for sale",
        "lead_type": "Acquisition",
        "score": 82,
        "days_posted": 3,
        "tags": ["Acquisition", "Succession"],
        "post_description": (
            "Founded in 2001, Acme PM manages 450 residential units across "
            "Brooklyn and Queens. Owner is retiring and seeking qualified buyer. "
            "Strong team in place, transferable contracts."
        ),
        "asking_price": "$1,200,000",
        "revenue": "$380,000/yr",
        "email": ["owner@acmepm.nyc"],
        "phone": ["(718) 555-0101"],
        "contacts": [
            {
                "name": "David Goldstein",
                "title": "Owner / Principal",
                "email": "owner@acmepm.nyc",
                "phone": ["(718) 555-0101"],
                "source": "Apollo.io",
            }
        ],
        "link": "https://www.bizbuysell.com/listing/acme-pm/12345",
    }

    prop_pdf = generate_property_report(sample_lead)
    with open("/tmp/camelot_property_report_test.pdf", "wb") as f:
        f.write(prop_pdf)
    print(f"Property report: {len(prop_pdf):,} bytes → /tmp/camelot_property_report_test.pdf")

    # --- Daily digest smoke test ---
    sample_leads = [sample_lead] * 10
    digest_pdf = generate_lead_report(sample_leads)
    with open("/tmp/camelot_daily_digest_test.pdf", "wb") as f:
        f.write(digest_pdf)
    print(f"Daily digest:    {len(digest_pdf):,} bytes → /tmp/camelot_daily_digest_test.pdf")
