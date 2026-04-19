"""
battlecard_generator.py — Camelot OS Deal Bot
===============================================
Generates one-page battlecards for pre-meeting preparation.

Battlecard sections:
  1. Company Snapshot (what they manage)
  2. Likely Pain Points (derived from public data)
  3. Camelot Value Props (tailored to angle + portfolio size)
  4. Suggested Discovery Questions
  5. Comparable Transactions / Similar Operators Camelot Has Onboarded

Output:
  - PDF (reportlab) — printer-ready, 1–2 pages
  - Markdown — for sharing in Slack / Notion

Author: Camelot OS
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
    KeepTogether,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from prospect_mapper import ProspectProfile

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("deal_bot.battlecard_generator")

# ---------------------------------------------------------------------------
# Brand colours
# ---------------------------------------------------------------------------
GOLD = colors.HexColor("#C9A84C")
NAVY = colors.HexColor("#1A2645")
LIGHT_GOLD = colors.HexColor("#F5EDD6")
LIGHT_GREY = colors.HexColor("#F5F5F5")
MID_GREY = colors.HexColor("#CCCCCC")
DARK_GREY = colors.HexColor("#555555")
GREEN = colors.HexColor("#2E7D32")
RED = colors.HexColor("#C62828")
WHITE = colors.white


# ---------------------------------------------------------------------------
# Comparable operators (anonymized case studies for reference)
# ---------------------------------------------------------------------------

COMPARABLE_OPERATORS = [
    {
        "description": "Bronx operator — 280 units, 25 years in business",
        "structure": "roll-up",
        "outcome": "60% stake acquisition; owner retained as managing partner with salary + exit timeline",
        "relevant_for": ["succession", "tired-operator"],
    },
    {
        "description": "Queens operator — 140 units, 18 years in business",
        "structure": "equity-sale",
        "outcome": "Majority stake; owner took liquidity, remained as property director for 2 years",
        "relevant_for": ["succession", "tired-operator"],
    },
    {
        "description": "Westchester operator — 95 units → 210 units in 18 months",
        "structure": "roll-up",
        "outcome": "Camelot OS deployment + capital access enabled expansion without debt",
        "relevant_for": ["growth"],
    },
    {
        "description": "Bronx operator — 47 open HPD violations at outset",
        "structure": "powered-by",
        "outcome": "Camelot OS reduced violations to 3 within 8 months; compliance cost savings > platform fee",
        "relevant_for": ["systems-upgrade"],
    },
    {
        "description": "Brooklyn operator — 180 rent-stabilized units, compliance burden",
        "structure": "roll-up",
        "outcome": "RS compliance automated; owner statement generation eliminated 12+ hours/week manual work",
        "relevant_for": ["systems-upgrade", "tired-operator"],
    },
    {
        "description": "NJ operator — 60 units, solo owner-operator",
        "structure": "powered-by",
        "outcome": '"Powered by Camelot OS" — technology partnership, owner retained full brand ownership',
        "relevant_for": ["growth", "systems-upgrade"],
    },
]


# ---------------------------------------------------------------------------
# Value proposition library
# ---------------------------------------------------------------------------

VALUE_PROPS = {
    "succession": [
        "Liquidity without a fire sale — Camelot pays fair value for established cash flows",
        "Owner stays involved as long (or short) as desired — no forced exit timeline",
        "Staff retention guaranteed as part of deal terms",
        "Portfolio continuity — tenants and building owners see zero disruption",
        "Legacy preserved: your name, your relationships, Camelot's infrastructure",
    ],
    "growth": [
        "Capital access for portfolio expansion without taking on personal debt",
        "Camelot OS platform scales with you — built for 20 to 2,000 units",
        "Shared vendor contracts reduce operating costs 20–30%",
        "Back-office support frees owner to focus on business development",
        "Collective buying power across the Camelot portfolio",
    ],
    "systems-upgrade": [
        "Camelot OS: automated HPD/DOB/ECB violation monitoring across entire portfolio",
        "Owner statements generated automatically — zero manual effort",
        "Tenant communication + work order management in one platform",
        "Real-time compliance alerts with cure deadline tracking",
        "Operators typically reduce violations 60–80% within 8 months",
    ],
    "tired-operator": [
        "Stop doing the parts you hate — Camelot handles compliance, reporting, vendor management",
        "Partial or full liquidity at fair value based on cash flows",
        "Reduce personal workload without selling the whole business",
        "Professional management infrastructure replaces founder bandwidth",
        "Clear exit timeline — your schedule, your terms",
    ],
}


DISCOVERY_QUESTIONS = {
    "succession": [
        "Have you thought about what happens to the business when you want to step back?",
        "What's most important to you in any transition — price, timing, staff, or continuity?",
        "How long have you been thinking about this?",
        "Have you ever had a valuation conversation before?",
        "What would ideal look like in five years?",
    ],
    "growth": [
        "What's been the biggest obstacle to growing past your current unit count?",
        "How are you financing new building acquisitions today?",
        "What does your current tech stack look like for managing operations?",
        "If capital and systems weren't constraints, where would you want to be in 3 years?",
        "Have you considered taking on a capital or operating partner before?",
    ],
    "systems-upgrade": [
        "How does your team currently monitor HPD and DOB violations across your portfolio?",
        "How much time per week goes into compliance tasks and owner reporting?",
        "What property management software are you using today?",
        "How many open violations are you managing right now?",
        "What would you do with 10 more hours a week?",
    ],
    "tired-operator": [
        "What parts of running the business do you actually enjoy?",
        "What would you do if you had more time outside the business?",
        "Have you looked at selling before? What stopped you?",
        "Is your family involved in the business, or is this a solo operation?",
        "What would staying involved look like — if you had the choice?",
    ],
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Battlecard:
    """Complete battlecard for a pre-meeting prep session."""

    company_name: str
    contact_name: str
    angle: str
    structure: str
    # Snapshot
    estimated_units: int
    geographies: list[str]
    years_in_business: Optional[int]
    owner_name: str
    website: str
    phone: str
    has_rent_stabilized: bool
    rs_unit_count: int
    open_violations: int
    # Content sections
    pain_points: list[str]
    value_props: list[str]
    discovery_questions: list[str]
    comparable_operators: list[dict]
    # Metadata
    generated_at: str

    def to_markdown(self) -> str:
        """Render battlecard as Markdown."""
        lines: list[str] = [
            f"# Battlecard — {self.company_name}",
            f"**Contact:** {self.contact_name}  |  "
            f"**Angle:** {self.angle}  |  "
            f"**Structure:** {self.structure}  |  "
            f"**Generated:** {self.generated_at[:10]}",
            "",
            "---",
            "",
            "## Company Snapshot",
            f"| | |",
            f"|---|---|",
            f"| Estimated Units | {self.estimated_units:,} |",
            f"| Geographies | {', '.join(self.geographies) or '—'} |",
            f"| Years in Business | {self.years_in_business or 'Unknown'} |",
            f"| Owner | {self.owner_name or '—'} |",
            f"| Website | {self.website or '—'} |",
            f"| Phone | {self.phone or '—'} |",
            f"| Rent Stabilized | {'Yes — ' + str(self.rs_unit_count) + ' RS units' if self.has_rent_stabilized else 'No'} |",
            f"| Open Violations | {self.open_violations} |",
            "",
            "---",
            "",
            "## Likely Pain Points",
        ]
        for p in self.pain_points:
            lines.append(f"- {p}")
        lines.extend([
            "",
            "---",
            "",
            "## Camelot Value Props (Tailored)",
        ])
        for v in self.value_props:
            lines.append(f"- {v}")
        lines.extend([
            "",
            "---",
            "",
            "## Suggested Discovery Questions",
        ])
        for i, q in enumerate(self.discovery_questions, 1):
            lines.append(f"{i}. {q}")
        lines.extend([
            "",
            "---",
            "",
            "## Comparable Operators We've Onboarded",
        ])
        for c in self.comparable_operators:
            lines.append(f"- **{c['description']}**")
            lines.append(f"  Structure: {c['structure']} — {c['outcome']}")
        lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Battlecard factory
# ---------------------------------------------------------------------------

def build_battlecard(profile: ProspectProfile) -> Battlecard:
    """Build a Battlecard from a ProspectProfile."""
    angle = profile.recommended_angle or "growth"
    structure = profile.recommended_structure or "roll-up"

    # Select value props and questions for the angle
    vp = VALUE_PROPS.get(angle, VALUE_PROPS["growth"])
    questions = DISCOVERY_QUESTIONS.get(angle, DISCOVERY_QUESTIONS["growth"])

    # Select relevant comps
    comps = [
        c for c in COMPARABLE_OPERATORS
        if angle in c["relevant_for"] or structure == c["structure"]
    ][:3]  # top 3

    contact_name = (
        profile.contacts[0].name if profile.contacts else (profile.owner_name or "—")
    )

    return Battlecard(
        company_name=profile.company_name,
        contact_name=contact_name,
        angle=angle,
        structure=structure,
        estimated_units=profile.estimated_units,
        geographies=profile.geographies_served,
        years_in_business=profile.years_in_business,
        owner_name=profile.owner_name,
        website=profile.website,
        phone=profile.phone,
        has_rent_stabilized=profile.has_rent_stabilized,
        rs_unit_count=profile.rs_unit_count,
        open_violations=profile.open_violation_count,
        pain_points=profile.pain_points or ["Operational efficiency opportunities through technology"],
        value_props=vp,
        discovery_questions=questions,
        comparable_operators=comps,
        generated_at=datetime.utcnow().isoformat(),
    )


# ---------------------------------------------------------------------------
# PDF renderer
# ---------------------------------------------------------------------------

class BattlecardPDFRenderer:
    """Renders a Battlecard to a professional PDF."""

    def __init__(self, battlecard: Battlecard) -> None:
        self.bc = battlecard
        self.styles = getSampleStyleSheet()

    def _section_header(self, title: str) -> list[Any]:
        style = ParagraphStyle(
            "SH",
            fontName="Helvetica-Bold",
            fontSize=10,
            textColor=WHITE,
            alignment=TA_LEFT,
        )
        cell = Table(
            [[Paragraph(title.upper(), style)]],
            colWidths=[7.0 * inch],
        )
        cell.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), NAVY),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ])
        )
        return [cell, Spacer(1, 4)]

    def _bullet_table(self, items: list[str], bullet: str = "•") -> Table:
        """Render a bulleted list as a 2-column table (bullet | text)."""
        rows = [
            [
                Paragraph(bullet, ParagraphStyle("b", fontSize=9, fontName="Helvetica-Bold", textColor=GOLD)),
                Paragraph(item, ParagraphStyle("t", fontSize=9, leading=13)),
            ]
            for item in items
        ]
        t = Table(rows, colWidths=[0.2 * inch, 6.8 * inch])
        t.setStyle(TableStyle([
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        return t

    def render(self, output_path: str) -> str:
        """Render and save the battlecard PDF. Returns output_path."""
        bc = self.bc
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=letter,
            rightMargin=0.6 * inch,
            leftMargin=0.6 * inch,
            topMargin=0.6 * inch,
            bottomMargin=0.6 * inch,
            title=f"Battlecard — {bc.company_name}",
        )

        story: list[Any] = []

        # ── Header bar ────────────────────────────────────────────────────
        header_data = [[
            Paragraph(
                f"<b>{bc.company_name}</b>",
                ParagraphStyle("hh", fontName="Helvetica-Bold", fontSize=14, textColor=WHITE),
            ),
            Paragraph(
                f"<b>Angle:</b> {bc.angle.title()}  ·  <b>Structure:</b> {bc.structure.title()}",
                ParagraphStyle("hm", fontName="Helvetica", fontSize=9, textColor=LIGHT_GOLD, alignment=TA_LEFT),
            ),
            Paragraph(
                f"<b>Contact:</b> {bc.contact_name}",
                ParagraphStyle("hr", fontName="Helvetica", fontSize=9, textColor=LIGHT_GOLD, alignment=TA_LEFT),
            ),
        ]]
        header_table = Table(header_data, colWidths=[3.2 * inch, 2.0 * inch, 2.0 * inch])
        header_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), NAVY),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LINEBELOW", (0, 0), (-1, -1), 2, GOLD),
        ]))
        story.append(header_table)
        story.append(Spacer(1, 8))

        # ── Company Snapshot ──────────────────────────────────────────────
        story.extend(self._section_header("Company Snapshot"))

        snapshot_items = [
            ["Estimated Units", f"{bc.estimated_units:,}"],
            ["Geographies", ", ".join(bc.geographies) if bc.geographies else "—"],
            ["Years in Business", str(bc.years_in_business) if bc.years_in_business else "Unknown"],
            ["Owner / Principal", bc.owner_name or "—"],
            ["Website", bc.website or "—"],
            ["Phone", bc.phone or "—"],
            ["Rent Stabilized", f"Yes — {bc.rs_unit_count:,} RS units" if bc.has_rent_stabilized else "No"],
            ["Open Violations", str(bc.open_violations)],
        ]

        snap_style_l = ParagraphStyle("sl", fontName="Helvetica-Bold", fontSize=9, textColor=NAVY)
        snap_style_r = ParagraphStyle("sr", fontName="Helvetica", fontSize=9)
        snap_rows: list[list[Any]] = []
        for label, val in snapshot_items:
            # Colour-code violations
            if label == "Open Violations" and bc.open_violations > 10:
                val_p = Paragraph(
                    f'<font color="#{self._hex(RED)}">{val} ⚠</font>', snap_style_r
                )
            else:
                val_p = Paragraph(val, snap_style_r)
            snap_rows.append([Paragraph(label, snap_style_l), val_p])

        snap_table = Table(snap_rows, colWidths=[1.8 * inch, 5.2 * inch])
        snap_table.setStyle(TableStyle([
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [WHITE, LIGHT_GREY]),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
        ]))
        story.append(snap_table)
        story.append(Spacer(1, 8))

        # ── Pain Points ───────────────────────────────────────────────────
        story.extend(self._section_header("Likely Pain Points"))
        story.append(self._bullet_table(bc.pain_points))
        story.append(Spacer(1, 8))

        # ── Value Props ───────────────────────────────────────────────────
        story.extend(self._section_header("Camelot Value Props (Tailored)"))
        story.append(self._bullet_table(bc.value_props))
        story.append(Spacer(1, 8))

        # ── Discovery Questions ───────────────────────────────────────────
        story.extend(self._section_header("Suggested Discovery Questions"))
        numbered = [f"{i}. {q}" for i, q in enumerate(bc.discovery_questions, 1)]
        story.append(self._bullet_table(numbered, bullet=" "))
        story.append(Spacer(1, 8))

        # ── Comparable Operators ─────────────────────────────────────────
        if bc.comparable_operators:
            story.extend(self._section_header("Comparable Operators Camelot Has Onboarded"))
            for comp in bc.comparable_operators:
                desc_style = ParagraphStyle(
                    "cd", fontName="Helvetica-Bold", fontSize=9, textColor=NAVY, spaceAfter=1
                )
                outcome_style = ParagraphStyle(
                    "co", fontName="Helvetica", fontSize=9, textColor=DARK_GREY, spaceAfter=4
                )
                story.append(Paragraph(f"• {comp['description']} ({comp['structure']})", desc_style))
                story.append(Paragraph(f"  → {comp['outcome']}", outcome_style))

        # ── Footer ────────────────────────────────────────────────────────
        story.append(Spacer(1, 0.1 * inch))
        story.append(HRFlowable(width="100%", thickness=1, color=GOLD))
        footer_style = ParagraphStyle(
            "ft", fontName="Helvetica", fontSize=7, textColor=DARK_GREY,
            alignment=TA_CENTER
        )
        story.append(Paragraph(
            f"Camelot Property Management Services Corp — CONFIDENTIAL — "
            f"Generated {bc.generated_at[:10]} by Camelot OS Deal Bot",
            footer_style,
        ))

        doc.build(story)

        with open(output_path, "wb") as f:
            f.write(buf.getvalue())

        logger.info("Battlecard PDF written to %s", output_path)
        return output_path

    @staticmethod
    def _hex(color: Any) -> str:
        try:
            h = color.hexval()
            return h.lstrip("#").upper() if h.startswith("#") else h.upper()
        except AttributeError:
            return "000000"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_battlecard(
    profile: ProspectProfile,
    output_dir: str = ".",
    formats: Optional[list[str]] = None,
) -> dict[str, str]:
    """
    Generate a battlecard from a ProspectProfile in PDF and/or Markdown.

    Args:
        profile:    ProspectProfile from prospect_mapper.
        output_dir: Directory to write output files.
        formats:    List of 'pdf' and/or 'md'. Defaults to both.

    Returns:
        Dict with keys 'pdf_path' and/or 'md_path' (only keys for formats generated).
    """
    if formats is None:
        formats = ["pdf", "md"]

    os.makedirs(output_dir, exist_ok=True)
    battlecard = build_battlecard(profile)

    # Safe filename from company name
    safe_name = "".join(
        c if c.isalnum() or c in (" ", "-", "_") else "_"
        for c in profile.company_name
    ).strip()[:50]

    results: dict[str, str] = {}

    if "pdf" in formats:
        pdf_path = os.path.join(output_dir, f"battlecard_{safe_name}.pdf")
        BattlecardPDFRenderer(battlecard).render(pdf_path)
        results["pdf_path"] = pdf_path

    if "md" in formats:
        md_path = os.path.join(output_dir, f"battlecard_{safe_name}.md")
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(battlecard.to_markdown())
        logger.info("Battlecard Markdown written to %s", md_path)
        results["md_path"] = md_path

    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json
    import sys

    from dataclasses import fields as dc_fields

    parser = argparse.ArgumentParser(description="Generate battlecard for a prospect")
    parser.add_argument("--profile", required=True, help="Path to prospect JSON file")
    parser.add_argument("--output-dir", default="output/battlecards")
    parser.add_argument(
        "--format",
        nargs="+",
        choices=["pdf", "md"],
        default=["pdf", "md"],
        help="Output formats",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print Markdown to stdout instead of writing files",
    )
    args = parser.parse_args()

    with open(args.profile) as f:
        data = json.load(f)

    valid_keys = {f.name for f in dc_fields(ProspectProfile)}
    profile = ProspectProfile(**{k: v for k, v in data.items() if k in valid_keys})

    if args.preview:
        bc = build_battlecard(profile)
        print(bc.to_markdown())
        sys.exit(0)

    results = generate_battlecard(
        profile=profile,
        output_dir=args.output_dir,
        formats=args.format,
    )
    for key, path in results.items():
        print(f"{key}: {path}")
