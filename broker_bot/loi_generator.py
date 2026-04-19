"""
loi_generator.py — Camelot Realty Group Letter of Intent Generator

Generates commercial real estate LOIs in NYC/Westchester format.
Supports Markdown output and optional PDF generation via reportlab.

Author: Camelot OS / Broker Bot
"""

import logging
import os
from datetime import date, timedelta
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class PropertyData:
    address: str
    borough_or_county: str
    block: Optional[str] = None
    lot: Optional[str] = None
    asset_type: str = "Multifamily"          # Multifamily | Mixed-Use | Commercial
    year_built: Optional[int] = None
    total_units: Optional[int] = None
    gross_sq_ft: Optional[int] = None
    zoning: Optional[str] = None
    current_use: Optional[str] = None


@dataclass
class BuyerData:
    entity_name: str = "Camelot Acquisitions LLC"
    contact_name: str = "Eleni Palmeri"
    contact_title: str = "Broker of Record, Camelot Realty Group"
    contact_email: str = "epalmeri@camelot.nyc"
    contact_phone: str = ""
    state_of_formation: str = "New York"
    attorney_name: Optional[str] = None
    attorney_firm: Optional[str] = None


@dataclass
class OfferTerms:
    purchase_price: float
    earnest_money_pct: float = 2.0            # % of purchase price
    due_diligence_days: int = 30
    closing_days_after_contract: int = 60
    financing_contingency: bool = False
    financing_amount: Optional[float] = None  # if financing_contingency is True
    financing_days: Optional[int] = None      # days to secure financing commitment
    inspection_contingency: bool = True
    inspection_days: int = 15
    as_is: bool = False
    additional_terms: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core generator
# ---------------------------------------------------------------------------

LEGAL_DISCLAIMER = """
---
**LEGAL DISCLAIMER**

This Letter of Intent is a non-binding expression of interest and does not constitute a 
legally binding offer, contract, or commitment of any kind. This LOI is intended solely 
as a basis for further negotiation of a definitive Purchase and Sale Agreement. Neither 
party shall have any legal obligation to the other unless and until a definitive written 
agreement has been duly executed by authorized representatives of both parties.

This document should be reviewed by qualified legal counsel prior to execution. 
Camelot Realty Group and its affiliates make no representations as to the legal 
sufficiency of this document.
"""


def generate_loi(
    property_data: PropertyData,
    buyer_data: BuyerData,
    offer_terms: OfferTerms,
    seller_name: str = "[Seller Name]",
    loi_date: Optional[date] = None,
    output_pdf: bool = False,
    pdf_output_path: Optional[str] = None,
) -> str:
    """
    Generate a formatted Letter of Intent.

    Args:
        property_data:    Physical property details.
        buyer_data:       Buyer entity and contact information.
        offer_terms:      Financial and contingency terms.
        seller_name:      Name of property seller or their representative.
        loi_date:         Date of LOI (defaults to today).
        output_pdf:       If True, also generate a PDF file.
        pdf_output_path:  Path for PDF output. Defaults to ./LOI_{address}.pdf

    Returns:
        Formatted LOI as a Markdown string.
    """
    if loi_date is None:
        loi_date = date.today()

    earnest_money = offer_terms.purchase_price * (offer_terms.earnest_money_pct / 100.0)
    dd_deadline = loi_date + timedelta(days=offer_terms.due_diligence_days)
    closing_date = loi_date + timedelta(
        days=offer_terms.due_diligence_days + offer_terms.closing_days_after_contract
    )

    address_slug = property_data.address.replace(" ", "_").replace(",", "")

    # Build block/lot string if available
    tax_lot = ""
    if property_data.block and property_data.lot:
        tax_lot = f"\n- **Tax Block/Lot:** Block {property_data.block}, Lot {property_data.lot}"

    # Financing section
    if offer_terms.financing_contingency and offer_terms.financing_amount:
        financing_section = (
            f"\n### 6. Financing Contingency\n\n"
            f"This offer is contingent upon Buyer securing a financing commitment for "
            f"**${offer_terms.financing_amount:,.0f}** within "
            f"**{offer_terms.financing_days or 21} days** of full execution of the Purchase "
            f"and Sale Agreement. Buyer shall use commercially reasonable efforts to secure "
            f"such financing commitment."
        )
    else:
        financing_section = (
            "\n### 6. All-Cash Offer\n\n"
            "This offer is **all-cash** and is **not subject to a financing contingency**. "
            "Buyer is prepared to provide proof of funds within five (5) business days of "
            "mutual acceptance of this LOI."
        )

    # Inspection contingency
    if offer_terms.inspection_contingency:
        inspection_section = (
            f"\n### 7. Inspection Contingency\n\n"
            f"Buyer shall have **{offer_terms.inspection_days} days** from full execution "
            f"of the Purchase and Sale Agreement to conduct physical inspections, environmental "
            f"assessments, review of leases, rent rolls, service contracts, violations, "
            f"permits, and all other due diligence. If Buyer is not satisfied in its sole "
            f"discretion, Buyer may terminate by written notice and receive a full refund "
            f"of Earnest Money."
        )
    else:
        inspection_section = (
            "\n### 7. As-Is Purchase\n\n"
            "Buyer agrees to purchase the Property in its **AS-IS, WHERE-IS** condition "
            "with no inspection contingency. Buyer has had the opportunity to inspect the "
            "Property prior to submission of this LOI."
        )

    # Additional terms
    additional_section = ""
    if offer_terms.additional_terms:
        items = "\n".join(f"- {t}" for t in offer_terms.additional_terms)
        additional_section = f"\n### 10. Additional Terms\n\n{items}"

    # Attorney section
    attorney_section = ""
    if buyer_data.attorney_name:
        attorney_section = (
            f"\n### 9. Buyer's Counsel\n\n"
            f"Buyer's attorney: **{buyer_data.attorney_name}**"
            + (f", {buyer_data.attorney_firm}" if buyer_data.attorney_firm else "")
        )

    loi_text = f"""# LETTER OF INTENT

**DRAFT — NOT EXECUTED | FOR DISCUSSION PURPOSES ONLY**

---

**Date:** {loi_date.strftime("%B %d, %Y")}

**To:** {seller_name} ("Seller")

**From:** {buyer_data.entity_name} ("Buyer")  
{buyer_data.contact_name}, {buyer_data.contact_title}  
{buyer_data.contact_email}{f" | {buyer_data.contact_phone}" if buyer_data.contact_phone else ""}

---

Dear {seller_name},

{buyer_data.entity_name}, a {buyer_data.state_of_formation}-organized entity ("Buyer"), 
is pleased to submit this Letter of Intent ("LOI") for the acquisition of the property 
described below. This LOI is intended to outline the principal terms under which Buyer 
is prepared to proceed toward a definitive Purchase and Sale Agreement ("PSA").

---

## PROPERTY

- **Address:** {property_data.address}
- **Borough/County:** {property_data.borough_or_county}
- **Asset Type:** {property_data.asset_type}{tax_lot}
{f"- **Year Built:** {property_data.year_built}" if property_data.year_built else ""}
{f"- **Total Units:** {property_data.total_units}" if property_data.total_units else ""}
{f"- **Gross Square Feet:** {property_data.gross_sq_ft:,}" if property_data.gross_sq_ft else ""}
{f"- **Zoning:** {property_data.zoning}" if property_data.zoning else ""}

---

## PROPOSED TERMS

### 1. Purchase Price

Buyer proposes to acquire the Property for a purchase price of:

**${offer_terms.purchase_price:,.0f}** ({_price_to_words(offer_terms.purchase_price)})

{f"*(Implied ${offer_terms.purchase_price / property_data.total_units:,.0f} per unit)*" if property_data.total_units else ""}
{f"*(Implied ${offer_terms.purchase_price / property_data.gross_sq_ft:,.2f} per sq ft)*" if property_data.gross_sq_ft else ""}

### 2. Earnest Money Deposit

Within **five (5) business days** of full execution of the Purchase and Sale Agreement, 
Buyer shall deposit Earnest Money of **${earnest_money:,.0f}** 
({offer_terms.earnest_money_pct:.1f}% of Purchase Price) with the escrow agent. 
Earnest Money shall be held in a mutually agreed upon escrow account and applied to 
the Purchase Price at closing.

### 3. Due Diligence Period

Buyer shall have **{offer_terms.due_diligence_days} days** from the date of full 
execution of the PSA to complete all due diligence (estimated deadline: 
**{dd_deadline.strftime("%B %d, %Y")}**). During this period, Seller shall provide 
Buyer with full access to the Property and all related documents, including but not 
limited to: rent rolls, leases, financial statements, service contracts, utility bills, 
violation reports, permits, certificates of occupancy, environmental reports, and 
engineering reports.

### 4. Closing Date

The closing shall occur on or before **{closing_date.strftime("%B %d, %Y")}**, 
approximately {offer_terms.closing_days_after_contract} days following expiration 
of the Due Diligence Period, subject to customary closing conditions.

### 5. Closing Costs

Closing costs shall be allocated in accordance with customary practice in 
{property_data.borough_or_county}, New York (or applicable jurisdiction), 
unless otherwise agreed in the PSA.
{financing_section}
{inspection_section}

### 8. Exclusivity

Upon Seller's counter-signature of this LOI, Seller agrees to grant Buyer an 
exclusive negotiating period of **{offer_terms.due_diligence_days} days** to 
negotiate and execute a definitive PSA, during which Seller shall not solicit, 
negotiate, or enter into any agreement with any other party for the sale of the Property.
{attorney_section}
{additional_section}

---

## NON-BINDING NATURE

This Letter of Intent is a non-binding expression of interest. Neither party shall 
be legally bound unless and until a definitive Purchase and Sale Agreement has been 
fully negotiated and executed by both parties. Buyer reserves the right to withdraw 
this LOI at any time prior to execution of a PSA.

---

## ACCEPTANCE

Please indicate your acceptance of the terms set forth herein by countersigning below 
and returning a copy to Buyer no later than **five (5) business days** from the date 
of this LOI. If not accepted within this period, this LOI shall be deemed withdrawn.

---

**BUYER:**

{buyer_data.entity_name}

By: ___________________________________  
Name: {buyer_data.contact_name}  
Title: {buyer_data.contact_title}  
Date: ____________________

---

**SELLER:**

{seller_name}

By: ___________________________________  
Name: ____________________  
Title: ____________________  
Date: ____________________

---
{LEGAL_DISCLAIMER}
"""

    logger.info(f"Generated LOI for {property_data.address} — purchase price ${offer_terms.purchase_price:,.0f}")

    if output_pdf:
        _generate_pdf(loi_text, pdf_output_path or f"LOI_{address_slug}_{loi_date.isoformat()}.pdf")

    return loi_text


def _price_to_words(price: float) -> str:
    """Convert a dollar amount to a rough human-readable word string."""
    millions = price / 1_000_000
    if price >= 1_000_000:
        return f"{millions:.2f} Million Dollars"
    thousands = price / 1_000
    return f"{thousands:.0f} Thousand Dollars"


def _generate_pdf(loi_markdown: str, output_path: str) -> None:
    """
    Generate a PDF version of the LOI from Markdown text using reportlab.
    Strips Markdown formatting and renders plain text with basic styling.
    """
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
        from reportlab.lib.enums import TA_LEFT, TA_CENTER
        import re

        doc = SimpleDocTemplate(
            output_path,
            pagesize=letter,
            rightMargin=1 * inch,
            leftMargin=1 * inch,
            topMargin=1 * inch,
            bottomMargin=1 * inch,
        )

        styles = getSampleStyleSheet()
        GOLD = colors.HexColor("#C9A84C")
        NAVY = colors.HexColor("#0D1B2A")

        title_style = ParagraphStyle(
            "CamelotTitle",
            parent=styles["Heading1"],
            textColor=NAVY,
            fontSize=16,
            spaceAfter=6,
            alignment=TA_CENTER,
        )
        heading_style = ParagraphStyle(
            "CamelotH2",
            parent=styles["Heading2"],
            textColor=GOLD,
            fontSize=12,
            spaceBefore=12,
            spaceAfter=4,
        )
        body_style = ParagraphStyle(
            "CamelotBody",
            parent=styles["Normal"],
            fontSize=9,
            leading=13,
            spaceAfter=6,
        )
        disclaimer_style = ParagraphStyle(
            "Disclaimer",
            parent=styles["Normal"],
            fontSize=7.5,
            leading=11,
            textColor=colors.grey,
            spaceAfter=4,
        )

        story = []
        lines = loi_markdown.split("\n")

        for line in lines:
            stripped = line.strip()
            if not stripped:
                story.append(Spacer(1, 4))
                continue
            if stripped.startswith("---"):
                story.append(HRFlowable(width="100%", thickness=0.5, color=GOLD, spaceAfter=6))
                continue
            # Strip Markdown bold/italic markers for PDF
            clean = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", stripped)
            clean = re.sub(r"\*(.+?)\*", r"<i>\1</i>", clean)
            clean = re.sub(r"^#{1}\s+", "", clean)  # h1
            if re.match(r"^#{2}\s+", stripped):
                clean = re.sub(r"^#{2}\s+", "", stripped)
                story.append(Paragraph(clean, heading_style))
            elif re.match(r"^#{3}\s+", stripped):
                clean = re.sub(r"^#{3}\s+", "", stripped)
                story.append(Paragraph(f"<b>{clean}</b>", body_style))
            elif "LEGAL DISCLAIMER" in stripped:
                story.append(Paragraph(stripped, disclaimer_style))
            elif re.match(r"^-\s+", stripped):
                clean = re.sub(r"^-\s+", "• ", stripped)
                clean = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", clean)
                story.append(Paragraph(clean, body_style))
            else:
                story.append(Paragraph(clean, body_style))

        doc.build(story)
        logger.info(f"PDF generated: {output_path}")

    except ImportError:
        logger.warning("reportlab not installed — skipping PDF generation. Install with: pip install reportlab")
    except Exception as e:
        logger.error(f"PDF generation failed: {e}")


# ---------------------------------------------------------------------------
# CLI / quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    prop = PropertyData(
        address="123 Main Street, Bronx, NY 10452",
        borough_or_county="Bronx",
        block="2501",
        lot="12",
        asset_type="Multifamily",
        year_built=1965,
        total_units=24,
        gross_sq_ft=18500,
        zoning="R7-1",
    )

    buyer = BuyerData(
        entity_name="Camelot Acquisitions LLC",
        contact_name="Eleni Palmeri",
        contact_title="Broker of Record, Camelot Realty Group",
        contact_email="epalmeri@camelot.nyc",
        contact_phone="(212) 555-0100",
    )

    terms = OfferTerms(
        purchase_price=4_200_000,
        earnest_money_pct=2.0,
        due_diligence_days=30,
        closing_days_after_contract=60,
        financing_contingency=False,
        inspection_contingency=True,
        inspection_days=15,
        additional_terms=[
            "Sale is contingent on the delivery of vacant possession of Unit 4B at closing.",
            "Seller to provide a Phase I Environmental Site Assessment within 10 days of PSA execution.",
        ],
    )

    loi_md = generate_loi(prop, buyer, terms, seller_name="XYZ Realty Corp", output_pdf=False)
    print(loi_md)
