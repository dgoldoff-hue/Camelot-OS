"""
investor_update.py — Camelot OS Report Bot
===========================================
Generates quarterly investor update reports in PDF format.

Report sections:
  1. Portfolio Overview
  2. Financial Performance (NOI, revenue, expenses, variance)
  3. Market Commentary
  4. Occupancy Trends
  5. Capital Improvements
  6. Outlook & Forward Guidance

Data sources:
  - Supabase: financial data, occupancy history, capex records
  - HubSpot: pipeline value
  - MDS export cache (CSV) for property-level financials

Author: Camelot OS
"""

from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
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

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("report_bot.investor_update")

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
# HTTP session with retry
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _make_session()


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class QuarterPeriod:
    """Represents a fiscal quarter."""

    year: int
    quarter: int   # 1–4

    @property
    def label(self) -> str:
        return f"Q{self.quarter} {self.year}"

    @property
    def start_date(self) -> date:
        month = (self.quarter - 1) * 3 + 1
        return date(self.year, month, 1)

    @property
    def end_date(self) -> date:
        month = self.quarter * 3
        if month == 12:
            return date(self.year, 12, 31)
        next_month_start = date(self.year, month + 1, 1)
        return date(next_month_start.year, next_month_start.month, 1).__class__(
            next_month_start.year,
            next_month_start.month,
            1,
        ) - __import__("datetime").timedelta(days=1)

    @classmethod
    def current(cls) -> "QuarterPeriod":
        today = date.today()
        return cls(year=today.year, quarter=(today.month - 1) // 3 + 1)

    @classmethod
    def prior(cls) -> "QuarterPeriod":
        current = cls.current()
        if current.quarter == 1:
            return cls(year=current.year - 1, quarter=4)
        return cls(year=current.year, quarter=current.quarter - 1)


@dataclass
class PropertyFinancials:
    """Financial data for a single property for the quarter."""

    property_name: str
    address: str
    units: int
    gross_revenue: float
    operating_expenses: float
    noi: float
    occupancy_pct: float
    prior_noi: Optional[float] = None
    capex: float = 0.0

    @property
    def noi_variance(self) -> Optional[float]:
        if self.prior_noi is None:
            return None
        return self.noi - self.prior_noi

    @property
    def noi_variance_pct(self) -> Optional[float]:
        if self.prior_noi is None or self.prior_noi == 0:
            return None
        return ((self.noi - self.prior_noi) / abs(self.prior_noi)) * 100


@dataclass
class CapExItem:
    """A single capital improvement project."""

    property_name: str
    description: str
    amount: float
    completion_date: Optional[date]
    status: str  # "completed", "in_progress", "planned"


@dataclass
class InvestorUpdateData:
    """All data needed to render the investor update PDF."""

    quarter: QuarterPeriod
    properties: list[PropertyFinancials] = field(default_factory=list)
    capex_items: list[CapExItem] = field(default_factory=list)
    market_commentary: str = ""
    outlook: str = ""
    hubspot_pipeline_value: float = 0.0
    generated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def total_units(self) -> int:
        return sum(p.units for p in self.properties)

    @property
    def total_gross_revenue(self) -> float:
        return sum(p.gross_revenue for p in self.properties)

    @property
    def total_opex(self) -> float:
        return sum(p.operating_expenses for p in self.properties)

    @property
    def total_noi(self) -> float:
        return sum(p.noi for p in self.properties)

    @property
    def portfolio_occupancy(self) -> float:
        if not self.properties:
            return 0.0
        weighted = sum(p.occupancy_pct * p.units for p in self.properties)
        return weighted / self.total_units

    @property
    def total_capex(self) -> float:
        return sum(c.amount for c in self.capex_items)


# ---------------------------------------------------------------------------
# Data collector
# ---------------------------------------------------------------------------

class InvestorDataCollector:
    """Pulls quarterly financial data from Supabase."""

    def __init__(self) -> None:
        self.supabase_url: str = os.environ["SUPABASE_URL"]
        self.supabase_key: str = os.environ["SUPABASE_SERVICE_KEY"]

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

    def fetch_property_financials(
        self, quarter: QuarterPeriod
    ) -> list[PropertyFinancials]:
        """
        Query Supabase `quarterly_financials` view/table.

        Expected schema:
            quarterly_financials(
                property_name TEXT, address TEXT, units INT,
                period_year INT, period_quarter INT,
                gross_revenue NUMERIC, operating_expenses NUMERIC,
                noi NUMERIC, occupancy_pct NUMERIC, capex NUMERIC
            )
        """
        url = f"{self.supabase_url}/rest/v1/quarterly_financials"
        params = {
            "select": "property_name,address,units,gross_revenue,operating_expenses,noi,occupancy_pct,capex",
            "period_year": f"eq.{quarter.year}",
            "period_quarter": f"eq.{quarter.quarter}",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=20)
            resp.raise_for_status()
            rows = resp.json()
        except requests.RequestException as exc:
            logger.error("Failed to fetch quarterly financials: %s", exc)
            return []

        # Fetch prior quarter for variance
        prior = QuarterPeriod.prior() if quarter == QuarterPeriod.current() else None
        prior_map: dict[str, float] = {}
        if prior:
            prior_map = self._fetch_prior_noi(prior)

        results: list[PropertyFinancials] = []
        for row in rows:
            name = row.get("property_name", "Unknown")
            results.append(
                PropertyFinancials(
                    property_name=name,
                    address=row.get("address", ""),
                    units=int(row.get("units", 0)),
                    gross_revenue=float(row.get("gross_revenue", 0)),
                    operating_expenses=float(row.get("operating_expenses", 0)),
                    noi=float(row.get("noi", 0)),
                    occupancy_pct=float(row.get("occupancy_pct", 0)),
                    capex=float(row.get("capex", 0)),
                    prior_noi=prior_map.get(name),
                )
            )
        logger.info("Fetched financials for %d properties", len(results))
        return results

    def _fetch_prior_noi(self, quarter: QuarterPeriod) -> dict[str, float]:
        url = f"{self.supabase_url}/rest/v1/quarterly_financials"
        params = {
            "select": "property_name,noi",
            "period_year": f"eq.{quarter.year}",
            "period_quarter": f"eq.{quarter.quarter}",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=20)
            resp.raise_for_status()
            return {r["property_name"]: float(r["noi"]) for r in resp.json()}
        except requests.RequestException as exc:
            logger.warning("Could not fetch prior quarter NOI: %s", exc)
            return {}

    def fetch_capex_items(self, quarter: QuarterPeriod) -> list[CapExItem]:
        """
        Query Supabase `capex_projects` table for items in the given quarter.
        """
        url = f"{self.supabase_url}/rest/v1/capex_projects"
        params = {
            "select": "property_name,description,amount,completion_date,status",
            "or": (
                f"(completion_date.gte.{quarter.start_date.isoformat()},"
                f"completion_date.lte.{quarter.end_date.isoformat()},"
                f"status.eq.in_progress)"
            ),
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=20)
            resp.raise_for_status()
            rows = resp.json()
        except requests.RequestException as exc:
            logger.warning("CapEx fetch failed: %s", exc)
            return []

        items: list[CapExItem] = []
        for row in rows:
            comp_str = row.get("completion_date")
            comp_date: Optional[date] = None
            if comp_str:
                try:
                    comp_date = date.fromisoformat(comp_str[:10])
                except ValueError:
                    pass
            items.append(
                CapExItem(
                    property_name=row.get("property_name", ""),
                    description=row.get("description", ""),
                    amount=float(row.get("amount", 0)),
                    completion_date=comp_date,
                    status=row.get("status", "unknown"),
                )
            )
        logger.info("Fetched %d CapEx items", len(items))
        return items

    def fetch_pipeline_value(self) -> float:
        """Get HubSpot pipeline value (total open deals)."""
        token = os.getenv("HUBSPOT_ACCESS_TOKEN")
        if not token:
            return 0.0

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        # Get pipeline IDs first
        try:
            resp = SESSION.get(
                "https://api.hubapi.com/crm/v3/pipelines/deals",
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            pipelines = resp.json().get("results", [])
            target_names = {"Camelot Roll-Up", "Camelot Brokerage"}
            pipeline_ids = [
                p["id"] for p in pipelines if p.get("label") in target_names
            ]
        except requests.RequestException as exc:
            logger.warning("HubSpot pipeline lookup failed: %s", exc)
            return 0.0

        total = 0.0
        for pid in pipeline_ids:
            payload = {
                "filterGroups": [
                    {
                        "filters": [
                            {"propertyName": "pipeline", "operator": "EQ", "value": pid},
                            {
                                "propertyName": "dealstage",
                                "operator": "NOT_IN",
                                "values": ["closedwon", "closedlost"],
                            },
                        ]
                    }
                ],
                "properties": ["amount"],
                "limit": 100,
            }
            try:
                resp = SESSION.post(
                    "https://api.hubapi.com/crm/v3/objects/deals/search",
                    headers=headers,
                    json=payload,
                    timeout=20,
                )
                resp.raise_for_status()
                for deal in resp.json().get("results", []):
                    amt = deal.get("properties", {}).get("amount")
                    if amt:
                        try:
                            total += float(amt)
                        except ValueError:
                            pass
            except requests.RequestException as exc:
                logger.warning("Deal sum failed: %s", exc)

        return total


# ---------------------------------------------------------------------------
# Default commentary templates (overridden by env/config if set)
# ---------------------------------------------------------------------------

def _default_market_commentary(quarter: QuarterPeriod) -> str:
    return (
        f"The New York metropolitan area multifamily market continued to demonstrate "
        f"resilience during {quarter.label}. Rental demand remained robust across all "
        f"sub-markets in our portfolio — Manhattan, the outer boroughs, Westchester, "
        f"and Northern New Jersey — driven by continued household formation and constrained "
        f"new supply. Interest rate stabilization has improved the transaction environment, "
        f"and we are monitoring select acquisition opportunities that align with our "
        f"roll-up thesis. Our Connecticut and Florida assets are performing above "
        f"underwriting expectations on an occupancy basis."
    )


def _default_outlook(quarter: QuarterPeriod) -> str:
    next_q = QuarterPeriod(
        year=quarter.year + (1 if quarter.quarter == 4 else 0),
        quarter=1 if quarter.quarter == 4 else quarter.quarter + 1,
    )
    return (
        f"Looking ahead to {next_q.label}, management expects continued strong occupancy "
        f"across the portfolio. We are actively pursuing operator acquisitions in the "
        f"tri-state area through our Camelot Roll-Up program, with several prospects in "
        f"the Term Sheet stage. Camelot OS deployment will be extended to five additional "
        f"buildings, further improving operational efficiency and data visibility. "
        f"We anticipate NOI growth in the range of 3–5% driven by rent renewals and "
        f"ongoing expense rationalization through technology."
    )


# ---------------------------------------------------------------------------
# PDF renderer
# ---------------------------------------------------------------------------

class InvestorUpdatePDFRenderer:
    """Renders an InvestorUpdateData object to a professional PDF."""

    # Page number support
    _page_num: int = 0

    def __init__(self, data: InvestorUpdateData) -> None:
        self.data = data
        self.styles = getSampleStyleSheet()
        self._build_styles()

    def _build_styles(self) -> None:
        """Define all custom paragraph styles."""
        self.s_title = ParagraphStyle(
            "InvTitle",
            fontName="Helvetica-Bold",
            fontSize=22,
            textColor=WHITE,
            alignment=TA_CENTER,
            spaceAfter=4,
        )
        self.s_subtitle = ParagraphStyle(
            "InvSubtitle",
            fontName="Helvetica",
            fontSize=12,
            textColor=LIGHT_GOLD,
            alignment=TA_CENTER,
            spaceAfter=2,
        )
        self.s_section = ParagraphStyle(
            "SectionHead",
            fontName="Helvetica-Bold",
            fontSize=13,
            textColor=NAVY,
            spaceBefore=14,
            spaceAfter=6,
            borderPad=4,
            leftIndent=0,
        )
        self.s_body = ParagraphStyle(
            "Body",
            fontName="Helvetica",
            fontSize=10,
            textColor=colors.black,
            leading=14,
            alignment=TA_JUSTIFY,
            spaceAfter=6,
        )
        self.s_caption = ParagraphStyle(
            "Caption",
            fontName="Helvetica",
            fontSize=8,
            textColor=DARK_GREY,
            alignment=TA_CENTER,
            spaceAfter=4,
        )
        self.s_footer = ParagraphStyle(
            "Footer",
            fontName="Helvetica",
            fontSize=7,
            textColor=colors.grey,
            alignment=TA_CENTER,
        )
        self.s_highlight = ParagraphStyle(
            "Highlight",
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=NAVY,
            alignment=TA_CENTER,
        )
        self.s_label = ParagraphStyle(
            "Label",
            fontName="Helvetica",
            fontSize=8,
            textColor=DARK_GREY,
            alignment=TA_CENTER,
        )

    def _section_header(self, title: str) -> list[Any]:
        """Return flowables for a section header with gold underline."""
        return [
            Paragraph(title.upper(), self.s_section),
            HRFlowable(width="100%", thickness=1.5, color=GOLD, spaceAfter=8),
        ]

    def _summary_table(self) -> Table:
        """Portfolio summary metrics in a 3×2 highlights grid."""
        d = self.data
        metrics = [
            ("Total Properties", str(len(d.properties))),
            ("Total Units", f"{d.total_units:,}"),
            ("Portfolio Occupancy", f"{d.portfolio_occupancy:.1f}%"),
            ("Gross Revenue", f"${d.total_gross_revenue:,.0f}"),
            ("Net Operating Income", f"${d.total_noi:,.0f}"),
            ("HubSpot Pipeline", f"${d.hubspot_pipeline_value:,.0f}"),
        ]

        # Build 2-column rows
        rows: list[list[Any]] = []
        for i in range(0, len(metrics), 2):
            row: list[Any] = []
            for label, value in metrics[i : i + 2]:
                cell = [
                    Paragraph(value, self.s_highlight),
                    Paragraph(label, self.s_label),
                ]
                row.append(cell)
            if len(row) < 2:
                row.append("")
            rows.append(row)

        t = Table(rows, colWidths=[3.5 * inch, 3.5 * inch])
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), LIGHT_GREY),
                    ("ROWBACKGROUNDS", (0, 0), (-1, -1), [LIGHT_GREY, LIGHT_GOLD]),
                    ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
                    ("TOPPADDING", (0, 0), (-1, -1), 10),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                    ("LEFTPADDING", (0, 0), (-1, -1), 12),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        return t

    def _financial_table(self) -> Table:
        """Per-property financial performance table."""
        header = [
            Paragraph("<b>Property</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold")),
            Paragraph("<b>Units</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>Gross Revenue</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>OpEx</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>NOI</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>Occ%</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>NOI Δ</b>", ParagraphStyle("th", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
        ]
        table_data: list[list[Any]] = [header]

        cell_style_r = ParagraphStyle("tdr", fontSize=9, alignment=TA_RIGHT)
        cell_style_l = ParagraphStyle("tdl", fontSize=9, alignment=TA_LEFT)

        for p in self.data.properties:
            noi_delta = "—"
            delta_color = colors.black
            if p.noi_variance_pct is not None:
                sign = "+" if p.noi_variance_pct >= 0 else ""
                noi_delta = f"{sign}{p.noi_variance_pct:.1f}%"
                delta_color = GREEN if p.noi_variance_pct >= 0 else RED

            table_data.append([
                Paragraph(p.property_name, cell_style_l),
                Paragraph(f"{p.units:,}", cell_style_r),
                Paragraph(f"${p.gross_revenue:,.0f}", cell_style_r),
                Paragraph(f"${p.operating_expenses:,.0f}", cell_style_r),
                Paragraph(f"${p.noi:,.0f}", cell_style_r),
                Paragraph(f"{p.occupancy_pct:.1f}%", cell_style_r),
                Paragraph(
                    f'<font color="#{self._hex(delta_color)}">{noi_delta}</font>',
                    cell_style_r,
                ),
            ])

        # Totals row
        total_var = "—"
        total_var_color = colors.black
        total_prior = sum(p.prior_noi for p in self.data.properties if p.prior_noi)
        if total_prior:
            v = ((self.data.total_noi - total_prior) / abs(total_prior)) * 100
            sign = "+" if v >= 0 else ""
            total_var = f"{sign}{v:.1f}%"
            total_var_color = GREEN if v >= 0 else RED

        totals_style = ParagraphStyle("tots", fontSize=9, fontName="Helvetica-Bold", alignment=TA_RIGHT)
        totals_style_l = ParagraphStyle("totsl", fontSize=9, fontName="Helvetica-Bold")
        table_data.append([
            Paragraph("<b>TOTAL PORTFOLIO</b>", totals_style_l),
            Paragraph(f"<b>{self.data.total_units:,}</b>", totals_style),
            Paragraph(f"<b>${self.data.total_gross_revenue:,.0f}</b>", totals_style),
            Paragraph(f"<b>${self.data.total_opex:,.0f}</b>", totals_style),
            Paragraph(f"<b>${self.data.total_noi:,.0f}</b>", totals_style),
            Paragraph(
                f"<b>{self.data.portfolio_occupancy:.1f}%</b>", totals_style
            ),
            Paragraph(
                f'<b><font color="#{self._hex(total_var_color)}">{total_var}</font></b>',
                totals_style,
            ),
        ])

        col_widths = [2.1 * inch, 0.5 * inch, 1.1 * inch, 1.1 * inch, 1.1 * inch, 0.6 * inch, 0.7 * inch]
        t = Table(table_data, colWidths=col_widths, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, LIGHT_GREY]),
                    ("BACKGROUND", (0, -1), (-1, -1), LIGHT_GOLD),
                    ("LINEABOVE", (0, -1), (-1, -1), 1.5, GOLD),
                    ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, GOLD),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        return t

    def _capex_table(self) -> Optional[Table]:
        """Capital improvements table. Returns None if no items."""
        if not self.data.capex_items:
            return None

        header = [
            Paragraph("<b>Property</b>", ParagraphStyle("ch", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold")),
            Paragraph("<b>Description</b>", ParagraphStyle("ch", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold")),
            Paragraph("<b>Amount</b>", ParagraphStyle("ch", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("<b>Status</b>", ParagraphStyle("ch", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_CENTER)),
            Paragraph("<b>Completion</b>", ParagraphStyle("ch", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_CENTER)),
        ]
        table_data = [header]

        for item in self.data.capex_items:
            status_colors = {
                "completed": GREEN,
                "in_progress": GOLD,
                "planned": DARK_GREY,
            }
            sc = status_colors.get(item.status.lower(), colors.black)
            comp = item.completion_date.strftime("%b %Y") if item.completion_date else "TBD"
            table_data.append([
                Paragraph(item.property_name, ParagraphStyle("cd", fontSize=9)),
                Paragraph(item.description, ParagraphStyle("cd", fontSize=9)),
                Paragraph(f"${item.amount:,.0f}", ParagraphStyle("cd", fontSize=9, alignment=TA_RIGHT)),
                Paragraph(
                    f'<font color="#{self._hex(sc)}">{item.status.title()}</font>',
                    ParagraphStyle("cd", fontSize=9, alignment=TA_CENTER),
                ),
                Paragraph(comp, ParagraphStyle("cd", fontSize=9, alignment=TA_CENTER)),
            ])

        # Total
        table_data.append([
            Paragraph("<b>TOTAL</b>", ParagraphStyle("ct", fontSize=9, fontName="Helvetica-Bold")),
            Paragraph("", ParagraphStyle("ct", fontSize=9)),
            Paragraph(f"<b>${self.data.total_capex:,.0f}</b>", ParagraphStyle("ct", fontSize=9, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            Paragraph("", ParagraphStyle("ct", fontSize=9)),
            Paragraph("", ParagraphStyle("ct", fontSize=9)),
        ])

        col_widths = [1.5 * inch, 2.6 * inch, 1.0 * inch, 1.0 * inch, 1.1 * inch]
        t = Table(table_data, colWidths=col_widths, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -2), [WHITE, LIGHT_GREY]),
                    ("BACKGROUND", (0, -1), (-1, -1), LIGHT_GOLD),
                    ("LINEABOVE", (0, -1), (-1, -1), 1.5, GOLD),
                    ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, GOLD),
                ]
            )
        )
        return t

    @staticmethod
    def _hex(color: Any) -> str:
        """Return 6-char hex code from a reportlab HexColor."""
        try:
            h = color.hexval()
            return h.lstrip("#").upper() if h.startswith("#") else h.upper()
        except AttributeError:
            return "000000"

    def _cover_page(self, story: list[Any]) -> None:
        """Full navy cover page."""
        d = self.data
        quarter_label = d.quarter.label
        generated = d.generated_at.strftime("%B %d, %Y")

        # Use a colored table as a cover background block
        cover_content = [
            [Paragraph("CAMELOT PROPERTY MANAGEMENT", self.s_title)],
            [Paragraph("SERVICES CORP", self.s_title)],
            [Spacer(1, 0.25 * inch)],
            [Paragraph(f"Investor Update — {quarter_label}", self.s_subtitle)],
            [Paragraph("CONFIDENTIAL", ParagraphStyle(
                "conf", fontName="Helvetica",
                fontSize=10, textColor=GOLD,
                alignment=TA_CENTER, spaceAfter=2,
            ))],
            [Spacer(1, 0.4 * inch)],
            [Paragraph(f"Prepared: {generated}", ParagraphStyle(
                "prepd", fontName="Helvetica",
                fontSize=9, textColor=LIGHT_GOLD,
                alignment=TA_CENTER,
            ))],
        ]
        cover = Table(
            cover_content,
            colWidths=[7.0 * inch],
            rowHeights=None,
        )
        cover.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), NAVY),
                    ("TOPPADDING", (0, 0), (-1, -1), 14),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
                    ("LEFTPADDING", (0, 0), (-1, -1), 24),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 24),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        story.append(Spacer(1, 1.5 * inch))
        story.append(cover)
        story.append(PageBreak())

    def render(self, output_path: str) -> str:
        """Render the investor update to output_path and return path."""
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
            title=f"Camelot Investor Update — {self.data.quarter.label}",
            author="Camelot Property Management Services Corp",
        )

        story: list[Any] = []

        # ── Cover page ───────────────────────────────────────────────────────
        self._cover_page(story)

        # ── 1. Portfolio Overview ────────────────────────────────────────────
        story.extend(self._section_header("1. Portfolio Overview"))
        story.append(self._summary_table())
        story.append(Spacer(1, 0.15 * inch))
        overview_text = (
            f"Camelot Property Management Services Corp manages a diversified portfolio of "
            f"{len(self.data.properties)} properties comprising {self.data.total_units:,} residential "
            f"and mixed-use units across New York City, Westchester County, Connecticut, "
            f"New Jersey, and Florida. During {self.data.quarter.label}, the portfolio achieved "
            f"a weighted average occupancy rate of {self.data.portfolio_occupancy:.1f}%, "
            f"generating gross revenues of ${self.data.total_gross_revenue:,.0f} and "
            f"net operating income of ${self.data.total_noi:,.0f}."
        )
        story.append(Paragraph(overview_text, self.s_body))

        # ── 2. Financial Performance ─────────────────────────────────────────
        story.extend(self._section_header("2. Financial Performance"))
        story.append(self._financial_table())
        story.append(Spacer(1, 0.1 * inch))

        noi_margin = (
            (self.data.total_noi / self.data.total_gross_revenue * 100)
            if self.data.total_gross_revenue > 0
            else 0.0
        )
        fin_text = (
            f"Portfolio NOI margin for {self.data.quarter.label} was {noi_margin:.1f}%, "
            f"with total operating expenses of ${self.data.total_opex:,.0f} representing "
            f"{100 - noi_margin:.1f}% of gross revenue. Management continues to implement "
            f"operational efficiencies through the Camelot OS platform, including automated "
            f"vendor invoicing, predictive maintenance scheduling, and real-time expense tracking."
        )
        story.append(Paragraph(fin_text, self.s_body))

        # ── 3. Market Commentary ─────────────────────────────────────────────
        story.extend(self._section_header("3. Market Commentary"))
        story.append(Paragraph(self.data.market_commentary, self.s_body))

        # ── 4. Occupancy Trends ──────────────────────────────────────────────
        story.extend(self._section_header("4. Occupancy Trends"))
        occ_data = [
            [
                Paragraph("<b>Property</b>", ParagraphStyle("oh", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold")),
                Paragraph("<b>Units</b>", ParagraphStyle("oh", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
                Paragraph("<b>Occupancy</b>", ParagraphStyle("oh", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
                Paragraph("<b>Occupied</b>", ParagraphStyle("oh", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
                Paragraph("<b>Vacant</b>", ParagraphStyle("oh", fontSize=9, textColor=WHITE, fontName="Helvetica-Bold", alignment=TA_RIGHT)),
            ]
        ]
        for p in sorted(self.data.properties, key=lambda x: x.occupancy_pct, reverse=True):
            occupied_units = round(p.units * p.occupancy_pct / 100)
            vacant_units = p.units - occupied_units
            occ_color = GREEN if p.occupancy_pct >= 95 else (GOLD if p.occupancy_pct >= 85 else RED)
            occ_data.append([
                Paragraph(p.property_name, ParagraphStyle("od", fontSize=9)),
                Paragraph(f"{p.units:,}", ParagraphStyle("od", fontSize=9, alignment=TA_RIGHT)),
                Paragraph(
                    f'<font color="#{self._hex(occ_color)}">{p.occupancy_pct:.1f}%</font>',
                    ParagraphStyle("od", fontSize=9, alignment=TA_RIGHT),
                ),
                Paragraph(f"{occupied_units:,}", ParagraphStyle("od", fontSize=9, alignment=TA_RIGHT)),
                Paragraph(f"{vacant_units:,}", ParagraphStyle("od", fontSize=9, alignment=TA_RIGHT)),
            ])

        occ_table = Table(
            occ_data,
            colWidths=[2.8 * inch, 0.8 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch],
            repeatRows=1,
        )
        occ_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT_GREY]),
                    ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, GOLD),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 6),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        story.append(occ_table)

        # ── 5. Capital Improvements ──────────────────────────────────────────
        story.extend(self._section_header("5. Capital Improvements"))
        capex_tbl = self._capex_table()
        if capex_tbl:
            story.append(capex_tbl)
        else:
            story.append(Paragraph(
                f"No capital improvement projects were recorded during {self.data.quarter.label}.",
                self.s_body,
            ))

        # ── 6. Outlook ───────────────────────────────────────────────────────
        story.extend(self._section_header("6. Outlook & Forward Guidance"))
        story.append(Paragraph(self.data.outlook, self.s_body))

        # Pipeline highlight box
        pipeline_box_data = [[
            Paragraph(
                f"<b>Active Acquisition Pipeline:</b>  ${self.data.hubspot_pipeline_value:,.0f}  "
                f"(Camelot Roll-Up + Camelot Brokerage)",
                ParagraphStyle("pb", fontSize=10, textColor=NAVY, fontName="Helvetica-Bold"),
            )
        ]]
        pipeline_box = Table(pipeline_box_data, colWidths=[7.0 * inch])
        pipeline_box.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), LIGHT_GOLD),
                    ("TOPPADDING", (0, 0), (-1, -1), 10),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                    ("LEFTPADDING", (0, 0), (-1, -1), 12),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                    ("BOX", (0, 0), (-1, -1), 1.5, GOLD),
                ]
            )
        )
        story.append(Spacer(1, 0.1 * inch))
        story.append(pipeline_box)
        story.append(Spacer(1, 0.3 * inch))

        # ── Disclaimer ───────────────────────────────────────────────────────
        story.append(HRFlowable(width="100%", thickness=1, color=MID_GREY, spaceBefore=6))
        story.append(Paragraph(
            "This report is prepared by Camelot Property Management Services Corp for "
            "informational purposes only and is intended solely for the recipient(s) named above. "
            "Financial data reflects management estimates and may be subject to audit adjustment. "
            "Forward-looking statements involve risks and uncertainties. "
            "Confidential — Do Not Distribute.",
            ParagraphStyle("disc", fontSize=7, textColor=DARK_GREY, alignment=TA_JUSTIFY, leading=10),
        ))

        doc.build(story)

        with open(output_path, "wb") as f:
            f.write(buf.getvalue())

        logger.info("Investor update PDF written to %s", output_path)
        return output_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_investor_update(
    quarter: Optional[QuarterPeriod] = None,
    output_dir: str = ".",
    market_commentary: Optional[str] = None,
    outlook: Optional[str] = None,
) -> str:
    """
    Build and render the investor update PDF for the given quarter.

    Args:
        quarter:           QuarterPeriod to report on (defaults to current).
        output_dir:        Directory to write the PDF.
        market_commentary: Override default market commentary text.
        outlook:           Override default outlook text.

    Returns:
        Absolute path to the generated PDF file.
    """
    if quarter is None:
        quarter = QuarterPeriod.current()

    os.makedirs(output_dir, exist_ok=True)
    logger.info("Generating investor update for %s", quarter.label)

    collector = InvestorDataCollector()
    properties = collector.fetch_property_financials(quarter)
    capex_items = collector.fetch_capex_items(quarter)
    pipeline_value = collector.fetch_pipeline_value()

    data = InvestorUpdateData(
        quarter=quarter,
        properties=properties,
        capex_items=capex_items,
        market_commentary=market_commentary or _default_market_commentary(quarter),
        outlook=outlook or _default_outlook(quarter),
        hubspot_pipeline_value=pipeline_value,
    )

    filename = f"investor_update_{quarter.year}_Q{quarter.quarter}.pdf"
    output_path = os.path.join(output_dir, filename)

    renderer = InvestorUpdatePDFRenderer(data)
    return renderer.render(output_path)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate Camelot Investor Update PDF")
    parser.add_argument("--year", type=int, default=None, help="Report year")
    parser.add_argument("--quarter", type=int, choices=[1, 2, 3, 4], default=None)
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()

    q: Optional[QuarterPeriod] = None
    if args.year and args.quarter:
        q = QuarterPeriod(year=args.year, quarter=args.quarter)

    path = generate_investor_update(quarter=q, output_dir=args.output_dir)
    print(f"Generated: {path}")
