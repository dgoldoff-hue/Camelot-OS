"""
owner_statement.py — Monthly Owner Statement Generator
Camelot Property Management Services Corp / Report Bot

Generates professional monthly owner statements as PDF using reportlab.
Camelot branding: Gold #C9A84C + Dark Navy #1A2645.

Author: Camelot OS
"""

import io
import logging
from datetime import date, datetime
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Camelot brand colors
GOLD = "#C9A84C"
NAVY = "#1A2645"
LIGHT_GRAY = "#F5F5F5"
MID_GRAY = "#888888"
WHITE = "#FFFFFF"

# Reportlab color objects (created lazily)
def _color(hex_str: str):
    from reportlab.lib import colors
    hex_clean = hex_str.lstrip("#")
    r, g, b = int(hex_clean[0:2], 16), int(hex_clean[2:4], 16), int(hex_clean[4:6], 16)
    return colors.Color(r / 255, g / 255, b / 255)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class FinancialLine:
    label: str
    current_month: float
    ytd: float
    budget_monthly: float = 0.0

    @property
    def variance(self) -> float:
        return self.current_month - self.budget_monthly

    @property
    def variance_pct(self) -> Optional[float]:
        if self.budget_monthly == 0:
            return None
        return (self.variance / self.budget_monthly) * 100


@dataclass
class IncomeSection:
    scheduled_rent: FinancialLine
    vacancy_loss: FinancialLine
    credit_loss: FinancialLine
    other_income: FinancialLine

    @property
    def effective_gross_income(self) -> float:
        return (
            self.scheduled_rent.current_month
            - self.vacancy_loss.current_month
            - self.credit_loss.current_month
            + self.other_income.current_month
        )

    @property
    def egi_ytd(self) -> float:
        return (
            self.scheduled_rent.ytd
            - self.vacancy_loss.ytd
            - self.credit_loss.ytd
            + self.other_income.ytd
        )


@dataclass
class ExpenseSection:
    real_estate_taxes: FinancialLine
    insurance: FinancialLine
    utilities: FinancialLine
    repairs_maintenance: FinancialLine
    management_fee: FinancialLine
    payroll: FinancialLine
    administrative: FinancialLine
    capital_reserves: FinancialLine
    other_expenses: list[FinancialLine] = field(default_factory=list)

    @property
    def total_expenses(self) -> float:
        base = (
            self.real_estate_taxes.current_month
            + self.insurance.current_month
            + self.utilities.current_month
            + self.repairs_maintenance.current_month
            + self.management_fee.current_month
            + self.payroll.current_month
            + self.administrative.current_month
            + self.capital_reserves.current_month
        )
        return base + sum(e.current_month for e in self.other_expenses)

    @property
    def total_expenses_ytd(self) -> float:
        base = (
            self.real_estate_taxes.ytd
            + self.insurance.ytd
            + self.utilities.ytd
            + self.repairs_maintenance.ytd
            + self.management_fee.ytd
            + self.payroll.ytd
            + self.administrative.ytd
            + self.capital_reserves.ytd
        )
        return base + sum(e.ytd for e in self.other_expenses)


@dataclass
class ViolationSummary:
    class_a: int = 0
    class_b: int = 0
    class_c: int = 0
    description: str = ""


@dataclass
class WorkOrderSummary:
    open_count: int = 0
    closed_this_month: int = 0
    upcoming: list[str] = field(default_factory=list)


@dataclass
class BuildingData:
    mds_code: str
    address: str
    owner_name: str
    owner_email: str
    total_units: int
    occupied_units: int
    year_built: Optional[int]
    asset_type: str


@dataclass
class StatementPeriod:
    year: int
    month: int

    @property
    def month_name(self) -> str:
        return date(self.year, self.month, 1).strftime("%B")

    @property
    def period_label(self) -> str:
        return f"{self.month_name} {self.year}"

    @property
    def ytd_label(self) -> str:
        return f"YTD {self.year}"


# ---------------------------------------------------------------------------
# Core generator
# ---------------------------------------------------------------------------

def generate_owner_statement(
    building_data: BuildingData,
    income: IncomeSection,
    expenses: ExpenseSection,
    period: StatementPeriod,
    violations: Optional[ViolationSummary] = None,
    work_orders: Optional[WorkOrderSummary] = None,
    notes: Optional[str] = None,
) -> bytes:
    """
    Generate a monthly owner statement PDF.

    Args:
        building_data:  Building info (address, owner, units, etc.)
        income:         Income section data
        expenses:       Expense section data
        period:         Statement period (month/year)
        violations:     Open violation summary (optional)
        work_orders:    Work order summary (optional)
        notes:          Optional narrative notes for this period

    Returns:
        PDF content as bytes.
    """
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
            HRFlowable, KeepTogether,
        )
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
    except ImportError:
        raise ImportError("reportlab required: pip install reportlab")

    buffer = io.BytesIO()
    GOLD_COLOR = _color(GOLD)
    NAVY_COLOR = _color(NAVY)
    LGRAY_COLOR = _color(LIGHT_GRAY)
    MGRAY_COLOR = _color(MID_GRAY)

    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    styles = getSampleStyleSheet()

    def style(name, **kwargs):
        base = styles.get(name, styles["Normal"])
        return ParagraphStyle(name + "_custom", parent=base, **kwargs)

    title_style = style("Heading1", fontSize=18, textColor=NAVY_COLOR, spaceAfter=2, leading=22)
    subtitle_style = style("Normal", fontSize=10, textColor=MGRAY_COLOR, spaceAfter=6)
    section_header_style = style("Heading2", fontSize=11, textColor=colors.white,
                                  backColor=NAVY_COLOR, spaceAfter=0, spaceBefore=12,
                                  leftPadding=6, leading=18)
    body_style = style("Normal", fontSize=9, leading=13)
    note_style = style("Normal", fontSize=8, leading=12, textColor=MGRAY_COLOR)
    footer_style = style("Normal", fontSize=8, textColor=MGRAY_COLOR, alignment=TA_CENTER)

    def section_header(title: str) -> list:
        return [
            Spacer(1, 10),
            Table(
                [[Paragraph(title, style("Normal", fontSize=11, textColor=colors.white,
                                         fontName="Helvetica-Bold"))]],
                colWidths=[7.0 * inch],
                style=TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), NAVY_COLOR),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ]),
            ),
        ]

    def financial_table(rows: list[tuple], show_variance: bool = True) -> Table:
        """Build a financial data table."""
        header = ["", period.period_label, period.ytd_label]
        if show_variance:
            header += ["Budget", "Variance"]

        col_widths = [3.0 * inch, 1.2 * inch, 1.2 * inch]
        if show_variance:
            col_widths += [1.0 * inch, 1.0 * inch]

        data = [header]
        ts = [
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, _color(LIGHT_GRAY)]),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("LINEBELOW", (0, 0), (-1, 0), 0.5, GOLD_COLOR),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
        ]

        for i, row in enumerate(rows):
            label, line = row
            is_total = label.startswith("TOTAL") or label.startswith("NET")
            vals = [
                Paragraph(f"<b>{label}</b>" if is_total else label, body_style),
                _fmt_currency(line.current_month),
                _fmt_currency(line.ytd),
            ]
            if show_variance:
                variance_str = _fmt_currency(line.variance)
                variance_color = colors.red if line.variance < 0 else colors.green
                vals += [
                    _fmt_currency(line.budget_monthly),
                    Paragraph(
                        f"<font color={'red' if line.variance < 0 else 'green'}>{variance_str}</font>",
                        body_style,
                    ),
                ]
            data.append(vals)

            if is_total:
                row_idx = len(data) - 1
                ts.append(("FONTNAME", (0, row_idx), (-1, row_idx), "Helvetica-Bold"))
                ts.append(("LINEABOVE", (0, row_idx), (-1, row_idx), 0.5, GOLD_COLOR))
                ts.append(("BACKGROUND", (0, row_idx), (-1, row_idx), _color(LIGHT_GRAY)))

        return Table(data, colWidths=col_widths, style=TableStyle(ts))

    # ---------------------------------------------------------------------------
    # Build document story
    # ---------------------------------------------------------------------------
    story = []

    # Header banner
    noi = income.effective_gross_income - expenses.total_expenses
    noi_ytd = income.egi_ytd - expenses.total_expenses_ytd
    occupancy_pct = (building_data.occupied_units / building_data.total_units * 100) if building_data.total_units > 0 else 0

    header_data = [
        [
            Paragraph(
                f"<b>OWNER STATEMENT</b><br/>"
                f"<font size=10 color='#{GOLD.lstrip('#')}'>{period.period_label}</font>",
                style("Normal", fontSize=16, textColor=colors.white, fontName="Helvetica-Bold"),
            ),
            Paragraph(
                f"<b>{building_data.address}</b><br/>"
                f"<font size=9>MDS Code: {building_data.mds_code} | {building_data.total_units} Units</font>",
                style("Normal", fontSize=11, textColor=colors.white),
            ),
            Paragraph(
                f"Prepared for:<br/><b>{building_data.owner_name}</b><br/>"
                f"<font size=8>{building_data.owner_email}</font>",
                style("Normal", fontSize=9, textColor=colors.white),
            ),
        ]
    ]

    header_table = Table(
        header_data,
        colWidths=[2.5 * inch, 2.5 * inch, 2.0 * inch],
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), NAVY_COLOR),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 12),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("LINEBELOW", (0, 0), (-1, -1), 2, GOLD_COLOR),
        ]),
    )
    story.append(header_table)
    story.append(Spacer(1, 8))

    # Quick stats bar
    quick_stats = [
        [
            _stat_cell("OCCUPANCY", f"{occupancy_pct:.1f}%",
                       f"{building_data.occupied_units}/{building_data.total_units} units"),
            _stat_cell("MONTH NOI", _fmt_currency(noi), f"YTD: {_fmt_currency(noi_ytd)}"),
            _stat_cell("OPEN VIOLATIONS",
                       str((violations.class_a + violations.class_b + violations.class_c) if violations else 0),
                       f"A:{violations.class_a if violations else 0} B:{violations.class_b if violations else 0} C:{violations.class_c if violations else 0}"),
            _stat_cell("WORK ORDERS",
                       str(work_orders.open_count if work_orders else 0),
                       f"{work_orders.closed_this_month if work_orders else 0} closed this month"),
        ]
    ]

    story.append(Table(
        quick_stats,
        colWidths=[1.75 * inch] * 4,
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), _color(LIGHT_GRAY)),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("GRID", (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ]),
    ))
    story.append(Spacer(1, 12))

    # --- Income Section ---
    story.extend(section_header("1. INCOME SUMMARY"))
    income_rows = [
        ("Scheduled Gross Rent", income.scheduled_rent),
        ("Less: Vacancy Loss", income.vacancy_loss),
        ("Less: Credit Loss / Bad Debt", income.credit_loss),
        ("Other Income (Laundry/Fees)", income.other_income),
        ("TOTAL EFFECTIVE GROSS INCOME",
         FinancialLine("EGI", income.effective_gross_income, income.egi_ytd)),
    ]
    story.append(financial_table(income_rows))
    story.append(Spacer(1, 8))

    # --- Expense Section ---
    story.extend(section_header("2. OPERATING EXPENSES"))
    expense_rows = [
        ("Real Estate Taxes", expenses.real_estate_taxes),
        ("Insurance", expenses.insurance),
        ("Utilities", expenses.utilities),
        ("Repairs & Maintenance", expenses.repairs_maintenance),
        ("Management Fee", expenses.management_fee),
        ("Payroll & Benefits", expenses.payroll),
        ("Administrative", expenses.administrative),
        ("Capital Reserves", expenses.capital_reserves),
    ]
    for extra in expenses.other_expenses:
        expense_rows.append((extra.label, extra))
    expense_rows.append(
        ("TOTAL OPERATING EXPENSES",
         FinancialLine("Total Exp", expenses.total_expenses, expenses.total_expenses_ytd))
    )
    story.append(financial_table(expense_rows))
    story.append(Spacer(1, 8))

    # --- NOI Section ---
    story.extend(section_header("3. NET OPERATING INCOME"))
    noi_table_data = [
        ["", period.period_label, period.ytd_label],
        ["Effective Gross Income", _fmt_currency(income.effective_gross_income), _fmt_currency(income.egi_ytd)],
        ["Total Operating Expenses", f"({_fmt_currency(expenses.total_expenses)})", f"({_fmt_currency(expenses.total_expenses_ytd)})"],
        ["NET OPERATING INCOME", _fmt_currency(noi), _fmt_currency(noi_ytd)],
    ]
    noi_tbl = Table(
        noi_table_data,
        colWidths=[3.5 * inch, 1.75 * inch, 1.75 * inch],
        style=TableStyle([
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTNAME", (0, 3), (-1, 3), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("LINEBELOW", (0, 0), (-1, 0), 0.5, GOLD_COLOR),
            ("LINEABOVE", (0, 3), (-1, 3), 0.5, GOLD_COLOR),
            ("LINEBELOW", (0, 3), (-1, 3), 1.5, NAVY_COLOR),
            ("BACKGROUND", (0, 3), (-1, 3), _color(LIGHT_GRAY)),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]),
    )
    story.append(noi_tbl)
    story.append(Spacer(1, 12))

    # --- Violations ---
    if violations:
        story.extend(section_header("4. OPEN VIOLATIONS"))
        viol_data = [
            ["Class A (Non-Hazardous)", str(violations.class_a)],
            ["Class B (Hazardous)", str(violations.class_b)],
            ["Class C (Immediately Hazardous)", str(violations.class_c)],
            ["TOTAL OPEN VIOLATIONS",
             str(violations.class_a + violations.class_b + violations.class_c)],
        ]
        if violations.description:
            viol_data.append(["Notes", violations.description[:100]])

        story.append(Table(
            viol_data,
            colWidths=[4.0 * inch, 3.0 * inch],
            style=TableStyle([
                ("FONTNAME", (0, 3), (-1, 3), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, _color(LIGHT_GRAY)]),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("LINEABOVE", (0, 3), (-1, 3), 0.5, GOLD_COLOR),
            ]),
        ))
        story.append(Spacer(1, 12))

    # --- Work Orders ---
    if work_orders:
        story.extend(section_header("5. WORK ORDERS & UPCOMING MAINTENANCE"))
        wo_data = [
            ["Open Work Orders", str(work_orders.open_count)],
            ["Closed This Month", str(work_orders.closed_this_month)],
        ]
        story.append(Table(
            wo_data,
            colWidths=[4.0 * inch, 3.0 * inch],
            style=TableStyle([
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, _color(LIGHT_GRAY)]),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]),
        ))
        if work_orders.upcoming:
            story.append(Spacer(1, 4))
            story.append(Paragraph("<b>Upcoming Maintenance:</b>", body_style))
            for item in work_orders.upcoming[:5]:
                story.append(Paragraph(f"• {item}", body_style))
        story.append(Spacer(1, 12))

    # --- Notes ---
    if notes:
        story.extend(section_header("PROPERTY MANAGER NOTES"))
        story.append(Paragraph(notes, body_style))
        story.append(Spacer(1, 12))

    # Footer
    story.append(HRFlowable(width="100%", thickness=0.5, color=GOLD_COLOR))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"Camelot Property Management Services Corp | Generated: "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')} | "
        f"This statement is confidential and intended solely for {building_data.owner_name}",
        footer_style,
    ))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    logger.info(
        f"Owner statement generated for {building_data.address} "
        f"({period.period_label}) — {len(pdf_bytes)} bytes"
    )
    return pdf_bytes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_currency(amount: float) -> str:
    if amount < 0:
        return f"(${abs(amount):,.0f})"
    return f"${amount:,.0f}"


def _stat_cell(label: str, value: str, sub: str = ""):
    from reportlab.platypus import Paragraph
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_CENTER
    styles = getSampleStyleSheet()

    NAVY_COLOR = _color(NAVY)
    GOLD_COLOR = _color(GOLD)

    def cs(name, **kw):
        from reportlab.lib.styles import ParagraphStyle
        return ParagraphStyle(name, parent=styles["Normal"], **kw)

    return [
        Paragraph(label, cs("stat_lbl", fontSize=7, textColor=_color(MID_GRAY), alignment=TA_CENTER)),
        Paragraph(f"<b>{value}</b>", cs("stat_val", fontSize=13, textColor=NAVY_COLOR, alignment=TA_CENTER, fontName="Helvetica-Bold")),
        Paragraph(sub, cs("stat_sub", fontSize=7, textColor=_color(MID_GRAY), alignment=TA_CENTER)),
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    today = date.today()

    building = BuildingData(
        mds_code="552", address="552 Sample Ave, Bronx, NY 10452",
        owner_name="John Doe", owner_email="owner@example.com",
        total_units=24, occupied_units=22, year_built=1965, asset_type="Multifamily"
    )
    income = IncomeSection(
        scheduled_rent=FinancialLine("Scheduled Rent", 38400, 192000, 38400),
        vacancy_loss=FinancialLine("Vacancy", 1920, 9600, 1920),
        credit_loss=FinancialLine("Credit Loss", 384, 1920, 384),
        other_income=FinancialLine("Other Income", 500, 2500, 500),
    )
    expenses = ExpenseSection(
        real_estate_taxes=FinancialLine("RE Taxes", 6000, 30000, 6000),
        insurance=FinancialLine("Insurance", 1500, 7500, 1500),
        utilities=FinancialLine("Utilities", 3000, 15000, 3000),
        repairs_maintenance=FinancialLine("Repairs", 2000, 10000, 2000),
        management_fee=FinancialLine("Mgmt Fee", 1830, 9150, 1830),
        payroll=FinancialLine("Payroll", 0, 0, 0),
        administrative=FinancialLine("Admin", 500, 2500, 500),
        capital_reserves=FinancialLine("Reserves", 500, 2500, 500),
    )
    period = StatementPeriod(year=today.year, month=today.month)
    violations = ViolationSummary(class_a=2, class_b=1, class_c=0)
    work_orders = WorkOrderSummary(open_count=3, closed_this_month=5, upcoming=["Boiler inspection (May 15)", "Roof repair (May 22)"])

    pdf = generate_owner_statement(building, income, expenses, period, violations, work_orders)

    out_path = f"/home/user/workspace/test_owner_statement.pdf"
    with open(out_path, "wb") as f:
        f.write(pdf)
    print(f"Statement generated: {out_path} ({len(pdf)} bytes)")
