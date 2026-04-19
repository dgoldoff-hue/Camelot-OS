"""
kpi_dashboard.py — Camelot OS Report Bot
=========================================
Generates weekly KPI dashboards for Camelot Property Management.

Metrics collected:
  - Occupancy Rate (%)
  - Rent Collection Rate (%)
  - Open HPD / DOB Violations
  - Work Orders: open vs. closed
  - New Scout Leads (HubSpot CRM)
  - HubSpot Pipeline Value (Camelot Roll-Up + Camelot Brokerage)

Output:
  - Markdown report with ▲▼ trend indicators vs. prior week
  - PDF report via reportlab

Author: Camelot OS
"""

from __future__ import annotations

import io
import logging
import os
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    HRFlowable,
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
logger = logging.getLogger("report_bot.kpi_dashboard")

# ---------------------------------------------------------------------------
# Brand colours
# ---------------------------------------------------------------------------
GOLD = colors.HexColor("#C9A84C")
NAVY = colors.HexColor("#1A2645")
LIGHT_GREY = colors.HexColor("#F5F5F5")
MID_GREY = colors.HexColor("#CCCCCC")
GREEN = colors.HexColor("#2E7D32")
RED = colors.HexColor("#C62828")

# ---------------------------------------------------------------------------
# Retry-enabled HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    """Return a requests.Session with exponential-backoff retry logic."""
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
class KPIMetric:
    """A single KPI value with optional prior-week comparison."""

    label: str
    value: float
    unit: str = ""                 # e.g. "%", "$", ""
    prior_value: Optional[float] = None
    higher_is_better: bool = True  # controls ▲▼ colour logic
    fmt: str = "{:.1f}"            # format string for value display

    @property
    def delta(self) -> Optional[float]:
        if self.prior_value is None:
            return None
        return self.value - self.prior_value

    @property
    def trend_symbol(self) -> str:
        if self.delta is None:
            return "—"
        return "▲" if self.delta >= 0 else "▼"

    @property
    def trend_is_positive(self) -> bool:
        """True when the trend is good for the business."""
        if self.delta is None:
            return True
        if self.higher_is_better:
            return self.delta >= 0
        return self.delta <= 0  # e.g. violations — lower is better

    @property
    def formatted_value(self) -> str:
        try:
            base = self.fmt.format(self.value)
        except (ValueError, TypeError):
            base = str(self.value)
        return f"{base}{self.unit}"

    @property
    def formatted_delta(self) -> str:
        if self.delta is None:
            return "—"
        sign = "+" if self.delta >= 0 else ""
        try:
            base = self.fmt.format(self.delta)
        except (ValueError, TypeError):
            base = str(self.delta)
        return f"{sign}{base}{self.unit}"


@dataclass
class KPIDashboard:
    """Container for all KPI metrics for a given reporting week."""

    week_ending: date
    metrics: list[KPIMetric] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def add(self, metric: KPIMetric) -> None:
        self.metrics.append(metric)

    def to_markdown(self) -> str:
        lines: list[str] = [
            f"# Camelot OS — Weekly KPI Dashboard",
            f"**Week Ending:** {self.week_ending.strftime('%B %d, %Y')}",
            f"**Generated:** {self.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            "| KPI | Current | vs. Last Week |",
            "|-----|---------|---------------|",
        ]
        for m in self.metrics:
            trend = f"{m.trend_symbol} {m.formatted_delta}" if m.delta is not None else "—"
            lines.append(f"| {m.label} | {m.formatted_value} | {trend} |")
        lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Data collectors
# ---------------------------------------------------------------------------

class OccupancyCollector:
    """
    Pulls occupancy data from Supabase `units` table.

    Expected schema:
        units(id, building_id, status TEXT)   -- status in ('occupied','vacant','down')
    """

    def __init__(self) -> None:
        self.supabase_url: str = os.environ["SUPABASE_URL"]
        self.supabase_key: str = os.environ["SUPABASE_SERVICE_KEY"]

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

    def fetch(self) -> tuple[float, Optional[float]]:
        """
        Returns (current_rate_pct, prior_week_rate_pct).
        prior_week_rate_pct is None if historical data unavailable.
        """
        # Current snapshot
        current = self._query_occupancy()

        # Prior week snapshot from kpi_snapshots table
        prior = self._query_prior_snapshot("occupancy_rate")

        return current, prior

    def _query_occupancy(self) -> float:
        """Query Supabase units table and compute occupancy rate."""
        url = f"{self.supabase_url}/rest/v1/units"
        params = {"select": "status"}
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            units: list[dict] = resp.json()
            if not units:
                logger.warning("No units returned from Supabase — returning 0")
                return 0.0
            total = len(units)
            occupied = sum(1 for u in units if u.get("status") == "occupied")
            rate = (occupied / total) * 100
            logger.info("Occupancy: %d/%d = %.1f%%", occupied, total, rate)
            return round(rate, 2)
        except requests.RequestException as exc:
            logger.error("Failed to fetch occupancy: %s", exc)
            raise

    def _query_prior_snapshot(self, metric_name: str) -> Optional[float]:
        """Retrieve last week's snapshot value from kpi_snapshots table."""
        url = f"{self.supabase_url}/rest/v1/kpi_snapshots"
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        params = {
            "select": "value",
            "metric_name": f"eq.{metric_name}",
            "snapshot_date": f"gte.{week_ago}",
            "order": "snapshot_date.asc",
            "limit": "1",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            rows: list[dict] = resp.json()
            if rows:
                return float(rows[0]["value"])
            return None
        except requests.RequestException as exc:
            logger.warning("Could not fetch prior snapshot for %s: %s", metric_name, exc)
            return None


class RentCollectionCollector:
    """
    Pulls rent collection data from Supabase `rent_payments` table.

    Expected schema:
        rent_payments(id, unit_id, period_year INT, period_month INT,
                      amount_due NUMERIC, amount_paid NUMERIC, paid_date DATE)
    """

    def __init__(self) -> None:
        self.supabase_url: str = os.environ["SUPABASE_URL"]
        self.supabase_key: str = os.environ["SUPABASE_SERVICE_KEY"]

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

    def fetch(self) -> tuple[float, Optional[float]]:
        """Returns (current_collection_rate_pct, prior_month_rate_pct)."""
        today = date.today()
        current = self._query_collection(today.year, today.month)

        # Compare to prior month
        if today.month == 1:
            prior_year, prior_month = today.year - 1, 12
        else:
            prior_year, prior_month = today.year, today.month - 1
        prior = self._query_collection(prior_year, prior_month)

        return current, prior

    def _query_collection(self, year: int, month: int) -> float:
        """Compute collection rate for a given year/month."""
        url = f"{self.supabase_url}/rest/v1/rent_payments"
        params = {
            "select": "amount_due,amount_paid",
            "period_year": f"eq.{year}",
            "period_month": f"eq.{month}",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            rows: list[dict] = resp.json()
            if not rows:
                logger.warning("No rent rows for %d-%02d", year, month)
                return 0.0
            total_due = sum(float(r.get("amount_due", 0)) for r in rows)
            total_paid = sum(float(r.get("amount_paid", 0)) for r in rows)
            if total_due == 0:
                return 0.0
            rate = (total_paid / total_due) * 100
            logger.info("Rent collection %d-%02d: %.1f%%", year, month, rate)
            return round(rate, 2)
        except requests.RequestException as exc:
            logger.error("Failed to fetch rent collection: %s", exc)
            raise


class ViolationCollector:
    """Queries NYC Open Data for open HPD + DOB violations across all buildings."""

    HPD_URL = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
    DOB_URL = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"

    def __init__(self) -> None:
        self.app_token: str = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
        self.buildings: list[str] = self._load_buildings()

    def _load_buildings(self) -> list[str]:
        """Load BIN/BBL list from env or config file."""
        raw = os.getenv("PORTFOLIO_BBLS", "")
        if raw:
            return [b.strip() for b in raw.split(",") if b.strip()]
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        if os.path.exists(config_path):
            import yaml  # type: ignore
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
            return cfg.get("portfolio", {}).get("bbls", [])
        return []

    def _headers(self) -> dict[str, str]:
        h: dict[str, str] = {"Accept": "application/json"}
        if self.app_token:
            h["X-App-Token"] = self.app_token
        return h

    def fetch_hpd_open(self) -> int:
        """Count open HPD violations across portfolio buildings."""
        if not self.buildings:
            logger.warning("No buildings configured — skipping HPD check")
            return 0

        bbl_list = ", ".join(f"'{b}'" for b in self.buildings)
        params = {
            "$where": f"bbl IN ({bbl_list}) AND currentstatusid NOT IN (19,21)",
            "$select": "COUNT(*) AS cnt",
        }
        try:
            resp = SESSION.get(
                self.HPD_URL, headers=self._headers(), params=params, timeout=20
            )
            resp.raise_for_status()
            data = resp.json()
            count = int(data[0].get("cnt", 0)) if data else 0
            logger.info("Open HPD violations: %d", count)
            return count
        except (requests.RequestException, KeyError, IndexError, ValueError) as exc:
            logger.error("HPD violation fetch failed: %s", exc)
            return -1  # signal error without crashing

    def fetch_dob_open(self) -> int:
        """Count open DOB violations across portfolio buildings."""
        if not self.buildings:
            return 0

        bbl_list = ", ".join(f"'{b}'" for b in self.buildings)
        params = {
            "$where": f"bbl IN ({bbl_list}) AND isndobvclosed = 'NO'",
            "$select": "COUNT(*) AS cnt",
        }
        try:
            resp = SESSION.get(
                self.DOB_URL, headers=self._headers(), params=params, timeout=20
            )
            resp.raise_for_status()
            data = resp.json()
            count = int(data[0].get("cnt", 0)) if data else 0
            logger.info("Open DOB violations: %d", count)
            return count
        except (requests.RequestException, KeyError, IndexError, ValueError) as exc:
            logger.error("DOB violation fetch failed: %s", exc)
            return -1


class WorkOrderCollector:
    """
    Pulls open/closed work order counts from Supabase `work_orders` table.

    Expected schema:
        work_orders(id, status TEXT, created_at TIMESTAMPTZ, closed_at TIMESTAMPTZ)
        status values: 'open', 'in_progress', 'closed', 'cancelled'
    """

    def __init__(self) -> None:
        self.supabase_url: str = os.environ["SUPABASE_URL"]
        self.supabase_key: str = os.environ["SUPABASE_SERVICE_KEY"]

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

    def fetch(self) -> tuple[int, int, Optional[int], Optional[int]]:
        """
        Returns (open_count, closed_this_week, prior_open, prior_closed).
        prior_* may be None if snapshot table unavailable.
        """
        open_count = self._count_by_status("open") + self._count_by_status("in_progress")
        closed_this_week = self._count_closed_this_week()
        prior_open = self._prior_snapshot("work_orders_open")
        prior_closed = self._prior_snapshot("work_orders_closed_week")
        return open_count, closed_this_week, prior_open, prior_closed

    def _count_by_status(self, status: str) -> int:
        url = f"{self.supabase_url}/rest/v1/work_orders"
        params = {"select": "id", "status": f"eq.{status}"}
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            # Supabase returns array; count via response header Prefer: count=exact
            return len(resp.json())
        except requests.RequestException as exc:
            logger.error("Work order count failed for status=%s: %s", status, exc)
            return 0

    def _count_closed_this_week(self) -> int:
        url = f"{self.supabase_url}/rest/v1/work_orders"
        week_start = (date.today() - timedelta(days=7)).isoformat()
        params = {
            "select": "id",
            "status": "eq.closed",
            "closed_at": f"gte.{week_start}",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            return len(resp.json())
        except requests.RequestException as exc:
            logger.error("Closed-this-week work order count failed: %s", exc)
            return 0

    def _prior_snapshot(self, metric_name: str) -> Optional[int]:
        url = f"{self.supabase_url}/rest/v1/kpi_snapshots"
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        params = {
            "select": "value",
            "metric_name": f"eq.{metric_name}",
            "snapshot_date": f"gte.{week_ago}",
            "order": "snapshot_date.asc",
            "limit": "1",
        }
        try:
            resp = SESSION.get(url, headers=self._headers(), params=params, timeout=15)
            resp.raise_for_status()
            rows = resp.json()
            return int(rows[0]["value"]) if rows else None
        except requests.RequestException as exc:
            logger.warning("Prior snapshot fetch failed for %s: %s", metric_name, exc)
            return None


class HubSpotKPICollector:
    """Pulls Scout lead count and pipeline value from HubSpot CRM."""

    BASE_URL = "https://api.hubapi.com"

    def __init__(self) -> None:
        token = os.environ["HUBSPOT_ACCESS_TOKEN"]
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Scout leads (contacts created this week tagged as "scout")
    # ------------------------------------------------------------------

    def fetch_new_scout_leads(self) -> tuple[int, Optional[int]]:
        """
        Returns (new_leads_this_week, new_leads_prior_week).
        Counts HubSpot contacts with lifecyclestage='lead' created in the
        last 7 days.
        """
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        week_ago_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
        two_weeks_ago_ms = int((datetime.utcnow() - timedelta(days=14)).timestamp() * 1000)

        current = self._count_contacts_in_range(week_ago_ms, now_ms)
        prior = self._count_contacts_in_range(two_weeks_ago_ms, week_ago_ms)
        return current, prior

    def _count_contacts_in_range(self, from_ms: int, to_ms: int) -> int:
        url = f"{self.BASE_URL}/crm/v3/objects/contacts/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "createdate",
                            "operator": "BETWEEN",
                            "value": str(from_ms),
                            "highValue": str(to_ms),
                        },
                        {
                            "propertyName": "lifecyclestage",
                            "operator": "EQ",
                            "value": "lead",
                        },
                    ]
                }
            ],
            "properties": ["createdate"],
            "limit": 1,
        }
        try:
            resp = SESSION.post(url, headers=self._headers, json=payload, timeout=20)
            resp.raise_for_status()
            return resp.json().get("total", 0)
        except requests.RequestException as exc:
            logger.error("HubSpot lead count failed: %s", exc)
            return 0

    # ------------------------------------------------------------------
    # Pipeline values
    # ------------------------------------------------------------------

    def fetch_pipeline_value(self) -> tuple[float, Optional[float]]:
        """
        Returns (total_pipeline_value_usd, prior_week_value).
        Sums amount for all open deals in 'Camelot Roll-Up' and
        'Camelot Brokerage' pipelines.
        """
        pipeline_names = ["Camelot Roll-Up", "Camelot Brokerage"]
        pipeline_ids = self._resolve_pipeline_ids(pipeline_names)

        total = 0.0
        for pid in pipeline_ids:
            total += self._sum_pipeline_deals(pid)

        # Prior week: attempt from Supabase snapshot
        prior = self._prior_snapshot_value("hubspot_pipeline_value")
        return total, prior

    def _resolve_pipeline_ids(self, names: list[str]) -> list[str]:
        url = f"{self.BASE_URL}/crm/v3/pipelines/deals"
        try:
            resp = SESSION.get(url, headers=self._headers, timeout=15)
            resp.raise_for_status()
            pipelines = resp.json().get("results", [])
            ids: list[str] = []
            for p in pipelines:
                if p.get("label") in names:
                    ids.append(p["id"])
            return ids
        except requests.RequestException as exc:
            logger.error("Failed to resolve pipeline IDs: %s", exc)
            return []

    def _sum_pipeline_deals(self, pipeline_id: str) -> float:
        url = f"{self.BASE_URL}/crm/v3/objects/deals/search"
        payload = {
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "pipeline",
                            "operator": "EQ",
                            "value": pipeline_id,
                        },
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
        total = 0.0
        after: Optional[str] = None
        while True:
            if after:
                payload["after"] = after
            try:
                resp = SESSION.post(url, headers=self._headers, json=payload, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                for deal in data.get("results", []):
                    amt = deal.get("properties", {}).get("amount")
                    if amt:
                        try:
                            total += float(amt)
                        except ValueError:
                            pass
                paging = data.get("paging", {}).get("next", {})
                after = paging.get("after")
                if not after:
                    break
            except requests.RequestException as exc:
                logger.error("Deal sum failed for pipeline %s: %s", pipeline_id, exc)
                break
        return total

    def _prior_snapshot_value(self, metric_name: str) -> Optional[float]:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
        if not supabase_url or not supabase_key:
            return None
        url = f"{supabase_url}/rest/v1/kpi_snapshots"
        week_ago = (date.today() - timedelta(days=7)).isoformat()
        params = {
            "select": "value",
            "metric_name": f"eq.{metric_name}",
            "snapshot_date": f"gte.{week_ago}",
            "order": "snapshot_date.asc",
            "limit": "1",
        }
        headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }
        try:
            resp = SESSION.get(url, headers=headers, params=params, timeout=15)
            resp.raise_for_status()
            rows = resp.json()
            return float(rows[0]["value"]) if rows else None
        except requests.RequestException as exc:
            logger.warning("Prior pipeline snapshot failed: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Snapshot persister
# ---------------------------------------------------------------------------

class SnapshotPersister:
    """Writes current KPI values to Supabase kpi_snapshots for future comparison."""

    def __init__(self) -> None:
        self.supabase_url: str = os.environ["SUPABASE_URL"]
        self.supabase_key: str = os.environ["SUPABASE_SERVICE_KEY"]

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }

    def save(self, dashboard: KPIDashboard) -> None:
        """Upsert all metric values into kpi_snapshots."""
        url = f"{self.supabase_url}/rest/v1/kpi_snapshots"
        today_str = dashboard.week_ending.isoformat()

        rows: list[dict[str, Any]] = []
        for m in dashboard.metrics:
            rows.append(
                {
                    "metric_name": m.label.lower().replace(" ", "_").replace("%", "pct"),
                    "value": m.value,
                    "snapshot_date": today_str,
                }
            )

        try:
            resp = SESSION.post(
                url, headers=self._headers(), json=rows, timeout=20
            )
            resp.raise_for_status()
            logger.info("Saved %d KPI snapshots for %s", len(rows), today_str)
        except requests.RequestException as exc:
            logger.error("Failed to persist KPI snapshots: %s", exc)


# ---------------------------------------------------------------------------
# PDF renderer
# ---------------------------------------------------------------------------

class KPIDashboardPDFRenderer:
    """Renders a KPIDashboard to a professional PDF using reportlab."""

    def __init__(self, dashboard: KPIDashboard) -> None:
        self.dashboard = dashboard

    def render(self, output_path: str) -> str:
        """Write PDF to output_path and return the path."""
        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
            title=f"Camelot OS KPI Dashboard — {self.dashboard.week_ending}",
        )

        styles = getSampleStyleSheet()
        story: list[Any] = []

        # ── Header ──────────────────────────────────────────────────────────
        header_style = ParagraphStyle(
            "CamelotHeader",
            parent=styles["Title"],
            fontSize=20,
            textColor=NAVY,
            fontName="Helvetica-Bold",
            spaceAfter=4,
        )
        story.append(Paragraph("CAMELOT PROPERTY MANAGEMENT", header_style))

        subheader_style = ParagraphStyle(
            "CamelotSubheader",
            parent=styles["Normal"],
            fontSize=12,
            textColor=GOLD,
            fontName="Helvetica-Bold",
            spaceAfter=2,
        )
        story.append(Paragraph("Weekly KPI Dashboard", subheader_style))

        meta_style = ParagraphStyle(
            "Meta",
            parent=styles["Normal"],
            fontSize=9,
            textColor=colors.grey,
            spaceAfter=10,
        )
        story.append(
            Paragraph(
                f"Week Ending {self.dashboard.week_ending.strftime('%B %d, %Y')}  ·  "
                f"Generated {self.dashboard.generated_at.strftime('%Y-%m-%d %H:%M UTC')}",
                meta_style,
            )
        )
        story.append(HRFlowable(width="100%", thickness=2, color=NAVY, spaceAfter=12))

        # ── KPI Table ────────────────────────────────────────────────────────
        table_data: list[list[Any]] = [
            ["KPI", "Current Value", "vs. Last Week", "Trend"],
        ]

        for m in self.dashboard.metrics:
            trend_text = m.formatted_delta
            if m.delta is not None:
                color = GREEN if m.trend_is_positive else RED
                symbol = m.trend_symbol
                trend_cell = Paragraph(
                    f'<font color="#{color.hexval()[1:] if hasattr(color, "hexval") else "000000"}">'
                    f"{symbol} {trend_text}</font>",
                    ParagraphStyle("tc", fontSize=10, alignment=TA_CENTER),
                )
            else:
                trend_cell = Paragraph(
                    "—",
                    ParagraphStyle("tc", fontSize=10, alignment=TA_CENTER),
                )

            table_data.append(
                [
                    Paragraph(m.label, ParagraphStyle("kl", fontSize=10, fontName="Helvetica-Bold")),
                    Paragraph(
                        m.formatted_value,
                        ParagraphStyle("kv", fontSize=10, alignment=TA_RIGHT),
                    ),
                    Paragraph(
                        trend_text,
                        ParagraphStyle("td", fontSize=10, alignment=TA_RIGHT),
                    ),
                    trend_cell,
                ]
            )

        col_widths = [2.8 * inch, 1.5 * inch, 1.5 * inch, 1.0 * inch]
        kpi_table = Table(table_data, colWidths=col_widths, repeatRows=1)
        kpi_table.setStyle(
            TableStyle(
                [
                    # Header row
                    ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 10),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                    ("TOPPADDING", (0, 0), (-1, 0), 8),
                    # Data rows
                    ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GREY]),
                    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                    ("FONTSIZE", (0, 1), (-1, -1), 10),
                    ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                    ("ALIGN", (0, 1), (0, -1), "LEFT"),
                    ("TOPPADDING", (0, 1), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    # Grid
                    ("GRID", (0, 0), (-1, -1), 0.5, MID_GREY),
                    ("LINEBELOW", (0, 0), (-1, 0), 2, GOLD),
                ]
            )
        )
        story.append(kpi_table)
        story.append(Spacer(1, 0.3 * inch))

        # ── Footer ───────────────────────────────────────────────────────────
        story.append(HRFlowable(width="100%", thickness=1, color=MID_GREY, spaceBefore=6))
        footer_style = ParagraphStyle(
            "Footer",
            parent=styles["Normal"],
            fontSize=7,
            textColor=colors.grey,
            alignment=TA_CENTER,
        )
        story.append(
            Paragraph(
                "Camelot Property Management Services Corp — Confidential — Generated by Camelot OS",
                footer_style,
            )
        )

        doc.build(story)

        # Write buffer to file
        with open(output_path, "wb") as f:
            f.write(buf.getvalue())

        logger.info("KPI Dashboard PDF written to %s", output_path)
        return output_path


# ---------------------------------------------------------------------------
# Dashboard builder — orchestrates all collectors
# ---------------------------------------------------------------------------

def build_dashboard(persist_snapshot: bool = True) -> KPIDashboard:
    """
    Collect all KPI metrics and return a populated KPIDashboard.

    Args:
        persist_snapshot: If True, save current values to kpi_snapshots
                          table for future week-over-week comparison.

    Returns:
        KPIDashboard with all metrics populated.
    """
    week_ending = date.today()
    dashboard = KPIDashboard(week_ending=week_ending)
    logger.info("Building KPI dashboard for week ending %s", week_ending)

    # ── 1. Occupancy Rate ────────────────────────────────────────────────
    try:
        occ_current, occ_prior = OccupancyCollector().fetch()
        dashboard.add(
            KPIMetric(
                label="Occupancy Rate",
                value=occ_current,
                unit="%",
                prior_value=occ_prior,
                higher_is_better=True,
                fmt="{:.1f}",
            )
        )
    except Exception as exc:
        logger.error("Occupancy collection failed: %s", exc)
        dashboard.add(KPIMetric(label="Occupancy Rate", value=0.0, unit="%", fmt="{:.1f}"))

    # ── 2. Rent Collection Rate ──────────────────────────────────────────
    try:
        rent_current, rent_prior = RentCollectionCollector().fetch()
        dashboard.add(
            KPIMetric(
                label="Rent Collection Rate",
                value=rent_current,
                unit="%",
                prior_value=rent_prior,
                higher_is_better=True,
                fmt="{:.1f}",
            )
        )
    except Exception as exc:
        logger.error("Rent collection failed: %s", exc)
        dashboard.add(KPIMetric(label="Rent Collection Rate", value=0.0, unit="%", fmt="{:.1f}"))

    # ── 3. Open Violations (HPD + DOB) ───────────────────────────────────
    try:
        vc = ViolationCollector()
        hpd_open = vc.fetch_hpd_open()
        dob_open = vc.fetch_dob_open()
        total_violations = (max(hpd_open, 0) + max(dob_open, 0))
        dashboard.add(
            KPIMetric(
                label="Open HPD Violations",
                value=float(max(hpd_open, 0)),
                unit="",
                higher_is_better=False,
                fmt="{:.0f}",
            )
        )
        dashboard.add(
            KPIMetric(
                label="Open DOB Violations",
                value=float(max(dob_open, 0)),
                unit="",
                higher_is_better=False,
                fmt="{:.0f}",
            )
        )
        dashboard.add(
            KPIMetric(
                label="Total Open Violations",
                value=float(total_violations),
                unit="",
                higher_is_better=False,
                fmt="{:.0f}",
            )
        )
    except Exception as exc:
        logger.error("Violation collection failed: %s", exc)

    # ── 4. Work Orders ───────────────────────────────────────────────────
    try:
        wo_open, wo_closed, wo_prior_open, wo_prior_closed = WorkOrderCollector().fetch()
        dashboard.add(
            KPIMetric(
                label="Open Work Orders",
                value=float(wo_open),
                unit="",
                prior_value=float(wo_prior_open) if wo_prior_open is not None else None,
                higher_is_better=False,
                fmt="{:.0f}",
            )
        )
        dashboard.add(
            KPIMetric(
                label="Work Orders Closed (7d)",
                value=float(wo_closed),
                unit="",
                prior_value=float(wo_prior_closed) if wo_prior_closed is not None else None,
                higher_is_better=True,
                fmt="{:.0f}",
            )
        )
    except Exception as exc:
        logger.error("Work order collection failed: %s", exc)

    # ── 5. HubSpot Scout Leads + Pipeline ────────────────────────────────
    try:
        hs = HubSpotKPICollector()
        leads_current, leads_prior = hs.fetch_new_scout_leads()
        dashboard.add(
            KPIMetric(
                label="New Scout Leads (7d)",
                value=float(leads_current),
                unit="",
                prior_value=float(leads_prior) if leads_prior is not None else None,
                higher_is_better=True,
                fmt="{:.0f}",
            )
        )
        pipeline_value, prior_pipeline = hs.fetch_pipeline_value()
        dashboard.add(
            KPIMetric(
                label="HubSpot Pipeline Value",
                value=pipeline_value,
                unit="",
                prior_value=prior_pipeline,
                higher_is_better=True,
                fmt="${:,.0f}",
            )
        )
    except Exception as exc:
        logger.error("HubSpot KPI collection failed: %s", exc)

    # ── Persist snapshot for next week's comparison ──────────────────────
    if persist_snapshot:
        try:
            SnapshotPersister().save(dashboard)
        except Exception as exc:
            logger.warning("Snapshot persist failed (non-fatal): %s", exc)

    logger.info("KPI dashboard built with %d metrics", len(dashboard.metrics))
    return dashboard


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_kpi_dashboard(
    output_dir: str = ".",
    persist: bool = True,
) -> dict[str, str]:
    """
    Build the KPI dashboard, write both Markdown and PDF, and return
    a dict with keys 'markdown_path' and 'pdf_path'.

    Args:
        output_dir: Directory to write output files.
        persist:    Whether to save KPI snapshots to Supabase.

    Returns:
        {'markdown_path': '...', 'pdf_path': '...'}
    """
    os.makedirs(output_dir, exist_ok=True)

    dashboard = build_dashboard(persist_snapshot=persist)
    week_str = dashboard.week_ending.strftime("%Y-%m-%d")

    # Markdown
    md_path = os.path.join(output_dir, f"kpi_dashboard_{week_str}.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(dashboard.to_markdown())
    logger.info("Markdown dashboard written to %s", md_path)

    # PDF
    pdf_path = os.path.join(output_dir, f"kpi_dashboard_{week_str}.pdf")
    KPIDashboardPDFRenderer(dashboard).render(pdf_path)

    return {"markdown_path": md_path, "pdf_path": pdf_path}


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate Camelot KPI Dashboard")
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory for output files (default: output)",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Skip persisting KPI snapshots to Supabase",
    )
    args = parser.parse_args()

    results = generate_kpi_dashboard(
        output_dir=args.output_dir,
        persist=not args.no_persist,
    )
    print(f"Markdown: {results['markdown_path']}")
    print(f"PDF:      {results['pdf_path']}")
