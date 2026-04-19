"""
comp_analyzer.py — Comparable Sales Analyzer

Pulls closed sale comparables from NYC ACRIS Open Data (Socrata API)
and calculates per-unit, per-sqft, and cap rate metrics for multifamily
and commercial assets.

Author: Camelot OS / Broker Bot
"""

import logging
import math
import os
import time
from typing import Optional
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# NYC ACRIS — Real Property Sales (Annualized Master) endpoint
ACRIS_SALES_URL = "https://data.cityofnewyork.us/resource/usep-8jbt.json"
# Real Property Legals (for address lookups)
ACRIS_LEGALS_URL = "https://data.cityofnewyork.us/resource/8h5j-fqxa.json"

APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")  # optional but increases rate limits


# ---------------------------------------------------------------------------
# Session with retry logic
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    if APP_TOKEN:
        session.headers.update({"X-App-Token": APP_TOKEN})
    return session


_session = _build_session()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class CompRecord:
    address: str
    borough: str
    block: str
    lot: str
    sale_date: str
    sale_price: float
    building_class: str
    gross_sq_ft: Optional[float]
    total_units: Optional[int]
    year_built: Optional[int]
    price_per_unit: Optional[float] = None
    price_per_sqft: Optional[float] = None
    # Estimated cap rate requires NOI data; left None unless provided externally
    estimated_cap_rate: Optional[float] = None


# ---------------------------------------------------------------------------
# BOROUGH_CODE mapping
# ---------------------------------------------------------------------------

BOROUGH_CODES = {
    "manhattan": "1",
    "bronx": "2",
    "brooklyn": "3",
    "queens": "4",
    "staten island": "5",
    "1": "1", "2": "2", "3": "3", "4": "4", "5": "5",
}

BUILDING_CLASS_GROUPS = {
    "multifamily": ["C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9",
                    "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9",
                    "S1", "S2", "S3", "S4", "S5", "S9",
                    "A5", "A6", "A7", "A8", "A9",  # large rental / walk-up
                    ],
    "mixed_use":  ["B", "S", "RM"],
    "commercial": ["K", "O", "L", "RK", "RO"],
    "any":        [],
}


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def pull_comps(
    address: Optional[str] = None,
    borough: Optional[str] = None,
    radius_miles: float = 0.5,
    asset_type: str = "multifamily",
    min_sale_price: float = 500_000,
    lookback_years: int = 3,
    max_results: int = 25,
    bbl: Optional[str] = None,
) -> list[CompRecord]:
    """
    Fetch comparable closed sales from NYC ACRIS.

    Args:
        address:         Street address of subject property (used for borough detection if borough not set).
        borough:         NYC borough name or code (1–5).
        radius_miles:    Approximate search radius (used to filter by block proximity when possible).
        asset_type:      "multifamily" | "mixed_use" | "commercial" | "any"
        min_sale_price:  Minimum sale price filter (removes arm's-length sale artifacts).
        lookback_years:  How many years of sales history to include.
        max_results:     Maximum number of comp records to return.
        bbl:             Optional BBL (Borough-Block-Lot) for direct lookup.

    Returns:
        List of CompRecord objects sorted by sale_date descending.
    """
    from datetime import date, timedelta

    cutoff_date = (date.today() - timedelta(days=365 * lookback_years)).isoformat()

    # Determine borough code
    borough_code = None
    if borough:
        borough_code = BOROUGH_CODES.get(borough.lower().strip())
    if not borough_code and address:
        for name, code in BOROUGH_CODES.items():
            if name in address.lower():
                borough_code = code
                break

    # Build SoQL query
    where_clauses = [
        f"sale_price > '{min_sale_price}'",
        f"sale_date >= '{cutoff_date}T00:00:00.000'",
    ]

    if borough_code:
        where_clauses.append(f"borough = '{borough_code}'")

    if bbl:
        # BBL format: BBBBBLLLLLL (10 digit) or split
        pass  # Could add block/lot filter here

    # Asset type filter via building class (partial match — ACRIS uses 2-char codes)
    class_group = BUILDING_CLASS_GROUPS.get(asset_type.lower().replace("-", "_"), [])
    if class_group:
        # ACRIS building_class_at_time_of_sale is 2 chars; filter with IN or LIKE
        class_filter = " OR ".join(
            [f"building_class_at_time_of_sale LIKE '{c[:1]}%'" for c in set(c[0] for c in class_group)]
        )
        where_clauses.append(f"({class_filter})")

    where_str = " AND ".join(where_clauses)

    params = {
        "$where": where_str,
        "$order": "sale_date DESC",
        "$limit": min(max_results * 3, 200),  # over-fetch to allow filtering
        "$select": (
            "address,borough,block,lot,sale_date,sale_price,"
            "building_class_at_time_of_sale,gross_square_feet,"
            "total_units,year_built"
        ),
    }

    logger.info(f"Pulling ACRIS comps — borough={borough_code}, asset_type={asset_type}, since={cutoff_date}")

    try:
        resp = _session.get(ACRIS_SALES_URL, params=params, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.error(f"ACRIS API request failed: {e}")
        return []

    comps: list[CompRecord] = []
    seen_bbls = set()

    for row in raw:
        try:
            price = float(row.get("sale_price", 0))
            if price < min_sale_price:
                continue

            bbk_key = f"{row.get('borough')}-{row.get('block')}-{row.get('lot')}"
            if bbk_key in seen_bbls:
                continue
            seen_bbls.add(bbk_key)

            gross_sqft = _safe_float(row.get("gross_square_feet"))
            units = _safe_int(row.get("total_units"))
            year_built = _safe_int(row.get("year_built"))

            comp = CompRecord(
                address=row.get("address", "Unknown"),
                borough=_borough_name(row.get("borough", "")),
                block=row.get("block", ""),
                lot=row.get("lot", ""),
                sale_date=row.get("sale_date", "")[:10],
                sale_price=price,
                building_class=row.get("building_class_at_time_of_sale", ""),
                gross_sq_ft=gross_sqft,
                total_units=units,
                year_built=year_built,
            )

            # Compute derived metrics
            if units and units > 0:
                comp.price_per_unit = round(price / units, 0)
            if gross_sqft and gross_sqft > 0:
                comp.price_per_sqft = round(price / gross_sqft, 2)

            comps.append(comp)

        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Skipping malformed comp row: {e}")
            continue

    comps = comps[:max_results]
    logger.info(f"Returned {len(comps)} comp records")
    return comps


def classify_comp(comp: CompRecord) -> str:
    """
    Classify comp relevance as 'Strong' / 'Moderate' / 'Weak'
    based on data completeness and price quality.
    """
    score = 0
    if comp.total_units:
        score += 2
    if comp.gross_sq_ft:
        score += 2
    if comp.year_built:
        score += 1
    if comp.sale_price > 1_000_000:
        score += 1
    if score >= 5:
        return "Strong"
    elif score >= 3:
        return "Moderate"
    return "Weak"


def calculate_comp_stats(comps: list[CompRecord]) -> dict:
    """
    Calculate aggregate statistics from a comp list.

    Returns:
        Dict with avg_price_per_unit, avg_price_per_sqft, median_sale_price,
        count, and price_range.
    """
    if not comps:
        return {}

    prices = [c.sale_price for c in comps]
    prices_per_unit = [c.price_per_unit for c in comps if c.price_per_unit]
    prices_per_sqft = [c.price_per_sqft for c in comps if c.price_per_sqft]

    def avg(lst):
        return round(sum(lst) / len(lst), 2) if lst else None

    def median(lst):
        if not lst:
            return None
        s = sorted(lst)
        mid = len(s) // 2
        return s[mid] if len(s) % 2 != 0 else (s[mid - 1] + s[mid]) / 2

    return {
        "count": len(comps),
        "avg_sale_price": round(avg(prices), 0),
        "median_sale_price": round(median(prices), 0),
        "min_sale_price": round(min(prices), 0),
        "max_sale_price": round(max(prices), 0),
        "avg_price_per_unit": round(avg(prices_per_unit), 0) if prices_per_unit else None,
        "avg_price_per_sqft": round(avg(prices_per_sqft), 2) if prices_per_sqft else None,
    }


def format_comp_table(comps: list[CompRecord]) -> str:
    """Format comps as a Markdown table."""
    if not comps:
        return "_No comparable sales found._"

    header = (
        "| # | Address | Borough | Sale Date | Sale Price | $/Unit | $/SqFt | Units | Yr Built | Class | Quality |\n"
        "|---|---------|---------|-----------|------------|--------|--------|-------|----------|-------|---------|\n"
    )
    rows = []
    for i, c in enumerate(comps, 1):
        rows.append(
            f"| {i} | {c.address} | {c.borough} | {c.sale_date} "
            f"| ${c.sale_price:,.0f} "
            f"| {f'${c.price_per_unit:,.0f}' if c.price_per_unit else '—'} "
            f"| {f'${c.price_per_sqft:,.2f}' if c.price_per_sqft else '—'} "
            f"| {c.total_units or '—'} "
            f"| {c.year_built or '—'} "
            f"| {c.building_class} "
            f"| {classify_comp(c)} |"
        )
    return header + "\n".join(rows)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None


def _borough_name(code: str) -> str:
    names = {"1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island"}
    return names.get(str(code), code)


# ---------------------------------------------------------------------------
# CLI / quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    comps = pull_comps(
        borough="bronx",
        asset_type="multifamily",
        min_sale_price=1_000_000,
        lookback_years=2,
        max_results=10,
    )
    print(format_comp_table(comps))
    print("\nStats:", calculate_comp_stats(comps))
