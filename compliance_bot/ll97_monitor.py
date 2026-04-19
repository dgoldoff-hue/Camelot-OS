"""
ll97_monitor.py — Local Law 97 (Carbon Emissions) Compliance Monitor
Camelot Property Management Services Corp / Compliance Bot

Calculates LL97 penalty exposure and checks Energy Star scores
via NYC Benchmarking Open Data.

Endpoint: https://data.cityofnewyork.us/resource/utjn-ijm2.json

Reference: NYC Local Law 97 of 2019
  - Phase 1 limits: 2024–2029
  - Phase 2 limits: 2030–2034
  - Penalty: $268 per metric ton CO₂e over limit

Author: Camelot OS
"""

import logging
import os
from typing import Optional
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BENCHMARKING_URL = "https://data.cityofnewyork.us/resource/utjn-ijm2.json"
APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")

# LL97 Penalty rate: $268 per metric ton CO₂e over limit
LL97_PENALTY_PER_TON = 268.0

# ---------------------------------------------------------------------------
# LL97 Carbon Intensity Limits (kgCO₂e/sqft/year)
# Per NYC Local Law 97, by Occupancy Group
# Phase 1: 2024–2029 | Phase 2: 2030–2034
# ---------------------------------------------------------------------------

LL97_LIMITS: dict[str, dict[str, float]] = {
    # Occupancy Group → {phase_1: limit, phase_2: limit}
    "R-2":  {"phase_1": 0.00675, "phase_2": 0.00374},  # Residential/Multifamily
    "B":    {"phase_1": 0.00846, "phase_2": 0.00453},  # Office
    "M":    {"phase_1": 0.01074, "phase_2": 0.00420},  # Mercantile/Retail
    "S-1":  {"phase_1": 0.01074, "phase_2": 0.00420},  # Storage
    "A-2":  {"phase_1": 0.01129, "phase_2": 0.00420},  # Assembly (restaurants)
    "I-2":  {"phase_1": 0.02381, "phase_2": 0.00420},  # Institutional (healthcare)
    "E":    {"phase_1": 0.00758, "phase_2": 0.00420},  # Educational
    "F":    {"phase_1": 0.01074, "phase_2": 0.00420},  # Factory/Industrial
    "H":    {"phase_1": 0.01074, "phase_2": 0.00420},  # High Hazard
    "mixed_use": {"phase_1": 0.00846, "phase_2": 0.00453},  # Mixed-use (weighted avg default)
}

# Asset type → Occupancy Group mapping
ASSET_TYPE_TO_OCC: dict[str, str] = {
    "multifamily": "R-2",
    "residential": "R-2",
    "apartment": "R-2",
    "office": "B",
    "retail": "M",
    "mixed_use": "mixed_use",
    "mixed-use": "mixed_use",
    "warehouse": "S-1",
    "restaurant": "A-2",
    "healthcare": "I-2",
    "school": "E",
    "industrial": "F",
}

# NYC GHG Conversion Factors (lbCO₂e/kBtu) — Local Law 84/97 reference table
# Source: NYC Mayor's Office of Climate and Environmental Justice, 2023
GHG_FACTORS: dict[str, float] = {
    "electricity":  0.000288562,  # metric tons CO₂e / kWh (NYC grid 2024)
    "natural_gas":  0.0000530,    # metric tons CO₂e / kBtu
    "fuel_oil_2":   0.0000734,    # metric tons CO₂e / kBtu
    "fuel_oil_4":   0.0000787,    # metric tons CO₂e / kBtu
    "fuel_oil_6":   0.0000804,    # metric tons CO₂e / kBtu
    "steam":        0.0000439,    # metric tons CO₂e / kBtu (Con Ed district steam)
    "district_chw": 0.0000000,    # chilled water (no direct emissions)
}

KWH_PER_KBTU = 0.293071  # 1 kBtu = 0.293071 kWh


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class EnergyProfile:
    """Annual energy consumption for a building."""
    gross_sq_ft: float
    occupancy_group: str                        # R-2, B, M, etc.
    electricity_kwh: float = 0.0
    natural_gas_kbtu: float = 0.0
    fuel_oil_2_kbtu: float = 0.0
    fuel_oil_4_kbtu: float = 0.0
    fuel_oil_6_kbtu: float = 0.0
    steam_kbtu: float = 0.0
    # Derived
    total_ghg_metric_tons: float = 0.0
    carbon_intensity_kgco2e_sqft: float = 0.0


@dataclass
class LL97Result:
    bbl: Optional[str]
    address: str
    gross_sq_ft: float
    occupancy_group: str
    phase_1_limit: float                        # kgCO₂e/sqft/yr
    phase_2_limit: float
    actual_carbon_intensity: float              # kgCO₂e/sqft/yr
    total_ghg_metric_tons: float                # actual annual GHG
    phase_1_limit_tons: float                   # total tons allowed (Phase 1)
    phase_2_limit_tons: float
    phase_1_excess_tons: float                  # tons over limit (0 if compliant)
    phase_2_excess_tons: float
    phase_1_annual_penalty: float               # $ penalty estimate
    phase_2_annual_penalty: float
    phase_1_status: str                         # COMPLIANT / NON-COMPLIANT / MARGINAL
    phase_2_status: str
    energy_star_score: Optional[int]
    energy_star_source: str
    recommended_actions: list[str] = field(default_factory=list)
    scan_timestamp: str = ""


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=0.75, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    if APP_TOKEN:
        s.headers.update({"X-App-Token": APP_TOKEN})
    return s


_session = _build_session()


# ---------------------------------------------------------------------------
# Core: LL97 Exposure Calculator
# ---------------------------------------------------------------------------

def calculate_ll97_exposure(
    building_data: dict,
    phase_year: int = 1,
) -> LL97Result:
    """
    Calculate LL97 carbon emissions and penalty exposure for a building.

    Args:
        building_data: Dict with keys:
            - bbl (optional str)
            - address (str)
            - gross_sq_ft (float)
            - asset_type or occupancy_group (str)
            - electricity_kwh (float, annual)
            - natural_gas_kbtu (float, annual)
            - fuel_oil_2_kbtu (float, optional)
            - fuel_oil_4_kbtu (float, optional)
            - fuel_oil_6_kbtu (float, optional)
            - steam_kbtu (float, optional)
        phase_year: Which phase to emphasize in reporting (1 or 2)

    Returns:
        LL97Result with compliance status and penalty estimates.
    """
    from datetime import datetime

    gross_sq_ft = float(building_data.get("gross_sq_ft", 0))
    if gross_sq_ft <= 0:
        raise ValueError("gross_sq_ft must be positive")

    # Resolve occupancy group
    occ_group = building_data.get("occupancy_group")
    if not occ_group:
        asset_type = building_data.get("asset_type", "multifamily").lower().replace(" ", "_")
        occ_group = ASSET_TYPE_TO_OCC.get(asset_type, "R-2")

    limits = LL97_LIMITS.get(occ_group, LL97_LIMITS["R-2"])

    # Calculate GHG emissions in metric tons CO₂e
    electricity_kwh = float(building_data.get("electricity_kwh", 0))
    natural_gas_kbtu = float(building_data.get("natural_gas_kbtu", 0))
    fuel_oil_2_kbtu = float(building_data.get("fuel_oil_2_kbtu", 0))
    fuel_oil_4_kbtu = float(building_data.get("fuel_oil_4_kbtu", 0))
    fuel_oil_6_kbtu = float(building_data.get("fuel_oil_6_kbtu", 0))
    steam_kbtu = float(building_data.get("steam_kbtu", 0))

    total_ghg = (
        electricity_kwh * GHG_FACTORS["electricity"]
        + natural_gas_kbtu * GHG_FACTORS["natural_gas"]
        + fuel_oil_2_kbtu * GHG_FACTORS["fuel_oil_2"]
        + fuel_oil_4_kbtu * GHG_FACTORS["fuel_oil_4"]
        + fuel_oil_6_kbtu * GHG_FACTORS["fuel_oil_6"]
        + steam_kbtu * GHG_FACTORS["steam"]
    )

    # Carbon intensity in kgCO₂e/sqft/yr
    carbon_intensity_kg = (total_ghg * 1000) / gross_sq_ft  # convert tons → kg

    # Allowed total emissions
    phase_1_allowed_tons = limits["phase_1"] * gross_sq_ft / 1000  # kgCO₂e → metric tons
    phase_2_allowed_tons = limits["phase_2"] * gross_sq_ft / 1000

    # Excess
    phase_1_excess = max(0.0, total_ghg - phase_1_allowed_tons)
    phase_2_excess = max(0.0, total_ghg - phase_2_allowed_tons)

    # Annual penalties
    phase_1_penalty = phase_1_excess * LL97_PENALTY_PER_TON
    phase_2_penalty = phase_2_excess * LL97_PENALTY_PER_TON

    # Status
    def _status(excess: float, limit: float) -> str:
        if total_ghg <= 0:
            return "NO_DATA"
        ratio = total_ghg / limit if limit > 0 else float("inf")
        if excess <= 0:
            if ratio > 0.9:
                return "MARGINAL"  # within 10% of limit
            return "COMPLIANT"
        return "NON-COMPLIANT"

    p1_status = _status(phase_1_excess, phase_1_allowed_tons)
    p2_status = _status(phase_2_excess, phase_2_allowed_tons)

    # Recommended actions
    actions = _build_recommendations(
        p1_status, p2_status, phase_1_penalty, phase_2_penalty,
        electricity_kwh, natural_gas_kbtu, steam_kbtu, gross_sq_ft, occ_group,
    )

    result = LL97Result(
        bbl=building_data.get("bbl"),
        address=building_data.get("address", "Unknown"),
        gross_sq_ft=gross_sq_ft,
        occupancy_group=occ_group,
        phase_1_limit=limits["phase_1"],
        phase_2_limit=limits["phase_2"],
        actual_carbon_intensity=round(carbon_intensity_kg, 6),
        total_ghg_metric_tons=round(total_ghg, 4),
        phase_1_limit_tons=round(phase_1_allowed_tons, 4),
        phase_2_limit_tons=round(phase_2_allowed_tons, 4),
        phase_1_excess_tons=round(phase_1_excess, 4),
        phase_2_excess_tons=round(phase_2_excess, 4),
        phase_1_annual_penalty=round(phase_1_penalty, 2),
        phase_2_annual_penalty=round(phase_2_penalty, 2),
        phase_1_status=p1_status,
        phase_2_status=p2_status,
        energy_star_score=None,
        energy_star_source="not_fetched",
        recommended_actions=actions,
        scan_timestamp=datetime.utcnow().isoformat() + "Z",
    )

    logger.info(
        f"LL97 scan for {result.address}: "
        f"GHG={total_ghg:.2f}t, Phase1={p1_status} (penalty ${phase_1_penalty:,.0f})"
    )
    return result


# ---------------------------------------------------------------------------
# Energy Star Score Lookup
# ---------------------------------------------------------------------------

def check_energy_star_score(
    bbl: str,
    year: Optional[int] = None,
) -> dict:
    """
    Fetch Energy Star score and benchmarking data from NYC Open Data.

    Args:
        bbl:  BBL in NYC format (borough+block+lot, 10 digits or hyphenated)
        year: Benchmarking year to fetch (defaults to most recent available)

    Returns:
        Dict with energy_star_score, site_eui, source_eui, total_ghg_emissions,
        property_name, address, year_ending, and compliance notes.
    """
    # Normalize BBL — benchmarking data uses numeric BBL
    bbl_clean = "".join(filter(str.isdigit, bbl))

    params: dict = {
        "$where": f"bbl = '{bbl_clean}'",
        "$order": "year_ending DESC",
        "$limit": 5,
    }
    if year:
        params["$where"] += f" AND year_ending = '{year}-12-31T00:00:00.000'"

    logger.info(f"Fetching Energy Star benchmarking data for BBL {bbl}")

    try:
        resp = _session.get(BENCHMARKING_URL, params=params, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
    except requests.RequestException as e:
        logger.error(f"Benchmarking API failed for BBL {bbl}: {e}")
        return {"error": str(e), "bbl": bbl}

    if not rows:
        logger.warning(f"No benchmarking data found for BBL {bbl}")
        return {
            "bbl": bbl,
            "error": "No benchmarking data found — building may not be subject to Local Law 84 reporting",
            "energy_star_score": None,
        }

    # Use most recent row
    row = rows[0]
    score_raw = row.get("energy_star_score", row.get("energystarscore"))
    score = None
    if score_raw and str(score_raw).strip() not in ("N/A", "", "Not Available"):
        try:
            score = int(float(str(score_raw)))
        except (ValueError, TypeError):
            pass

    site_eui = _safe_float(row.get("site_eui__kbtu_ft_", row.get("site_eui")))
    total_ghg = _safe_float(
        row.get("total_ghg_emissions_metric_tons_co2e_",
                row.get("total_ghg_emissions"))
    )
    gross_sqft = _safe_float(row.get("largest_property_use_type_gross_floor_area__ft_",
                                      row.get("gross_floor_area_buildings__ft_")))

    result = {
        "bbl": bbl,
        "property_name": row.get("property_name", row.get("building_name", "")),
        "address": row.get("address_1_self_reported", row.get("address", "")),
        "year_ending": row.get("year_ending", "")[:10] if row.get("year_ending") else "",
        "energy_star_score": score,
        "site_eui_kbtu_sqft": site_eui,
        "total_ghg_metric_tons": total_ghg,
        "gross_sq_ft": gross_sqft,
        "primary_use": row.get("largest_property_use_type", ""),
        "ll84_compliance": row.get("reported_"),
        "score_interpretation": _interpret_energy_star_score(score),
        "ll97_flag": _ll97_flag_from_score(score),
    }

    logger.info(
        f"Energy Star for BBL {bbl}: score={score}, "
        f"GHG={total_ghg}t, site_eui={site_eui}"
    )
    return result


# ---------------------------------------------------------------------------
# Format results
# ---------------------------------------------------------------------------

def format_ll97_report(result: LL97Result, energy_star: Optional[dict] = None) -> str:
    """Format LL97Result as a Markdown report."""
    lines = [
        f"## LL97 Compliance Report — {result.address}",
        f"*Occupancy Group: {result.occupancy_group} | Gross SF: {result.gross_sq_ft:,.0f}*\n",
        "### Emissions Summary",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Annual GHG Emissions | {result.total_ghg_metric_tons:,.2f} metric tons CO₂e |",
        f"| Carbon Intensity | {result.actual_carbon_intensity:.5f} kgCO₂e/sqft/yr |",
        "",
        "### Phase 1 (2024–2029)",
        f"| Item | Value |",
        f"|------|-------|",
        f"| Limit | {result.phase_1_limit:.5f} kgCO₂e/sqft/yr ({result.phase_1_limit_tons:,.2f} tons) |",
        f"| Actual | {result.actual_carbon_intensity:.5f} kgCO₂e/sqft/yr ({result.total_ghg_metric_tons:,.2f} tons) |",
        f"| Excess | {result.phase_1_excess_tons:,.2f} tons |",
        f"| **Status** | **{result.phase_1_status}** |",
        f"| **Est. Annual Penalty** | **${result.phase_1_annual_penalty:,.0f}** |",
        "",
        "### Phase 2 (2030–2034)",
        f"| Item | Value |",
        f"|------|-------|",
        f"| Limit | {result.phase_2_limit:.5f} kgCO₂e/sqft/yr ({result.phase_2_limit_tons:,.2f} tons) |",
        f"| Excess | {result.phase_2_excess_tons:,.2f} tons |",
        f"| **Status** | **{result.phase_2_status}** |",
        f"| **Est. Annual Penalty** | **${result.phase_2_annual_penalty:,.0f}** |",
    ]

    if energy_star:
        lines += [
            "",
            "### Energy Star Benchmarking",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Energy Star Score | {energy_star.get('energy_star_score', 'N/A')} |",
            f"| Site EUI | {energy_star.get('site_eui_kbtu_sqft', 'N/A')} kBtu/sqft |",
            f"| Year | {energy_star.get('year_ending', 'N/A')} |",
            f"| LL97 Risk Flag | {energy_star.get('ll97_flag', 'N/A')} |",
        ]

    if result.recommended_actions:
        lines += ["", "### Recommended Actions"]
        for i, action in enumerate(result.recommended_actions, 1):
            lines.append(f"{i}. {action}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_recommendations(
    p1_status: str, p2_status: str,
    p1_penalty: float, p2_penalty: float,
    elec_kwh: float, gas_kbtu: float, steam_kbtu: float,
    sqft: float, occ_group: str,
) -> list[str]:
    actions = []

    if p1_status == "NON-COMPLIANT":
        actions.append(
            f"PRIORITY: Building is NON-COMPLIANT with Phase 1 LL97 limits. "
            f"Estimated annual penalty: ${p1_penalty:,.0f}. "
            "Engage a sustainability consultant or energy auditor immediately."
        )
    elif p1_status == "MARGINAL":
        actions.append(
            "WARNING: Building is within 10% of Phase 1 LL97 limit. "
            "Proactive energy upgrades recommended to avoid penalty exposure."
        )

    if p2_status == "NON-COMPLIANT":
        actions.append(
            f"Phase 2 (2030–2034) penalty exposure: ${p2_penalty:,.0f}/year. "
            "Begin capital planning for electrification or deep energy retrofits."
        )

    # Fuel-specific recommendations
    if gas_kbtu > 0:
        actions.append(
            "Natural gas is your largest carbon driver. "
            "Consider: high-efficiency boiler replacement, heat pump conversion (hybrid or all-electric), "
            "or fuel switching to green steam if Con Ed district steam is available."
        )
    if elec_kwh > 0 and elec_kwh / (sqft or 1) > 10:
        actions.append(
            "Above-average electricity consumption detected. "
            "Audit lighting (LED upgrade), HVAC controls, and common area loads. "
            "Consider green electricity procurement (NYSERDA, RECs)."
        )
    if steam_kbtu > 0:
        actions.append(
            "Con Ed district steam is counted in LL97. "
            "Ensure steam traps are maintained; steam trap losses increase carbon emissions."
        )

    actions.append(
        "Commission a Local Law 97 Compliance Study from a licensed MEP engineer "
        "to develop a building-specific decarbonization roadmap."
    )
    actions.append(
        "Apply for NYSERDA Clean Heat or Con Edison Multifamily Energy Efficiency programs "
        "for incentives on heat pump and insulation upgrades."
    )

    return actions


def _interpret_energy_star_score(score: Optional[int]) -> str:
    if score is None:
        return "Not rated"
    if score >= 75:
        return "Excellent — ENERGY STAR Certified eligible"
    if score >= 50:
        return "Average — meets median performance"
    if score >= 25:
        return "Below average — energy efficiency improvements needed"
    return "Poor — significant energy waste; high LL97 risk"


def _ll97_flag_from_score(score: Optional[int]) -> str:
    if score is None:
        return "UNKNOWN"
    if score >= 70:
        return "LOW_RISK"
    if score >= 50:
        return "MODERATE_RISK"
    if score >= 25:
        return "HIGH_RISK"
    return "CRITICAL_RISK"


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys, json
    logging.basicConfig(level=logging.INFO)

    # Example: python ll97_monitor.py <bbl>
    if len(sys.argv) > 1:
        bbl = sys.argv[1]
        es = check_energy_star_score(bbl)
        print(json.dumps(es, indent=2, default=str))
    else:
        # Demo calculation
        sample = {
            "address": "123 Main St, Bronx NY",
            "gross_sq_ft": 25000,
            "asset_type": "multifamily",
            "electricity_kwh": 180000,
            "natural_gas_kbtu": 1200000,
            "steam_kbtu": 0,
        }
        result = calculate_ll97_exposure(sample)
        print(format_ll97_report(result))
