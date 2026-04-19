"""
rent_stab_checker.py — Rent Stabilization Status Checker
Camelot Property Management Services Corp / Compliance Bot

Checks HCR/DHCR rent stabilization registration status for NYC buildings.
Flags buildings that appear rent-stabilized but are not registered.
Attempts to retrieve legal regulated rent information.

Data Sources:
  - NYC DHCR Building Registration (HCR Open Data)
  - NYC Rent Stabilized Building List (NYCHPD / JustFix.nyc PLUTO + RS data)
  - NYC Open Data: https://data.cityofnewyork.us/resource/xt2h-yqhm.json (HPD Registrations)

Author: Camelot OS
"""

import logging
import os
import re
import time
from typing import Optional
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# NYC HPD Registration — property owner registrations
HPD_REGISTRATION_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.json"
# NYC Rent Stabilized units by tax lot (MapPLUTO-derived)
PLUTO_URL = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
# DHCR-registered RS buildings (approximate via HCR)
HCR_REGISTRATIONS_URL = "https://data.cityofnewyork.us/resource/qb38-trtu.json"

APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")

# Buildings built before 1974 with 6+ units are presumptively rent-stabilized
# unless they have been deregulated via high-rent vacancy or owner occupancy
RS_PRESUMPTION_YEAR = 1974
RS_MINIMUM_UNITS = 6


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
# Data models
# ---------------------------------------------------------------------------

@dataclass
class RentStabResult:
    bbl: str
    address: str
    borough: str
    year_built: Optional[int]
    total_units: Optional[int]
    rs_units: Optional[int]                  # Rent-stabilized units from PLUTO/registration
    hpd_registered: bool
    hcr_registered: bool
    appears_rent_stabilized: bool            # Based on age/size heuristic
    registration_current: bool
    registration_expiration: Optional[str]
    owner_name: Optional[str]
    managing_agent: Optional[str]
    flags: list[str] = field(default_factory=list)
    status: str = "UNKNOWN"                  # COMPLIANT / FLAGGED / UNKNOWN / NOT_APPLICABLE
    recommended_actions: list[str] = field(default_factory=list)
    scan_timestamp: str = ""


@dataclass
class LegalRentRecord:
    address: str
    unit: Optional[str]
    legal_regulated_rent: Optional[float]
    effective_date: Optional[str]
    order_number: Optional[str]
    registration_id: Optional[str]
    source: str                              # DHCR_REGISTRATION / DHCR_ORDER / ESTIMATED
    notes: str = ""


# ---------------------------------------------------------------------------
# Core: Rent Stabilization Status Check
# ---------------------------------------------------------------------------

def check_rent_stab_status(
    address: str,
    bbl: Optional[str] = None,
    borough: Optional[str] = None,
) -> RentStabResult:
    """
    Check rent stabilization registration status for a building.

    Args:
        address:  Street address of property
        bbl:      BBL (Borough-Block-Lot) — preferred for accuracy
        borough:  Borough name (used if BBL not provided)

    Returns:
        RentStabResult with registration status, flags, and recommendations.
    """
    from datetime import datetime

    logger.info(f"Checking rent stabilization for: {address} (BBL: {bbl})")

    result = RentStabResult(
        bbl=bbl or "",
        address=address,
        borough=borough or _detect_borough(address),
        year_built=None,
        total_units=None,
        rs_units=None,
        hpd_registered=False,
        hcr_registered=False,
        appears_rent_stabilized=False,
        registration_current=False,
        registration_expiration=None,
        owner_name=None,
        managing_agent=None,
        scan_timestamp=datetime.utcnow().isoformat() + "Z",
    )

    # Step 1: Check HPD registration
    hpd_data = _check_hpd_registration(address=address, bbl=bbl)
    if hpd_data:
        result.hpd_registered = True
        result.owner_name = hpd_data.get("ownername") or hpd_data.get("owner_name")
        result.managing_agent = hpd_data.get("agentname") or hpd_data.get("agent_name")
        reg_end = hpd_data.get("registrationenddate") or hpd_data.get("registration_end_date")
        if reg_end:
            result.registration_expiration = str(reg_end)[:10]
            # Check if expired (HPD registrations expire annually — must be renewed)
            try:
                from datetime import date
                exp = date.fromisoformat(result.registration_expiration)
                result.registration_current = exp >= date.today()
            except ValueError:
                pass
        result.total_units = _safe_int(hpd_data.get("units") or hpd_data.get("ownertotal"))

    # Step 2: Pull PLUTO data for year_built and RS units
    pluto_data = _check_pluto_rs(bbl=bbl, address=address)
    if pluto_data:
        result.year_built = _safe_int(pluto_data.get("yearbuilt"))
        result.total_units = result.total_units or _safe_int(pluto_data.get("unitstotal"))
        rs_raw = pluto_data.get("unitsres") or pluto_data.get("numprops")
        # PLUTO doesn't directly have RS count; approximate from exemption codes
        if pluto_data.get("exempttot") or pluto_data.get("yearbuilt"):
            rs_estimate = _estimate_rs_units(
                pluto_data, result.year_built, result.total_units
            )
            result.rs_units = rs_estimate

    # Step 3: Apply RS presumption rules
    result.appears_rent_stabilized = _presume_rent_stabilized(
        year_built=result.year_built,
        total_units=result.total_units,
        borough=result.borough,
    )

    # Step 4: Check HCR registration (via Open Data if available)
    hcr_registered = _check_hcr_registration(bbl=bbl, address=address)
    result.hcr_registered = hcr_registered

    # Step 5: Build flags and recommendations
    result.flags, result.recommended_actions, result.status = _evaluate_rs_status(result)

    logger.info(
        f"RS check for {address}: appears_RS={result.appears_rent_stabilized}, "
        f"hpd={result.hpd_registered}, hcr={result.hcr_registered}, "
        f"status={result.status}"
    )
    return result


def _check_hpd_registration(
    address: Optional[str] = None,
    bbl: Optional[str] = None,
) -> Optional[dict]:
    """Fetch HPD building registration record."""
    filters = []
    if bbl:
        bbl_clean = "".join(filter(str.isdigit, bbl))
        if len(bbl_clean) >= 9:
            filters.append(f"bbl = '{bbl_clean}'")
    elif address:
        house, *rest = address.upper().strip().split()
        filters.append(f"housenumber = '{house}'")
        if rest:
            street = rest[0].split(",")[0]
            filters.append(f"streetname LIKE '%{street}%'")

    if not filters:
        return None

    params = {
        "$where": " AND ".join(filters),
        "$order": "lastregistrationdate DESC",
        "$limit": 1,
    }

    try:
        resp = _session.get(HPD_REGISTRATION_URL, params=params, timeout=12)
        resp.raise_for_status()
        rows = resp.json()
        return rows[0] if rows else None
    except requests.RequestException as e:
        logger.warning(f"HPD registration lookup failed: {e}")
        return None


def _check_pluto_rs(
    bbl: Optional[str] = None,
    address: Optional[str] = None,
) -> Optional[dict]:
    """Fetch MapPLUTO tax lot data for building attributes."""
    if not bbl:
        return None

    # PLUTO via NYC Open Data (MapPLUTO)
    pluto_socrata = "https://data.cityofnewyork.us/resource/64uk-42ks.json"
    bbl_clean = "".join(filter(str.isdigit, bbl))
    params = {
        "$where": f"bbl = '{bbl_clean}'",
        "$limit": 1,
    }
    try:
        resp = _session.get(pluto_socrata, params=params, timeout=12)
        resp.raise_for_status()
        rows = resp.json()
        return rows[0] if rows else None
    except requests.RequestException as e:
        logger.debug(f"PLUTO lookup failed for BBL {bbl}: {e}")
        return None


def _check_hcr_registration(
    bbl: Optional[str] = None,
    address: Optional[str] = None,
) -> bool:
    """
    Check HCR rent stabilization registration.
    Uses NYC Open Data HCR dataset if available; returns False on data gap.
    """
    if not bbl:
        return False

    bbl_clean = "".join(filter(str.isdigit, bbl))
    params = {
        "$where": f"bbl = '{bbl_clean}'",
        "$limit": 1,
    }
    try:
        resp = _session.get(HCR_REGISTRATIONS_URL, params=params, timeout=12)
        resp.raise_for_status()
        rows = resp.json()
        return len(rows) > 0
    except requests.RequestException as e:
        logger.debug(f"HCR registration check failed for BBL {bbl}: {e}")
        return False  # Data gap — do not flag as unregistered based on this alone


# ---------------------------------------------------------------------------
# Legal Regulated Rent Lookup
# ---------------------------------------------------------------------------

def get_legal_regulated_rent(
    address: str,
    unit: Optional[str] = None,
    bbl: Optional[str] = None,
) -> LegalRentRecord:
    """
    Attempt to retrieve the legal regulated rent for a rent-stabilized unit.

    This primarily relies on the NYC DHCR order history and registration data.
    Full DHCR individual unit lookups require direct DHCR portal access (no open API);
    this function returns the best available open-data approximation and guidance.

    Args:
        address:  Building street address
        unit:     Unit number (apartment)
        bbl:      BBL for higher precision lookup

    Returns:
        LegalRentRecord with best available data and source.
    """
    logger.info(f"Looking up legal regulated rent for {address} Unit {unit}")

    # Check DHCR via web search approximation (no direct API)
    # Direct DHCR lookup: https://apps.hcr.ny.gov/BuildingSearch/
    record = LegalRentRecord(
        address=address,
        unit=unit,
        legal_regulated_rent=None,
        effective_date=None,
        order_number=None,
        registration_id=None,
        source="GUIDANCE",
        notes=(
            "DHCR does not provide a public API for individual unit rent records. "
            "To retrieve the legal regulated rent: "
            "1. Visit https://apps.hcr.ny.gov/BuildingSearch/ and search by address. "
            "2. Request rent history via DHCR Form RA-89 (Tenant) or the FOIL process. "
            "3. For Camelot-managed properties, check DHCR registration in MDS under "
            "the building's rent stabilization module. "
            "4. The most recent DHCR order (or rent registration form RR-1) shows "
            "the last legally registered rent for each stabilized unit. "
            f"5. Annual registration with DHCR is required by April 1 each year. "
            "Non-registration: landlord cannot collect rent increases and may face "
            "overcharge complaints."
        ),
    )

    # Try to pull HPD registration data for owner/agent info
    hpd = _check_hpd_registration(address=address, bbl=bbl)
    if hpd:
        owner = hpd.get("ownername", "")
        record.notes += f"\n\nHPD Owner of Record: {owner}"
        record.registration_id = hpd.get("registrationid")

    return record


# ---------------------------------------------------------------------------
# Flag evaluation
# ---------------------------------------------------------------------------

def _evaluate_rs_status(result: RentStabResult) -> tuple[list[str], list[str], str]:
    flags = []
    actions = []

    if result.appears_rent_stabilized and not result.hcr_registered and not result.hpd_registered:
        flags.append(
            "UNREGISTERED: Building appears rent-stabilized (pre-1974, 6+ units) "
            "but shows no HCR or HPD registration."
        )
        actions.append(
            "Register building with DHCR immediately using Form RR-1 (Annual Rent Registration). "
            "Failure to register bars landlord from collecting legal rent increases. "
            "Tenants may file overcharge complaints for up to 6 years (or unlimited if willful)."
        )
        status = "FLAGGED"

    elif result.appears_rent_stabilized and result.hpd_registered and not result.registration_current:
        flags.append(
            f"HPD REGISTRATION EXPIRED: Registration expired {result.registration_expiration}. "
            "Must renew annually by October 1."
        )
        actions.append(
            "Renew HPD building registration at portal.hpd.nyc.gov immediately. "
            "Fine for non-registration: $250–$500 plus ongoing HPD Class A violations."
        )
        status = "FLAGGED"

    elif result.appears_rent_stabilized and result.hpd_registered and result.registration_current:
        if not result.hcr_registered:
            flags.append(
                "PARTIAL COMPLIANCE: HPD registration is current, but HCR/DHCR "
                "rent stabilization registration could not be confirmed. "
                "Verify DHCR annual registration is filed."
            )
            actions.append(
                "Confirm DHCR rent stabilization registration at https://apps.hcr.ny.gov. "
                "DHCR and HPD registrations are separate requirements."
            )
            status = "FLAGGED"
        else:
            status = "COMPLIANT"
    elif not result.appears_rent_stabilized:
        status = "NOT_APPLICABLE"
        flags.append(
            "Building does not appear to meet rent stabilization presumption "
            "(post-1974 construction, or fewer than 6 units). "
            "Verify if building received J-51 or 421-a tax benefits "
            "(which trigger RS requirements regardless of age/size)."
        )
        actions.append(
            "Review tax benefit history (J-51, 421-a) via NYC DOF. "
            "J-51 and 421-a benefits create RS obligations for the benefit period."
        )
    else:
        status = "UNKNOWN"
        actions.append(
            "Manual DHCR registration verification required. "
            "Search at https://apps.hcr.ny.gov/BuildingSearch/"
        )

    # Universal actions for RS buildings
    if result.appears_rent_stabilized:
        actions.append(
            "Ensure all RS units have current lease riders (DHCR required rider language). "
            "File annual DHCR rent registration by April 1 each year."
        )
        actions.append(
            "Review for improper deregulation: any unit deregulated above the high-rent "
            "threshold ($2,700/month before 2019 HSTPA) must be reviewed under HSTPA "
            "which eliminated high-rent deregulation effective June 2019."
        )

    return flags, actions, status


# ---------------------------------------------------------------------------
# Heuristics
# ---------------------------------------------------------------------------

def _presume_rent_stabilized(
    year_built: Optional[int],
    total_units: Optional[int],
    borough: str = "",
) -> bool:
    """
    Apply RS presumption: buildings built before 1974 with 6+ units
    are presumptively rent-stabilized in NYC.
    """
    if year_built and total_units:
        return year_built < RS_PRESUMPTION_YEAR and total_units >= RS_MINIMUM_UNITS
    if year_built and year_built < RS_PRESUMPTION_YEAR:
        return True  # Assume 6+ units if we don't have unit count
    return False


def _estimate_rs_units(
    pluto_row: dict,
    year_built: Optional[int],
    total_units: Optional[int],
) -> Optional[int]:
    """Rough estimate of RS units from PLUTO data."""
    if not total_units:
        return None
    if year_built and year_built < RS_PRESUMPTION_YEAR:
        # All residential units presumed RS unless evidence of deregulation
        return total_units
    return 0


def _detect_borough(address: str) -> str:
    address_lower = address.lower()
    if "manhattan" in address_lower or ", ny 100" in address_lower:
        return "Manhattan"
    if "bronx" in address_lower or ", ny 104" in address_lower:
        return "Bronx"
    if "brooklyn" in address_lower or ", ny 11" in address_lower:
        return "Brooklyn"
    if "queens" in address_lower:
        return "Queens"
    if "staten island" in address_lower:
        return "Staten Island"
    return "Unknown"


def _safe_int(val) -> Optional[int]:
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_rs_report(result: RentStabResult) -> str:
    lines = [
        f"## Rent Stabilization Report — {result.address}",
        f"*BBL: {result.bbl} | Status: **{result.status}***\n",
        "| Attribute | Value |",
        "|-----------|-------|",
        f"| Year Built | {result.year_built or 'Unknown'} |",
        f"| Total Units | {result.total_units or 'Unknown'} |",
        f"| RS Units (est.) | {result.rs_units or 'Unknown'} |",
        f"| HPD Registered | {'Yes' if result.hpd_registered else 'No'} |",
        f"| HPD Reg. Current | {'Yes' if result.registration_current else 'No'} |",
        f"| HPD Reg. Expires | {result.registration_expiration or 'N/A'} |",
        f"| HCR/DHCR Registered | {'Yes' if result.hcr_registered else 'Not confirmed'} |",
        f"| Owner of Record | {result.owner_name or 'Unknown'} |",
        f"| Managing Agent | {result.managing_agent or 'Unknown'} |",
        "",
    ]
    if result.flags:
        lines.append("### Flags")
        for f in result.flags:
            lines.append(f"- **{f}**")
        lines.append("")
    if result.recommended_actions:
        lines.append("### Recommended Actions")
        for i, a in enumerate(result.recommended_actions, 1):
            lines.append(f"{i}. {a}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    addr = sys.argv[1] if len(sys.argv) > 1 else "123 Main Street, Bronx, NY"
    bbl = sys.argv[2] if len(sys.argv) > 2 else None
    result = check_rent_stab_status(addr, bbl=bbl)
    print(format_rs_report(result))
