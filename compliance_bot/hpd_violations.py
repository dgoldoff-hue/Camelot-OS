"""
hpd_violations.py — HPD Violations Monitor
Camelot Property Management Services Corp / Compliance Bot

Fetches, classifies, and deadline-checks HPD open violations via NYC Open Data Socrata API.
Endpoint: https://data.cityofnewyork.us/resource/wvxf-dwi5.json

Author: Camelot OS
"""

import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from dataclasses import dataclass, field

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

HPD_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"
APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")

# Heat season: Oct 1 – May 31
HEAT_SEASON_START_MONTH = 10
HEAT_SEASON_END_MONTH = 5


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------

def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=0.75, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    if APP_TOKEN:
        session.headers.update({"X-App-Token": APP_TOKEN})
    return session


_session = _build_session()


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class HPDViolation:
    violation_id: str
    building_id: Optional[str]
    bbl: Optional[str]
    address: str
    apartment: Optional[str]
    inspection_date: Optional[str]
    approved_date: Optional[str]
    original_certify_by_date: Optional[str]
    original_correct_by_date: Optional[str]
    current_status: str
    violation_status: str
    class_code: str                    # A, B, C
    violation_type: str                # e.g. "HEAT", "MOLD", "LEAD", "STRUCTURAL", etc.
    novdescription: str
    violation_category: str            # Classified by classify_violation()
    severity_label: str                # Non-Hazardous / Hazardous / Immediately Hazardous
    days_to_deadline: Optional[int] = None
    deadline_status: str = "OK"        # OK / WARNING / CRITICAL / OVERDUE
    recommended_action: str = ""


# ---------------------------------------------------------------------------
# Fetch violations
# ---------------------------------------------------------------------------

def get_open_violations(
    bbl: Optional[str] = None,
    address: Optional[str] = None,
    borough: Optional[str] = None,
    limit: int = 200,
) -> list[HPDViolation]:
    """
    Fetch open HPD violations for a property via Socrata API.

    Args:
        bbl:     Borough-Block-Lot string (e.g. "2-02501-0012" or "2025010012")
        address: Street address (used when BBL is not available)
        borough: Borough name (Manhattan/Bronx/Brooklyn/Queens/Staten Island)
        limit:   Max records to return

    Returns:
        List of HPDViolation objects, sorted by most recent inspection date.
    """
    if not bbl and not address:
        raise ValueError("Must provide either bbl or address")

    filters = ["violationstatus = 'Open'"]

    if bbl:
        # Normalize BBL to 10-digit format if needed
        normalized = _normalize_bbl(bbl)
        if normalized:
            filters.append(f"bbl = '{normalized}'")
        else:
            logger.warning(f"Could not normalize BBL: {bbl}")

    if address and not bbl:
        # Clean address for Socrata contains search
        clean_addr = address.upper().strip()
        # Extract street number + name
        parts = clean_addr.split(",")[0].strip()
        filters.append(f"address LIKE '%{parts}%'")
        if borough:
            filters.append(f"boro = '{_boro_code(borough)}'")

    where_clause = " AND ".join(filters)

    params = {
        "$where": where_clause,
        "$order": "inspectiondate DESC",
        "$limit": limit,
    }

    logger.info(f"Fetching HPD open violations — BBL: {bbl}, Address: {address}")

    try:
        resp = _session.get(HPD_VIOLATIONS_URL, params=params, timeout=15)
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as e:
        logger.error(f"HPD API request failed: {e}")
        return []

    violations = []
    for row in raw:
        try:
            v = _parse_violation_row(row)
            violations.append(v)
        except Exception as e:
            logger.debug(f"Skipping malformed HPD row: {e}")
            continue

    logger.info(f"Fetched {len(violations)} open HPD violations")
    return violations


def _parse_violation_row(row: dict) -> HPDViolation:
    """Parse a raw Socrata row into an HPDViolation."""
    class_code = row.get("class", row.get("violationclass", "B")).upper().strip()
    description = row.get("novdescription", row.get("violationdescription", ""))

    violation = HPDViolation(
        violation_id=str(row.get("violationid", row.get("liid", ""))),
        building_id=row.get("buildingid", row.get("buildingaddressid")),
        bbl=row.get("bbl"),
        address=_build_address(row),
        apartment=row.get("apartment"),
        inspection_date=_parse_date_str(row.get("inspectiondate")),
        approved_date=_parse_date_str(row.get("approveddate")),
        original_certify_by_date=_parse_date_str(row.get("originalcertifybydate")),
        original_correct_by_date=_parse_date_str(row.get("originalcorrectbydate")),
        current_status=row.get("currentstatus", ""),
        violation_status=row.get("violationstatus", "Open"),
        class_code=class_code,
        violation_type="OTHER",
        novdescription=description,
        violation_category="",
        severity_label=_class_to_severity(class_code),
        recommended_action="",
    )

    # Classify and annotate
    classified = classify_violation(violation)
    violation.violation_type = classified["type"]
    violation.violation_category = classified["category"]
    violation.recommended_action = classified["recommended_action"]

    return violation


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

VIOLATION_TYPE_RULES = [
    # (keywords_in_description, type_label, category)
    (["heat", "heating", "radiator", "boiler", "hot water", "hw "], "HEAT", "Heat & Hot Water"),
    (["mold", "mildew", "fungus"], "MOLD", "Mold & Moisture"),
    (["lead", "lead-based paint", "lead paint", "lead-safe"], "LEAD", "Lead Paint"),
    (["structural", "crack", "collapse", "wall defect", "ceiling", "floor defect", "sagging"], "STRUCTURAL", "Structural"),
    (["vermin", "roach", "rodent", "mice", "rat", "pest", "bedbug", "bed bug", "insect"], "PEST", "Pest & Vermin"),
    (["elevator", "lift"], "ELEVATOR", "Elevator"),
    (["gas", "gas leak", "carbon monoxide", "co detector"], "GAS", "Gas & CO"),
    (["window guard", "window fall", "window safety"], "WINDOW", "Window Guards"),
    (["smoke detector", "fire alarm", "sprinkler", "fire escape"], "FIRE_SAFETY", "Fire Safety"),
    (["electric", "wiring", "outlet", "circuit"], "ELECTRICAL", "Electrical"),
    (["plumbing", "sewage", "drain", "toilet", "sink"], "PLUMBING", "Plumbing"),
    (["door", "lock", "entry", "intercom"], "DOOR_LOCK", "Doors & Locks"),
    (["paint", "peeling paint", "defective plaster"], "PAINT_PLASTER", "Paint & Plaster"),
    (["light", "lighting", "bulb", "illuminat"], "LIGHTING", "Lighting"),
]

CLASS_ACTIONS = {
    "C": {
        "immediately_hazardous": True,
        "correction_window_days": 24,
        "base_action": "IMMEDIATE: Correct within 24 hours. Class C violation is immediately hazardous. Contact licensed contractor NOW. Notify HPD of correction.",
    },
    "B": {
        "immediately_hazardous": False,
        "correction_window_days": 30,
        "base_action": "URGENT: Correct within 30 days. Schedule licensed contractor. Document correction with HPD.",
    },
    "A": {
        "immediately_hazardous": False,
        "correction_window_days": 90,
        "base_action": "ROUTINE: Correct within 90 days. Document repair. Certify correction with HPD.",
    },
}


def classify_violation(violation: HPDViolation) -> dict:
    """
    Classify an HPD violation by type, category, and recommended action.

    Args:
        violation: HPDViolation object (violation_type will be set from description)

    Returns:
        Dict with keys: type, category, recommended_action
    """
    description_lower = violation.novdescription.lower()
    vtype = "OTHER"
    category = "General"

    for keywords, type_label, cat in VIOLATION_TYPE_RULES:
        if any(kw in description_lower for kw in keywords):
            vtype = type_label
            category = cat
            break

    class_info = CLASS_ACTIONS.get(violation.class_code.upper(), CLASS_ACTIONS["B"])
    base_action = class_info["base_action"]

    # Type-specific action overrides
    type_actions = {
        "HEAT": (
            "HEAT/HOT WATER VIOLATION: Restore heat/hot water immediately. "
            "NYC law requires minimum 68°F (6am–10pm) and 55°F (10pm–6am) Oct 1–May 31. "
            "Document boiler/system repair. Notify HPD. Fine risk: up to $1,000/day during heat season."
        ),
        "LEAD": (
            "LEAD PAINT VIOLATION: Must use EPA-certified Lead-Safe contractor. "
            "Complete Lead Paint Abatement Report (LPAR). Re-inspection required. "
            "Do NOT use dry sanding. Notify residents with children under 6."
        ),
        "MOLD": (
            "MOLD VIOLATION: Identify and remediate moisture source first. "
            "Areas >10 sq ft require licensed mold remediation contractor. "
            "Document with before/after photos. Air quality test if extensive."
        ),
        "STRUCTURAL": (
            "STRUCTURAL VIOLATION: Retain licensed PE or RA immediately. "
            "Assess structural integrity. File DOB Emergency Declaration if building safety at risk. "
            "Do not permit occupancy of affected area until cleared."
        ),
        "GAS": (
            "GAS/CO VIOLATION: Immediately contact Con Edison or NYSEG. "
            "Evacuate if gas odor present. Call 911. "
            "Cannot reoccupy until utility company clears and inspector approves."
        ),
        "FIRE_SAFETY": (
            "FIRE SAFETY VIOLATION: Test/replace smoke detectors and CO detectors. "
            "Inspect fire escapes. Ensure sprinkler system operational. "
            "FDNY may issue ECB summons — respond promptly."
        ),
    }

    recommended_action = type_actions.get(vtype, base_action)

    return {
        "type": vtype,
        "category": category,
        "recommended_action": recommended_action,
    }


# ---------------------------------------------------------------------------
# Deadline checking
# ---------------------------------------------------------------------------

def check_violation_deadlines(
    violations: list[HPDViolation],
    warning_days: int = 30,
    critical_days: int = 7,
) -> list[HPDViolation]:
    """
    Flag violations with approaching correction deadlines.

    Args:
        violations:    List of HPDViolation objects.
        warning_days:  Days-to-deadline threshold for WARNING status.
        critical_days: Days-to-deadline threshold for CRITICAL status.

    Returns:
        Same list with days_to_deadline and deadline_status populated,
        sorted by urgency (OVERDUE first, then CRITICAL, WARNING, OK).
    """
    today = date.today()
    in_heat_season = _is_heat_season(today)

    for v in violations:
        # Determine the operative deadline
        deadline_str = v.original_correct_by_date or v.original_certify_by_date
        if not deadline_str:
            # Estimate from class code if no explicit deadline
            class_info = CLASS_ACTIONS.get(v.class_code.upper(), CLASS_ACTIONS["B"])
            if v.inspection_date:
                try:
                    insp = date.fromisoformat(v.inspection_date)
                    deadline = insp + timedelta(days=class_info["correction_window_days"])
                    deadline_str = deadline.isoformat()
                except ValueError:
                    pass

        if deadline_str:
            try:
                deadline = date.fromisoformat(deadline_str[:10])
                days_remaining = (deadline - today).days
                v.days_to_deadline = days_remaining

                if days_remaining < 0:
                    v.deadline_status = "OVERDUE"
                elif days_remaining <= critical_days:
                    v.deadline_status = "CRITICAL"
                elif days_remaining <= warning_days:
                    v.deadline_status = "WARNING"
                else:
                    v.deadline_status = "OK"
            except ValueError:
                logger.debug(f"Could not parse deadline date: {deadline_str}")

        # Class C violations are always CRITICAL regardless of deadline
        if v.class_code.upper() == "C" and v.deadline_status not in ("OVERDUE", "CRITICAL"):
            v.deadline_status = "CRITICAL"

        # Heat season escalation: heat/hot water violations are CRITICAL
        if in_heat_season and v.violation_type in ("HEAT",) and v.deadline_status == "OK":
            v.deadline_status = "WARNING"

    # Sort: OVERDUE → CRITICAL → WARNING → OK
    priority = {"OVERDUE": 0, "CRITICAL": 1, "WARNING": 2, "OK": 3}
    violations.sort(key=lambda v: (priority.get(v.deadline_status, 9), v.class_code))

    flagged = [v for v in violations if v.deadline_status in ("OVERDUE", "CRITICAL", "WARNING")]
    logger.info(
        f"Deadline check complete: {len(flagged)} violations flagged "
        f"({sum(1 for v in violations if v.deadline_status == 'OVERDUE')} overdue, "
        f"{sum(1 for v in violations if v.deadline_status == 'CRITICAL')} critical, "
        f"{sum(1 for v in violations if v.deadline_status == 'WARNING')} warning)"
    )
    return violations


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_violations_table(violations: list[HPDViolation]) -> str:
    """Format violations as a Markdown table."""
    if not violations:
        return "_No open HPD violations found._"

    header = (
        "| Class | Type | Address | Apt | Deadline | Status | Days Left | Description |\n"
        "|-------|------|---------|-----|----------|--------|-----------|-------------|\n"
    )
    rows = []
    for v in violations:
        desc = v.novdescription[:60] + "..." if len(v.novdescription) > 60 else v.novdescription
        days = str(v.days_to_deadline) if v.days_to_deadline is not None else "—"
        rows.append(
            f"| **{v.class_code}** | {v.violation_type} | {v.address} | "
            f"{v.apartment or '—'} | {v.original_correct_by_date or '—'} | "
            f"**{v.deadline_status}** | {days} | {desc} |"
        )
    return header + "\n".join(rows)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_bbl(bbl: str) -> Optional[str]:
    """Normalize BBL to Socrata format (10-digit string)."""
    digits = "".join(filter(str.isdigit, bbl))
    if len(digits) == 10:
        return digits
    if len(digits) == 9:
        return "0" + digits
    if "-" in bbl:
        parts = bbl.split("-")
        if len(parts) == 3:
            try:
                b = int(parts[0])
                block = parts[1].zfill(5)
                lot = parts[2].zfill(4)
                return f"{b}{block}{lot}"
            except ValueError:
                pass
    return None


def _boro_code(borough_name: str) -> str:
    codes = {
        "manhattan": "MANHATTAN",
        "bronx": "BRONX",
        "brooklyn": "BROOKLYN",
        "queens": "QUEENS",
        "staten island": "STATEN ISLAND",
    }
    return codes.get(borough_name.lower(), borough_name.upper())


def _class_to_severity(class_code: str) -> str:
    return {
        "A": "Non-Hazardous",
        "B": "Hazardous",
        "C": "Immediately Hazardous",
        "I": "Informational",
    }.get(class_code.upper(), "Unknown")


def _build_address(row: dict) -> str:
    num = row.get("housenumber", "")
    street = row.get("streetname", row.get("street", ""))
    boro = row.get("boro", row.get("borough", ""))
    return f"{num} {street}, {boro}".strip().strip(",")


def _parse_date_str(val) -> Optional[str]:
    if not val:
        return None
    if isinstance(val, str):
        return val[:10] if len(val) >= 10 else val
    return None


def _is_heat_season(today: date) -> bool:
    m = today.month
    return m >= HEAT_SEASON_START_MONTH or m <= HEAT_SEASON_END_MONTH


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    bbl_arg = sys.argv[1] if len(sys.argv) > 1 else None
    addr_arg = sys.argv[2] if len(sys.argv) > 2 else None
    violations = get_open_violations(bbl=bbl_arg, address=addr_arg)
    violations = check_violation_deadlines(violations)
    print(format_violations_table(violations))
