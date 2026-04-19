"""
dob_violations.py — DOB Violations & Permits Monitor
Camelot Property Management Services Corp / Compliance Bot

Fetches DOB violations and active permits from NYC Open Data.
Flags: expired permits, open ECB violations, stop work orders.

Endpoints:
  Violations: https://data.cityofnewyork.us/resource/3h2n-5cm9.json
  Permits:    https://data.cityofnewyork.us/resource/ipu4-2q9a.json  (DOB Permit Issuance)
  ECB:        https://data.cityofnewyork.us/resource/6bgk-3dad.json

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

DOB_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"
DOB_PERMITS_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
ECB_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/6bgk-3dad.json"

APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")


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
class DOBViolation:
    isn_dob_bis_viol: str
    boro: str
    bin: Optional[str]
    block: Optional[str]
    lot: Optional[str]
    issue_date: Optional[str]
    violation_type_code: str
    violation_category: str
    violation_type: str
    respondent_name: Optional[str]
    description: Optional[str]
    disposition_date: Optional[str]
    disposition_comments: Optional[str]
    device_number: Optional[str]
    ecb_number: Optional[str]
    is_ecb: bool = False
    is_stop_work_order: bool = False
    severity: str = "STANDARD"          # STANDARD / ECB / STOP_WORK
    days_open: Optional[int] = None
    recommended_action: str = ""


@dataclass
class DOBPermit:
    permit_number: str
    bin: Optional[str]
    bbl: Optional[str]
    address: str
    permit_type: str                     # NB, A1, A2, A3, DM, etc.
    permit_subtype: Optional[str]
    filing_date: Optional[str]
    issuance_date: Optional[str]
    expiration_date: Optional[str]
    job_type: Optional[str]
    work_type: Optional[str]
    applicant_name: Optional[str]
    owner_name: Optional[str]
    status: str
    is_expired: bool = False
    days_until_expiration: Optional[int] = None
    flag: str = "OK"                     # OK / EXPIRING_SOON / EXPIRED / STOP_WORK


@dataclass
class DOBSummary:
    bin: Optional[str]
    address: str
    open_violations: list[DOBViolation] = field(default_factory=list)
    ecb_violations: list[DOBViolation] = field(default_factory=list)
    stop_work_orders: list[DOBViolation] = field(default_factory=list)
    active_permits: list[DOBPermit] = field(default_factory=list)
    expired_permits: list[DOBPermit] = field(default_factory=list)
    critical_flags: list[str] = field(default_factory=list)
    warning_flags: list[str] = field(default_factory=list)
    scan_timestamp: str = ""


# ---------------------------------------------------------------------------
# Fetch DOB Violations
# ---------------------------------------------------------------------------

def get_dob_violations(
    bin_number: Optional[str] = None,
    address: Optional[str] = None,
    boro: Optional[str] = None,
    include_ecb: bool = True,
    limit: int = 200,
) -> list[DOBViolation]:
    """
    Fetch open DOB violations for a property.

    Args:
        bin_number: Building Identification Number (7-digit string)
        address:    Street address (fallback if no BIN)
        boro:       Borough name or number (1–5)
        include_ecb: Also fetch ECB violations
        limit:      Max records

    Returns:
        List of DOBViolation objects sorted by issue_date descending.
    """
    if not bin_number and not address:
        raise ValueError("Must provide bin_number or address")

    # Build filter
    filters = []
    if bin_number:
        filters.append(f"bin__ = '{bin_number.strip()}'")
    elif address:
        house, *rest = address.upper().split()
        street_part = " ".join(rest).split(",")[0].strip() if rest else ""
        filters.append(f"house_no = '{house}'")
        if street_part:
            filters.append(f"street LIKE '%{street_part}%'")
        if boro:
            filters.append(f"boro = '{_boro_num(boro)}'")

    # Only open violations (no disposition date or not dismissed)
    filters.append("disposition_date IS NULL")

    where = " AND ".join(filters)
    params = {
        "$where": where,
        "$order": "issue_date DESC",
        "$limit": limit,
    }

    logger.info(f"Fetching DOB violations — BIN: {bin_number}, Address: {address}")

    violations: list[DOBViolation] = []
    try:
        resp = _session.get(DOB_VIOLATIONS_URL, params=params, timeout=15)
        resp.raise_for_status()
        for row in resp.json():
            try:
                v = _parse_dob_violation(row)
                violations.append(v)
            except Exception as e:
                logger.debug(f"Skipping DOB row: {e}")
    except requests.RequestException as e:
        logger.error(f"DOB violations API failed: {e}")

    if include_ecb and bin_number:
        ecb = _fetch_ecb_violations(bin_number, limit=50)
        violations.extend(ecb)

    # Check for Stop Work Orders
    for v in violations:
        if "stop work" in (v.violation_type or "").lower() or \
           "stop work" in (v.description or "").lower() or \
           v.violation_type_code.upper() in ("SWO", "SW"):
            v.is_stop_work_order = True
            v.severity = "STOP_WORK"

    # Compute days open
    today = date.today()
    for v in violations:
        if v.issue_date:
            try:
                issued = date.fromisoformat(v.issue_date[:10])
                v.days_open = (today - issued).days
            except ValueError:
                pass

    # Assign recommended actions
    for v in violations:
        v.recommended_action = _get_dob_action(v)

    # Sort: SWO first, then ECB, then standard
    severity_order = {"STOP_WORK": 0, "ECB": 1, "STANDARD": 2}
    violations.sort(key=lambda v: severity_order.get(v.severity, 9))

    logger.info(
        f"Found {len(violations)} DOB violations "
        f"({sum(1 for v in violations if v.is_stop_work_order)} SWOs, "
        f"{sum(1 for v in violations if v.is_ecb)} ECB)"
    )
    return violations


def _fetch_ecb_violations(bin_number: str, limit: int = 50) -> list[DOBViolation]:
    """Fetch ECB (Environmental Control Board) violations by BIN."""
    params = {
        "$where": f"bin = '{bin_number}' AND hearing_status NOT IN ('DISMISSED','RESOLVED')",
        "$order": "issue_date DESC",
        "$limit": limit,
    }
    try:
        resp = _session.get(ECB_VIOLATIONS_URL, params=params, timeout=15)
        resp.raise_for_status()
        ecb_list = []
        for row in resp.json():
            v = DOBViolation(
                isn_dob_bis_viol=row.get("ecb_violation_number", ""),
                boro=row.get("boro", ""),
                bin=bin_number,
                block=row.get("block"),
                lot=row.get("lot"),
                issue_date=row.get("issue_date", "")[:10] if row.get("issue_date") else None,
                violation_type_code="ECB",
                violation_category="ECB",
                violation_type=row.get("violation_type", "ECB Violation"),
                respondent_name=row.get("respondent_name"),
                description=row.get("severity", "") + " " + row.get("violation_description", ""),
                disposition_date=None,
                disposition_comments=None,
                device_number=None,
                ecb_number=row.get("ecb_violation_number"),
                is_ecb=True,
                severity="ECB",
            )
            ecb_list.append(v)
        return ecb_list
    except requests.RequestException as e:
        logger.warning(f"ECB fetch failed for BIN {bin_number}: {e}")
        return []


def _parse_dob_violation(row: dict) -> DOBViolation:
    vtype_code = str(row.get("violation_type_code", row.get("violation_type", "")))
    description = row.get("violation_ordinance", row.get("description", ""))

    v = DOBViolation(
        isn_dob_bis_viol=str(row.get("isn_dob_bis_viol", row.get("isndob_bisviol", ""))),
        boro=str(row.get("boro", "")),
        bin=str(row.get("bin__", row.get("bin", ""))),
        block=row.get("block"),
        lot=row.get("lot"),
        issue_date=_date_str(row.get("issue_date")),
        violation_type_code=vtype_code,
        violation_category=row.get("violation_category", "DOB"),
        violation_type=row.get("violation_type", vtype_code),
        respondent_name=row.get("respondent_name"),
        description=description,
        disposition_date=_date_str(row.get("disposition_date")),
        disposition_comments=row.get("disposition_comments"),
        device_number=row.get("device_number"),
        ecb_number=row.get("ecb_number"),
        is_ecb=bool(row.get("ecb_number")),
        severity="ECB" if row.get("ecb_number") else "STANDARD",
    )
    return v


# ---------------------------------------------------------------------------
# Fetch Permits
# ---------------------------------------------------------------------------

def get_active_permits(
    bin_number: str,
    warn_expiring_days: int = 30,
) -> list[DOBPermit]:
    """
    Fetch active construction permits for a building by BIN.

    Args:
        bin_number:          7-digit BIN
        warn_expiring_days:  Flag permits expiring within this many days.

    Returns:
        List of DOBPermit objects. Expired permits marked with is_expired=True.
    """
    params = {
        "$where": f"bin__ = '{bin_number}' AND job_status NOT IN ('X','3')",
        "$order": "filing_date DESC",
        "$limit": 100,
        "$select": (
            "job__,bin__,bbl,house__,street_name,permit_type,"
            "permit_subtype,filing_date,issuance_date,expiration_date,"
            "job_type,work_type,applicant_s_first_name,applicant_s_last_name,"
            "owner_s_first_name,owner_s_last_name,job_status_descrp"
        ),
    }

    logger.info(f"Fetching DOB permits for BIN {bin_number}")

    permits: list[DOBPermit] = []
    today = date.today()
    warn_threshold = today + timedelta(days=warn_expiring_days)

    try:
        resp = _session.get(DOB_PERMITS_URL, params=params, timeout=15)
        resp.raise_for_status()
        for row in resp.json():
            try:
                exp_str = _date_str(row.get("expiration_date"))
                exp_date = date.fromisoformat(exp_str) if exp_str else None
                is_expired = (exp_date < today) if exp_date else False
                days_until = (exp_date - today).days if exp_date else None

                if is_expired:
                    flag = "EXPIRED"
                elif exp_date and exp_date <= warn_threshold:
                    flag = "EXPIRING_SOON"
                else:
                    flag = "OK"

                applicant = " ".join(filter(None, [
                    row.get("applicant_s_first_name", ""),
                    row.get("applicant_s_last_name", ""),
                ])) or None
                owner = " ".join(filter(None, [
                    row.get("owner_s_first_name", ""),
                    row.get("owner_s_last_name", ""),
                ])) or None

                permit = DOBPermit(
                    permit_number=str(row.get("job__", "")),
                    bin=str(row.get("bin__", "")),
                    bbl=row.get("bbl"),
                    address=f"{row.get('house__', '')} {row.get('street_name', '')}".strip(),
                    permit_type=row.get("permit_type", ""),
                    permit_subtype=row.get("permit_subtype"),
                    filing_date=_date_str(row.get("filing_date")),
                    issuance_date=_date_str(row.get("issuance_date")),
                    expiration_date=exp_str,
                    job_type=row.get("job_type"),
                    work_type=row.get("work_type"),
                    applicant_name=applicant,
                    owner_name=owner,
                    status=row.get("job_status_descrp", ""),
                    is_expired=is_expired,
                    days_until_expiration=days_until,
                    flag=flag,
                )
                permits.append(permit)
            except Exception as e:
                logger.debug(f"Skipping permit row: {e}")
    except requests.RequestException as e:
        logger.error(f"DOB permits API failed for BIN {bin_number}: {e}")

    logger.info(
        f"Found {len(permits)} permits for BIN {bin_number} "
        f"({sum(1 for p in permits if p.is_expired)} expired, "
        f"{sum(1 for p in permits if p.flag == 'EXPIRING_SOON')} expiring soon)"
    )
    return permits


# ---------------------------------------------------------------------------
# Full building summary
# ---------------------------------------------------------------------------

def get_building_dob_summary(
    bin_number: Optional[str] = None,
    address: Optional[str] = None,
) -> DOBSummary:
    """
    Generate a complete DOB compliance summary for a building.

    Returns DOBSummary with violations, permits, and critical/warning flags.
    """
    violations = get_dob_violations(bin_number=bin_number, address=address, include_ecb=True)
    permits = get_active_permits(bin_number) if bin_number else []

    display_address = address or f"BIN {bin_number}"
    summary = DOBSummary(
        bin=bin_number,
        address=display_address,
        open_violations=[v for v in violations if not v.is_ecb and not v.is_stop_work_order],
        ecb_violations=[v for v in violations if v.is_ecb],
        stop_work_orders=[v for v in violations if v.is_stop_work_order],
        active_permits=[p for p in permits if not p.is_expired],
        expired_permits=[p for p in permits if p.is_expired],
        scan_timestamp=datetime.utcnow().isoformat() + "Z",
    )

    # Build flags
    if summary.stop_work_orders:
        summary.critical_flags.append(
            f"STOP WORK ORDER: {len(summary.stop_work_orders)} SWO(s) on file. "
            "Construction must cease. Engage DOB expeditor immediately."
        )
    if summary.ecb_violations:
        summary.critical_flags.append(
            f"ECB VIOLATIONS: {len(summary.ecb_violations)} open ECB violation(s). "
            "Fines accumulating daily. Schedule hearing response."
        )
    if summary.expired_permits:
        summary.warning_flags.append(
            f"EXPIRED PERMITS: {len(summary.expired_permits)} permit(s) expired. "
            "Renew immediately to avoid DOB action."
        )
    expiring = [p for p in summary.active_permits if p.flag == "EXPIRING_SOON"]
    if expiring:
        summary.warning_flags.append(
            f"PERMITS EXPIRING SOON: {len(expiring)} permit(s) expire within 30 days."
        )

    return summary


def format_dob_summary(summary: DOBSummary) -> str:
    """Format DOBSummary as Markdown."""
    lines = [f"## DOB Compliance — {summary.address}", f"*Scanned: {summary.scan_timestamp}*\n"]

    if summary.critical_flags:
        lines.append("### 🚨 CRITICAL FLAGS")
        for f in summary.critical_flags:
            lines.append(f"- **{f}**")
        lines.append("")

    if summary.warning_flags:
        lines.append("### ⚠️ WARNING FLAGS")
        for f in summary.warning_flags:
            lines.append(f"- {f}")
        lines.append("")

    lines.append(f"**Open DOB Violations:** {len(summary.open_violations)}")
    lines.append(f"**ECB Violations:** {len(summary.ecb_violations)}")
    lines.append(f"**Stop Work Orders:** {len(summary.stop_work_orders)}")
    lines.append(f"**Active Permits:** {len(summary.active_permits)}")
    lines.append(f"**Expired Permits:** {len(summary.expired_permits)}\n")

    if summary.open_violations:
        lines.append("### Open DOB Violations")
        lines.append("| Date | Code | Type | Days Open | Action |")
        lines.append("|------|------|------|-----------|--------|")
        for v in summary.open_violations[:10]:
            desc = (v.description or "")[:50]
            lines.append(
                f"| {v.issue_date or '—'} | {v.violation_type_code} | "
                f"{v.violation_type[:30]} | {v.days_open or '—'} | {desc} |"
            )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _boro_num(borough: str) -> str:
    nums = {"manhattan": "1", "bronx": "2", "brooklyn": "3", "queens": "4", "staten island": "5"}
    return nums.get(borough.lower(), borough)


def _date_str(val) -> Optional[str]:
    if not val:
        return None
    s = str(val)
    return s[:10] if len(s) >= 10 else s


def _get_dob_action(v: DOBViolation) -> str:
    if v.is_stop_work_order:
        return (
            "STOP WORK ORDER: All construction must cease immediately. "
            "Engage a DOB expeditor/code compliance attorney. "
            "File a post-approval amendment or resolve the underlying issue. "
            "Do not resume work until SWO is lifted by DOB."
        )
    if v.is_ecb:
        return (
            "ECB VIOLATION: Request a hearing date or negotiate default cure. "
            "Fines accrue daily on default. Retain ECB representative. "
            "Correct underlying condition before hearing."
        )
    return (
        "Review violation details. Retain licensed contractor to correct condition. "
        "File certificate of correction with DOB. Consider hiring DOB expeditor."
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    bin_arg = sys.argv[1] if len(sys.argv) > 1 else None
    addr_arg = sys.argv[2] if len(sys.argv) > 2 else None
    summary = get_building_dob_summary(bin_number=bin_arg, address=addr_arg)
    print(format_dob_summary(summary))
