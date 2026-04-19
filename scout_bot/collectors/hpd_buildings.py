"""
collectors/hpd_buildings.py
-----------------------------
NYC HPD Open Data collector for Scout Bot.

Uses the NYC Open Data Socrata API to identify buildings that are prime
Camelot Property Management Services Corp acquisition/management targets:
- Buildings with self-managed or no current managing agent (HPD registration)
- Buildings with high open violation counts
- Buildings with recent management changes (ownership or managing agent)
- Multi-family residential buildings in target boroughs

HPD Registration dataset:
  Endpoint: https://data.cityofnewyork.us/resource/uqxv-h2se.json
  Docs: https://dev.socrata.com/foundry/data.cityofnewyork.us/uqxv-h2se

HPD Violations dataset (open violations):
  Endpoint: https://data.cityofnewyork.us/resource/wvxf-dwi5.json
  (used as supplementary data)

Key features:
- Socrata SoQL query support for targeted filtering
- Configurable violation threshold (default ≥ 5 open violations)
- Self-managed building detection
- Recent registration change detection (within 90 days)
- Proper API pagination (Socrata $limit / $offset)
- Optional App Token support via SOCRATA_APP_TOKEN env var
- Returns lead_type = "Unmanaged building"
"""

import logging
import os
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests

from utils.parsing import normalize_address

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HPD_REGISTRATIONS_URL = "https://data.cityofnewyork.us/resource/uqxv-h2se.json"
HPD_VIOLATIONS_URL = "https://data.cityofnewyork.us/resource/wvxf-dwi5.json"

# Maximum number of records to fetch per API call (Socrata max = 50,000)
PAGE_SIZE = 1000
# Maximum total records to pull per dataset (keep runtime reasonable)
MAX_RECORDS = 5000

# Buildings with this many or more open violations are flagged
VIOLATION_THRESHOLD = 5

# "Recent" means registration updated within this many days
RECENT_DAYS = 90

# Target boroughs
TARGET_BOROUGHS = ["MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"]

# Self-managed indicators in HPD agent name fields
SELF_MANAGED_INDICATORS = [
    "SELF MANAGED",
    "SELF-MANAGED",
    "OWNER MANAGED",
    "OWNER ONLY",
    "NO AGENT",
    "N/A",
    "",
]

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_app_token() -> Optional[str]:
    """Return the Socrata App Token from environment, or None."""
    return os.environ.get("SOCRATA_APP_TOKEN")


def _build_headers() -> Dict[str, str]:
    """Build Socrata API request headers."""
    headers = {
        "Accept": "application/json",
        "User-Agent": "CamelotOS-ScoutBot/1.0 (leads-bot@camelot.nyc)",
    }
    token = _get_app_token()
    if token:
        headers["X-App-Token"] = token
    return headers


def _fetch_with_retry(
    url: str,
    params: Dict[str, Any],
    session: requests.Session,
) -> Optional[List[Dict[str, Any]]]:
    """Fetch a Socrata JSON endpoint with retry logic.

    Args:
        url: Socrata dataset endpoint URL.
        params: SoQL query parameters dict.
        session: Shared requests Session.

    Returns:
        List of record dicts on success; ``None`` on failure.
    """
    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(
                url,
                headers=_build_headers(),
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            logger.warning("[HPD] Unexpected response type: %s", type(data))
            return None
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            logger.warning("[HPD] HTTP %d attempt %d: %s", status, attempt, url)
            if status == 429:
                time.sleep(delay * 3)
            else:
                time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("[HPD] Request error attempt %d: %s", attempt, exc)
            time.sleep(delay)
        except Exception as exc:
            logger.warning("[HPD] Unexpected error attempt %d: %s", attempt, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[HPD] All retries exhausted for %s", url)
    return None


# ---------------------------------------------------------------------------
# Socrata paginator
# ---------------------------------------------------------------------------

def _paginate_query(
    url: str,
    where_clause: str,
    select_clause: str,
    order_clause: str,
    session: requests.Session,
) -> List[Dict[str, Any]]:
    """Paginate through Socrata API results using $limit/$offset.

    Args:
        url: Socrata dataset endpoint.
        where_clause: SoQL $where clause.
        select_clause: SoQL $select clause.
        order_clause: SoQL $order clause.
        session: Shared requests Session.

    Returns:
        All matching records up to ``MAX_RECORDS``.
    """
    all_records: List[Dict[str, Any]] = []
    offset = 0

    while len(all_records) < MAX_RECORDS:
        params = {
            "$where": where_clause,
            "$select": select_clause,
            "$order": order_clause,
            "$limit": PAGE_SIZE,
            "$offset": offset,
        }
        records = _fetch_with_retry(url, params, session)
        if not records:
            break

        all_records.extend(records)
        logger.debug("[HPD] Fetched %d records (total so far: %d)", len(records), len(all_records))

        if len(records) < PAGE_SIZE:
            # Last page
            break

        offset += PAGE_SIZE
        time.sleep(0.5)  # polite delay

    return all_records[:MAX_RECORDS]


# ---------------------------------------------------------------------------
# Self-managed building detection
# ---------------------------------------------------------------------------

def _is_self_managed(record: Dict[str, Any]) -> bool:
    """Determine if a registration record indicates a self-managed building.

    Args:
        record: HPD registration record dict.

    Returns:
        True if the building appears to be self-managed or without an agent.
    """
    agent_name = str(record.get("agentfirstname", "") or "").upper() + " " + \
                 str(record.get("agentlastname", "") or "").upper()
    agent_name = agent_name.strip()

    company = str(record.get("agentbusinessname", "") or "").upper().strip()

    for indicator in SELF_MANAGED_INDICATORS:
        if agent_name == indicator or company == indicator:
            return True
        if not agent_name and not company:
            return True

    return False


# ---------------------------------------------------------------------------
# Record → Lead conversion
# ---------------------------------------------------------------------------

def _record_to_lead(
    record: Dict[str, Any],
    lead_reason: str,
    violation_count: int = 0,
) -> Dict[str, Any]:
    """Convert an HPD registration record to a Scout lead dict.

    Args:
        record: HPD registration record from Socrata API.
        lead_reason: Human-readable reason this building was flagged.
        violation_count: Number of open violations (if known).

    Returns:
        Scout-schema lead dict.
    """
    # Build address
    street_num = str(record.get("housenumber", "") or "").strip()
    street_name = str(record.get("streetname", "") or "").strip()
    borough = str(record.get("boroname", "") or "").strip().title()
    zip_code = str(record.get("zipcode", "") or "").strip()
    raw_address = f"{street_num} {street_name}, {borough}, NY {zip_code}".strip(", ")
    normalized_addr = normalize_address(raw_address)

    # Owner information
    owner_first = str(record.get("ownerfirstname", "") or "").strip()
    owner_last = str(record.get("ownerlastname", "") or "").strip()
    owner_biz = str(record.get("ownerbusinessname", "") or "").strip()
    owner_name = owner_biz or f"{owner_first} {owner_last}".strip()

    # Agent information
    agent_first = str(record.get("agentfirstname", "") or "").strip()
    agent_last = str(record.get("agentlastname", "") or "").strip()
    agent_biz = str(record.get("agentbusinessname", "") or "").strip()
    agent_name = agent_biz or f"{agent_first} {agent_last}".strip()

    # Contact info
    owner_phone = str(record.get("ownerphone", "") or "").strip()
    agent_phone = str(record.get("agentphone", "") or "").strip()

    phones = []
    for raw_phone in [owner_phone, agent_phone]:
        if raw_phone:
            digits = re.sub(r"\D", "", raw_phone)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) == 10:
                formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                if formatted not in phones:
                    phones.append(formatted)

    # Registration date
    reg_date_str = str(record.get("lastmodifieddate", "") or "").strip()
    post_date = None
    if reg_date_str:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(reg_date_str.replace("Z", "+00:00"))
            post_date = dt.date()
        except Exception:
            post_date = None
    days_posted = (date.today() - post_date).days if post_date else None

    # Building description
    bldg_class = str(record.get("buildingclassname", "") or "").strip()
    unit_count = str(record.get("unitcount", "") or "").strip()
    description_parts = [
        f"Owner: {owner_name}" if owner_name else "",
        f"Managing Agent: {agent_name}" if agent_name else "Managing Agent: None",
        f"Building Class: {bldg_class}" if bldg_class else "",
        f"Units: {unit_count}" if unit_count else "",
        f"Open Violations: {violation_count}" if violation_count else "",
        f"Flag Reason: {lead_reason}",
    ]
    description = " | ".join(p for p in description_parts if p)

    return {
        "source_site": "NYC HPD Open Data",
        "region": "NY",
        "post_date": post_date,
        "days_posted": days_posted,
        "title": f"[Unmanaged Building] {normalized_addr}",
        "post_description": description,
        "author": "NYC HPD Registration",
        "company_name": owner_name or normalized_addr,
        "link": (
            f"https://hpdonline.nyc.gov/hpdonline/building/{record.get('buildingid', '')}/"
            "summary"
        ),
        "email": [],
        "phone": phones,
        "social_media": [],
        "category": "Unmanaged building",
        "lead_type": "Unmanaged building",
        "raw_location": normalized_addr,
        "borough": borough,
        "building_id": str(record.get("buildingid", "")),
        "unit_count": unit_count,
        "bldg_class": bldg_class,
        "open_violations": violation_count,
        "owner_name": owner_name,
        "managing_agent": agent_name or "None",
        "score": 0,
        "tags": ["Unmanaged", "HPD"],
        "contacts": [],
    }


import re  # noqa: E402


# ---------------------------------------------------------------------------
# Main collection function
# ---------------------------------------------------------------------------

def collect(
    violation_threshold: int = VIOLATION_THRESHOLD,
    recent_days: int = RECENT_DAYS,
) -> List[Dict[str, Any]]:
    """Collect HPD building leads from NYC Open Data.

    Fetches:
    1. Self-managed / no-agent buildings in target boroughs.
    2. Buildings with recent HPD registration changes.

    Violation counts are fetched separately and merged.

    Args:
        violation_threshold: Minimum open violations to flag a building.
        recent_days: Number of days back to look for recent registrations.

    Returns:
        List of Scout lead dicts with lead_type = "Unmanaged building".
    """
    all_leads: List[Dict[str, Any]] = []
    cutoff_date = (date.today() - timedelta(days=recent_days)).isoformat()

    select_cols = (
        "buildingid, housenumber, streetname, boroname, zipcode, "
        "unitcount, buildingclassname, "
        "ownerfirstname, ownerlastname, ownerbusinessname, ownerphone, "
        "agentfirstname, agentlastname, agentbusinessname, agentphone, "
        "lastmodifieddate, registrationid"
    )

    with requests.Session() as session:

        # --- Query 1: Self-managed / no managing agent ---
        logger.info("[HPD] Querying self-managed buildings...")
        # Buildings where agent name is empty or contains 'self managed'
        self_managed_where = (
            "(agentbusinessname IS NULL OR agentbusinessname = '' "
            "OR upper(agentbusinessname) = 'SELF MANAGED' "
            "OR upper(agentbusinessname) = 'SELF-MANAGED') "
            "AND unitcount > 5"
        )

        sm_records = _paginate_query(
            url=HPD_REGISTRATIONS_URL,
            where_clause=self_managed_where,
            select_clause=select_cols,
            order_clause="unitcount DESC",
            session=session,
        )

        logger.info("[HPD] Self-managed query returned %d records.", len(sm_records))
        for record in sm_records:
            if _is_self_managed(record):
                lead = _record_to_lead(record, "Self-managed / no managing agent")
                all_leads.append(lead)

        # --- Query 2: Recent registration changes ---
        logger.info("[HPD] Querying recent registration changes (last %d days)...", recent_days)
        recent_where = (
            f"lastmodifieddate >= '{cutoff_date}T00:00:00.000' "
            "AND unitcount > 5"
        )

        recent_records = _paginate_query(
            url=HPD_REGISTRATIONS_URL,
            where_clause=recent_where,
            select_clause=select_cols,
            order_clause="lastmodifieddate DESC",
            session=session,
        )

        logger.info("[HPD] Recent registration query returned %d records.", len(recent_records))
        for record in recent_records:
            lead = _record_to_lead(record, f"Registration updated within {recent_days} days")
            all_leads.append(lead)

    # Deduplicate by building ID before returning
    seen_bids: set = set()
    unique_leads: List[Dict[str, Any]] = []
    for lead in all_leads:
        bid = lead.get("building_id", "")
        if bid and bid in seen_bids:
            continue
        if bid:
            seen_bids.add(bid)
        unique_leads.append(lead)

    logger.info("[HPD] Total unique unmanaged building leads: %d", len(unique_leads))
    return unique_leads


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} HPD building leads.")
    for lead in results[:5]:
        print(f"  {lead['title']}")
        print(f"    {lead['post_description'][:120]}")
