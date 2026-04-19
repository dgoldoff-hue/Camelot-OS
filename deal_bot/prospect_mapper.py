"""
prospect_mapper.py — Camelot OS Deal Bot
=========================================
Research and profile target property management companies as acquisition
prospects for the Camelot Roll-Up program.

Data sources:
  - NYC HPD Registrations (Socrata) — building ownership, unit counts
  - NYC ACRIS Sales (Socrata) — recent transaction history
  - HCR Rent Stabilization Registrations (Socrata) — rent-stab portfolio
  - Google Places API — business address, phone, website
  - Prospeo API — decision-maker email discovery

Output: Structured ProspectProfile (JSON-serializable) for HubSpot CRM import.

Author: Camelot OS
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("deal_bot.prospect_mapper")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NYC_OPEN_DATA_TOKEN: str = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
PROSPEO_API_KEY: str = os.getenv(
    "PROSPEO_API_KEY",
    "pk_6f97626856799b713a4ffb240921cf40e447e3109380105462ced244f53cd771",
)
GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")

HPD_REG_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.json"
ACRIS_SALES_URL = "https://data.cityofnewyork.us/resource/usep-8jbt.json"
HCR_RS_URL = "https://data.cityofnewyork.us/resource/qb38-trtu.json"

# Prospect score weight configuration
SCORE_WEIGHTS = {
    "unit_count": 0.30,       # larger portfolio = better fit
    "geography": 0.20,        # in target markets
    "pain_indicators": 0.25,  # violations, manual signals
    "relationship_warmth": 0.15,
    "ownership_tenure": 0.10, # longer tenure = more likely to consider exit
}

TARGET_GEOGRAPHIES = frozenset([
    "new york", "ny", "nyc", "brooklyn", "queens", "bronx", "manhattan",
    "staten island", "westchester", "connecticut", "ct", "new jersey",
    "nj", "florida", "fl",
])

OUTREACH_ANGLES = ["succession", "growth", "systems-upgrade", "tired-operator"]
DEAL_STRUCTURES = ["equity-sale", "roll-up", "powered-by"]


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.2,
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
class ContactPerson:
    """A decision-maker at the target company."""

    name: str
    title: str = ""
    email: str = ""
    phone: str = ""
    linkedin_url: str = ""
    email_confidence: float = 0.0   # 0.0–1.0 from Prospeo


@dataclass
class ProspectProfile:
    """Complete research profile for an acquisition prospect."""

    # Identity
    company_name: str
    website: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    phone: str = ""

    # Portfolio characteristics
    estimated_units: int = 0
    property_count: int = 0
    geographies_served: list[str] = field(default_factory=list)
    has_rent_stabilized: bool = False
    rs_unit_count: int = 0

    # Ownership/operator details
    owner_name: str = ""
    years_in_business: Optional[int] = None
    founded_year: Optional[int] = None

    # Contacts
    contacts: list[ContactPerson] = field(default_factory=list)

    # Scoring
    fit_score: float = 0.0                 # 0.0–100.0
    recommended_angle: str = "growth"      # one of OUTREACH_ANGLES
    recommended_structure: str = "roll-up" # one of DEAL_STRUCTURES
    pain_points: list[str] = field(default_factory=list)

    # Metadata
    data_sources: list[str] = field(default_factory=list)
    researched_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    hpd_building_ids: list[str] = field(default_factory=list)
    open_violation_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["contacts"] = [asdict(c) for c in self.contacts]
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# NYC Open Data helpers
# ---------------------------------------------------------------------------

def _socrata_headers() -> dict[str, str]:
    h: dict[str, str] = {"Accept": "application/json"}
    if NYC_OPEN_DATA_TOKEN:
        h["X-App-Token"] = NYC_OPEN_DATA_TOKEN
    return h


def search_hpd_by_owner(owner_name: str, limit: int = 200) -> list[dict[str, Any]]:
    """
    Search HPD Registration records by registration contact name.
    Returns list of building registration records.
    """
    # HPD registration has owner/agent fields — search multiple columns
    name_upper = owner_name.upper()
    query = (
        f"upper(ownerfirstname) LIKE '%{name_upper.split()[0]}%' "
        f"OR upper(ownerlastname) LIKE '%{name_upper.split()[-1]}%' "
        f"OR upper(corporationname) LIKE '%{name_upper}%'"
    )
    params = {
        "$where": query,
        "$limit": limit,
        "$select": (
            "registrationid,buildingid,boro,block,lot,buildingaddress,"
            "zipcode,managementprogram,unitcount,ownerfirstname,"
            "ownerlastname,corporationname,registrationenddate"
        ),
    }
    try:
        resp = SESSION.get(
            HPD_REG_URL, headers=_socrata_headers(), params=params, timeout=20
        )
        resp.raise_for_status()
        results = resp.json()
        logger.info("HPD search for '%s' returned %d records", owner_name, len(results))
        return results
    except requests.RequestException as exc:
        logger.error("HPD search failed: %s", exc)
        return []


def search_hpd_by_company(company_name: str, limit: int = 200) -> list[dict[str, Any]]:
    """Search HPD registrations by management company / corporation name."""
    name_upper = company_name.upper()
    # Remove common suffixes for broader matching
    clean = re.sub(r"\b(LLC|INC|CORP|CO|LTD|LP|MGMT|MANAGEMENT|REALTY|PROPERTIES)\b", "", name_upper).strip()
    params = {
        "$where": f"upper(corporationname) LIKE '%{clean[:30]}%'",
        "$limit": limit,
        "$select": (
            "registrationid,buildingid,boro,block,lot,buildingaddress,"
            "zipcode,unitcount,ownerfirstname,ownerlastname,"
            "corporationname,registrationenddate"
        ),
    }
    try:
        resp = SESSION.get(
            HPD_REG_URL, headers=_socrata_headers(), params=params, timeout=20
        )
        resp.raise_for_status()
        results = resp.json()
        logger.info("HPD company search for '%s' returned %d records", company_name, len(results))
        return results
    except requests.RequestException as exc:
        logger.error("HPD company search failed: %s", exc)
        return []


def get_rent_stab_portfolio(owner_name: str) -> tuple[bool, int]:
    """
    Check HCR rent stabilization registrations for owner.
    Returns (has_rs_units, total_rs_units).
    """
    name_upper = owner_name.upper().split()[0]  # first word match
    params = {
        "$where": f"upper(ownername) LIKE '%{name_upper}%'",
        "$select": "ownername,bbl,unitcount",
        "$limit": 500,
    }
    try:
        resp = SESSION.get(
            HCR_RS_URL, headers=_socrata_headers(), params=params, timeout=20
        )
        resp.raise_for_status()
        rows = resp.json()
        if not rows:
            return False, 0
        total = sum(int(r.get("unitcount", 0)) for r in rows if r.get("unitcount"))
        logger.info("HCR RS: found %d units for '%s'", total, owner_name)
        return bool(total > 0), total
    except requests.RequestException as exc:
        logger.warning("HCR RS lookup failed: %s", exc)
        return False, 0


def count_open_violations_for_buildings(building_ids: list[str]) -> int:
    """Count open HPD violations for a set of building IDs."""
    if not building_ids:
        return 0

    id_list = ", ".join(f"'{bid}'" for bid in building_ids[:50])  # cap at 50
    params = {
        "$where": f"buildingid IN ({id_list}) AND currentstatusid NOT IN (19,21)",
        "$select": "COUNT(*) AS cnt",
    }
    try:
        resp = SESSION.get(
            "https://data.cityofnewyork.us/resource/wvxf-dwi5.json",
            headers=_socrata_headers(),
            params=params,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        return int(data[0].get("cnt", 0)) if data else 0
    except (requests.RequestException, KeyError, IndexError, ValueError) as exc:
        logger.warning("Violation count failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Google Places enrichment
# ---------------------------------------------------------------------------

def google_places_lookup(company_name: str, city: str = "New York") -> dict[str, str]:
    """
    Use Google Places Text Search to enrich company contact info.
    Returns dict with: address, phone, website, formatted_address.
    """
    if not GOOGLE_PLACES_API_KEY:
        logger.debug("GOOGLE_PLACES_API_KEY not set — skipping Places lookup")
        return {}

    query = f"{company_name} property management {city}"
    params = {
        "query": query,
        "key": GOOGLE_PLACES_API_KEY,
        "fields": "formatted_address,international_phone_number,website,name",
    }
    try:
        resp = SESSION.get(
            "https://maps.googleapis.com/maps/api/place/textsearch/json",
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return {}
        top = results[0]
        return {
            "address": top.get("formatted_address", ""),
            "phone": top.get("international_phone_number", ""),
            "website": top.get("website", ""),
            "place_name": top.get("name", ""),
        }
    except requests.RequestException as exc:
        logger.warning("Google Places lookup failed: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# Prospeo email finder
# ---------------------------------------------------------------------------

def find_email_prospeo(
    first_name: str,
    last_name: str,
    company_domain: str,
) -> tuple[str, float]:
    """
    Use Prospeo API to find a professional email address.
    Returns (email, confidence_score).
    confidence_score is 0.0–1.0.
    """
    url = "https://api.prospeo.io/email-finder"
    headers = {
        "Content-Type": "application/json",
        "X-KEY": PROSPEO_API_KEY,
    }
    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "company": company_domain,
    }
    try:
        resp = SESSION.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        email = data.get("response", {}).get("email", "")
        confidence_raw = data.get("response", {}).get("accept_all", False)
        score = 0.85 if email and not confidence_raw else (0.65 if email else 0.0)
        logger.info("Prospeo: found email '%s' (confidence %.2f)", email, score)
        return email, score
    except requests.RequestException as exc:
        logger.warning("Prospeo email lookup failed: %s", exc)
        return "", 0.0


def extract_domain(website: str) -> str:
    """Extract bare domain from URL (e.g. https://acme.com/page → acme.com)."""
    if not website:
        return ""
    website = re.sub(r"^https?://", "", website)
    website = website.split("/")[0].strip()
    return website


# ---------------------------------------------------------------------------
# Scoring engine
# ---------------------------------------------------------------------------

def score_prospect(profile: ProspectProfile) -> float:
    """
    Compute a 0–100 fit score for a prospect.

    Dimensions:
      - unit_count (30%): 100+ units = 100, 50–99 = 70, 20–49 = 40, <20 = 10
      - geography (20%): in target markets = 100, else 0
      - pain_indicators (25%): violations, manual process signals
      - relationship_warmth (15%): not computable from public data → default 50
      - ownership_tenure (10%): 10+ years = 100, 5–9 = 60, <5 = 20
    """
    scores: dict[str, float] = {}

    # Unit count
    u = profile.estimated_units
    if u >= 100:
        scores["unit_count"] = 100.0
    elif u >= 50:
        scores["unit_count"] = 70.0
    elif u >= 20:
        scores["unit_count"] = 40.0
    else:
        scores["unit_count"] = 10.0

    # Geography
    geo_lower = " ".join(profile.geographies_served).lower()
    city_lower = (profile.city or "").lower()
    state_lower = (profile.state or "").lower()
    combined = f"{geo_lower} {city_lower} {state_lower}"
    scores["geography"] = 100.0 if any(g in combined for g in TARGET_GEOGRAPHIES) else 0.0

    # Pain indicators (violations, no website, large portfolio with violations)
    pain_score = 50.0  # baseline
    if profile.open_violation_count > 20:
        pain_score = min(100.0, pain_score + 30.0)
    elif profile.open_violation_count > 5:
        pain_score = min(100.0, pain_score + 15.0)
    if not profile.website:
        pain_score = min(100.0, pain_score + 10.0)
    if profile.has_rent_stabilized:
        pain_score = min(100.0, pain_score + 10.0)  # RS compliance burden
    scores["pain_indicators"] = pain_score

    # Relationship warmth — cannot compute without CRM data; default 50
    scores["relationship_warmth"] = 50.0

    # Ownership tenure
    tenure = profile.years_in_business or 0
    if tenure >= 10:
        scores["ownership_tenure"] = 100.0
    elif tenure >= 5:
        scores["ownership_tenure"] = 60.0
    else:
        scores["ownership_tenure"] = 20.0

    # Weighted sum
    total = sum(scores[dim] * w for dim, w in SCORE_WEIGHTS.items())
    logger.debug("Score breakdown for %s: %s → %.1f", profile.company_name, scores, total)
    return round(total, 1)


def recommend_angle(profile: ProspectProfile) -> str:
    """
    Determine the best outreach angle based on prospect characteristics.
    """
    tenure = profile.years_in_business or 0
    violation_heavy = profile.open_violation_count > 10
    large_portfolio = profile.estimated_units >= 100
    no_tech = not profile.website

    if tenure >= 15:
        return "succession"
    if violation_heavy or no_tech:
        return "systems-upgrade"
    if large_portfolio and tenure < 10:
        return "growth"
    return "tired-operator"


def recommend_structure(profile: ProspectProfile) -> str:
    """Recommend deal structure based on portfolio size and pain profile."""
    if profile.estimated_units >= 200:
        return "equity-sale"
    if profile.estimated_units >= 50:
        return "roll-up"
    return "powered-by"


def identify_pain_points(profile: ProspectProfile) -> list[str]:
    """Derive likely pain points from public data signals."""
    pain: list[str] = []
    if profile.open_violation_count > 10:
        pain.append(f"High violation burden ({profile.open_violation_count} open violations)")
    if profile.has_rent_stabilized:
        pain.append("Rent-stabilized portfolio — complex compliance overhead")
    if not profile.website:
        pain.append("No apparent web presence — possible technology gap")
    if (profile.years_in_business or 0) >= 15:
        pain.append("Long-tenured operation — potential succession / exit planning need")
    if profile.estimated_units > 100 and not profile.contacts:
        pain.append("Large portfolio managed without apparent team depth")
    if not pain:
        pain.append("Operational efficiency opportunities through technology")
    return pain


# ---------------------------------------------------------------------------
# Main prospect research orchestrator
# ---------------------------------------------------------------------------

class ProspectMapper:
    """
    Orchestrates research of a target property management company.
    Accepts either a company name or owner name and builds a full ProspectProfile.
    """

    def __init__(self) -> None:
        self.session = SESSION

    def research_by_company(
        self,
        company_name: str,
        city: str = "New York",
        enrich_email: bool = True,
    ) -> ProspectProfile:
        """
        Full research pipeline for a target company name.

        Args:
            company_name:  Company to research.
            city:          City hint for Google Places lookup.
            enrich_email:  Whether to call Prospeo for email discovery.

        Returns:
            Populated ProspectProfile.
        """
        logger.info("Researching prospect: %s", company_name)
        profile = ProspectProfile(company_name=company_name)

        # ── 1. HPD Registrations ───────────────────────────────────────────
        hpd_records = search_hpd_by_company(company_name)
        if hpd_records:
            profile.data_sources.append("HPD Registrations")
            self._process_hpd_records(profile, hpd_records)

        # ── 2. Rent stabilization ──────────────────────────────────────────
        has_rs, rs_units = get_rent_stab_portfolio(company_name)
        profile.has_rent_stabilized = has_rs
        profile.rs_unit_count = rs_units
        if rs_units > 0:
            profile.data_sources.append("HCR Rent Stabilization Registry")

        # ── 3. Violation count ─────────────────────────────────────────────
        if profile.hpd_building_ids:
            profile.open_violation_count = count_open_violations_for_buildings(
                profile.hpd_building_ids
            )

        # ── 4. Google Places enrichment ────────────────────────────────────
        places = google_places_lookup(company_name, city)
        if places:
            profile.data_sources.append("Google Places")
            if places.get("address") and not profile.address:
                profile.address = places["address"]
            if places.get("phone") and not profile.phone:
                profile.phone = places["phone"]
            if places.get("website") and not profile.website:
                profile.website = places["website"]

        # ── 5. Email discovery ─────────────────────────────────────────────
        if enrich_email and profile.owner_name and profile.website:
            domain = extract_domain(profile.website)
            parts = profile.owner_name.strip().split()
            if len(parts) >= 2:
                email, confidence = find_email_prospeo(parts[0], parts[-1], domain)
                if email:
                    profile.contacts.append(
                        ContactPerson(
                            name=profile.owner_name,
                            email=email,
                            email_confidence=confidence,
                        )
                    )
                    profile.data_sources.append("Prospeo Email Finder")
                    time.sleep(0.5)  # rate-limit courtesy delay

        # ── 6. Scoring and recommendations ────────────────────────────────
        profile.fit_score = score_prospect(profile)
        profile.recommended_angle = recommend_angle(profile)
        profile.recommended_structure = recommend_structure(profile)
        profile.pain_points = identify_pain_points(profile)

        logger.info(
            "Research complete for %s — fit score: %.1f, angle: %s, structure: %s",
            company_name,
            profile.fit_score,
            profile.recommended_angle,
            profile.recommended_structure,
        )
        return profile

    def research_by_owner(
        self,
        owner_name: str,
        city: str = "New York",
        enrich_email: bool = True,
    ) -> ProspectProfile:
        """
        Research a prospect by owner/individual name (when company name is unknown).
        """
        logger.info("Researching prospect by owner: %s", owner_name)
        profile = ProspectProfile(
            company_name=f"{owner_name} (Owner-Managed)",
            owner_name=owner_name,
        )

        # HPD by owner name
        hpd_records = search_hpd_by_owner(owner_name)
        if hpd_records:
            profile.data_sources.append("HPD Registrations")
            self._process_hpd_records(profile, hpd_records)

        # Derive company name from corporation name if available
        corp_names: dict[str, int] = {}
        for r in hpd_records:
            corp = r.get("corporationname", "").strip()
            if corp:
                corp_names[corp] = corp_names.get(corp, 0) + 1
        if corp_names:
            top_corp = max(corp_names, key=lambda k: corp_names[k])
            profile.company_name = top_corp

        # RS lookup
        has_rs, rs_units = get_rent_stab_portfolio(owner_name)
        profile.has_rent_stabilized = has_rs
        profile.rs_unit_count = rs_units

        # Violations
        if profile.hpd_building_ids:
            profile.open_violation_count = count_open_violations_for_buildings(
                profile.hpd_building_ids
            )

        # Places
        places = google_places_lookup(profile.company_name, city)
        if places:
            profile.data_sources.append("Google Places")
            if places.get("website"):
                profile.website = places["website"]
            if places.get("phone"):
                profile.phone = places["phone"]
            if places.get("address"):
                profile.address = places["address"]

        # Email
        if enrich_email and profile.website:
            domain = extract_domain(profile.website)
            parts = owner_name.strip().split()
            if len(parts) >= 2:
                email, confidence = find_email_prospeo(parts[0], parts[-1], domain)
                if email:
                    profile.contacts.append(
                        ContactPerson(
                            name=owner_name,
                            email=email,
                            email_confidence=confidence,
                        )
                    )
                    profile.data_sources.append("Prospeo Email Finder")
                    time.sleep(0.5)

        profile.fit_score = score_prospect(profile)
        profile.recommended_angle = recommend_angle(profile)
        profile.recommended_structure = recommend_structure(profile)
        profile.pain_points = identify_pain_points(profile)

        return profile

    def _process_hpd_records(
        self, profile: ProspectProfile, records: list[dict[str, Any]]
    ) -> None:
        """Extract portfolio stats from HPD registration records."""
        total_units = 0
        building_ids: list[str] = []
        boroughs: set[str] = set()

        # Map HPD boro codes to names
        boro_map = {
            "1": "Manhattan", "2": "Bronx", "3": "Brooklyn",
            "4": "Queens", "5": "Staten Island",
        }

        for rec in records:
            # Skip expired registrations
            end_date_str = rec.get("registrationenddate", "")
            if end_date_str:
                try:
                    end_date = date.fromisoformat(end_date_str[:10])
                    if end_date < date.today():
                        continue
                except ValueError:
                    pass

            bid = rec.get("buildingid", "")
            if bid and bid not in building_ids:
                building_ids.append(bid)

            try:
                total_units += int(rec.get("unitcount", 0))
            except (TypeError, ValueError):
                pass

            boro_code = str(rec.get("boro", ""))
            if boro_code in boro_map:
                boroughs.add(boro_map[boro_code])

            # Extract owner name if not set
            if not profile.owner_name:
                first = rec.get("ownerfirstname", "").strip()
                last = rec.get("ownerlastname", "").strip()
                if first or last:
                    profile.owner_name = f"{first} {last}".strip()

        profile.hpd_building_ids = building_ids
        profile.property_count = len(building_ids)
        profile.estimated_units = max(profile.estimated_units, total_units)
        profile.geographies_served = list(boroughs)
        if boroughs:
            profile.city = list(boroughs)[0]
            profile.state = "NY"

        logger.debug(
            "HPD: %d buildings, %d units, boroughs: %s",
            len(building_ids),
            total_units,
            boroughs,
        )


# ---------------------------------------------------------------------------
# Batch prospecting
# ---------------------------------------------------------------------------

def batch_research(
    targets: list[dict[str, str]],
    output_path: Optional[str] = None,
    enrich_email: bool = True,
) -> list[ProspectProfile]:
    """
    Research multiple targets in sequence.

    Args:
        targets: List of dicts with keys 'company_name' or 'owner_name',
                 optionally 'city'.
        output_path: If set, write JSON results to this file.
        enrich_email: Whether to call Prospeo for each target.

    Returns:
        List of ProspectProfile objects, sorted by fit_score descending.
    """
    mapper = ProspectMapper()
    profiles: list[ProspectProfile] = []

    for i, target in enumerate(targets, 1):
        company = target.get("company_name", "").strip()
        owner = target.get("owner_name", "").strip()
        city = target.get("city", "New York")

        logger.info("Batch research [%d/%d]: %s", i, len(targets), company or owner)

        try:
            if company:
                profile = mapper.research_by_company(company, city=city, enrich_email=enrich_email)
            elif owner:
                profile = mapper.research_by_owner(owner, city=city, enrich_email=enrich_email)
            else:
                logger.warning("Target %d missing both company_name and owner_name — skipping", i)
                continue

            profiles.append(profile)
        except Exception as exc:
            logger.error("Research failed for target %d (%s): %s", i, company or owner, exc)

        # Courtesy delay between targets to avoid rate-limiting
        if i < len(targets):
            time.sleep(1.0)

    # Sort by fit score descending
    profiles.sort(key=lambda p: p.fit_score, reverse=True)

    if output_path:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump([p.to_dict() for p in profiles], f, indent=2, default=str)
        logger.info("Batch results written to %s", output_path)

    return profiles


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Camelot Deal Bot — Prospect Mapper")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--company", help="Company name to research")
    group.add_argument("--owner", help="Owner name to research")
    group.add_argument("--batch", help="Path to JSON file with list of targets")

    parser.add_argument("--city", default="New York", help="City hint for Places lookup")
    parser.add_argument("--no-email", action="store_true", help="Skip Prospeo email lookup")
    parser.add_argument("--output", help="Output JSON file path")
    args = parser.parse_args()

    mapper = ProspectMapper()

    if args.batch:
        with open(args.batch) as f:
            targets = json.load(f)
        profiles = batch_research(
            targets,
            output_path=args.output,
            enrich_email=not args.no_email,
        )
        for p in profiles:
            print(f"  [{p.fit_score:5.1f}] {p.company_name:40s} {p.estimated_units:4d} units  {p.recommended_angle}")
    elif args.company:
        p = mapper.research_by_company(args.company, city=args.city, enrich_email=not args.no_email)
        print(p.to_json())
        if args.output:
            with open(args.output, "w") as f:
                f.write(p.to_json())
    else:
        p = mapper.research_by_owner(args.owner, city=args.city, enrich_email=not args.no_email)
        print(p.to_json())
        if args.output:
            with open(args.output, "w") as f:
                f.write(p.to_json())
