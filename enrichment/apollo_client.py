"""
enrichment/apollo_client.py
-----------------------------
Apollo.io API client for Scout Bot contact enrichment.

Provides:
- ``search_people(company_name, domain)`` — search for contacts at a company
- ``enrich_contact(email)`` — enrich a single contact by email address

API documentation: https://apolloio.github.io/apollo-api-docs/

Authentication:
    Set the APOLLO_API_KEY environment variable.

Rate limiting:
    Apollo's free tier allows ~50 requests/minute. This client enforces
    1 request/second by default (60 req/min) with configurable throttle.

Error handling:
    - 401 Unauthorized → raises ValueError (bad/missing API key)
    - 422 Unprocessable → logs warning and returns empty list
    - 429 Too Many Requests → backs off 60 seconds and retries
    - Network errors → exponential backoff, 3 retries
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

APOLLO_BASE_URL = "https://api.apollo.io/v1"
PEOPLE_SEARCH_ENDPOINT = f"{APOLLO_BASE_URL}/mixed_people/search"
PEOPLE_ENRICH_ENDPOINT = f"{APOLLO_BASE_URL}/people/match"

RATE_LIMIT_DELAY = 1.0    # seconds between requests (1 req/sec)
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0

# Max contacts to return per search (Apollo pagination)
MAX_CONTACTS_PER_SEARCH = 10

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Retrieve Apollo API key from environment.

    Returns:
        API key string.

    Raises:
        ValueError: If APOLLO_API_KEY is not set.
    """
    key = os.environ.get("APOLLO_API_KEY", "").strip()
    if not key:
        raise ValueError(
            "APOLLO_API_KEY environment variable is not set. "
            "Set it before running Scout Bot."
        )
    return key


def _build_headers() -> Dict[str, str]:
    """Build standard Apollo API request headers."""
    return {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "User-Agent": "CamelotOS-ScoutBot/1.0",
    }


def _normalize_contact(raw: Dict[str, Any], source_query: str = "") -> Dict[str, Any]:
    """Normalize a raw Apollo person record to Scout's contact schema.

    Args:
        raw: Raw person dict from Apollo API response.
        source_query: The query that produced this contact (for traceability).

    Returns:
        Standardized contact dict.
    """
    # Phone numbers — Apollo provides an array of phone objects
    phones: List[str] = []
    for phone_obj in raw.get("phone_numbers", []) or []:
        number = (phone_obj.get("sanitized_number") or
                  phone_obj.get("raw_number") or "").strip()
        if number and number not in phones:
            phones.append(number)

    return {
        "name": (raw.get("name") or "").strip(),
        "first_name": (raw.get("first_name") or "").strip(),
        "last_name": (raw.get("last_name") or "").strip(),
        "title": (raw.get("title") or "").strip(),
        "email": (raw.get("email") or "").strip().lower(),
        "phone": phones,
        "linkedin_url": (raw.get("linkedin_url") or "").strip(),
        "company": (raw.get("organization_name") or "").strip(),
        "city": (raw.get("city") or "").strip(),
        "state": (raw.get("state") or "").strip(),
        "country": (raw.get("country") or "").strip(),
        "seniority": (raw.get("seniority") or "").strip(),
        "departments": raw.get("departments") or [],
        "source": "Apollo.io",
        "source_query": source_query,
        "apollo_id": (raw.get("id") or "").strip(),
    }


# ---------------------------------------------------------------------------
# Core API caller
# ---------------------------------------------------------------------------

def _post_with_retry(
    endpoint: str,
    payload: Dict[str, Any],
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """POST to an Apollo endpoint with exponential-backoff retry.

    Enforces the global rate-limit delay before each request.

    Args:
        endpoint: Full API endpoint URL.
        payload: JSON request body dict.
        api_key: Apollo API key.

    Returns:
        Parsed JSON response dict on success; ``None`` on failure.

    Raises:
        ValueError: On 401 Unauthorized (bad API key).
    """
    # Enforce rate limit
    time.sleep(RATE_LIMIT_DELAY)

    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.post(
                endpoint,
                headers=_build_headers(),
                json={**payload, "api_key": api_key},
                timeout=20,
            )

            if resp.status_code == 401:
                raise ValueError(
                    "Apollo API key is invalid or unauthorized. "
                    "Check the APOLLO_API_KEY environment variable."
                )

            if resp.status_code == 422:
                logger.warning(
                    "[Apollo] 422 Unprocessable for payload %s: %s",
                    payload,
                    resp.text[:200],
                )
                return None

            if resp.status_code == 429:
                wait = 60.0
                logger.warning("[Apollo] Rate-limited. Sleeping %.1fs...", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except ValueError:
            raise
        except requests.exceptions.RequestException as exc:
            logger.warning("[Apollo] Request error attempt %d: %s", attempt, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[Apollo] All retries exhausted for %s", endpoint)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_people(
    company_name: str,
    domain: Optional[str] = None,
    titles: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Search for people (contacts) at a company using Apollo.io.

    Targets decision-maker roles relevant to property management acquisitions:
    - Owner, Principal, President, CEO, COO
    - VP/Director of Property Management
    - Managing Director, Managing Partner

    Args:
        company_name: Name of the company to search.
        domain: Company website domain (e.g. ``"example.com"``). Optional
                but greatly improves result accuracy.
        titles: Optional list of job title filters. Defaults to a broad
                set of property management decision-maker titles.

    Returns:
        List of standardized contact dicts. Empty list if no contacts found
        or on API error.
    """
    if not titles:
        titles = [
            "Owner",
            "Principal",
            "President",
            "CEO",
            "COO",
            "Managing Director",
            "Managing Partner",
            "VP Property Management",
            "Director of Property Management",
            "Vice President",
            "Founder",
        ]

    try:
        api_key = _get_api_key()
    except ValueError as exc:
        logger.error("[Apollo] %s", exc)
        return []

    payload: Dict[str, Any] = {
        "q_organization_name": company_name,
        "page": 1,
        "per_page": MAX_CONTACTS_PER_SEARCH,
        "person_titles": titles,
    }
    if domain:
        payload["q_organization_domains"] = [domain]

    logger.info(
        "[Apollo] Searching people for company=%r domain=%r", company_name, domain
    )

    response = _post_with_retry(PEOPLE_SEARCH_ENDPOINT, payload, api_key)
    if not response:
        return []

    people = response.get("people") or []
    contacts = [_normalize_contact(p, source_query=company_name) for p in people]

    logger.info(
        "[Apollo] search_people(%r) → %d contacts", company_name, len(contacts)
    )
    return contacts


def enrich_contact(email: str) -> Optional[Dict[str, Any]]:
    """Enrich a single contact record by email address.

    Args:
        email: Email address to look up.

    Returns:
        Standardized contact dict if found; ``None`` otherwise.
    """
    if not email or "@" not in email:
        logger.warning("[Apollo] Invalid email for enrichment: %r", email)
        return None

    try:
        api_key = _get_api_key()
    except ValueError as exc:
        logger.error("[Apollo] %s", exc)
        return None

    payload = {
        "email": email,
        "reveal_personal_emails": True,
        "reveal_phone_number": True,
    }

    logger.info("[Apollo] Enriching contact by email: %s", email)

    response = _post_with_retry(PEOPLE_ENRICH_ENDPOINT, payload, api_key)
    if not response:
        return None

    person = response.get("person")
    if not person:
        logger.info("[Apollo] No match found for email: %s", email)
        return None

    contact = _normalize_contact(person, source_query=email)
    logger.info("[Apollo] Enriched contact: %s <%s>", contact.get("name"), email)
    return contact


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Quick smoke test — replace with a real company name
    test_contacts = search_people("Example Property Management LLC")
    for c in test_contacts:
        print(f"  {c['name']} | {c['title']} | {c['email']}")
