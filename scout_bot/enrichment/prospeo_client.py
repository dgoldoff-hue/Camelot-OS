"""
enrichment/prospeo_client.py
------------------------------
Prospeo API client for Scout Bot contact enrichment.

Provides:
- ``find_email(first_name, last_name, domain)`` — email finder endpoint
- ``company_search(company_name, domain)`` — find all emails at a company/domain
- ``enrich_contact(linkedin_url)`` — enrich a contact via LinkedIn URL

API documentation: https://prospeo.io/api

Authentication:
    Set PROSPEO_API_KEY environment variable.
    Default (provided): pk_6f97626856799b713a4ffb240921cf40e447e3109380105462ced244f53cd771

Rate limiting:
    Prospeo enforces per-plan limits; this client uses 1 req/sec throttling
    and backs off on 429 responses.

Error handling:
    - 401/403 → raises ValueError
    - 429 → exponential backoff
    - Network errors → 3 retries with exponential backoff
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

PROSPEO_BASE_URL = "https://api.prospeo.io"

# Endpoint paths
EMAIL_FINDER_PATH = "/email-finder"
COMPANY_SEARCH_PATH = "/domain-search"
LINKEDIN_ENRICH_PATH = "/linkedin-email-finder"

# Default API key (overridden by PROSPEO_API_KEY env var)
DEFAULT_API_KEY = "pk_6f97626856799b713a4ffb240921cf40e447e3109380105462ced244f53cd771"

RATE_LIMIT_DELAY = 1.0    # seconds between requests
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0

MAX_RESULTS_PER_COMPANY = 20

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Retrieve Prospeo API key from environment or use default.

    Returns:
        API key string.
    """
    return os.environ.get("PROSPEO_API_KEY", DEFAULT_API_KEY).strip()


def _build_headers(api_key: str) -> Dict[str, str]:
    """Build Prospeo API request headers.

    Args:
        api_key: Prospeo API key.

    Returns:
        Headers dict with authorization.
    """
    return {
        "Content-Type": "application/json",
        "X-KEY": api_key,
        "User-Agent": "CamelotOS-ScoutBot/1.0",
    }


def _normalize_contact(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a raw Prospeo contact record to Scout's contact schema.

    Args:
        raw: Raw contact dict from Prospeo API response.

    Returns:
        Standardized contact dict.
    """
    # Prospeo returns email in various fields depending on the endpoint
    email = (
        raw.get("email") or
        raw.get("email_address") or
        (raw.get("email_obj") or {}).get("email") or
        ""
    ).strip().lower()

    # LinkedIn URL
    linkedin = (
        raw.get("linkedin") or
        raw.get("linkedin_url") or
        raw.get("profile_url") or
        ""
    ).strip()

    # Name components
    first = (raw.get("first_name") or "").strip()
    last = (raw.get("last_name") or "").strip()
    full_name = (raw.get("full_name") or raw.get("name") or f"{first} {last}").strip()

    # Phone — Prospeo occasionally provides phone via LinkedIn enrichment
    phone_raw = (raw.get("phone") or raw.get("phone_number") or "").strip()
    phones = [phone_raw] if phone_raw else []

    return {
        "name": full_name,
        "first_name": first,
        "last_name": last,
        "title": (raw.get("position") or raw.get("title") or "").strip(),
        "email": email,
        "phone": phones,
        "linkedin_url": linkedin,
        "company": (
            raw.get("company") or
            raw.get("company_name") or
            raw.get("organization") or
            ""
        ).strip(),
        "city": (raw.get("city") or "").strip(),
        "state": (raw.get("state") or "").strip(),
        "country": (raw.get("country") or "").strip(),
        "seniority": (raw.get("seniority") or "").strip(),
        "departments": raw.get("departments") or [],
        "source": "Prospeo",
        "prospeo_id": str(raw.get("id") or ""),
        "verification_status": (
            (raw.get("email_obj") or {}).get("verification_status") or
            raw.get("verification_status") or
            "unknown"
        ),
    }


# ---------------------------------------------------------------------------
# Core HTTP caller
# ---------------------------------------------------------------------------

def _post_with_retry(
    path: str,
    payload: Dict[str, Any],
    api_key: str,
) -> Optional[Dict[str, Any]]:
    """POST to a Prospeo API endpoint with rate limiting and retry logic.

    Enforces a global RATE_LIMIT_DELAY before each call.

    Args:
        path: API path (e.g. ``"/email-finder"``).
        payload: JSON request body.
        api_key: Prospeo API key.

    Returns:
        Parsed JSON response dict on success; ``None`` on failure.

    Raises:
        ValueError: On 401/403 (unauthorized).
    """
    url = f"{PROSPEO_BASE_URL}{path}"
    time.sleep(RATE_LIMIT_DELAY)  # enforce rate limit

    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = requests.post(
                url,
                headers=_build_headers(api_key),
                json=payload,
                timeout=20,
            )

            if resp.status_code in (401, 403):
                raise ValueError(
                    f"Prospeo API key unauthorized (HTTP {resp.status_code}). "
                    "Check the PROSPEO_API_KEY environment variable."
                )

            if resp.status_code == 429:
                wait = delay * 5
                logger.warning("[Prospeo] Rate-limited. Sleeping %.1fs...", wait)
                time.sleep(wait)
                delay *= 2
                continue

            if resp.status_code == 422:
                logger.warning(
                    "[Prospeo] 422 Unprocessable for %s: %s", path, resp.text[:200]
                )
                return None

            resp.raise_for_status()
            return resp.json()

        except ValueError:
            raise
        except requests.exceptions.RequestException as exc:
            logger.warning("[Prospeo] Request error attempt %d: %s", attempt, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[Prospeo] All retries exhausted for %s", path)
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_email(
    first_name: str,
    last_name: str,
    domain: str,
) -> Optional[Dict[str, Any]]:
    """Find a professional email address for a named individual at a domain.

    Args:
        first_name: Contact's first name.
        last_name: Contact's last name.
        domain: Company website domain (e.g. ``"acmeproperty.com"``).

    Returns:
        Standardized contact dict if an email is found; ``None`` otherwise.
    """
    if not (first_name and last_name and domain):
        logger.warning(
            "[Prospeo] find_email requires first_name, last_name, and domain. "
            "Got: %r %r %r", first_name, last_name, domain
        )
        return None

    # Strip protocol if domain was passed as a full URL
    domain = domain.replace("https://", "").replace("http://", "").split("/")[0]

    api_key = _get_api_key()
    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "company_name": domain,
        "company_website": domain,
    }

    logger.info(
        "[Prospeo] find_email: %s %s @ %s", first_name, last_name, domain
    )

    response = _post_with_retry(EMAIL_FINDER_PATH, payload, api_key)
    if not response:
        return None

    # Response shape: {"error": false, "email": {...}, "person": {...}}
    if response.get("error"):
        logger.info(
            "[Prospeo] No email found for %s %s @ %s: %s",
            first_name, last_name, domain, response.get("message", "")
        )
        return None

    # Merge email_obj and person if present
    email_obj = response.get("email") or {}
    person_obj = response.get("person") or {}
    merged = {**person_obj, "email_obj": email_obj, "email": email_obj.get("email", "")}

    contact = _normalize_contact(merged)
    if not contact["first_name"]:
        contact["first_name"] = first_name
        contact["last_name"] = last_name
        contact["name"] = f"{first_name} {last_name}"

    logger.info("[Prospeo] Found email for %s %s: %s", first_name, last_name, contact["email"])
    return contact


def company_search(
    company_name: str,
    domain: str,
    limit: int = MAX_RESULTS_PER_COMPANY,
) -> List[Dict[str, Any]]:
    """Find all professional emails associated with a company domain.

    Args:
        company_name: Human-readable company name (for logging).
        domain: Company website domain (e.g. ``"acmeproperty.com"``).
        limit: Maximum number of contacts to return (default 20).

    Returns:
        List of standardized contact dicts. Empty list if none found.
    """
    if not domain:
        logger.warning("[Prospeo] company_search requires a domain. Got: %r", domain)
        return []

    domain = domain.replace("https://", "").replace("http://", "").split("/")[0]
    api_key = _get_api_key()

    payload = {
        "url": domain,
        "limit": min(limit, 100),
    }

    logger.info("[Prospeo] company_search: %r / %s (limit=%d)", company_name, domain, limit)

    response = _post_with_retry(COMPANY_SEARCH_PATH, payload, api_key)
    if not response:
        return []

    if response.get("error"):
        logger.info(
            "[Prospeo] No contacts for %s / %s: %s",
            company_name, domain, response.get("message", "")
        )
        return []

    raw_contacts = response.get("contacts") or response.get("emails") or []
    contacts = [_normalize_contact(c) for c in raw_contacts]
    # Set company name on each contact
    for c in contacts:
        if not c["company"]:
            c["company"] = company_name

    logger.info(
        "[Prospeo] company_search(%r) → %d contacts", company_name, len(contacts)
    )
    return contacts


def enrich_contact(linkedin_url: str) -> Optional[Dict[str, Any]]:
    """Enrich a contact record using their LinkedIn profile URL.

    Args:
        linkedin_url: Full LinkedIn profile URL
                      (e.g. ``"https://www.linkedin.com/in/johndoe"``).

    Returns:
        Standardized contact dict with email/phone if available; ``None``
        if no match found or enrichment fails.
    """
    if not linkedin_url or "linkedin.com" not in linkedin_url:
        logger.warning("[Prospeo] Invalid LinkedIn URL: %r", linkedin_url)
        return None

    api_key = _get_api_key()
    payload = {"url": linkedin_url}

    logger.info("[Prospeo] LinkedIn enrichment: %s", linkedin_url)

    response = _post_with_retry(LINKEDIN_ENRICH_PATH, payload, api_key)
    if not response:
        return None

    if response.get("error"):
        logger.info(
            "[Prospeo] LinkedIn enrichment failed for %s: %s",
            linkedin_url, response.get("message", "")
        )
        return None

    person = response.get("person") or response
    contact = _normalize_contact({**person, "linkedin_url": linkedin_url})
    logger.info(
        "[Prospeo] Enriched via LinkedIn: %s <%s>", contact.get("name"), contact.get("email")
    )
    return contact


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    # Quick smoke test
    result = company_search("Example Realty", "examplerealty.com")
    for c in result:
        print(f"  {c['name']} | {c['title']} | {c['email']}")
