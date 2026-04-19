"""
collectors/nyc_rfps.py
----------------------
NYC government RFP/RFQ/RFEI collector for Scout Bot.

Collects procurement opportunities from:
- NYC DCAS (Department of Citywide Administrative Services) real-estate RFPs
- NYC HPD (Housing Preservation & Development) RFPs/RFQs/RFEIs
- NYC EDC (Economic Development Corporation) upcoming procurements

Filters results by property-management-relevant keywords and returns
normalized lead dicts with category="RFP" and lead_type="Management mandate".

Key features:
- Three agency source URLs
- Keyword filtering for property-management relevance
- Exponential-backoff retry (3 attempts)
- Extracts title, description, deadline, contact info, and link
- Fallback parsing for multiple page layouts
"""

import logging
import random
import re
import time
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils.parsing import clean_text, extract_emails, extract_phones, parse_post_date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Source agency configurations
SOURCES: List[Dict[str, str]] = [
    {
        "agency": "NYC DCAS",
        "region": "NY",
        "url": "https://www.nyc.gov/site/dcas/business/real-estate-rfps-rfbs-rfeis.page",
    },
    {
        "agency": "NYC HPD",
        "region": "NY",
        "url": "https://www.nyc.gov/site/hpd/services-and-information/rfps-rfqs-rfeis.page",
    },
    {
        "agency": "NYC EDC",
        "region": "NY",
        "url": "https://edc.nyc/upcoming-procurement-opportunities",
    },
]

# Keywords that indicate property-management relevance
RELEVANT_KEYWORDS: List[str] = [
    "property management",
    "managing agent",
    "building management",
    "asset management",
    "condominium",
    "co-op",
    "cooperative",
    "hoa",
    "homeowner association",
    "residential management",
    "multi-family",
    "affordable housing",
    "housing management",
    "facilities management",
    "real estate management",
]

REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _random_headers() -> Dict[str, str]:
    """Return browser-like HTTP request headers."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
    }


def _fetch_with_retry(url: str, session: requests.Session) -> Optional[requests.Response]:
    """Fetch *url* with exponential-backoff retry.

    Args:
        url: Target URL.
        session: Shared requests Session.

    Returns:
        Response on success; ``None`` if all retries fail.
    """
    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(
                url,
                headers=_random_headers(),
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            logger.warning("[NYC RFPs] HTTP %d attempt %d: %s", status, attempt, url)
            time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("[NYC RFPs] Request error attempt %d: %s — %s", attempt, url, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[NYC RFPs] Exhausted retries for %s", url)
    return None


# ---------------------------------------------------------------------------
# Keyword filtering
# ---------------------------------------------------------------------------

def _is_relevant(text: str) -> bool:
    """Return True if *text* contains at least one relevant keyword.

    Args:
        text: Lowercase text to search.

    Returns:
        True if any keyword matches.
    """
    lower = text.lower()
    return any(kw in lower for kw in RELEVANT_KEYWORDS)


# ---------------------------------------------------------------------------
# Parser: NYC.gov pages (DCAS + HPD)
# ---------------------------------------------------------------------------

def _parse_nyc_gov_page(
    soup: BeautifulSoup,
    source: Dict[str, str],
    base_url: str,
) -> List[Dict[str, Any]]:
    """Parse RFP listings from an NYC.gov agency page.

    NYC.gov pages use various layouts; we attempt multiple selectors.

    Args:
        soup: Parsed page BeautifulSoup.
        source: Source config dict with ``agency`` and ``region`` keys.
        base_url: Base URL for resolving relative links.

    Returns:
        List of lead dicts for relevant RFPs.
    """
    leads: List[Dict[str, Any]] = []
    agency = source["agency"]
    region = source["region"]

    # Try multiple table/list selectors used across NYC.gov pages
    # Pattern 1: <table> rows with links
    rows = soup.select("table tr, .rfp-table tr, .procurement-table tr")
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        row_text = clean_text(row.get_text())
        if not _is_relevant(row_text):
            continue

        # First cell with a link is usually the RFP title
        link_el = row.find("a", href=True)
        title = clean_text(link_el.get_text()) if link_el else clean_text(cells[0].get_text())
        href = link_el.get("href", "") if link_el else ""
        link = urljoin(base_url, href) if href else source["url"]

        description = row_text
        emails = extract_emails(row_text)
        phones = extract_phones(row_text)

        # Try to find a date in any cell
        post_date = None
        for cell in cells:
            cell_text = clean_text(cell.get_text())
            parsed = parse_post_date(cell_text)
            if parsed:
                post_date = parsed
                break

        days_posted = (date.today() - post_date).days if post_date else None

        leads.append(_build_rfp_lead(
            agency=agency,
            region=region,
            title=title,
            description=description,
            link=link,
            emails=emails,
            phones=phones,
            post_date=post_date,
            days_posted=days_posted,
        ))

    # Pattern 2: <li> / <div> based listing blocks
    if not leads:
        blocks = soup.select(
            ".rfp-item, .procurement-item, article.rfp, "
            ".content-item, .accordion-item, li.listing"
        )
        for block in blocks:
            block_text = clean_text(block.get_text())
            if not _is_relevant(block_text):
                continue

            link_el = block.find("a", href=True)
            title_el = block.select_one("h2, h3, h4, strong, .title")
            title = clean_text(title_el.get_text()) if title_el else clean_text(block_text[:120])
            href = link_el.get("href", "") if link_el else ""
            link = urljoin(base_url, href) if href else source["url"]

            emails = extract_emails(block_text)
            phones = extract_phones(block_text)
            post_date = parse_post_date(block_text)
            days_posted = (date.today() - post_date).days if post_date else None

            leads.append(_build_rfp_lead(
                agency=agency,
                region=region,
                title=title,
                description=block_text[:500],
                link=link,
                emails=emails,
                phones=phones,
                post_date=post_date,
                days_posted=days_posted,
            ))

    # Pattern 3: Plain paragraph links (fallback)
    if not leads:
        for a_tag in soup.find_all("a", href=True):
            link_text = clean_text(a_tag.get_text())
            if not link_text or not _is_relevant(link_text):
                continue
            href = a_tag.get("href", "")
            link = urljoin(base_url, href)
            # Grab surrounding paragraph for description
            parent = a_tag.find_parent(["p", "li", "div"])
            description = clean_text(parent.get_text()) if parent else link_text

            leads.append(_build_rfp_lead(
                agency=agency,
                region=region,
                title=link_text[:200],
                description=description[:500],
                link=link,
                emails=extract_emails(description),
                phones=extract_phones(description),
                post_date=None,
                days_posted=None,
            ))

    return leads


# ---------------------------------------------------------------------------
# Parser: NYC EDC
# ---------------------------------------------------------------------------

def _parse_edc_page(
    soup: BeautifulSoup,
    source: Dict[str, str],
    base_url: str,
) -> List[Dict[str, Any]]:
    """Parse procurement opportunities from the NYC EDC page.

    Args:
        soup: Parsed page BeautifulSoup.
        source: Source config dict.
        base_url: Base URL for resolving relative links.

    Returns:
        List of relevant lead dicts.
    """
    leads: List[Dict[str, Any]] = []
    agency = source["agency"]
    region = source["region"]

    # EDC uses card/block layouts
    cards = soup.select(
        ".procurement-item, .opportunity-card, article, "
        ".content-block, .listing-item, .post"
    )

    for card in cards:
        card_text = clean_text(card.get_text())
        if not _is_relevant(card_text):
            continue

        link_el = card.find("a", href=True)
        title_el = card.select_one("h2, h3, h4, .title, .headline")
        title = clean_text(title_el.get_text()) if title_el else card_text[:150]
        href = link_el.get("href", "") if link_el else ""
        link = urljoin(base_url, href) if href else source["url"]

        emails = extract_emails(card_text)
        phones = extract_phones(card_text)
        post_date = parse_post_date(card_text)
        days_posted = (date.today() - post_date).days if post_date else None

        leads.append(_build_rfp_lead(
            agency=agency,
            region=region,
            title=title,
            description=card_text[:600],
            link=link,
            emails=emails,
            phones=phones,
            post_date=post_date,
            days_posted=days_posted,
        ))

    return leads


# ---------------------------------------------------------------------------
# Lead builder
# ---------------------------------------------------------------------------

def _build_rfp_lead(
    agency: str,
    region: str,
    title: str,
    description: str,
    link: str,
    emails: List[str],
    phones: List[str],
    post_date: Optional[date],
    days_posted: Optional[int],
) -> Dict[str, Any]:
    """Build a normalized RFP lead dict.

    Args:
        agency: Agency name (e.g. ``"NYC HPD"``).
        region: Region code.
        title: RFP title.
        description: Full text description.
        link: URL to the RFP.
        emails: Extracted email addresses.
        phones: Extracted phone numbers.
        post_date: Parsed posting date or ``None``.
        days_posted: Days since posting, or ``None``.

    Returns:
        Scout-schema lead dict.
    """
    return {
        "source_site": f"NYC Gov — {agency}",
        "region": region,
        "post_date": post_date,
        "days_posted": days_posted,
        "title": title,
        "post_description": description,
        "author": agency,
        "company_name": agency,
        "link": link,
        "email": emails,
        "phone": phones,
        "social_media": [],
        "category": "RFP",
        "lead_type": "Management mandate",
        "raw_location": f"{agency}, New York City",
        "score": 0,
        "tags": ["Management mandate", "RFP", agency],
        "contacts": [],
    }


# ---------------------------------------------------------------------------
# Main collection function
# ---------------------------------------------------------------------------

def collect() -> List[Dict[str, Any]]:
    """Collect property-management RFPs from all NYC government sources.

    Returns:
        Combined list of filtered, normalized lead dicts.
    """
    all_leads: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for source in SOURCES:
            url = source["url"]
            agency = source["agency"]
            logger.info("[NYC RFPs] Fetching %s → %s", agency, url)

            resp = _fetch_with_retry(url, session)
            if resp is None:
                logger.warning("[NYC RFPs] No response from %s. Skipping.", agency)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            if "edc.nyc" in url:
                leads = _parse_edc_page(soup, source, url)
            else:
                leads = _parse_nyc_gov_page(soup, source, url)

            logger.info("[NYC RFPs] %s: %d relevant RFPs found.", agency, len(leads))
            all_leads.extend(leads)

            # Polite delay between agency requests
            time.sleep(random.uniform(2.0, 4.0))

    logger.info("[NYC RFPs] Total RFP leads: %d", len(all_leads))
    return all_leads


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} NYC RFP leads.")
    for lead in results[:5]:
        print(f"  [{lead['source_site']}] {lead['title']}")
        print(f"    Link: {lead['link']}")
