"""
collectors/bizbuysell.py
------------------------
BizBuySell property-management business-for-sale collector for Scout Bot.

Fetches listings from bizbuysell.com across NY, FL, CT, and NJ regions,
parses listing cards, extracts contact information, and returns normalized
lead dicts conforming to the Scout schema.

Key features:
- Multi-region support (NY, FL, CT, NJ)
- Pagination up to 5 pages per region
- Exponential-backoff retry (3 attempts)
- User-agent rotation
- Contact info (email + phone) extraction from descriptions
"""

import logging
import random
import time
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from utils.parsing import clean_text, extract_emails, extract_phones, parse_post_date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://www.bizbuysell.com"

# Region slug → display name mapping
REGION_SLUGS: Dict[str, str] = {
    "NY": "new-york",
    "FL": "florida",
    "CT": "connecticut",
    "NJ": "new-jersey",
}

# Category path for property-management businesses
CATEGORY_PATH = "/property-management-businesses-for-sale"

MAX_PAGES = 5
REQUEST_TIMEOUT = 15       # seconds
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0     # seconds (doubled on each retry)

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _random_headers() -> Dict[str, str]:
    """Return a dict of browser-like HTTP headers with a random User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _fetch_with_retry(url: str, session: requests.Session) -> Optional[requests.Response]:
    """Fetch *url* with exponential-backoff retry.

    Args:
        url: Full URL to fetch.
        session: Shared :class:`requests.Session`.

    Returns:
        :class:`requests.Response` on success, or ``None`` after all retries
        are exhausted.
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
            # 429 Too Many Requests — back off longer
            if exc.response is not None and exc.response.status_code == 429:
                wait = delay * 3
                logger.warning("Rate-limited on %s. Waiting %.1fs...", url, wait)
                time.sleep(wait)
            else:
                logger.warning("HTTP error on attempt %d for %s: %s", attempt, url, exc)
                time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("Request error on attempt %d for %s: %s", attempt, url, exc)
            time.sleep(delay)
        delay *= 2  # exponential backoff

    logger.error("All %d retry attempts exhausted for %s", RETRY_ATTEMPTS, url)
    return None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _build_listing_url(region_slug: str, page: int) -> str:
    """Construct a BizBuySell listing page URL.

    Args:
        region_slug: e.g. ``"new-york"``
        page: 1-based page number.

    Returns:
        Fully-qualified URL string.
    """
    path = f"/{region_slug}{CATEGORY_PATH}/"
    params = {"page": page} if page > 1 else {}
    url = urljoin(BASE_URL, path)
    if params:
        url = f"{url}?{urlencode(params)}"
    return url


def _parse_price(text: str) -> str:
    """Extract a clean price string (e.g. ``'$1,200,000'``)."""
    if not text:
        return ""
    # Keep dollar sign, digits, commas, periods, and 'Not Disclosed'
    cleaned = clean_text(text)
    price_match = re.search(r"\$[\d,]+(?:\.\d+)?|Not Disclosed|N/A", cleaned, re.IGNORECASE)
    return price_match.group(0) if price_match else cleaned[:50]


def _parse_listing_card(card: BeautifulSoup, region: str, page_url: str) -> Optional[Dict[str, Any]]:
    """Parse a single listing card element into a lead dict.

    Args:
        card: BeautifulSoup element representing one listing card.
        region: Region code, e.g. ``"NY"``.
        page_url: URL of the page this card was found on (for context).

    Returns:
        Lead dict or ``None`` if the card lacks a title/link.
    """
    try:
        # --- Title & link ---
        title_el = card.select_one("a.title, h3 a, .listing-title a, a[data-test='listing-title']")
        if not title_el:
            # Fallback: any prominent anchor
            title_el = card.find("a", href=True)
        if not title_el:
            return None

        title = clean_text(title_el.get_text())
        link_href = title_el.get("href", "")
        link = urljoin(BASE_URL, link_href) if link_href else ""

        if not title:
            return None

        # --- Description ---
        desc_el = card.select_one(".description, .listing-description, p.body")
        description = clean_text(desc_el.get_text()) if desc_el else ""

        # --- Price / asking price ---
        price_el = card.select_one(".price, .asking-price, [data-test='price']")
        asking_price = _parse_price(price_el.get_text()) if price_el else ""

        # --- Revenue ---
        revenue = ""
        for label_el in card.select(".key-figure, .financials li, .stat"):
            label_text = clean_text(label_el.get_text()).lower()
            if "revenue" in label_text or "gross" in label_text:
                revenue = clean_text(label_el.get_text())
                break

        # --- Location ---
        loc_el = card.select_one(".location, .city-state, [data-test='location']")
        raw_location = clean_text(loc_el.get_text()) if loc_el else region

        # --- Post date ---
        date_el = card.select_one(".post-date, .listed-date, time, [data-test='post-date']")
        date_text = clean_text(date_el.get_text()) if date_el else ""
        post_date = parse_post_date(date_text)
        days_posted = (date.today() - post_date).days if post_date else None

        # --- Contact info from description ---
        search_text = f"{title} {description}"
        emails = extract_emails(search_text)
        phones = extract_phones(search_text)

        # --- Company name heuristic: first line of description or title ---
        company_name = title  # listings are often the company name

        return {
            "source_site": "BizBuySell",
            "region": region,
            "post_date": post_date,
            "days_posted": days_posted,
            "title": title,
            "post_description": description,
            "author": "",
            "company_name": company_name,
            "link": link,
            "email": emails,
            "phone": phones,
            "social_media": [],
            "category": "Business for sale",
            "lead_type": "Acquisition",
            "raw_location": raw_location,
            "asking_price": asking_price,
            "revenue": revenue,
            "score": 0,
            "tags": ["Acquisition"],
            "contacts": [],
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error parsing listing card on %s: %s", page_url, exc)
        return None


# ---------------------------------------------------------------------------
# Per-region collector
# ---------------------------------------------------------------------------

def collect_region(region: str, session: requests.Session) -> List[Dict[str, Any]]:
    """Collect all listings for a single *region*.

    Args:
        region: Region code — one of ``NY``, ``FL``, ``CT``, ``NJ``.
        session: Shared :class:`requests.Session`.

    Returns:
        List of lead dicts.
    """
    slug = REGION_SLUGS.get(region)
    if not slug:
        logger.error("Unknown region: %s", region)
        return []

    leads: List[Dict[str, Any]] = []

    for page in range(1, MAX_PAGES + 1):
        url = _build_listing_url(slug, page)
        logger.info("[BizBuySell] Fetching region=%s page=%d → %s", region, page, url)

        resp = _fetch_with_retry(url, session)
        if resp is None:
            logger.warning("[BizBuySell] Skipping %s page %d — no response.", region, page)
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find listing cards — BizBuySell uses various class names across versions
        cards = soup.select(
            ".listing-item, .business-listing, article.result, "
            "[data-test='listing-card'], .srp-list-item"
        )

        if not cards:
            logger.info("[BizBuySell] No cards found on page %d for region %s. Stopping.", page, region)
            break

        page_leads: List[Dict[str, Any]] = []
        for card in cards:
            lead = _parse_listing_card(card, region, url)
            if lead:
                page_leads.append(lead)

        logger.info("[BizBuySell] region=%s page=%d → %d leads", region, page, len(page_leads))
        leads.extend(page_leads)

        # Polite delay between pages
        time.sleep(random.uniform(1.5, 3.0))

        # If we got fewer cards than expected, assume last page
        if len(cards) < 10:
            break

    return leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def collect(regions: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Collect BizBuySell property-management listings across all regions.

    Args:
        regions: List of region codes to collect. Defaults to all four
                 supported regions: ``["NY", "FL", "CT", "NJ"]``.

    Returns:
        Combined list of lead dicts from all regions.
    """
    if regions is None:
        regions = list(REGION_SLUGS.keys())

    all_leads: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for region in regions:
            try:
                region_leads = collect_region(region, session)
                all_leads.extend(region_leads)
                logger.info("[BizBuySell] Region %s: %d leads collected.", region, len(region_leads))
            except Exception as exc:  # noqa: BLE001
                logger.error("[BizBuySell] Unhandled error for region %s: %s", region, exc)

    logger.info("[BizBuySell] Total leads collected: %d", len(all_leads))
    return all_leads


# ---------------------------------------------------------------------------
# Module-level import needed inside _parse_listing_card
# ---------------------------------------------------------------------------
import re  # noqa: E402 (placed here to avoid circular import issues at module level)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} BizBuySell leads.")
    for lead in results[:5]:
        print(f"  [{lead['region']}] {lead['title']} | {lead['link']}")
