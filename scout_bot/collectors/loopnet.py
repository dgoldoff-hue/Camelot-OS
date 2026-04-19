"""
collectors/loopnet.py
---------------------
LoopNet collector for property-management company listings and management
mandates in NYC, Westchester, CT, NJ, and FL markets.

LoopNet primarily serves commercial real estate, so this collector targets:
- Property management service businesses listed for sale
- Management company listings that signal acquisition opportunities
- "For lease" office space listings by property management firms (signals)

Key features:
- Multi-market search across NYC, Westchester, CT, NJ, FL
- Searches LoopNet's public search endpoint for "property management" businesses
- Pagination up to 5 pages per market
- Exponential-backoff retry (3 attempts)
- User-agent rotation
- Strict rate-limiting to respect LoopNet's crawler policies
"""

import logging
import random
import re
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

BASE_URL = "https://www.loopnet.com"

# LoopNet market location strings used in search queries
MARKETS: Dict[str, Dict[str, str]] = {
    "NYC": {
        "region": "NY",
        "location": "New York, NY",
        "slug": "new-york_ny",
    },
    "Westchester": {
        "region": "NY",
        "location": "Westchester County, NY",
        "slug": "westchester-county_ny",
    },
    "NJ": {
        "region": "NJ",
        "location": "New Jersey",
        "slug": "new-jersey",
    },
    "CT": {
        "region": "CT",
        "location": "Connecticut",
        "slug": "connecticut",
    },
    "FL": {
        "region": "FL",
        "location": "Florida",
        "slug": "florida",
    },
}

SEARCH_KEYWORD = "property management"
PROPERTY_TYPE = "businesses-for-sale"   # LoopNet property type path segment

MAX_PAGES = 5
REQUEST_TIMEOUT = 15
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 3.0   # longer base for LoopNet (more aggressive blocking)
PAGE_DELAY_MIN = 3.0
PAGE_DELAY_MAX = 6.0

USER_AGENTS: List[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _random_headers() -> Dict[str, str]:
    """Return browser-like request headers with a rotated User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    }


def _fetch_with_retry(url: str, session: requests.Session) -> Optional[requests.Response]:
    """Fetch *url* with exponential-backoff retry.

    Args:
        url: Target URL.
        session: Shared requests Session.

    Returns:
        Response on success; ``None`` after retries exhausted.
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
            if status in (429, 503):
                wait = delay * 5
                logger.warning("[LoopNet] Blocked (HTTP %d). Sleeping %.1fs...", status, wait)
                time.sleep(wait)
            else:
                logger.warning("[LoopNet] HTTP %d attempt %d: %s", status, attempt, url)
                time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("[LoopNet] Request error attempt %d: %s — %s", attempt, url, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[LoopNet] All retries exhausted for %s", url)
    return None


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

def _build_search_url(market_slug: str, keyword: str, page: int) -> str:
    """Build a LoopNet search URL for property management businesses.

    LoopNet search URL pattern:
    ``/search/businesses-for-sale/<location-slug>/?sk=<keyword>&pg=<N>``

    Args:
        market_slug: e.g. ``"new-york_ny"``
        keyword: Search keyword string.
        page: 1-based page number.

    Returns:
        Fully-qualified URL string.
    """
    params: Dict[str, Any] = {"sk": keyword}
    if page > 1:
        params["pg"] = page
    path = f"/search/{PROPERTY_TYPE}/{market_slug}/"
    url = urljoin(BASE_URL, path)
    return f"{url}?{urlencode(params)}"


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_listing_card(
    card: BeautifulSoup,
    market_key: str,
    region: str,
    page_url: str,
) -> Optional[Dict[str, Any]]:
    """Parse one LoopNet listing card element into a Scout lead dict.

    Args:
        card: BeautifulSoup element representing one listing.
        market_key: Market label, e.g. ``"NYC"``.
        region: State code, e.g. ``"NY"``.
        page_url: Source URL for debug logging.

    Returns:
        Lead dict or ``None`` on parse failure.
    """
    try:
        # Title + link
        title_el = card.select_one(
            "a.title, h4 a, h3 a, .listing-title a, "
            "[data-test='property-title'] a, .property-name a"
        )
        if not title_el:
            title_el = card.find("a", href=True)
        if not title_el:
            return None

        title = clean_text(title_el.get_text())
        href = title_el.get("href", "")
        link = urljoin(BASE_URL, href) if href else ""

        if not title:
            return None

        # Description / summary
        desc_el = card.select_one(
            ".listing-summary, .description, .property-description, p.body"
        )
        description = clean_text(desc_el.get_text()) if desc_el else ""

        # Price
        price_el = card.select_one(".price, .asking-price, .listing-price, [data-test='price']")
        asking_price = clean_text(price_el.get_text()) if price_el else ""

        # Location
        loc_el = card.select_one(
            ".location, .address, .city-state, [data-test='location'], .property-address"
        )
        raw_location = clean_text(loc_el.get_text()) if loc_el else market_key

        # Date
        date_el = card.select_one(".date, .listed-date, .post-date, time")
        date_text = clean_text(date_el.get_text()) if date_el else ""
        post_date = parse_post_date(date_text)
        days_posted = (date.today() - post_date).days if post_date else None

        # Broker / author
        broker_el = card.select_one(".broker-name, .contact-name, .agent, .seller")
        author = clean_text(broker_el.get_text()) if broker_el else ""

        # Contact extraction
        search_text = f"{title} {description}"
        emails = extract_emails(search_text)
        phones = extract_phones(search_text)

        # Lead type: LoopNet listings may be management mandates or acquisitions
        lead_type = "Acquisition"
        if any(kw in title.lower() or kw in description.lower()
               for kw in ["mandate", "rfp", "managing agent", "management contract"]):
            lead_type = "Management mandate"

        return {
            "source_site": "LoopNet",
            "region": region,
            "post_date": post_date,
            "days_posted": days_posted,
            "title": title,
            "post_description": description,
            "author": author,
            "company_name": title,
            "link": link,
            "email": emails,
            "phone": phones,
            "social_media": [],
            "category": "Business for sale",
            "lead_type": lead_type,
            "raw_location": raw_location,
            "asking_price": asking_price,
            "market": market_key,
            "score": 0,
            "tags": [lead_type],
            "contacts": [],
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("[LoopNet] Parse error on %s: %s", page_url, exc)
        return None


# ---------------------------------------------------------------------------
# Per-market collector
# ---------------------------------------------------------------------------

def collect_market(market_key: str, session: requests.Session) -> List[Dict[str, Any]]:
    """Collect LoopNet listings for a single market.

    Args:
        market_key: Key into the MARKETS dict, e.g. ``"NYC"``.
        session: Shared requests Session.

    Returns:
        List of lead dicts.
    """
    market = MARKETS.get(market_key)
    if not market:
        logger.error("[LoopNet] Unknown market: %s", market_key)
        return []

    region = market["region"]
    slug = market["slug"]
    leads: List[Dict[str, Any]] = []

    for page in range(1, MAX_PAGES + 1):
        url = _build_search_url(slug, SEARCH_KEYWORD, page)
        logger.info("[LoopNet] Fetching market=%s page=%d → %s", market_key, page, url)

        resp = _fetch_with_retry(url, session)
        if resp is None:
            logger.warning("[LoopNet] No response for %s page %d. Stopping.", market_key, page)
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        cards = soup.select(
            ".listing, .search-result, article.listing, "
            "[data-test='listing-card'], .property-listing, .biz-result"
        )

        if not cards:
            logger.info("[LoopNet] No cards on page %d for market %s. Done.", page, market_key)
            break

        page_leads: List[Dict[str, Any]] = []
        for card in cards:
            lead = _parse_listing_card(card, market_key, region, url)
            if lead:
                page_leads.append(lead)

        logger.info("[LoopNet] market=%s page=%d → %d leads", market_key, page, len(page_leads))
        leads.extend(page_leads)

        # LoopNet requires longer polite delays
        time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

        # Detect last page
        if len(cards) < 5:
            break

    return leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def collect(markets: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Collect LoopNet property-management listings across all target markets.

    Args:
        markets: List of market keys to collect. Defaults to all markets:
                 ``["NYC", "Westchester", "NJ", "CT", "FL"]``.

    Returns:
        Combined list of lead dicts from all markets.
    """
    if markets is None:
        markets = list(MARKETS.keys())

    all_leads: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for market_key in markets:
            try:
                market_leads = collect_market(market_key, session)
                all_leads.extend(market_leads)
                logger.info("[LoopNet] Market %s: %d leads.", market_key, len(market_leads))
            except Exception as exc:  # noqa: BLE001
                logger.error("[LoopNet] Unhandled error for market %s: %s", market_key, exc)

    logger.info("[LoopNet] Total leads: %d", len(all_leads))
    return all_leads


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} LoopNet leads.")
    for lead in results[:5]:
        print(f"  [{lead['region']}] {lead['title']} | {lead['link']}")
