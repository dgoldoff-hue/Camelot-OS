"""
collectors/bizquest.py
----------------------
BizQuest property-management business-for-sale collector for Scout Bot.

Fetches listings from bizquest.com across NY, FL, CT, and NJ regions,
parses listing cards, and returns normalized lead dicts conforming to the
Scout schema.

Key features:
- Multi-region support (NY, FL, CT, NJ) using BizQuest's category/state URLs
- Pagination up to 5 pages per region
- Exponential-backoff retry (3 attempts)
- User-agent rotation
- Email + phone extraction from listing descriptions
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

BASE_URL = "https://www.bizquest.com"

# BizQuest state slug → region code mapping
REGION_SLUGS: Dict[str, str] = {
    "NY": "new-york",
    "FL": "florida",
    "CT": "connecticut",
    "NJ": "new-jersey",
}

# BizQuest uses a category-based URL structure
# "property-management" falls under real-estate services
CATEGORY_SEGMENT = "property-management-businesses-for-sale"

MAX_PAGES = 5
REQUEST_TIMEOUT = 15
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0

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
    """Return browser-like HTTP headers with a rotated User-Agent."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.bizquest.com/",
    }


def _fetch_with_retry(url: str, session: requests.Session) -> Optional[requests.Response]:
    """Fetch *url* with exponential-backoff retry logic.

    Args:
        url: Target URL.
        session: Shared requests Session.

    Returns:
        Response on success; ``None`` after all retries are exhausted.
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
            if status == 429:
                wait = delay * 4
                logger.warning("[BizQuest] Rate-limited. Sleeping %.1fs...", wait)
                time.sleep(wait)
            else:
                logger.warning("[BizQuest] HTTP %d on attempt %d: %s", status, attempt, url)
                time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("[BizQuest] Request error attempt %d: %s — %s", attempt, url, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[BizQuest] Exhausted retries for %s", url)
    return None


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

def _build_page_url(region_slug: str, page: int) -> str:
    """Build a BizQuest search URL for a given region and page.

    BizQuest URL pattern:
    ``/state/new-york/property-management-businesses-for-sale/PgNum-<N>/``

    Args:
        region_slug: BizQuest state slug, e.g. ``"new-york"``.
        page: 1-based page number.

    Returns:
        Fully-qualified URL string.
    """
    if page <= 1:
        path = f"/buy/{region_slug}/{CATEGORY_SEGMENT}/"
    else:
        path = f"/buy/{region_slug}/{CATEGORY_SEGMENT}/pg{page}/"
    return urljoin(BASE_URL, path)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_listing_card(
    card: BeautifulSoup,
    region: str,
    page_url: str,
) -> Optional[Dict[str, Any]]:
    """Parse one BizQuest listing card into a Scout lead dict.

    Args:
        card: BeautifulSoup element for the listing card.
        region: Region code (e.g. ``"NY"``).
        page_url: Source page URL for logging.

    Returns:
        Lead dict or ``None`` on parse failure.
    """
    try:
        # Title + link
        title_el = card.select_one(
            "a.listingTitle, h2 a, h3 a, .listing-name a, "
            "[data-cy='listing-name'] a, .title a"
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

        # Description
        desc_el = card.select_one(
            ".listingDescription, .description, p.desc, .listing-blurb"
        )
        description = clean_text(desc_el.get_text()) if desc_el else ""

        # Asking price — look for labeled key figures
        asking_price = ""
        for kf in card.select(".keyFigure, .key-figure, .stat-item, li.stat"):
            kf_text = clean_text(kf.get_text()).lower()
            if "asking" in kf_text or "price" in kf_text:
                asking_price = clean_text(kf.get_text())
                break
        if not asking_price:
            price_el = card.select_one(".askingPrice, .price, [data-cy='price']")
            asking_price = clean_text(price_el.get_text()) if price_el else ""

        # Revenue
        revenue = ""
        for kf in card.select(".keyFigure, .key-figure, .stat-item"):
            kf_text = clean_text(kf.get_text()).lower()
            if "revenue" in kf_text or "gross" in kf_text:
                revenue = clean_text(kf.get_text())
                break

        # Location
        loc_el = card.select_one(".location, .city, [data-cy='location'], .listing-location")
        raw_location = clean_text(loc_el.get_text()) if loc_el else region

        # Post date
        date_el = card.select_one(".postDate, .date-posted, time, [data-cy='post-date']")
        date_text = clean_text(date_el.get_text()) if date_el else ""
        post_date = parse_post_date(date_text)
        days_posted = (date.today() - post_date).days if post_date else None

        # Seller / author name
        author_el = card.select_one(".sellerName, .broker-name, .agent-name")
        author = clean_text(author_el.get_text()) if author_el else ""

        # Contact extraction from description text
        search_text = f"{title} {description}"
        emails = extract_emails(search_text)
        phones = extract_phones(search_text)

        return {
            "source_site": "BizQuest",
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
            "lead_type": "Acquisition",
            "raw_location": raw_location,
            "asking_price": asking_price,
            "revenue": revenue,
            "score": 0,
            "tags": ["Acquisition"],
            "contacts": [],
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("[BizQuest] Error parsing card on %s: %s", page_url, exc)
        return None


# ---------------------------------------------------------------------------
# Per-region collector
# ---------------------------------------------------------------------------

def collect_region(region: str, session: requests.Session) -> List[Dict[str, Any]]:
    """Collect all BizQuest listings for a single region.

    Args:
        region: Region code — one of ``NY``, ``FL``, ``CT``, ``NJ``.
        session: Shared requests Session.

    Returns:
        List of lead dicts from all pages in the region.
    """
    slug = REGION_SLUGS.get(region)
    if not slug:
        logger.error("[BizQuest] Unknown region: %s", region)
        return []

    leads: List[Dict[str, Any]] = []

    for page in range(1, MAX_PAGES + 1):
        url = _build_page_url(slug, page)
        logger.info("[BizQuest] Fetching region=%s page=%d → %s", region, page, url)

        resp = _fetch_with_retry(url, session)
        if resp is None:
            logger.warning("[BizQuest] No response for %s page %d. Stopping.", region, page)
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # BizQuest listing selectors (robust multi-selector)
        cards = soup.select(
            ".listing, .listing-item, article.result, "
            "[data-cy='listing-card'], .searchResult, .biz-listing"
        )

        if not cards:
            logger.info("[BizQuest] No cards on page %d for region %s. Done.", page, region)
            break

        page_leads: List[Dict[str, Any]] = []
        for card in cards:
            lead = _parse_listing_card(card, region, url)
            if lead:
                page_leads.append(lead)

        logger.info("[BizQuest] region=%s page=%d → %d leads", region, page, len(page_leads))
        leads.extend(page_leads)

        # Polite crawl delay
        time.sleep(random.uniform(1.5, 3.5))

        # Detect last page
        has_next = bool(
            soup.select_one("a.next, a[rel='next'], .pagination .next:not(.disabled)")
        )
        if not has_next and page > 1:
            logger.info("[BizQuest] No 'next' link found after page %d. Done.", page)
            break

    return leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def collect(regions: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Collect BizQuest property-management listings across all regions.

    Args:
        regions: Region codes to collect. Defaults to
                 ``["NY", "FL", "CT", "NJ"]``.

    Returns:
        Combined list of lead dicts from all specified regions.
    """
    if regions is None:
        regions = list(REGION_SLUGS.keys())

    all_leads: List[Dict[str, Any]] = []

    with requests.Session() as session:
        for region in regions:
            try:
                region_leads = collect_region(region, session)
                all_leads.extend(region_leads)
                logger.info("[BizQuest] Region %s: %d leads.", region, len(region_leads))
            except Exception as exc:  # noqa: BLE001
                logger.error("[BizQuest] Unhandled error for region %s: %s", region, exc)

    logger.info("[BizQuest] Total leads: %d", len(all_leads))
    return all_leads


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} BizQuest leads.")
    for lead in results[:5]:
        print(f"  [{lead['region']}] {lead['title']} | {lead['link']}")
