"""
collectors/jobs_signals.py
---------------------------
Job-posting signals collector for Scout Bot.

Monitors Indeed and ZipRecruiter for property management job postings that
signal a company is growing (hiring) or struggling (high turnover). These
postings represent acquisition and management-mandate opportunities for
Camelot Property Management Services Corp.

Targeted job titles / search phrases:
- "property manager"
- "property management company hiring"
- "director of property management"
- "VP property management"
- "property management operations"

Key features:
- Searches both Indeed and ZipRecruiter
- Multi-region targeting: NY, NJ, CT, FL
- Pagination up to 3 pages per query per source
- Exponential-backoff retry (3 attempts)
- User-agent rotation
- Returns lead_type = "Hiring signal"
"""

import logging
import random
import re
import time
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus, urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from utils.parsing import clean_text, extract_emails, extract_phones, parse_post_date

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Search query → label mapping
JOB_QUERIES: List[str] = [
    "property manager",
    "director of property management",
    "VP property management",
    "property management operations manager",
    "building manager property management company",
]

# Region → location string for Indeed / ZipRecruiter
REGIONS: Dict[str, str] = {
    "NY": "New York, NY",
    "NJ": "New Jersey",
    "CT": "Connecticut",
    "FL": "Florida",
}

MAX_PAGES = 3
REQUEST_TIMEOUT = 15
RETRY_ATTEMPTS = 3
RETRY_BASE_DELAY = 2.0
PAGE_DELAY_MIN = 2.0
PAGE_DELAY_MAX = 5.0

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

def _random_headers(referer: str = "") -> Dict[str, str]:
    """Return browser-like HTTP headers with a rotated User-Agent."""
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def _fetch_with_retry(url: str, session: requests.Session, referer: str = "") -> Optional[requests.Response]:
    """Fetch *url* with exponential-backoff retry.

    Args:
        url: Target URL.
        session: Shared requests Session.
        referer: Optional Referer header value.

    Returns:
        Response on success; ``None`` after retries exhausted.
    """
    delay = RETRY_BASE_DELAY
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = session.get(
                url,
                headers=_random_headers(referer=referer),
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status in (429, 503):
                wait = delay * 4
                logger.warning("[JobSignals] Rate-limited (HTTP %d). Sleeping %.1fs...", status, wait)
                time.sleep(wait)
            else:
                logger.warning("[JobSignals] HTTP %d attempt %d: %s", status, attempt, url)
                time.sleep(delay)
        except requests.exceptions.RequestException as exc:
            logger.warning("[JobSignals] Request error attempt %d: %s — %s", attempt, url, exc)
            time.sleep(delay)
        delay *= 2

    logger.error("[JobSignals] All retries exhausted for %s", url)
    return None


# ---------------------------------------------------------------------------
# Indeed collector
# ---------------------------------------------------------------------------

def _build_indeed_url(query: str, location: str, page: int) -> str:
    """Build an Indeed search URL.

    Args:
        query: Job search string.
        location: Location string, e.g. ``"New York, NY"``.
        page: 1-based page number.

    Returns:
        Fully-qualified Indeed URL.
    """
    start = (page - 1) * 10  # Indeed uses 'start' offset (10 per page)
    params = {
        "q": query,
        "l": location,
        "sort": "date",
        "start": start,
    }
    return f"https://www.indeed.com/jobs?{urlencode(params)}"


def _parse_indeed_page(
    soup: BeautifulSoup,
    query: str,
    region: str,
    location: str,
    page_url: str,
) -> List[Dict[str, Any]]:
    """Parse job listing cards from an Indeed results page.

    Args:
        soup: Parsed BeautifulSoup of the page.
        query: Original search query (used for context).
        region: Region code.
        location: Location string.
        page_url: Source URL for debug logging.

    Returns:
        List of lead dicts.
    """
    leads: List[Dict[str, Any]] = []

    # Indeed uses various card selectors across its A/B versions
    cards = soup.select(
        "[data-jk], .job_seen_beacon, .tapItem, "
        ".result, article.resultContent, .jobsearch-ResultsList > li"
    )

    for card in cards:
        try:
            # Title
            title_el = card.select_one(
                "h2.jobTitle a, h2 a span[title], [data-testid='jobTitle'], "
                ".jobtitle a, h2 span[title]"
            )
            title = clean_text(title_el.get_text()) if title_el else ""
            if not title:
                continue

            # Link
            link_el = card.select_one("h2.jobTitle a, .jobtitle a, a[data-jk]")
            href = link_el.get("href", "") if link_el else ""
            # Indeed links may be relative
            link = urljoin("https://www.indeed.com", href) if href else ""

            # Company
            company_el = card.select_one(
                ".companyName, [data-testid='company-name'], .company, span.company"
            )
            company_name = clean_text(company_el.get_text()) if company_el else ""

            # Location
            loc_el = card.select_one(
                ".companyLocation, [data-testid='text-location'], .location, .locationName"
            )
            raw_location = clean_text(loc_el.get_text()) if loc_el else location

            # Snippet / description
            snippet_el = card.select_one(".summary, .job-snippet, [data-testid='job-snippet']")
            description = clean_text(snippet_el.get_text()) if snippet_el else ""

            # Post date
            date_el = card.select_one(".date, [data-testid='myJobsStateDate'], .result-link-bar")
            date_text = clean_text(date_el.get_text()) if date_el else ""
            post_date = parse_post_date(date_text)
            days_posted = (date.today() - post_date).days if post_date else None

            search_text = f"{title} {company_name} {description}"
            emails = extract_emails(search_text)
            phones = extract_phones(search_text)

            leads.append({
                "source_site": "Indeed",
                "region": region,
                "post_date": post_date,
                "days_posted": days_posted,
                "title": f"[Hiring] {title} — {company_name}",
                "post_description": description,
                "author": "",
                "company_name": company_name,
                "link": link,
                "email": emails,
                "phone": phones,
                "social_media": [],
                "category": "Hiring signal",
                "lead_type": "Hiring signal",
                "raw_location": raw_location,
                "job_query": query,
                "score": 0,
                "tags": ["Hiring signal"],
                "contacts": [],
            })

        except Exception as exc:  # noqa: BLE001
            logger.warning("[JobSignals/Indeed] Card parse error on %s: %s", page_url, exc)

    return leads


def collect_indeed(
    queries: List[str],
    regions: Dict[str, str],
    session: requests.Session,
) -> List[Dict[str, Any]]:
    """Collect job-signal leads from Indeed.

    Args:
        queries: List of job search strings.
        regions: Dict mapping region codes to location strings.
        session: Shared requests Session.

    Returns:
        List of lead dicts.
    """
    all_leads: List[Dict[str, Any]] = []

    for region_code, location in regions.items():
        for query in queries:
            for page in range(1, MAX_PAGES + 1):
                url = _build_indeed_url(query, location, page)
                logger.info(
                    "[Indeed] query=%r region=%s page=%d → %s",
                    query, region_code, page, url,
                )

                resp = _fetch_with_retry(url, session, referer="https://www.indeed.com/")
                if resp is None:
                    logger.warning("[Indeed] No response. Skipping.")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                page_leads = _parse_indeed_page(soup, query, region_code, location, url)
                all_leads.extend(page_leads)
                logger.info("[Indeed] page=%d → %d leads", page, len(page_leads))

                if not page_leads:
                    break

                time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

    return all_leads


# ---------------------------------------------------------------------------
# ZipRecruiter collector
# ---------------------------------------------------------------------------

def _build_ziprecruiter_url(query: str, location: str, page: int) -> str:
    """Build a ZipRecruiter search URL.

    Args:
        query: Job search string.
        location: Location string.
        page: 1-based page number.

    Returns:
        Fully-qualified ZipRecruiter URL.
    """
    params = {
        "search": query,
        "location": location,
        "days": 14,  # only last 14 days
    }
    if page > 1:
        params["page"] = page
    return f"https://www.ziprecruiter.com/jobs-search?{urlencode(params)}"


def _parse_ziprecruiter_page(
    soup: BeautifulSoup,
    query: str,
    region: str,
    location: str,
    page_url: str,
) -> List[Dict[str, Any]]:
    """Parse job listing cards from a ZipRecruiter results page.

    Args:
        soup: Parsed BeautifulSoup of the page.
        query: Original search query.
        region: Region code.
        location: Location string.
        page_url: Source URL for debug logging.

    Returns:
        List of lead dicts.
    """
    leads: List[Dict[str, Any]] = []

    cards = soup.select(
        "article.job_result, [data-testid='job-card'], .job_result_two_pane, "
        ".jobs_list li, .jobList-item"
    )

    for card in cards:
        try:
            title_el = card.select_one(
                "h2 a, .job_title a, [data-testid='job-title'], h2.title a"
            )
            title = clean_text(title_el.get_text()) if title_el else ""
            if not title:
                continue

            href = title_el.get("href", "") if title_el else ""
            link = urljoin("https://www.ziprecruiter.com", href) if href else ""

            company_el = card.select_one(
                ".hiring_company, [data-testid='job-card-hiring-company'], "
                ".company, .t_company_name"
            )
            company_name = clean_text(company_el.get_text()) if company_el else ""

            loc_el = card.select_one(".location, [data-testid='job-card-location'], .city_state")
            raw_location = clean_text(loc_el.get_text()) if loc_el else location

            snippet_el = card.select_one(".job_description, .snippet, p.job_description")
            description = clean_text(snippet_el.get_text()) if snippet_el else ""

            date_el = card.select_one(".posted_time, .date, time")
            date_text = clean_text(date_el.get_text()) if date_el else ""
            post_date = parse_post_date(date_text)
            days_posted = (date.today() - post_date).days if post_date else None

            search_text = f"{title} {company_name} {description}"
            emails = extract_emails(search_text)
            phones = extract_phones(search_text)

            leads.append({
                "source_site": "ZipRecruiter",
                "region": region,
                "post_date": post_date,
                "days_posted": days_posted,
                "title": f"[Hiring] {title} — {company_name}",
                "post_description": description,
                "author": "",
                "company_name": company_name,
                "link": link,
                "email": emails,
                "phone": phones,
                "social_media": [],
                "category": "Hiring signal",
                "lead_type": "Hiring signal",
                "raw_location": raw_location,
                "job_query": query,
                "score": 0,
                "tags": ["Hiring signal"],
                "contacts": [],
            })

        except Exception as exc:  # noqa: BLE001
            logger.warning("[JobSignals/ZipRecruiter] Card parse error on %s: %s", page_url, exc)

    return leads


def collect_ziprecruiter(
    queries: List[str],
    regions: Dict[str, str],
    session: requests.Session,
) -> List[Dict[str, Any]]:
    """Collect job-signal leads from ZipRecruiter.

    Args:
        queries: List of job search strings.
        regions: Dict mapping region codes to location strings.
        session: Shared requests Session.

    Returns:
        List of lead dicts.
    """
    all_leads: List[Dict[str, Any]] = []

    for region_code, location in regions.items():
        for query in queries:
            for page in range(1, MAX_PAGES + 1):
                url = _build_ziprecruiter_url(query, location, page)
                logger.info(
                    "[ZipRecruiter] query=%r region=%s page=%d → %s",
                    query, region_code, page, url,
                )

                resp = _fetch_with_retry(url, session, referer="https://www.ziprecruiter.com/")
                if resp is None:
                    logger.warning("[ZipRecruiter] No response. Skipping.")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                page_leads = _parse_ziprecruiter_page(soup, query, region_code, location, url)
                all_leads.extend(page_leads)
                logger.info("[ZipRecruiter] page=%d → %d leads", page, len(page_leads))

                if not page_leads:
                    break

                time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

    return all_leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def collect(
    regions: Optional[Dict[str, str]] = None,
    queries: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Collect job-posting signals from Indeed and ZipRecruiter.

    Args:
        regions: Region code → location string mapping.
                 Defaults to all four target regions.
        queries: Job search strings. Defaults to ``JOB_QUERIES``.

    Returns:
        Combined list of Hiring Signal lead dicts from both sources.
    """
    if regions is None:
        regions = REGIONS
    if queries is None:
        queries = JOB_QUERIES

    all_leads: List[Dict[str, Any]] = []

    with requests.Session() as session:
        try:
            indeed_leads = collect_indeed(queries, regions, session)
            logger.info("[JobSignals] Indeed: %d leads.", len(indeed_leads))
            all_leads.extend(indeed_leads)
        except Exception as exc:  # noqa: BLE001
            logger.error("[JobSignals] Indeed collection error: %s", exc)

        try:
            zip_leads = collect_ziprecruiter(queries, regions, session)
            logger.info("[JobSignals] ZipRecruiter: %d leads.", len(zip_leads))
            all_leads.extend(zip_leads)
        except Exception as exc:  # noqa: BLE001
            logger.error("[JobSignals] ZipRecruiter collection error: %s", exc)

    logger.info("[JobSignals] Total hiring signal leads: %d", len(all_leads))
    return all_leads


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = collect()
    print(f"\nCollected {len(results)} job signal leads.")
    for lead in results[:5]:
        print(f"  [{lead['region']}] {lead['title']} | {lead['company_name']}")
