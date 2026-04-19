"""
listing_analyzer.py — LoopNet/CoStar Listing Parser

Extracts key financial and physical attributes from commercial real estate
listing pages (LoopNet primary target). Returns a structured listing dict.

Author: Camelot OS / Broker Bot
"""

import logging
import re
import time
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Rotate UA to reduce bot detection friction
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
_ua_index = 0


def _get_session() -> requests.Session:
    global _ua_index
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": USER_AGENTS[_ua_index % len(USER_AGENTS)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    _ua_index += 1
    return session


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------

def parse_loopnet_listing(url: str, delay_seconds: float = 1.5) -> dict:
    """
    Parse a LoopNet property listing page and extract key attributes.

    Args:
        url:            Full LoopNet listing URL.
        delay_seconds:  Polite delay before fetching (rate limit courtesy).

    Returns:
        Dict with keys: url, address, asking_price, noi, cap_rate,
        unit_count, year_built, building_sqft, lot_sqft, zoning,
        asset_type, property_class, description, red_flags, raw_attributes.
        Values are None where not found.
    """
    time.sleep(delay_seconds)
    session = _get_session()

    result = _empty_listing(url)

    try:
        resp = session.get(url, timeout=20)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if resp.status_code == 403:
            logger.warning(f"LoopNet returned 403 (may require login): {url}")
            result["error"] = "403 Forbidden — LoopNet requires authentication for this listing."
        else:
            logger.error(f"HTTP error fetching {url}: {e}")
            result["error"] = str(e)
        return result
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        result["error"] = str(e)
        return result

    soup = BeautifulSoup(resp.text, "html.parser")
    page_text = soup.get_text(separator=" ", strip=True)

    # Address
    result["address"] = _extract_address(soup)

    # Asking price
    result["asking_price"] = _extract_price(soup, page_text)

    # NOI
    result["noi"] = _extract_noi(page_text)

    # Cap rate
    result["cap_rate"] = _extract_cap_rate(page_text)

    # Unit count
    result["unit_count"] = _extract_units(page_text)

    # Year built
    result["year_built"] = _extract_year_built(page_text)

    # Building square footage
    result["building_sqft"] = _extract_sqft(page_text, "building")

    # Lot size
    result["lot_sqft"] = _extract_sqft(page_text, "lot")

    # Asset type / property type
    result["asset_type"] = _extract_asset_type(soup, page_text)

    # Property class (Class A/B/C)
    result["property_class"] = _extract_property_class(page_text)

    # Zoning
    result["zoning"] = _extract_zoning(page_text)

    # Description
    result["description"] = _extract_description(soup)

    # All raw key-value pairs from listing details table
    result["raw_attributes"] = _extract_raw_attributes(soup)

    # Merge any additional values from raw_attributes
    _merge_from_raw(result)

    # Flag potential red flags
    result["red_flags"] = _detect_red_flags(page_text, result)

    logger.info(
        f"Parsed listing: {result['address']} | "
        f"Asking: {result['asking_price']} | Cap: {result['cap_rate']} | "
        f"Units: {result['unit_count']} | Flags: {len(result['red_flags'])}"
    )
    return result


def parse_generic_listing(url: str) -> dict:
    """
    Generic listing parser for non-LoopNet real estate sites.
    Uses heuristic extraction from page text.
    """
    return parse_loopnet_listing(url)  # Same logic applies generically


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------

def _empty_listing(url: str) -> dict:
    return {
        "url": url,
        "address": None,
        "asking_price": None,
        "noi": None,
        "cap_rate": None,
        "unit_count": None,
        "year_built": None,
        "building_sqft": None,
        "lot_sqft": None,
        "asset_type": None,
        "property_class": None,
        "zoning": None,
        "description": None,
        "red_flags": [],
        "raw_attributes": {},
        "error": None,
    }


def _extract_address(soup: BeautifulSoup) -> Optional[str]:
    for selector in [
        "h1.property-address",
        "h1[class*='address']",
        "[class*='property-address']",
        "[data-testid='property-address']",
        "h1",
    ]:
        el = soup.select_one(selector)
        if el and el.text.strip():
            addr = el.text.strip()
            # Validate it looks like an address (has a number)
            if re.search(r"\d", addr):
                return addr
    return None


def _extract_price(soup: BeautifulSoup, text: str) -> Optional[float]:
    # Try structured elements first
    for selector in ["[class*='price']", "[class*='asking']", "[class*='list-price']"]:
        el = soup.select_one(selector)
        if el:
            val = _parse_dollar(el.text)
            if val and val > 100_000:
                return val
    # Fallback: regex on page text
    # Patterns: $4,200,000 | $4.2M | $4.2 Million | Asking Price: $4,200,000
    patterns = [
        r"[Aa]sking\s*[Pp]rice[:\s]+\$?([\d,\.]+)\s*(M(?:illion)?)?",
        r"[Ll]ist(?:ing)?\s*[Pp]rice[:\s]+\$?([\d,\.]+)\s*(M(?:illion)?)?",
        r"\$\s*([\d,\.]+)\s*(M(?:illion)?|B(?:illion)?)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            val = _parse_dollar_match(m.group(1), m.group(2) if m.lastindex >= 2 else None)
            if val and val > 100_000:
                return val
    return None


def _extract_noi(text: str) -> Optional[float]:
    patterns = [
        r"NOI[:\s]+\$?([\d,\.]+)\s*(M(?:illion)?)?",
        r"[Nn]et\s+[Oo]perating\s+[Ii]ncome[:\s]+\$?([\d,\.]+)\s*(M(?:illion)?)?",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            val = _parse_dollar_match(m.group(1), m.group(2) if m.lastindex >= 2 else None)
            if val:
                return val
    return None


def _extract_cap_rate(text: str) -> Optional[float]:
    patterns = [
        r"[Cc]ap\s+[Rr]ate[:\s]+([\d\.]+)\s*%",
        r"[Cc]apitalization\s+[Rr]ate[:\s]+([\d\.]+)\s*%",
        r"([\d\.]+)\s*%\s+[Cc]ap\s+[Rr]ate",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                val = float(m.group(1))
                if 0.5 < val < 25:  # sanity check — realistic cap rate range
                    return val
            except ValueError:
                pass
    return None


def _extract_units(text: str) -> Optional[int]:
    patterns = [
        r"(\d+)\s+[Uu]nits",
        r"[Uu]nit\s+[Cc]ount[:\s]+(\d+)",
        r"[Tt]otal\s+[Uu]nits[:\s]+(\d+)",
        r"(\d+)[- ][Uu]nit\s+[Bb]uilding",
        r"(\d+)\s+[Aa]partments",
        r"[Nn]umber\s+of\s+[Uu]nits[:\s]+(\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                pass
    return None


def _extract_year_built(text: str) -> Optional[int]:
    patterns = [
        r"[Yy]ear\s+[Bb]uilt[:\s]+(\d{4})",
        r"[Bb]uilt\s+[Ii]n[:\s]+(\d{4})",
        r"[Cc]onstructed[:\s]+(\d{4})",
        r"[Bb]uilt[:\s]+(\d{4})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                year = int(m.group(1))
                if 1850 < year < 2030:
                    return year
            except ValueError:
                pass
    return None


def _extract_sqft(text: str, mode: str = "building") -> Optional[int]:
    if mode == "building":
        patterns = [
            r"[Bb]uilding\s+[Ss]ize[:\s]+([\d,]+)\s*[Ss][Ff]",
            r"[Gg]ross\s+[Ss]q(?:uare)?\s*[Ff](?:eet|t)?[:\s]+([\d,]+)",
            r"[Tt]otal\s+[Ss]q(?:uare)?\s*[Ff](?:eet|t)?[:\s]+([\d,]+)",
            r"([\d,]+)\s*[Ss][Ff]\s+[Bb]uilding",
        ]
    else:
        patterns = [
            r"[Ll]ot\s+[Ss]ize[:\s]+([\d,]+)\s*[Ss][Ff]",
            r"[Ll]ot\s+[Aa]rea[:\s]+([\d,]+)\s*[Ss][Ff]",
        ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return None


def _extract_asset_type(soup: BeautifulSoup, text: str) -> Optional[str]:
    keywords = [
        "Multifamily", "Multi-Family", "Apartment", "Mixed-Use", "Mixed Use",
        "Office", "Retail", "Industrial", "Warehouse", "Hotel", "Motel",
        "Land", "Development Site", "Commercial", "Net Lease",
    ]
    for kw in keywords:
        if kw.lower() in text.lower():
            return kw
    return None


def _extract_property_class(text: str) -> Optional[str]:
    m = re.search(r"[Cc]lass\s+([ABC])\b", text)
    if m:
        return f"Class {m.group(1)}"
    return None


def _extract_zoning(text: str) -> Optional[str]:
    # NYC zoning patterns: R6A, C4-2, M1-1, etc.
    m = re.search(r"\b([RCDM]\d[A-Za-z0-9\-]{0,5})\b", text)
    if m:
        return m.group(1)
    return None


def _extract_description(soup: BeautifulSoup) -> Optional[str]:
    for selector in [
        "[class*='property-description']",
        "[class*='description-text']",
        "[class*='listing-description']",
        "div.description",
        "section.description",
    ]:
        el = soup.select_one(selector)
        if el:
            desc = el.get_text(separator=" ", strip=True)
            if len(desc) > 50:
                return desc[:2000]  # truncate to 2000 chars
    return None


def _extract_raw_attributes(soup: BeautifulSoup) -> dict:
    """Extract all key-value pairs from listing detail tables/dl elements."""
    attrs = {}
    # Try definition lists
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            val = dd.get_text(strip=True)
            if key:
                attrs[key] = val
    # Try table rows
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) == 2:
                key = cells[0].get_text(strip=True)
                val = cells[1].get_text(strip=True)
                if key:
                    attrs[key] = val
    return attrs


def _merge_from_raw(result: dict) -> None:
    """Back-fill None fields from raw_attributes if possible."""
    raw = result.get("raw_attributes", {})
    field_map = {
        "Cap Rate": "cap_rate",
        "NOI": "noi",
        "No. Units": "unit_count",
        "Total Units": "unit_count",
        "Year Built": "year_built",
        "Building Size": "building_sqft",
        "Lot Size": "lot_sqft",
        "Zoning": "zoning",
    }
    for raw_key, result_key in field_map.items():
        if result[result_key] is None and raw_key in raw:
            val_str = raw[raw_key]
            if result_key == "cap_rate":
                m = re.search(r"([\d\.]+)", val_str)
                if m:
                    try:
                        result[result_key] = float(m.group(1))
                    except ValueError:
                        pass
            elif result_key in ("unit_count", "year_built"):
                m = re.search(r"(\d+)", val_str)
                if m:
                    try:
                        result[result_key] = int(m.group(1))
                    except ValueError:
                        pass
            elif result_key in ("noi", "asking_price", "building_sqft", "lot_sqft"):
                val = _parse_dollar(val_str)
                if val:
                    result[result_key] = val


def _detect_red_flags(text: str, listing: dict) -> list[str]:
    """Identify potential red flags in listing text."""
    flags = []
    red_flag_patterns = [
        (r"[Vv]acant|[Vv]acancy\s+issue", "High vacancy or vacant building mentioned"),
        (r"[Ll]itigation|[Ll]awsuit|[Ll]egal\s+issue", "Legal/litigation issues mentioned"),
        (r"[Ee]nvironmental|[Pp]hase\s+[Ii][I]?", "Environmental concerns mentioned"),
        (r"[Dd]eferred\s+[Mm]aintenance|[Rr]enovation\s+needed|[Ff]ixer", "Deferred maintenance mentioned"),
        (r"[Vv]iolation|[Cc]itation|[Nn]on[- ][Cc]ompliant", "Code violations mentioned"),
        (r"[Ss]top\s+[Ww]ork|[Ss][Ww][Oo]", "Stop work order mentioned"),
        (r"[Ff]ood\s+[Ss]tamp|[Ss]ection\s+8|[Vv]oucher", "Subsidized housing — verify regulatory restrictions"),
        (r"[Aa]s[- ][Ii]s|[Aa]s\s+is\s+where\s+is", "As-is sale — likely issues"),
        (r"[Ff]oreclosure|[Rr][Ee][Oo]|[Bb]ank[- ][Oo]wned|[Ss]hort\s+[Ss]ale", "Distressed sale"),
        (r"[Ll]ead\s+[Pp]aint|[Aa]sbestos|[Mm]old|[Pp][Cc][Bb]", "Hazardous materials mentioned"),
    ]
    for pattern, message in red_flag_patterns:
        if re.search(pattern, text):
            flags.append(message)

    # Data quality flags
    if listing.get("cap_rate") is None and listing.get("noi") is None:
        flags.append("No NOI or cap rate data — financial analysis not possible from listing")
    if listing.get("year_built") and listing["year_built"] < 1960:
        flags.append(f"Older building ({listing['year_built']}) — likely lead/asbestos concerns; verify capital needs")

    return flags


# ---------------------------------------------------------------------------
# Dollar parsing helpers
# ---------------------------------------------------------------------------

def _parse_dollar(text: str) -> Optional[float]:
    """Parse a dollar string like '$4,200,000' or '4.2M' into float."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("$", "")
    m = re.match(r"([\d\.]+)\s*(M(?:illion)?|B(?:illion)?|K)?", text, re.IGNORECASE)
    if m:
        return _parse_dollar_match(m.group(1), m.group(2))
    return None


def _parse_dollar_match(num_str: str, multiplier: Optional[str]) -> Optional[float]:
    try:
        val = float(num_str.replace(",", ""))
        if multiplier:
            mul = multiplier.upper()
            if mul.startswith("B"):
                val *= 1_000_000_000
            elif mul.startswith("M"):
                val *= 1_000_000
            elif mul.startswith("K"):
                val *= 1_000
        return val
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# CLI / quick test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    # Pass a real LoopNet URL as a CLI arg for testing
    import sys
    test_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.loopnet.com/listing/test"
    result = parse_loopnet_listing(test_url)
    print(json.dumps(result, indent=2, default=str))
