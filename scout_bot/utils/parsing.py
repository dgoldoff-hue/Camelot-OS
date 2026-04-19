"""
utils/parsing.py
----------------
Production-grade text parsing utilities for Scout Bot.

Provides:
- Email extraction (RFC-5321 compliant with practical edge-case coverage)
- US phone number extraction
- HTML cleaning
- Relative/absolute post-date parsing
- Address normalization
"""

import re
import logging
from datetime import date, timedelta
from typing import List, Optional
from html.parser import HTMLParser

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regular expressions
# ---------------------------------------------------------------------------

# Email regex: handles subdomains, plus-addressing, quoted locals, etc.
EMAIL_REGEX = re.compile(
    r"""
    (?:[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+           # local part
        (?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*    # dotted parts
    |"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]   # quoted string
          |\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")
    @
    (?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?  # domain label
        \.)+
    [a-zA-Z]{2,}                                         # TLD
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Phone regex: matches the major US formats with/without country code
PHONE_REGEX = re.compile(
    r"""
    (?:(?:\+?1[\s.\-]?)?              # optional country code
        (?:\(?\d{3}\)?[\s.\-]?)       # area code
        \d{3}[\s.\-]?\d{4}            # subscriber number
    )
    """,
    re.VERBOSE,
)

# Relative-date pattern fragments
_REL_DATE_RE = re.compile(
    r"(?:posted\s+)?(\d+)\s+(day|week|month|hour)s?\s+ago",
    re.IGNORECASE,
)

# Absolute date patterns: MM/DD/YYYY, YYYY-MM-DD, Month DD YYYY, etc.
_ABS_DATE_PATTERNS = [
    re.compile(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})"),          # M/D/YYYY or M-D-YYYY
    re.compile(r"(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})"),          # YYYY-M-D
    re.compile(
        r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|"
        r"May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|"
        r"Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+(\d{1,2}),?\s+(\d{4})",
        re.IGNORECASE,
    ),
]

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# ---------------------------------------------------------------------------
# HTML stripping helper
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Minimal HTMLParser subclass that accumulates text content."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: List[str] = []

    def handle_data(self, data: str) -> None:  # noqa: D102
        self._parts.append(data)

    def get_text(self) -> str:  # noqa: D102
        return " ".join(self._parts)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Strip HTML tags, decode entities, and normalize whitespace.

    Args:
        text: Raw string that may contain HTML markup.

    Returns:
        Plain-text string with collapsed whitespace.
    """
    if not text:
        return ""
    stripper = _HTMLStripper()
    try:
        stripper.feed(text)
        cleaned = stripper.get_text()
    except Exception:
        # Fallback: crude tag-stripping regex
        cleaned = re.sub(r"<[^>]+>", " ", text)
    # Collapse runs of whitespace (including \n, \t)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def extract_emails(text: str) -> List[str]:
    """Return a deduplicated list of email addresses found in *text*.

    Args:
        text: Any string (HTML or plain-text OK; HTML is stripped first).

    Returns:
        List of lowercase email strings, preserving discovery order.
    """
    plain = clean_text(text)
    found = EMAIL_REGEX.findall(plain)
    seen: set = set()
    result: List[str] = []
    for email in found:
        normalized = email.lower().strip(".")
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def extract_phones(text: str) -> List[str]:
    """Return a deduplicated list of US phone number strings found in *text*.

    Normalizes each match to the format ``(NXX) NXX-XXXX``.

    Args:
        text: Any string.

    Returns:
        List of formatted phone strings, preserving discovery order.
    """
    plain = clean_text(text)
    matches = PHONE_REGEX.findall(plain)
    seen: set = set()
    result: List[str] = []
    for raw in matches:
        digits = re.sub(r"\D", "", raw)
        # Strip leading country code "1" if 11 digits
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            continue
        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if formatted not in seen:
            seen.add(formatted)
            result.append(formatted)
    return result


def parse_post_date(text: str) -> Optional[date]:
    """Parse a posting date from a human-readable string.

    Handles formats such as:
    - ``"Posted 3 days ago"``
    - ``"2 weeks ago"``
    - ``"4/15/2026"``
    - ``"April 15, 2026"``
    - ``"2026-04-15"``

    Args:
        text: Raw date string from a listing page.

    Returns:
        A :class:`datetime.date` object, or ``None`` if unparseable.
    """
    if not text:
        return None

    text = text.strip()
    today = date.today()

    # --- Relative dates ---
    rel_match = _REL_DATE_RE.search(text)
    if rel_match:
        amount = int(rel_match.group(1))
        unit = rel_match.group(2).lower()
        if unit == "hour":
            return today
        if unit == "day":
            return today - timedelta(days=amount)
        if unit == "week":
            return today - timedelta(weeks=amount)
        if unit == "month":
            return today - timedelta(days=amount * 30)

    # --- Absolute dates ---
    # M/D/YYYY
    m = _ABS_DATE_PATTERNS[0].search(text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass

    # YYYY-M-D
    m = _ABS_DATE_PATTERNS[1].search(text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # Month DD, YYYY
    m = _ABS_DATE_PATTERNS[2].search(text)
    if m:
        month_str = m.group(1)[:3].lower()
        month_num = _MONTH_MAP.get(month_str)
        if month_num:
            try:
                return date(int(m.group(3)), month_num, int(m.group(2)))
            except ValueError:
                pass

    logger.debug("Could not parse date from: %r", text)
    return None


def normalize_address(addr: str) -> str:
    """Normalize a US postal address string.

    - Strips leading/trailing whitespace
    - Collapses internal whitespace
    - Title-cases words while preserving directional abbreviations (N, S, E, W, NE, etc.)
    - Normalizes common unit abbreviations: ``Apt``, ``Ste``, ``Unit``, ``Fl``

    Args:
        addr: Raw address string.

    Returns:
        Normalized address string.
    """
    if not addr:
        return ""

    addr = re.sub(r"\s+", " ", addr).strip()

    # Directionals that should remain uppercase
    directionals = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}

    # Unit keyword normalization map
    unit_map = {
        r"\bapt\.?(?:\s|$)": "Apt ",
        r"\bste\.?(?:\s|$)": "Ste ",
        r"\bunit\.?(?:\s|$)": "Unit ",
        r"\bfl\.?(?:\s|$)": "Fl ",
        r"\bfloor\.?(?:\s|$)": "Floor ",
    }

    words = addr.split()
    normalized_words: List[str] = []
    for word in words:
        clean_word = word.strip(",")
        suffix = word[len(clean_word):]  # trailing punctuation
        if clean_word.upper() in directionals:
            normalized_words.append(clean_word.upper() + suffix)
        else:
            normalized_words.append(clean_word.title() + suffix)

    result = " ".join(normalized_words)

    # Apply unit keyword normalization
    for pattern, replacement in unit_map.items():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    return result.strip()
