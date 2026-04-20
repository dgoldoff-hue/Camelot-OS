"""
utils/filters.py
----------------
Lead deduplication, quality scoring, filtering, and tagging for Scout Bot.

All functions operate on the canonical lead schema dict defined in the
project README / main.py.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Type alias for readability
Lead = Dict[str, Any]


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(leads: List[Lead]) -> List[Lead]:
    """Remove duplicate leads from *leads*.

    Deduplication strategy (applied in order):
    1. If a lead has a non-empty ``link`` field, deduplicate by URL.
    2. For leads without a link, deduplicate by the composite key
       ``(company_name.lower(), phone[0])`` if both are present.
    3. If neither criterion applies, the lead is kept as-is.

    The *first* occurrence is retained; subsequent duplicates are discarded.

    Args:
        leads: List of lead dicts (may be from multiple collectors).

    Returns:
        Deduplicated list preserving original order of first occurrences.
    """
    seen_links: set = set()
    seen_composite: set = set()
    result: List[Lead] = []

    for lead in leads:
        link: str = (lead.get("link") or "").strip().rstrip("/")
        company: str = (lead.get("company_name") or "").strip().lower()
        phones: List[str] = lead.get("phone") or []
        first_phone: str = phones[0].strip() if phones else ""

        if link:
            if link in seen_links:
                logger.debug("Dedup (link): %s", link)
                continue
            seen_links.add(link)
        elif company and first_phone:
            composite = (company, first_phone)
            if composite in seen_composite:
                logger.debug("Dedup (composite): %s | %s", company, first_phone)
                continue
            seen_composite.add(composite)

        result.append(lead)

    removed = len(leads) - len(result)
    if removed:
        logger.info("Deduplication removed %d duplicate lead(s). %d remain.", removed, len(result))

    return result


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

# Score weights — must sum to 100 for a "perfect" lead
_SCORE_HAS_EMAIL = 20
_SCORE_HAS_PHONE = 20
_SCORE_HAS_COMPANY = 15
_SCORE_RECENT_POST = 20       # ≤ 7 days old
_SCORE_ACQUISITION_TYPE = 25  # lead_type == "Acquisition"
_SCORE_RFP_TYPE = 20          # category == "RFP"


def score_lead(lead: Lead) -> int:
    """Compute a quality/priority score (0–100) for a single lead.

    Scoring criteria:
    - has_email      +20  (at least one email present)
    - has_phone      +20  (at least one phone present)
    - has_company    +15  (company_name is non-empty)
    - recent_post    +20  (post_date is within 7 days)
    - acquisition    +25  (lead_type == "Acquisition")
    - rfp            +20  (category == "RFP")

    Note: weights can push the theoretical max above 100 for a lead that
    is both an acquisition *and* an RFP; the score is capped at 100.

    Args:
        lead: A lead dict matching the Scout schema.

    Returns:
        Integer score in [0, 100].
    """
    score = 0

    # Email
    emails: List[str] = lead.get("email") or []
    if any(e for e in emails if e):
        score += _SCORE_HAS_EMAIL

    # Phone
    phones: List[str] = lead.get("phone") or []
    if any(p for p in phones if p):
        score += _SCORE_HAS_PHONE

    # Company name
    if (lead.get("company_name") or "").strip():
        score += _SCORE_HAS_COMPANY

    # Recency
    post_date: Optional[date] = lead.get("post_date")
    if post_date:
        try:
            days_old = (date.today() - post_date).days
            if days_old <= 7:
                score += _SCORE_RECENT_POST
        except Exception:
            pass

    # Lead type / category bonuses
    lead_type: str = (lead.get("lead_type") or "").strip()
    category: str = (lead.get("category") or "").strip()

    if lead_type.lower() == "acquisition":
        score += _SCORE_ACQUISITION_TYPE
    if category.lower() == "rfp":
        score += _SCORE_RFP_TYPE

    return min(score, 100)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def filter_leads(leads: List[Lead], min_score: int = 40) -> List[Lead]:
    """Keep only leads whose score meets or exceeds *min_score*.

    Each lead's ``score`` field is updated in-place by this function
    (re-computed via :func:`score_lead`) so the stored value is always
    current at filter time.

    Args:
        leads: List of lead dicts.
        min_score: Minimum acceptable score (default 40).

    Returns:
        Filtered list sorted by score descending.
    """
    scored: List[Lead] = []
    for lead in leads:
        s = score_lead(lead)
        lead["score"] = s
        if s >= min_score:
            scored.append(lead)

    scored.sort(key=lambda l: l["score"], reverse=True)
    logger.info(
        "filter_leads: %d/%d leads passed min_score=%d",
        len(scored), len(leads), min_score,
    )
    return scored


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

# Keyword sets for angle detection
_SUCCESSION_KEYWORDS = frozenset([
    "retire", "retirement", "succession", "exit", "seller financing",
    "seller carry", "transition", "owner retiring", "family business",
    "estate sale",
])

_ACQUISITION_KEYWORDS = frozenset([
    "business for sale", "acquisition", "buy", "purchase", "portfolio",
    "book of business", "management company for sale",
])

_MANAGEMENT_KEYWORDS = frozenset([
    "management mandate", "managing agent", "rfp", "request for proposal",
    "rfq", "bid", "contract", "building management", "property management services",
    "asset management",
])

_UNMANAGED_KEYWORDS = frozenset([
    "self-managed", "self managed", "unmanaged", "no managing agent",
    "hpd", "violation", "open violation",
])

_HIRING_KEYWORDS = frozenset([
    "hiring", "job posting", "open position", "property manager wanted",
    "seeking property manager", "hiring signal",
])


def tag_lead(lead: Lead) -> Lead:
    """Add or update the ``tags`` and ``lead_type`` fields on *lead*.

    Tagging is based on keyword matching across ``title``,
    ``post_description``, ``category``, and ``lead_type`` fields.
    Multiple tags may be assigned; a primary ``lead_type`` is chosen
    from the highest-priority match.

    Priority order: Acquisition > Management mandate > Succession > 
                    Hiring signal > Unmanaged

    Args:
        lead: A lead dict (mutated in place).

    Returns:
        The mutated lead dict (for chaining convenience).
    """
    # Build a single lowercase search corpus
    corpus_parts = [
        lead.get("title") or "",
        lead.get("post_description") or "",
        lead.get("category") or "",
        lead.get("lead_type") or "",
    ]
    corpus = " ".join(corpus_parts).lower()

    tags: List[str] = list(lead.get("tags") or [])
    matched_types: List[str] = []

    def _any_kw(keywords: frozenset) -> bool:
        return any(kw in corpus for kw in keywords)

    if _any_kw(_ACQUISITION_KEYWORDS):
        if "Acquisition" not in tags:
            tags.append("Acquisition")
        matched_types.append("Acquisition")

    if _any_kw(_MANAGEMENT_KEYWORDS):
        if "Management mandate" not in tags:
            tags.append("Management mandate")
        matched_types.append("Management mandate")

    if _any_kw(_SUCCESSION_KEYWORDS):
        if "Succession" not in tags:
            tags.append("Succession")
        matched_types.append("Succession")

    if _any_kw(_HIRING_KEYWORDS):
        if "Hiring signal" not in tags:
            tags.append("Hiring signal")
        matched_types.append("Hiring signal")

    if _any_kw(_UNMANAGED_KEYWORDS):
        if "Unmanaged" not in tags:
            tags.append("Unmanaged")
        matched_types.append("Unmanaged")

    lead["tags"] = tags

    # Set lead_type only if not already assigned (collectors may set it)
    if not lead.get("lead_type") and matched_types:
        # Map tag to canonical lead_type string
        type_map = {
            "Acquisition": "Acquisition",
            "Management mandate": "Management mandate",
            "Succession": "Succession",
            "Hiring signal": "Hiring signal",
            "Unmanaged": "Unmanaged",
        }
        lead["lead_type"] = type_map.get(matched_types[0], matched_types[0])

    return lead


# ---------------------------------------------------------------------------
# Convenience pipeline
# ---------------------------------------------------------------------------

def process_leads(leads: List[Lead], min_score: int = 40) -> List[Lead]:
    """Full pipeline: tag → deduplicate → score → filter.

    Convenience wrapper that applies all processing steps in the
    recommended order.

    Args:
        leads: Raw list of lead dicts from collectors.
        min_score: Minimum score threshold to pass filtering.

    Returns:
        Processed, deduplicated, scored, and filtered list of leads.
    """
    # Tag first so scoring can leverage lead_type/category
    for lead in leads:
        tag_lead(lead)

    deduped = deduplicate(leads)
    filtered = filter_leads(deduped, min_score=min_score)
    return filtered
