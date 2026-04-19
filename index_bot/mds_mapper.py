"""
mds_mapper.py — MDS Building Code Mapper
Camelot Property Management Services Corp / Index Bot

Maps building addresses and names to MDS building codes.
Generates standardized filenames using the Camelot naming convention.

Naming format: {MDS_CODE}_{DOC_TYPE}_{YYYY-MM-DD}_{VERSION}.{ext}
Example: 552_LEASE_2026-04-01_v1.pdf

Author: Camelot OS
"""

import logging
import os
import re
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MDS Building Codes — Canonical Registry
# Add all Camelot buildings here. Key = address/name variant, Value = MDS code.
# Multiple keys can map to the same code (aliases).
# ---------------------------------------------------------------------------

BUILDING_CODES: dict[str, str] = {
    # Building 552 — Pilot
    "552": "552",
    "building 552": "552",
    "552 main": "552",
    # Add actual address when confirmed:
    # "552 [street name], [borough], ny": "552",

    # Placeholder entries — expand with actual portfolio
    "100 camelot": "100",
    "building 100": "100",

    "200 camelot": "200",
    "building 200": "200",

    "300 camelot": "300",
    "building 300": "300",

    "400 camelot": "400",
    "building 400": "400",

    "500 camelot": "500",
    "building 500": "500",

    # Format: "full address lowercase": "MDS_CODE"
    # "123 grand concourse, bronx, ny": "GC123",
    # "456 bedford avenue, brooklyn, ny": "BD456",
}

# Document type codes (canonical)
DOCUMENT_TYPES = {
    "LEASE",
    "INVOICE",
    "PERMIT",
    "VIOLATION",
    "REPORT",
    "FINANCIAL",
    "CORRESPONDENCE",
    "INSURANCE",
    "CONTRACT",
    "CO",
}

# Document type keyword matchers → doc type code
DOC_TYPE_RULES: list[tuple[list[str], str]] = [
    (["lease", "rental agreement", "tenancy", "occupancy agreement", "sublease"], "LEASE"),
    (["invoice", "bill", "receipt", "statement", "charge", "payable", "utility"], "INVOICE"),
    (["permit", "dob permit", "work permit", "construction permit", "alteration"], "PERMIT"),
    (["violation", "notice of violation", "nov", "hpd", "ecb", "dob violation", "summons"], "VIOLATION"),
    (["inspection report", "engineering report", "assessment", "survey", "phase i", "phase ii",
      "boiler inspection", "elevator inspection", "facade report", "fisp"], "REPORT"),
    (["profit and loss", "p&l", "rent roll", "bank statement", "financial statement",
      "budget", "income statement", "balance sheet", "operating statement"], "FINANCIAL"),
    (["certificate of insurance", "coi", "insurance policy", "policy", "rider", "endorsement"], "INSURANCE"),
    (["certificate of occupancy", "c of o", "co ", "temporary co", "tco"], "CO"),
    (["contract", "agreement", "vendor", "service agreement", "maintenance contract",
      "management agreement", "operating agreement"], "CONTRACT"),
    (["letter", "correspondence", "notice", "memo", "email", "legal notice",
      "marshal", "court", "attorney"], "CORRESPONDENCE"),
]


# ---------------------------------------------------------------------------
# Core: MDS code lookup
# ---------------------------------------------------------------------------

def get_mds_code(
    address_or_name: str,
    fuzzy: bool = True,
    fuzzy_threshold: float = 0.6,
) -> Optional[str]:
    """
    Fuzzy-match an address or building name to an MDS building code.

    Args:
        address_or_name:    Address string or building name to look up.
        fuzzy:              If True, try fuzzy matching when exact match fails.
        fuzzy_threshold:    Minimum similarity ratio for fuzzy match (0–1).

    Returns:
        MDS code string if found, None otherwise.
    """
    if not address_or_name:
        return None

    # Normalize input
    normalized = _normalize_address(address_or_name)

    # Exact match first
    if normalized in BUILDING_CODES:
        code = BUILDING_CODES[normalized]
        logger.debug(f"Exact match: '{normalized}' → MDS {code}")
        return code

    # Partial containment match (e.g., just a number like "552")
    for key, code in BUILDING_CODES.items():
        if normalized in key or key in normalized:
            logger.debug(f"Partial match: '{normalized}' ⊆ '{key}' → MDS {code}")
            return code

    # Fuzzy match
    if fuzzy:
        best_match = None
        best_score = 0.0

        for key in BUILDING_CODES:
            score = _similarity(normalized, key)
            if score > best_score:
                best_score = score
                best_match = key

        if best_match and best_score >= fuzzy_threshold:
            code = BUILDING_CODES[best_match]
            logger.info(
                f"Fuzzy match: '{normalized}' → '{best_match}' "
                f"(score={best_score:.2f}) → MDS {code}"
            )
            return code

        logger.warning(
            f"No MDS code match for: '{address_or_name}' "
            f"(best fuzzy score: {best_score:.2f})"
        )

    return None


def classify_document_type(
    filename: str,
    content_hint: Optional[str] = None,
) -> str:
    """
    Classify a document's type from its filename and optional content hint.

    Args:
        filename:      Original filename (with or without extension)
        content_hint:  Optional text extracted from document content or description.

    Returns:
        DOC_TYPE string (e.g., "LEASE", "INVOICE"). Defaults to "CORRESPONDENCE".
    """
    # Check if filename already has a valid MDS doc type
    name_upper = filename.upper()
    for doc_type in DOCUMENT_TYPES:
        if f"_{doc_type}_" in name_upper or name_upper.startswith(doc_type):
            return doc_type

    # Check against classification rules
    search_text = (filename + " " + (content_hint or "")).lower()

    for keywords, doc_type in DOC_TYPE_RULES:
        if any(kw in search_text for kw in keywords):
            logger.debug(f"Doc type classified as {doc_type} for: {filename}")
            return doc_type

    # Extension-based fallback
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    ext_type_map = {
        "pdf": "CORRESPONDENCE",
        "docx": "CORRESPONDENCE",
        "xlsx": "FINANCIAL",
        "csv": "FINANCIAL",
        "png": "CORRESPONDENCE",
        "jpg": "CORRESPONDENCE",
        "jpeg": "CORRESPONDENCE",
    }
    doc_type = ext_type_map.get(ext, "CORRESPONDENCE")
    logger.debug(f"Doc type defaulted to {doc_type} (ext: .{ext}) for: {filename}")
    return doc_type


def generate_filename(
    mds_code: str,
    doc_type: str,
    doc_date: Optional[date] = None,
    version: int = 1,
    original_filename: Optional[str] = None,
) -> str:
    """
    Generate a standardized MDS filename.

    Args:
        mds_code:          MDS building code (e.g., "552")
        doc_type:          Document type code (e.g., "LEASE")
        doc_date:          Document date (defaults to today)
        version:           Version number (1, 2, 3...)
        original_filename: Original filename (used to preserve extension)

    Returns:
        Standardized filename string (e.g., "552_LEASE_2026-04-01_v1.pdf")
    """
    # Validate doc type
    doc_type_upper = doc_type.upper().strip()
    if doc_type_upper not in DOCUMENT_TYPES:
        logger.warning(f"Unknown doc type '{doc_type}' — using CORRESPONDENCE")
        doc_type_upper = "CORRESPONDENCE"

    # Validate MDS code
    mds_clean = _sanitize_mds_code(mds_code)

    # Date
    doc_date = doc_date or date.today()
    date_str = doc_date.strftime("%Y-%m-%d")

    # Extension from original filename
    ext = ""
    if original_filename and "." in original_filename:
        ext = "." + original_filename.rsplit(".", 1)[-1].lower()

    filename = f"{mds_clean}_{doc_type_upper}_{date_str}_v{version}{ext}"
    logger.debug(f"Generated filename: {filename}")
    return filename


def suggest_filename_from_original(
    original_filename: str,
    content_hint: Optional[str] = None,
    default_mds_code: Optional[str] = None,
    doc_date: Optional[date] = None,
) -> dict:
    """
    Suggest a complete MDS filename from an original unstructured filename.

    Args:
        original_filename:  Original file name (e.g., "john_invoice_march.pdf")
        content_hint:       Optional extracted text or description
        default_mds_code:   MDS code to use if can't be determined from filename
        doc_date:           Override date (otherwise extracted from filename or today)

    Returns:
        Dict with: mds_code, doc_type, doc_date, version, suggested_filename, confidence
    """
    # Try to extract MDS code from filename
    mds_code = None
    for key in BUILDING_CODES:
        if key in original_filename.lower():
            mds_code = BUILDING_CODES[key]
            break

    if not mds_code:
        mds_code = default_mds_code or get_mds_code(original_filename)

    # Classify doc type
    doc_type = classify_document_type(original_filename, content_hint)

    # Extract date from filename if possible
    if not doc_date:
        doc_date = _extract_date_from_filename(original_filename)

    # Confidence
    confidence = "high" if mds_code and doc_type != "CORRESPONDENCE" else \
                 "medium" if mds_code else "low"

    suggested = generate_filename(
        mds_code=mds_code or "UNKNOWN",
        doc_type=doc_type,
        doc_date=doc_date,
        version=1,
        original_filename=original_filename,
    )

    return {
        "mds_code": mds_code,
        "doc_type": doc_type,
        "doc_date": doc_date.isoformat() if doc_date else date.today().isoformat(),
        "version": 1,
        "suggested_filename": suggested,
        "confidence": confidence,
        "original_filename": original_filename,
    }


# ---------------------------------------------------------------------------
# Portfolio-level helpers
# ---------------------------------------------------------------------------

def list_all_buildings() -> list[dict]:
    """Return all buildings with their MDS codes."""
    seen = {}
    for address, code in BUILDING_CODES.items():
        if code not in seen:
            seen[code] = {"mds_code": code, "aliases": []}
        seen[code]["aliases"].append(address)
    return sorted(seen.values(), key=lambda b: b["mds_code"])


def add_building(mds_code: str, addresses: list[str]) -> None:
    """
    Add a new building to the BUILDING_CODES registry at runtime.
    Note: Changes are not persisted — update mds_mapper.py to make permanent.
    """
    clean_code = _sanitize_mds_code(mds_code)
    for addr in addresses:
        normalized = _normalize_address(addr)
        BUILDING_CODES[normalized] = clean_code
    logger.info(f"Added building MDS {clean_code} with {len(addresses)} address(es) — not persisted")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_address(address: str) -> str:
    """Normalize an address string for matching."""
    return re.sub(r"\s+", " ", address.lower().strip())


def _sanitize_mds_code(code: str) -> str:
    """Sanitize MDS code — alphanumeric and hyphens only."""
    return re.sub(r"[^A-Za-z0-9\-]", "", code.strip()).upper()


def _similarity(a: str, b: str) -> float:
    """
    Compute Jaccard similarity between two strings (token-based).
    Simple and fast; no external dependencies.
    """
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a and not tokens_b:
        return 1.0
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def _extract_date_from_filename(filename: str) -> Optional[date]:
    """Extract a date from a filename using common patterns."""
    patterns = [
        r"(\d{4})[-_\.](\d{2})[-_\.](\d{2})",    # 2026-04-01
        r"(\d{2})[-_\.](\d{2})[-_\.](\d{4})",    # 04-01-2026 or 01-04-2026
        r"(\d{4})(\d{2})(\d{2})",                  # 20260401
    ]
    for pattern in patterns:
        m = re.search(pattern, filename)
        if m:
            try:
                y, mon, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if y < 100:  # MM-DD-YYYY pattern
                    y, mon, d = int(m.group(3)), int(m.group(1)), int(m.group(2))
                if 1 <= mon <= 12 and 1 <= d <= 31 and 2000 <= y <= 2100:
                    return date(y, mon, d)
            except (ValueError, TypeError):
                continue
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        code = get_mds_code(query)
        print(f"MDS code for '{query}': {code}")
    else:
        # Demo
        test_files = [
            "john_invoice_march_2026.pdf",
            "lease_signed_unit4b.pdf",
            "HPD_violation_552.pdf",
            "building552_rent_roll_Q1_2026.xlsx",
            "scan001.pdf",
        ]
        for f in test_files:
            suggestion = suggest_filename_from_original(f, default_mds_code="552")
            print(f"{f!r:50} → {suggestion['suggested_filename']!r}  [{suggestion['confidence']}]")
