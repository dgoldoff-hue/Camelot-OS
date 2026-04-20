"""
reports/csv_exporter.py
------------------------
CSV export utilities for Scout Bot lead data.

Provides:
  ``export_leads_csv(leads)``     → raw bytes of a CSV with one row per lead
  ``export_enriched_csv(leads)``  → raw bytes of a CSV with one row per contact per lead
                                    (contacts flattened; lead fields repeated on each row)

All list-type fields (email, phone, social_media, tags) are serialised as
pipe-delimited strings so they remain in a single cell.

Both functions return ``bytes`` so they can be directly attached to emails
or written to disk without intermediate files.
"""

import csv
import io
import logging
from datetime import date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema definition — the canonical ordered column list
# ---------------------------------------------------------------------------

# Core lead columns — matches the Scout schema
LEAD_COLUMNS: List[str] = [
    "source_site",
    "region",
    "post_date",
    "days_posted",
    "title",
    "company_name",
    "lead_type",
    "category",
    "score",
    "raw_location",
    "link",
    "email",
    "phone",
    "social_media",
    "tags",
    "author",
    "post_description",
    # Optional extended fields (present on some lead types)
    "asking_price",
    "revenue",
    "borough",
    "building_id",
    "unit_count",
    "open_violations",
    "managing_agent",
    "owner_name",
    "job_query",
    "market",
]

# Additional contact columns appended for enriched CSV
CONTACT_COLUMNS: List[str] = [
    "contact_name",
    "contact_title",
    "contact_email",
    "contact_phone",
    "contact_linkedin",
    "contact_company",
    "contact_source",
    "contact_seniority",
    "contact_city",
    "contact_state",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_str(value: Any, max_len: int = 2000) -> str:
    """Convert a value to a CSV-safe string.

    Lists are joined with ``|``; None becomes empty string;
    dates are formatted as ISO-8601; long strings are truncated.

    Args:
        value: Any field value from a lead dict.
        max_len: Maximum character length before truncation.

    Returns:
        String representation safe for CSV.
    """
    if value is None:
        return ""
    if isinstance(value, list):
        return " | ".join(str(v) for v in value if v is not None)
    if isinstance(value, date):
        return value.isoformat()
    s = str(value)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def _extract_lead_row(lead: Dict[str, Any], columns: List[str]) -> List[str]:
    """Extract ordered column values from a lead dict.

    Args:
        lead: Scout lead dict.
        columns: Ordered list of field names.

    Returns:
        List of string values in column order.
    """
    return [_safe_str(lead.get(col)) for col in columns]


def _extract_contact_row(contact: Dict[str, Any]) -> List[str]:
    """Extract contact fields into ordered string values.

    Args:
        contact: Enriched contact dict.

    Returns:
        List of string values matching ``CONTACT_COLUMNS``.
    """
    phone = contact.get("phone") or []
    if isinstance(phone, list):
        phone_str = " | ".join(str(p) for p in phone if p)
    else:
        phone_str = str(phone)

    return [
        _safe_str(contact.get("name")),
        _safe_str(contact.get("title")),
        _safe_str(contact.get("email")),
        phone_str,
        _safe_str(contact.get("linkedin_url")),
        _safe_str(contact.get("company")),
        _safe_str(contact.get("source")),
        _safe_str(contact.get("seniority")),
        _safe_str(contact.get("city")),
        _safe_str(contact.get("state")),
    ]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def export_leads_csv(leads: List[Dict[str, Any]]) -> bytes:
    """Export Scout leads to CSV bytes — one row per lead.

    All list fields (email, phone, tags, social_media) are serialised as
    pipe-delimited strings (``a@example.com | b@example.com``).

    The ``contacts`` field is excluded from this export; use
    :func:`export_enriched_csv` for contact-level data.

    Args:
        leads: List of Scout lead dicts.

    Returns:
        UTF-8 encoded CSV bytes with a header row.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    # Header
    writer.writerow(LEAD_COLUMNS)

    row_count = 0
    for lead in leads:
        try:
            writer.writerow(_extract_lead_row(lead, LEAD_COLUMNS))
            row_count += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "export_leads_csv: skipped lead '%s' due to error: %s",
                lead.get("title", "?"), exc,
            )

    logger.info("export_leads_csv: exported %d leads.", row_count)
    return buf.getvalue().encode("utf-8-sig")  # UTF-8 with BOM for Excel compatibility


def export_enriched_csv(leads: List[Dict[str, Any]]) -> bytes:
    """Export enriched Scout leads to CSV bytes — one row per contact per lead.

    Each row contains the full lead data repeated alongside a single contact's
    details. Leads with no enriched contacts produce one row with empty
    contact fields (so all leads appear in the export).

    Args:
        leads: List of Scout lead dicts with populated ``contacts`` fields.

    Returns:
        UTF-8 encoded CSV bytes with a header row.
    """
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")

    all_columns = LEAD_COLUMNS + CONTACT_COLUMNS

    # Header
    writer.writerow(all_columns)

    row_count = 0
    for lead in leads:
        try:
            lead_row_base = _extract_lead_row(lead, LEAD_COLUMNS)
            contacts: List[Dict[str, Any]] = lead.get("contacts") or []

            if contacts:
                for contact in contacts:
                    try:
                        contact_row = _extract_contact_row(contact)
                        writer.writerow(lead_row_base + contact_row)
                        row_count += 1
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "export_enriched_csv: skipped contact for lead '%s': %s",
                            lead.get("title", "?"), exc,
                        )
            else:
                # No contacts — write the lead with empty contact columns
                empty_contact = [""] * len(CONTACT_COLUMNS)
                writer.writerow(lead_row_base + empty_contact)
                row_count += 1

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "export_enriched_csv: skipped lead '%s': %s",
                lead.get("title", "?"), exc,
            )

    logger.info("export_enriched_csv: exported %d rows.", row_count)
    return buf.getvalue().encode("utf-8-sig")


# ---------------------------------------------------------------------------
# Convenience: write to file
# ---------------------------------------------------------------------------

def save_leads_csv(leads: List[Dict[str, Any]], path: str) -> None:
    """Write lead CSV to a file path.

    Args:
        leads: List of lead dicts.
        path: Destination file path (will be overwritten).
    """
    data = export_leads_csv(leads)
    with open(path, "wb") as f:
        f.write(data)
    logger.info("Leads CSV saved to %s (%d bytes).", path, len(data))


def save_enriched_csv(leads: List[Dict[str, Any]], path: str) -> None:
    """Write enriched (contact-per-row) CSV to a file path.

    Args:
        leads: List of enriched lead dicts.
        path: Destination file path (will be overwritten).
    """
    data = export_enriched_csv(leads)
    with open(path, "wb") as f:
        f.write(data)
    logger.info("Enriched CSV saved to %s (%d bytes).", path, len(data))


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    from datetime import date as _date

    sample_leads = [
        {
            "source_site": "BizBuySell",
            "region": "NY",
            "post_date": _date(2026, 4, 10),
            "days_posted": 9,
            "title": "Established PM Company — Brooklyn",
            "company_name": "Acme Property Management LLC",
            "lead_type": "Acquisition",
            "category": "Business for sale",
            "score": 85,
            "raw_location": "Brooklyn, NY",
            "link": "https://www.bizbuysell.com/listing/12345",
            "email": ["owner@acmepm.nyc"],
            "phone": ["(718) 555-0101"],
            "social_media": [],
            "tags": ["Acquisition", "Succession"],
            "author": "",
            "post_description": "Owner retiring after 20 years.",
            "asking_price": "$1,200,000",
            "revenue": "$380,000",
            "contacts": [
                {
                    "name": "David Goldstein",
                    "title": "Owner",
                    "email": "owner@acmepm.nyc",
                    "phone": ["(718) 555-0101"],
                    "linkedin_url": "https://linkedin.com/in/dgoldstein",
                    "company": "Acme PM",
                    "source": "Apollo.io",
                    "seniority": "owner",
                    "city": "Brooklyn",
                    "state": "NY",
                }
            ],
        },
        {
            "source_site": "NYC HPD Open Data",
            "region": "NY",
            "post_date": _date(2026, 4, 15),
            "days_posted": 4,
            "title": "[Unmanaged Building] 45 Ocean Ave, Queens, NY 11367",
            "company_name": "45 Ocean Ave LLC",
            "lead_type": "Unmanaged building",
            "category": "Unmanaged building",
            "score": 55,
            "raw_location": "45 Ocean Ave, Queens, NY 11367",
            "link": "https://hpdonline.nyc.gov/building/12345/summary",
            "email": [],
            "phone": ["(718) 555-0202"],
            "social_media": [],
            "tags": ["Unmanaged", "HPD"],
            "author": "NYC HPD Registration",
            "post_description": "Self-managed | 12 units | 7 open violations",
            "contacts": [],
        },
    ]

    raw_csv = export_leads_csv(sample_leads)
    print("=== leads CSV (first 500 chars) ===")
    print(raw_csv.decode("utf-8-sig")[:500])

    enriched_csv = export_enriched_csv(sample_leads)
    print("\n=== enriched CSV (first 500 chars) ===")
    print(enriched_csv.decode("utf-8-sig")[:500])
