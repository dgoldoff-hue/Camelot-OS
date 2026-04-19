"""
enrichment/enricher.py
-----------------------
Two-source contact enrichment orchestrator for Scout Bot.

Enrichment strategy:
1. Run Apollo.io first (broader database, decision-maker title filtering)
2. Run Prospeo for any contact gaps Apollo didn't fill
   — if Apollo found fewer than MIN_CONTACTS contacts, run Prospeo company search
   — if a contact has no email from Apollo, try Prospeo email finder
3. Merge results, deduplicate by email
4. Tag each contact with source: "Apollo.io", "Prospeo", or "Apollo.io + Prospeo"

Usage::

    from enrichment.enricher import enrich_lead

    enriched = enrich_lead(lead)
    # lead["contacts"] now populated with merged, tagged contacts
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from enrichment.apollo_client import search_people as apollo_search_people
from enrichment.apollo_client import enrich_contact as apollo_enrich_contact
from enrichment.prospeo_client import company_search as prospeo_company_search
from enrichment.prospeo_client import find_email as prospeo_find_email
from enrichment.prospeo_client import enrich_contact as prospeo_enrich_contact

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Minimum contacts Apollo must return before we skip Prospeo company search
MIN_CONTACTS_FROM_APOLLO = 2

# Maximum total contacts to include per lead
MAX_CONTACTS_PER_LEAD = 10

# ---------------------------------------------------------------------------
# Domain extraction helper
# ---------------------------------------------------------------------------

def _extract_domain(lead: Dict[str, Any]) -> Optional[str]:
    """Extract the company's website domain from a lead dict.

    Tries ``lead["link"]`` and any emails found in the lead to infer the domain.

    Args:
        lead: Scout lead dict.

    Returns:
        Domain string (e.g. ``"acmeproperty.com"``) or ``None``.
    """
    # Try to parse domain from the lead's primary link
    link = (lead.get("link") or "").strip()
    if link:
        try:
            parsed = urlparse(link)
            netloc = parsed.netloc.lower()
            # Exclude known non-company domains (listing sites)
            known_sites = {
                "bizbuysell.com", "bizquest.com", "loopnet.com",
                "indeed.com", "ziprecruiter.com", "nyc.gov", "edc.nyc",
                "hpdonline.nyc.gov", "data.cityofnewyork.us",
            }
            if netloc and not any(site in netloc for site in known_sites):
                # Strip www.
                domain = re.sub(r"^www\.", "", netloc)
                if "." in domain:
                    return domain
        except Exception:
            pass

    # Try to infer domain from email addresses on the lead
    emails: List[str] = lead.get("email") or []
    for email in emails:
        if "@" in email:
            domain_part = email.split("@")[1].lower()
            # Skip generic free email providers
            generic = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"}
            if domain_part not in generic and "." in domain_part:
                return domain_part

    return None


# ---------------------------------------------------------------------------
# Contact merging helpers
# ---------------------------------------------------------------------------

def _dedup_contacts(contacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate contacts by email, merging data from multiple sources.

    If two contacts share the same email:
    - Data from the first occurrence is used as the base.
    - The second source is appended to the ``source`` tag.
    - Missing fields from the second record fill in gaps in the first.

    Args:
        contacts: Raw list of contact dicts (may have duplicates).

    Returns:
        Deduplicated list with merged source tags.
    """
    email_index: Dict[str, Dict[str, Any]] = {}
    no_email: List[Dict[str, Any]] = []

    for contact in contacts:
        email = (contact.get("email") or "").strip().lower()
        if not email:
            no_email.append(contact)
            continue

        if email in email_index:
            # Merge: fill gaps and update source tag
            existing = email_index[email]
            existing_source = existing.get("source", "")
            new_source = contact.get("source", "")

            # Tag with combined source
            if new_source and new_source not in existing_source:
                if existing_source:
                    existing["source"] = f"{existing_source} + {new_source}"
                else:
                    existing["source"] = new_source

            # Fill in missing fields from the new contact
            for field in ["name", "title", "phone", "linkedin_url", "company",
                          "city", "state", "seniority", "departments"]:
                if not existing.get(field) and contact.get(field):
                    existing[field] = contact[field]
        else:
            email_index[email] = dict(contact)

    result = list(email_index.values())

    # Add no-email contacts deduped by name
    seen_names: Set[str] = set()
    for c in no_email:
        name_key = (c.get("name") or "").lower().strip()
        if name_key and name_key in seen_names:
            continue
        if name_key:
            seen_names.add(name_key)
        result.append(c)

    return result


def _tag_contact_source(contact: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the contact has a clean source tag.

    Normalizes the source field to one of:
    - ``"Apollo.io"``
    - ``"Prospeo"``
    - ``"Apollo.io + Prospeo"``

    Args:
        contact: Contact dict (mutated in place).

    Returns:
        The same contact dict.
    """
    source = (contact.get("source") or "").strip()

    if "Apollo" in source and "Prospeo" in source:
        contact["source"] = "Apollo.io + Prospeo"
    elif "Apollo" in source:
        contact["source"] = "Apollo.io"
    elif "Prospeo" in source:
        contact["source"] = "Prospeo"
    elif not source:
        contact["source"] = "Unknown"

    return contact


# ---------------------------------------------------------------------------
# Main enrichment function
# ---------------------------------------------------------------------------

def enrich_lead(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich a single Scout lead with contacts from Apollo.io and Prospeo.

    Enrichment flow:
    1. Extract company name and domain from the lead.
    2. Call Apollo ``search_people(company_name, domain)``.
    3. If Apollo returns fewer than ``MIN_CONTACTS_FROM_APOLLO`` results,
       also call Prospeo ``company_search(company_name, domain)``.
    4. For any Apollo contact missing an email that has a LinkedIn URL,
       run Prospeo ``enrich_contact(linkedin_url)`` to fill the gap.
    5. Deduplicate merged contacts by email.
    6. Tag each contact with its source(s).
    7. Attach the final contacts list to ``lead["contacts"]``.

    Args:
        lead: Scout lead dict to enrich (mutated in place).

    Returns:
        The same lead dict with ``contacts`` populated.
    """
    company_name: str = (lead.get("company_name") or lead.get("title") or "").strip()
    domain: Optional[str] = _extract_domain(lead)

    if not company_name:
        logger.warning("[Enricher] Lead has no company name; skipping enrichment.")
        lead.setdefault("contacts", [])
        return lead

    logger.info(
        "[Enricher] Enriching lead: company=%r domain=%r", company_name, domain
    )

    all_contacts: List[Dict[str, Any]] = []

    # --- Step 1: Apollo.io ---
    apollo_contacts: List[Dict[str, Any]] = []
    try:
        apollo_contacts = apollo_search_people(company_name, domain=domain)
        logger.info("[Enricher] Apollo → %d contacts", len(apollo_contacts))
    except ValueError as exc:
        # Bad/missing API key — log and skip Apollo
        logger.warning("[Enricher] Apollo skipped: %s", exc)
    except Exception as exc:
        logger.error("[Enricher] Apollo error: %s", exc)

    all_contacts.extend(apollo_contacts)

    # --- Step 2: Prospeo company search (if Apollo came up short) ---
    if len(apollo_contacts) < MIN_CONTACTS_FROM_APOLLO and domain:
        prospeo_contacts: List[Dict[str, Any]] = []
        try:
            prospeo_contacts = prospeo_company_search(company_name, domain)
            logger.info("[Enricher] Prospeo company_search → %d contacts", len(prospeo_contacts))
        except ValueError as exc:
            logger.warning("[Enricher] Prospeo skipped: %s", exc)
        except Exception as exc:
            logger.error("[Enricher] Prospeo company_search error: %s", exc)

        all_contacts.extend(prospeo_contacts)

    # --- Step 3: LinkedIn enrichment for Apollo contacts missing email ---
    for apollo_contact in apollo_contacts:
        if apollo_contact.get("email"):
            continue  # already has email
        linkedin_url = apollo_contact.get("linkedin_url", "")
        if not linkedin_url:
            continue
        try:
            enriched = prospeo_enrich_contact(linkedin_url)
            if enriched and enriched.get("email"):
                # Mark as enriched by both sources
                enriched["source"] = "Apollo.io + Prospeo"
                # Merge original Apollo fields into enriched record
                for field in ["title", "seniority", "departments", "company"]:
                    if apollo_contact.get(field) and not enriched.get(field):
                        enriched[field] = apollo_contact[field]
                all_contacts.append(enriched)
                logger.info(
                    "[Enricher] LinkedIn fill for %s: %s",
                    apollo_contact.get("name"), enriched.get("email"),
                )
        except Exception as exc:
            logger.warning("[Enricher] LinkedIn enrich error for %s: %s", linkedin_url, exc)

    # --- Step 4: Enrich lead's own email(s) if no company contacts found ---
    if not all_contacts:
        lead_emails: List[str] = lead.get("email") or []
        for email in lead_emails[:3]:  # limit to avoid excessive API calls
            try:
                enriched = apollo_enrich_contact(email)
                if enriched:
                    all_contacts.append(enriched)
            except Exception as exc:
                logger.warning("[Enricher] Apollo enrich_contact error for %s: %s", email, exc)

    # --- Step 5: Deduplicate and tag ---
    unique_contacts = _dedup_contacts(all_contacts)
    for contact in unique_contacts:
        _tag_contact_source(contact)

    # Limit to top MAX_CONTACTS_PER_LEAD
    final_contacts = unique_contacts[:MAX_CONTACTS_PER_LEAD]

    logger.info(
        "[Enricher] Lead %r → %d final contacts (from %d raw)",
        company_name, len(final_contacts), len(all_contacts),
    )

    lead["contacts"] = final_contacts
    return lead


def enrich_leads_batch(
    leads: List[Dict[str, Any]],
    max_enrichments: int = 20,
) -> List[Dict[str, Any]]:
    """Enrich a batch of leads, processing the top-scored ones first.

    Args:
        leads: List of scored Scout lead dicts.
        max_enrichments: Maximum number of leads to enrich (default 20).

    Returns:
        The same list with ``contacts`` fields populated for enriched leads.
    """
    # Sort by score descending; enrich the best leads first
    sorted_leads = sorted(leads, key=lambda l: l.get("score", 0), reverse=True)
    to_enrich = sorted_leads[:max_enrichments]
    skip = sorted_leads[max_enrichments:]

    logger.info(
        "[Enricher] Enriching %d leads (skipping %d below threshold).",
        len(to_enrich), len(skip),
    )

    enriched: List[Dict[str, Any]] = []
    for i, lead in enumerate(to_enrich, start=1):
        logger.info(
            "[Enricher] [%d/%d] %s", i, len(to_enrich), lead.get("company_name", "?")
        )
        try:
            enriched.append(enrich_lead(lead))
        except Exception as exc:  # noqa: BLE001
            logger.error("[Enricher] Unhandled error enriching lead: %s", exc)
            lead.setdefault("contacts", [])
            enriched.append(lead)

    return enriched + skip
