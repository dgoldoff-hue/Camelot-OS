"""
outreach_generator.py — Camelot OS Deal Bot
=============================================
Generates personalized cold outreach emails for acquisition prospects.

Outreach angles:
  - succession    : Owner 60+, long tenure, legacy / exit planning
  - growth        : Growing operator, needs capital / systems to scale
  - systems-upgrade: High violations, manual processes, tech debt
  - tired-operator : Flat/declining portfolio, burnout, ready to step back

Deal structures pitched:
  - equity-sale    : Minority or majority stake acquisition
  - roll-up        : Join Camelot brand umbrella with capital + OS
  - powered-by     : Technology partnership; retain brand, deploy Camelot OS

Author: Camelot OS
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import Optional

from prospect_mapper import ProspectProfile

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("deal_bot.outreach_generator")

# Sender identity
SENDER_NAME: str = os.getenv("DEAL_BOT_SENDER_NAME", "David Goldoff")
SENDER_TITLE: str = os.getenv("DEAL_BOT_SENDER_TITLE", "Managing Partner, Camelot Property Management")
SENDER_EMAIL: str = os.getenv("DEAL_BOT_SENDER_EMAIL", "dgoldoff@camelot.nyc")
SENDER_PHONE: str = os.getenv("DEAL_BOT_SENDER_PHONE", "(212) 555-0100")
CAMELOT_WEBSITE: str = os.getenv("CAMELOT_WEBSITE", "https://camelot.nyc")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class OutreachEmail:
    """A generated outreach email with subject and body."""

    subject: str
    body: str
    angle: str
    structure: str
    prospect_name: str
    company_name: str

    def to_dict(self) -> dict[str, str]:
        return {
            "subject": self.subject,
            "body": self.body,
            "angle": self.angle,
            "structure": self.structure,
            "prospect_name": self.prospect_name,
            "company_name": self.company_name,
        }

    def __str__(self) -> str:
        return f"Subject: {self.subject}\n\n{self.body}"


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

def _first_name(full_name: str) -> str:
    """Extract first name from full name."""
    parts = full_name.strip().split()
    return parts[0] if parts else full_name


def _unit_descriptor(units: int) -> str:
    """Natural language description of unit count."""
    if units >= 500:
        return "a major portfolio"
    if units >= 200:
        return f"a {units}-unit portfolio"
    if units >= 50:
        return f"a {units}-unit operation"
    return "your portfolio"


def _geo_phrase(geographies: list[str]) -> str:
    """Build a geography phrase from list."""
    if not geographies:
        return "the New York metro area"
    if len(geographies) == 1:
        return geographies[0]
    if len(geographies) == 2:
        return f"{geographies[0]} and {geographies[1]}"
    return f"{', '.join(geographies[:-1])}, and {geographies[-1]}"


def _clean_company_name(name: str) -> str:
    """Remove LLC/Inc/Corp suffixes for friendlier use in email body."""
    return re.sub(
        r"\s+(LLC|INC|CORP|CO\.|LTD|LP|L\.L\.C\.|INC\.)$",
        "",
        name.strip(),
        flags=re.IGNORECASE,
    ).strip()


# ---------------------------------------------------------------------------
# Email templates by angle + structure
# ---------------------------------------------------------------------------
# Each template is a callable taking (profile, contact_name) → (subject, body)
# Templates are intentionally ≤ 200 words in the body.

def _template_succession_rollup(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)
    geo = _geo_phrase(profile.geographies_served)
    units = _unit_descriptor(profile.estimated_units)

    subject = f"Succession planning for {company}?"

    body = f"""Hi {first},

I'm {SENDER_NAME} at Camelot Property Management. We work with independent operators across {geo} who are thinking about what comes next — whether that's a structured exit, bringing in a capital partner, or simply building a team and systems that outlast the founder.

{company} manages {units} — that's a real asset. The question I hear from operators at your stage is usually: "I built this over 20 years. How do I make sure it continues, and that I get paid fairly for what I've built?"

We've helped three operators in similar situations transition under the Camelot umbrella — keeping staff, maintaining relationships, and letting owners step back on their own timeline.

Would you be open to a 20-minute call this week? No pitch deck, just a conversation.

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}
{CAMELOT_WEBSITE}"""

    return subject, body


def _template_succession_equity(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)
    units = _unit_descriptor(profile.estimated_units)

    subject = f"Partial liquidity opportunity — {company}"

    body = f"""Hi {first},

I'm {SENDER_NAME} at Camelot Property Management. I'll be brief.

We're acquiring minority and majority stakes in established property management firms — {company} caught my attention.

You've built {units} in this market. There are very few buyers who understand this business well enough to be a real partner rather than just a financial investor. We manage and operate properties ourselves, so we add value beyond capital.

What we offer:
— Immediate liquidity on a portion of your equity
— Operational support: Camelot OS technology, compliance systems, shared vendor contracts
— You remain in the operating seat as long as you want

If you've thought about partial liquidity while staying involved, I'd like to explore whether we're a fit.

15 minutes?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}"""

    return subject, body


def _template_growth_rollup(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)
    geo = _geo_phrase(profile.geographies_served)
    units = _unit_descriptor(profile.estimated_units)

    subject = f"Scaling {company} — want to compare notes?"

    body = f"""Hi {first},

I run business development at Camelot Property Management. We've been building an operating platform across {geo} and I've been following {company}'s growth — {units} is a strong base.

We work with operators who are in growth mode but running into the classic bottlenecks: compliance overhead, finding good staff, technology that can't keep up, and owners who want to reinvest but can't get capital efficiently.

Our model is a bit different: we provide the OS (literally — Camelot OS is our property management platform), compliance automation, and capital access in exchange for a partnership stake. You keep operating and growing; we handle the infrastructure.

Not sure if it's relevant to you, but happy to share what we've built. Would a brief call make sense?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}
{CAMELOT_WEBSITE}"""

    return subject, body


def _template_growth_poweredby(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"Technology for {company} — Camelot OS"

    body = f"""Hi {first},

Quick question: how much time does your team spend on HPD compliance, work order tracking, and chasing down owner statements each month?

Camelot OS is a property management platform we built specifically for NYC-area operators. It automates violation monitoring, tenant communication, owner reporting, and maintenance workflows — built around how this market actually works.

We deploy it as a "Powered by Camelot OS" partnership: {company} keeps its brand, client relationships, and operations — you get the technology backbone we've built over years of managing real assets.

No long-term lock-in. We prove value in 90 days or you're out.

Worth 20 minutes?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}"""

    return subject, body


def _template_systems_upgrade_rollup(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)
    violations = profile.open_violation_count

    subject = f"Compliance burden at {company} — how we can help"

    body = f"""Hi {first},

I'm {SENDER_NAME} at Camelot Property Management. We specialize in helping NYC operators get ahead of their compliance backlog — and we noticed {company} has some open violations that could escalate to ECB fines.

We don't just consult. We built Camelot OS — a compliance automation system that monitors HPD, DOB, and ECB violations in real time, auto-generates cure notices, and tracks resolution. Operators we've brought onto our platform have reduced violation counts by 60–80% in the first two quarters.

If you're interested in a partnership that includes compliance support, shared vendor contracts, and operational infrastructure — rather than just a technology license — I'd like to share how our roll-up model works.

Worth a short call this week?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}"""

    return subject, body


def _template_systems_upgrade_poweredby(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"Automating compliance for {company}"

    body = f"""Hi {first},

Do you have a system that automatically monitors your HPD and DOB violation status across your entire portfolio, or is someone on your team checking it manually?

I ask because we built Camelot OS specifically to solve that problem for NYC operators. It pulls violation data daily, generates alerts, tracks resolution deadlines, and produces reports for owners — automatically.

We deploy it as a technology partnership: {company} keeps full control of operations and client relationships; we provide the platform on a per-unit fee basis.

If you're running compliance manually right now, I'd like to show you what automated looks like.

15-minute demo this week?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}"""

    return subject, body


def _template_tired_operator_rollup(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)
    units = _unit_descriptor(profile.estimated_units)

    subject = f"A different kind of offer for {company}"

    body = f"""Hi {first},

I'll be direct: Camelot Property Management acquires and partners with established operators who've built something real — like {units} — but may be at a point where running it day-to-day isn't the most appealing way to spend the next decade.

We're not a private equity firm. We manage properties ourselves, and we understand what it actually takes. When we bring an operator into our platform, we typically:

— Handle the compliance, technology, and reporting overhead
— Bring in capital to stabilize or grow the portfolio
— Let the original owner step back, step up, or step out — your call

If any part of this resonates, I'd like to have a real conversation. No obligation, no pitch.

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}
{CAMELOT_WEBSITE}"""

    return subject, body


def _template_tired_operator_equity(
    profile: ProspectProfile, contact_name: str
) -> tuple[str, str]:
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"Thinking about an exit from {company}?"

    body = f"""Hi {first},

My name is {SENDER_NAME}. I run acquisitions for Camelot Property Management in the New York area.

We're actively looking to acquire equity stakes in established property management businesses — specifically operators who've built a strong book but are thinking about their next chapter.

If you've ever wondered what {company} is worth, or whether there's a way to get liquidity without just shutting it down or doing a fire sale, we should talk. We close quickly and we don't over-engineer the process.

Would you be open to a confidential conversation?

{SENDER_NAME}
{SENDER_TITLE}
{SENDER_EMAIL}  |  {SENDER_PHONE}"""

    return subject, body


# ---------------------------------------------------------------------------
# Template dispatch table
# ---------------------------------------------------------------------------

# Key: (angle, structure) → template function
TEMPLATE_MAP = {
    ("succession", "roll-up"):        _template_succession_rollup,
    ("succession", "equity-sale"):    _template_succession_equity,
    ("succession", "powered-by"):     _template_succession_rollup,   # fallback
    ("growth", "roll-up"):            _template_growth_rollup,
    ("growth", "powered-by"):         _template_growth_poweredby,
    ("growth", "equity-sale"):        _template_growth_rollup,       # fallback
    ("systems-upgrade", "roll-up"):   _template_systems_upgrade_rollup,
    ("systems-upgrade", "powered-by"):_template_systems_upgrade_poweredby,
    ("systems-upgrade", "equity-sale"):_template_systems_upgrade_rollup,  # fallback
    ("tired-operator", "roll-up"):    _template_tired_operator_rollup,
    ("tired-operator", "equity-sale"):_template_tired_operator_equity,
    ("tired-operator", "powered-by"): _template_tired_operator_rollup,    # fallback
}

# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

class OutreachGenerator:
    """Generates personalized outreach emails for acquisition prospects."""

    def generate(
        self,
        profile: ProspectProfile,
        angle: Optional[str] = None,
        structure: Optional[str] = None,
        contact_name: Optional[str] = None,
    ) -> OutreachEmail:
        """
        Generate a personalized outreach email for a prospect.

        Args:
            profile:      ProspectProfile from prospect_mapper.
            angle:        Outreach angle override (uses profile recommendation if None).
            structure:    Deal structure override (uses profile recommendation if None).
            contact_name: Name of recipient (uses profile.owner_name if None).

        Returns:
            OutreachEmail with subject + body.
        """
        resolved_angle = angle or profile.recommended_angle or "growth"
        resolved_structure = structure or profile.recommended_structure or "roll-up"

        # Determine contact name
        if not contact_name:
            if profile.contacts:
                contact_name = profile.contacts[0].name
            elif profile.owner_name:
                contact_name = profile.owner_name
            else:
                contact_name = "there"

        logger.info(
            "Generating outreach for %s — angle: %s, structure: %s, contact: %s",
            profile.company_name,
            resolved_angle,
            resolved_structure,
            contact_name,
        )

        # Look up template
        key = (resolved_angle, resolved_structure)
        template_fn = TEMPLATE_MAP.get(key)

        if template_fn is None:
            logger.warning(
                "No template for angle=%s, structure=%s — using default",
                resolved_angle,
                resolved_structure,
            )
            template_fn = _template_growth_rollup

        subject, body = template_fn(profile, contact_name)

        email = OutreachEmail(
            subject=subject,
            body=body,
            angle=resolved_angle,
            structure=resolved_structure,
            prospect_name=contact_name,
            company_name=profile.company_name,
        )
        logger.info(
            "Email generated: '%s' (%d chars body)",
            subject,
            len(body),
        )
        return email

    def generate_all_angles(
        self,
        profile: ProspectProfile,
        contact_name: Optional[str] = None,
    ) -> list[OutreachEmail]:
        """
        Generate emails for all angle × structure combinations for a prospect.
        Useful for A/B testing.

        Returns list of OutreachEmail objects.
        """
        emails: list[OutreachEmail] = []
        for (angle, structure) in TEMPLATE_MAP:
            try:
                email = self.generate(
                    profile,
                    angle=angle,
                    structure=structure,
                    contact_name=contact_name,
                )
                emails.append(email)
            except Exception as exc:
                logger.warning("Template %s/%s failed: %s", angle, structure, exc)
        return emails


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json
    import sys

    from prospect_mapper import ProspectProfile

    parser = argparse.ArgumentParser(description="Generate outreach email for a prospect")
    parser.add_argument("--profile", required=True, help="Path to prospect JSON file")
    parser.add_argument("--angle", choices=["succession", "growth", "systems-upgrade", "tired-operator"])
    parser.add_argument("--structure", choices=["equity-sale", "roll-up", "powered-by"])
    parser.add_argument("--contact", help="Contact name override")
    parser.add_argument("--all-angles", action="store_true", help="Generate all angle variants")
    args = parser.parse_args()

    with open(args.profile) as f:
        data = json.load(f)

    # Reconstruct minimal ProspectProfile from JSON
    from dataclasses import fields as dc_fields
    valid_keys = {f.name for f in dc_fields(ProspectProfile)}
    filtered = {k: v for k, v in data.items() if k in valid_keys}
    profile = ProspectProfile(**filtered)

    gen = OutreachGenerator()

    if args.all_angles:
        emails = gen.generate_all_angles(profile, contact_name=args.contact)
        for e in emails:
            print(f"\n{'='*60}")
            print(f"Angle: {e.angle} | Structure: {e.structure}")
            print(f"{'='*60}")
            print(e)
    else:
        email = gen.generate(
            profile,
            angle=args.angle,
            structure=args.structure,
            contact_name=args.contact,
        )
        print(email)
