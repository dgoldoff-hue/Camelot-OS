"""
email_sequences.py — Camelot OS Deal Bot
==========================================
Manages 5-email drip sequences for prospect outreach.

Sequence schedule:
  Email 1 — Day  1: Introduction / Hook
  Email 2 — Day  3: Follow-up / Social proof
  Email 3 — Day  7: Value proposition deep-dive
  Email 4 — Day 14: Case study / Proof point
  Email 5 — Day 30: Final breakup / last-call

Each email varies by:
  - Angle: succession / growth / systems-upgrade / tired-operator
  - Structure: equity-sale / roll-up / powered-by

Sequences are persisted to Supabase `email_sequences` table and
integrated with HubSpot deal timelines for tracking.

Author: Camelot OS
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import ssl
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from outreach_generator import OutreachEmail, OutreachGenerator
from prospect_mapper import ProspectProfile

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("deal_bot.email_sequences")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SUPABASE_URL: str = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY: str = os.environ.get("SUPABASE_SERVICE_KEY", "")
HUBSPOT_TOKEN: str = os.environ.get("HUBSPOT_ACCESS_TOKEN", "")

SMTP_HOST: str = os.environ.get("SMTP_HOST", "")
SMTP_PORT: int = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER: str = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD: str = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM: str = os.environ.get("SMTP_FROM", "dgoldoff@camelot.nyc")
SMTP_FROM_NAME: str = os.environ.get("SMTP_FROM_NAME", "David Goldoff — Camelot")

# Sequence day offsets
SEQUENCE_DAYS = [1, 3, 7, 14, 30]

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4, backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = _make_session()


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class SequenceEmail:
    """
    A single email in a drip sequence with its scheduled send date
    and delivery status.
    """
    sequence_id: str
    step_number: int          # 1–5
    day_offset: int           # days after sequence start: 1, 3, 7, 14, 30
    scheduled_date: str       # ISO date string
    subject: str
    body: str
    angle: str
    structure: str
    prospect_email: str
    prospect_name: str
    company_name: str
    hubspot_deal_id: str = ""
    status: str = "pending"   # pending / sent / opened / replied / bounced / skipped
    sent_at: Optional[str] = None
    opened_at: Optional[str] = None
    replied_at: Optional[str] = None
    supabase_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class EmailSequence:
    """A full 5-email drip sequence for one prospect."""

    sequence_id: str
    prospect_email: str
    prospect_name: str
    company_name: str
    hubspot_deal_id: str
    angle: str
    structure: str
    start_date: str           # ISO date
    emails: list[SequenceEmail] = field(default_factory=list)
    active: bool = True
    paused_reason: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["emails"] = [e.to_dict() for e in self.emails]
        return d

    def pending_emails(self) -> list[SequenceEmail]:
        """Return emails due to be sent today or earlier."""
        today = date.today().isoformat()
        return [
            e for e in self.emails
            if e.status == "pending" and e.scheduled_date <= today
        ]

    def next_email(self) -> Optional[SequenceEmail]:
        """Return the next pending email regardless of date."""
        pending = [e for e in self.emails if e.status == "pending"]
        return min(pending, key=lambda e: e.step_number) if pending else None


# ---------------------------------------------------------------------------
# Sequence templates — follow-up emails 2–5
# ---------------------------------------------------------------------------
# Email 1 is generated by OutreachGenerator. Emails 2–5 are defined here.

def _email2_followup(
    profile: ProspectProfile,
    contact_name: str,
    angle: str,
    structure: str,
) -> tuple[str, str]:
    """Day 3 — Follow-up with social proof."""
    from outreach_generator import _first_name, _clean_company_name
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"Following up — {company}"

    if angle == "succession":
        proof = (
            "We recently helped a 25-year Bronx operator transfer 280 units to Camelot, "
            "providing immediate liquidity while keeping his entire team employed. "
            "The process took 90 days from first call to close."
        )
    elif angle == "systems-upgrade":
        proof = (
            "One operator we work with had 47 open HPD violations when we started. "
            "Within 8 months on Camelot OS, that was down to 3. "
            "The compliance savings alone paid for the platform."
        )
    elif angle == "tired-operator":
        proof = (
            "One of our partners — a 30-year operator in Queens — put it simply: "
            "'I didn't want to sell, I wanted to stop doing the parts I hated.' "
            "That's exactly what the Camelot model allows."
        )
    else:  # growth
        proof = (
            "An operator we partnered with in Westchester was managing 95 units solo. "
            "Within 18 months under the Camelot umbrella, they're at 210 units "
            "with a full staff — without taking on debt."
        )

    body = f"""Hi {first},

Just following up on my note from a few days ago regarding {company}.

{proof}

Every situation is different — I'm not assuming yours is the same. But if any part of this resonates, I'm happy to share specifics.

15 minutes this week?

{os.getenv('DEAL_BOT_SENDER_NAME', 'David Goldoff')}
{os.getenv('DEAL_BOT_SENDER_EMAIL', 'dgoldoff@camelot.nyc')}"""

    return subject, body


def _email3_value_prop(
    profile: ProspectProfile,
    contact_name: str,
    angle: str,
    structure: str,
) -> tuple[str, str]:
    """Day 7 — Value proposition deep-dive."""
    from outreach_generator import _first_name, _clean_company_name
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"What Camelot actually offers {company}"

    if structure == "powered-by":
        prop_lines = (
            "— Camelot OS deployed across your portfolio\n"
            "— Automated HPD/DOB/ECB violation monitoring\n"
            "— Owner statement generation (zero manual effort)\n"
            "— Tenant communication + work order management\n"
            "— Per-unit pricing, no long-term contract"
        )
        closing = "You keep the brand, we handle the infrastructure."
    elif structure == "equity-sale":
        prop_lines = (
            "— Immediate liquidity on minority or majority stake\n"
            "— Camelot OS technology deployed across your portfolio\n"
            "— Shared vendor contracts (20–30% cost reduction)\n"
            "— Back-office support: bookkeeping, compliance, reporting\n"
            "— You remain in operating control"
        )
        closing = "We're operators, not financial engineers."
    else:  # roll-up
        prop_lines = (
            "— {company} joins the Camelot operating platform\n"
            "— Camelot OS: compliance, reporting, and workflow automation\n"
            "— Capital access for portfolio growth\n"
            "— Shared vendor relationships and buying power\n"
            "— Staff retention with professional HR/benefits backing\n"
            "— Gradual transition on your timeline"
        ).format(company=company)
        closing = "You keep running the business. We handle what slows you down."

    body = f"""Hi {first},

I want to be concrete about what Camelot would actually bring to {company}:

{prop_lines}

{closing}

We've done this with operators managing between 40 and 600 units. The model adapts to your situation.

If you have 20 minutes, I can walk through the specifics — and give you a real sense of what a partnership would look like financially.

{os.getenv('DEAL_BOT_SENDER_NAME', 'David Goldoff')}
{os.getenv('DEAL_BOT_SENDER_EMAIL', 'dgoldoff@camelot.nyc')}"""

    return subject, body


def _email4_case_study(
    profile: ProspectProfile,
    contact_name: str,
    angle: str,
    structure: str,
) -> tuple[str, str]:
    """Day 14 — Case study / proof point."""
    from outreach_generator import _first_name, _clean_company_name
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"A story that might be relevant — {company}"

    if angle in ("succession", "tired-operator"):
        story = (
            "A Brooklyn operator I'll call M — 28 years in the business, 180 units — "
            "was fielding calls from brokers who wanted to list his buildings. That wasn't "
            "what he wanted. He wanted to retire from the paperwork, not the relationships.\n\n"
            "We structured a roll-up: Camelot took a 60% stake, M stayed on as "
            "managing partner with a real salary for three years, and his team kept their jobs. "
            "He got eight-figure liquidity at close. Two years later, he calls it the "
            "best decision he made in three decades of this business."
        )
    elif angle == "systems-upgrade":
        story = (
            "An operator in the Bronx was spending 12 hours a week manually checking "
            "violation portals and drafting cure notices. They had 3 full-time staff "
            "for 140 units — and were still missing deadlines.\n\n"
            "Eight months after Camelot OS deployment: 2 staff, automated violation "
            "monitoring, owner reports generated automatically, and zero missed deadlines. "
            "The operator reinvested the savings into two new buildings."
        )
    else:  # growth
        story = (
            "A Westchester operator joined our platform at 85 units. "
            "Within 18 months: 210 units, 4 staff (up from 1), and a waiting list "
            "of building owners who want to move to Camelot.\n\n"
            "The difference wasn't just capital. It was having the systems — "
            "technology, compliance, vendor relationships — that made scaling "
            "operationally sane."
        )

    body = f"""Hi {first},

Still thinking about whether Camelot might be relevant for {company}. Wanted to share something concrete.

{story}

I could share more detail on the financials and structure in a call — anonymized, of course.

Are you open to 20 minutes?

{os.getenv('DEAL_BOT_SENDER_NAME', 'David Goldoff')}
{os.getenv('DEAL_BOT_SENDER_EMAIL', 'dgoldoff@camelot.nyc')}"""

    return subject, body


def _email5_breakup(
    profile: ProspectProfile,
    contact_name: str,
    angle: str,
    structure: str,
) -> tuple[str, str]:
    """Day 30 — Final breakup / last-call."""
    from outreach_generator import _first_name, _clean_company_name
    first = _first_name(contact_name)
    company = _clean_company_name(profile.company_name)

    subject = f"Closing the loop on {company}"

    body = f"""Hi {first},

I've reached out a few times about a potential partnership between Camelot and {company}. I don't want to keep filling your inbox if the timing isn't right.

Two things before I close out:

1. If you're ever ready to talk — about succession, capital, technology, or just comparing notes — I'm easy to reach: {os.getenv('DEAL_BOT_SENDER_EMAIL', 'dgoldoff@camelot.nyc')}

2. If the reason you haven't responded is that we're not a fit, I'd genuinely appreciate knowing why. It helps us serve this market better.

Either way, I wish {company} continued success. You've built something real.

{os.getenv('DEAL_BOT_SENDER_NAME', 'David Goldoff')}
{os.getenv('DEAL_BOT_SENDER_TITLE', 'Managing Partner, Camelot Property Management')}
{os.getenv('DEAL_BOT_SENDER_EMAIL', 'dgoldoff@camelot.nyc')}  |  {os.getenv('DEAL_BOT_SENDER_PHONE', '(212) 555-0100')}"""

    return subject, body


FOLLOWUP_TEMPLATES = {
    2: _email2_followup,
    3: _email3_value_prop,
    4: _email4_case_study,
    5: _email5_breakup,
}


# ---------------------------------------------------------------------------
# Sequence builder
# ---------------------------------------------------------------------------

class SequenceBuilder:
    """Builds a 5-email drip sequence for a prospect."""

    def __init__(self) -> None:
        self.generator = OutreachGenerator()

    def build(
        self,
        profile: ProspectProfile,
        prospect_email: str,
        hubspot_deal_id: str = "",
        angle: Optional[str] = None,
        structure: Optional[str] = None,
        start_date: Optional[date] = None,
    ) -> EmailSequence:
        """
        Build a complete 5-email sequence for a prospect.

        Args:
            profile:          ProspectProfile to personalize emails.
            prospect_email:   Recipient email address.
            hubspot_deal_id:  HubSpot deal ID to link emails to.
            angle:            Outreach angle (defaults to profile recommendation).
            structure:        Deal structure (defaults to profile recommendation).
            start_date:       Sequence start date (defaults to today).

        Returns:
            EmailSequence with all 5 emails scheduled.
        """
        import uuid

        resolved_angle = angle or profile.recommended_angle or "growth"
        resolved_structure = structure or profile.recommended_structure or "roll-up"
        start = start_date or date.today()

        contact_name = (
            profile.contacts[0].name
            if profile.contacts
            else (profile.owner_name or "there")
        )

        sequence_id = str(uuid.uuid4())

        logger.info(
            "Building sequence %s for %s <%s> — angle: %s, structure: %s",
            sequence_id,
            profile.company_name,
            prospect_email,
            resolved_angle,
            resolved_structure,
        )

        emails: list[SequenceEmail] = []

        for step, day_offset in enumerate(SEQUENCE_DAYS, start=1):
            scheduled = (start + timedelta(days=day_offset - 1)).isoformat()

            if step == 1:
                # First email from OutreachGenerator
                outreach: OutreachEmail = self.generator.generate(
                    profile,
                    angle=resolved_angle,
                    structure=resolved_structure,
                    contact_name=contact_name,
                )
                subject = outreach.subject
                body = outreach.body
            else:
                # Follow-up emails from FOLLOWUP_TEMPLATES
                tmpl_fn = FOLLOWUP_TEMPLATES.get(step, _email5_breakup)
                subject, body = tmpl_fn(profile, contact_name, resolved_angle, resolved_structure)

            seq_email = SequenceEmail(
                sequence_id=sequence_id,
                step_number=step,
                day_offset=day_offset,
                scheduled_date=scheduled,
                subject=subject,
                body=body,
                angle=resolved_angle,
                structure=resolved_structure,
                prospect_email=prospect_email,
                prospect_name=contact_name,
                company_name=profile.company_name,
                hubspot_deal_id=hubspot_deal_id,
            )
            emails.append(seq_email)

        sequence = EmailSequence(
            sequence_id=sequence_id,
            prospect_email=prospect_email,
            prospect_name=contact_name,
            company_name=profile.company_name,
            hubspot_deal_id=hubspot_deal_id,
            angle=resolved_angle,
            structure=resolved_structure,
            start_date=start.isoformat(),
            emails=emails,
        )
        return sequence


# ---------------------------------------------------------------------------
# Supabase persistence
# ---------------------------------------------------------------------------

class SequenceStore:
    """Persists and retrieves email sequences from Supabase."""

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
        }

    def save_sequence(self, sequence: EmailSequence) -> None:
        """Save all emails in a sequence to Supabase."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            logger.warning("Supabase not configured — sequence not persisted")
            return

        url = f"{SUPABASE_URL}/rest/v1/email_sequences"
        rows = [e.to_dict() for e in sequence.emails]

        try:
            resp = SESSION.post(
                url,
                headers={**self._headers(), "Prefer": "return=representation"},
                json=rows,
                timeout=20,
            )
            resp.raise_for_status()
            saved = resp.json()
            # Back-fill Supabase IDs
            for i, row in enumerate(saved):
                if i < len(sequence.emails):
                    sequence.emails[i].supabase_id = row.get("id")
            logger.info(
                "Saved sequence %s (%d emails) to Supabase",
                sequence.sequence_id,
                len(rows),
            )
        except requests.RequestException as exc:
            logger.error("Failed to save sequence to Supabase: %s", exc)

    def load_pending(self) -> list[SequenceEmail]:
        """Load all pending emails with scheduled_date <= today."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            return []

        url = f"{SUPABASE_URL}/rest/v1/email_sequences"
        today = date.today().isoformat()
        params = {
            "select": "*",
            "status": "eq.pending",
            "scheduled_date": f"lte.{today}",
        }
        try:
            resp = SESSION.get(
                url, headers=self._headers(), params=params, timeout=20
            )
            resp.raise_for_status()
            rows = resp.json()
            emails = [SequenceEmail(**r) for r in rows]
            logger.info("Loaded %d pending emails due today", len(emails))
            return emails
        except requests.RequestException as exc:
            logger.error("Failed to load pending emails: %s", exc)
            return []

    def update_status(
        self,
        supabase_id: str,
        status: str,
        timestamp_field: Optional[str] = None,
    ) -> None:
        """Update the status and optional timestamp of a sequence email."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            return

        url = f"{SUPABASE_URL}/rest/v1/email_sequences?id=eq.{supabase_id}"
        payload: dict[str, Any] = {"status": status}
        if timestamp_field:
            payload[timestamp_field] = datetime.utcnow().isoformat()

        try:
            resp = SESSION.patch(url, headers=self._headers(), json=payload, timeout=15)
            resp.raise_for_status()
            logger.debug("Updated email %s → status=%s", supabase_id, status)
        except requests.RequestException as exc:
            logger.error("Status update failed for %s: %s", supabase_id, exc)

    def pause_sequence(self, sequence_id: str, reason: str) -> None:
        """Mark all pending emails in a sequence as 'skipped'."""
        if not SUPABASE_URL or not SUPABASE_KEY:
            return

        url = (
            f"{SUPABASE_URL}/rest/v1/email_sequences"
            f"?sequence_id=eq.{sequence_id}&status=eq.pending"
        )
        payload = {"status": "skipped"}
        try:
            resp = SESSION.patch(url, headers=self._headers(), json=payload, timeout=15)
            resp.raise_for_status()
            logger.info("Paused sequence %s — reason: %s", sequence_id, reason)
        except requests.RequestException as exc:
            logger.error("Pause sequence failed: %s", exc)


# ---------------------------------------------------------------------------
# Email sender (SMTP)
# ---------------------------------------------------------------------------

class EmailSender:
    """Sends emails via SMTP with retry logic."""

    def send(self, seq_email: SequenceEmail) -> bool:
        """
        Send a single sequence email via SMTP.

        Returns:
            True on success, False on failure.
        """
        if not SMTP_HOST:
            logger.warning("SMTP_HOST not configured — cannot send email")
            return False

        msg = MIMEMultipart("alternative")
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_FROM}>"
        msg["To"] = seq_email.prospect_email
        msg["Subject"] = seq_email.subject
        msg["Reply-To"] = SMTP_FROM

        # Plain text part
        msg.attach(MIMEText(seq_email.body, "plain", "utf-8"))

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
                server.ehlo()
                server.starttls(context=context)
                if SMTP_USER and SMTP_PASSWORD:
                    server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_FROM, [seq_email.prospect_email], msg.as_string())

            logger.info(
                "Email sent: step %d → %s (%s)",
                seq_email.step_number,
                seq_email.prospect_email,
                seq_email.subject,
            )
            return True

        except smtplib.SMTPRecipientsRefused as exc:
            logger.error(
                "Recipient refused %s: %s", seq_email.prospect_email, exc
            )
            return False
        except smtplib.SMTPException as exc:
            logger.error("SMTP error sending to %s: %s", seq_email.prospect_email, exc)
            return False
        except OSError as exc:
            logger.error("Network error sending email: %s", exc)
            return False


# ---------------------------------------------------------------------------
# HubSpot activity logger
# ---------------------------------------------------------------------------

class HubSpotActivityLogger:
    """Logs sent emails as activities on HubSpot deal timelines."""

    BASE_URL = "https://api.hubapi.com"

    def log_email_activity(self, seq_email: SequenceEmail) -> bool:
        """
        Create an email engagement on the HubSpot deal timeline.
        Returns True on success.
        """
        if not HUBSPOT_TOKEN or not seq_email.hubspot_deal_id:
            return False

        headers = {
            "Authorization": f"Bearer {HUBSPOT_TOKEN}",
            "Content-Type": "application/json",
        }
        payload = {
            "engagement": {
                "active": True,
                "type": "EMAIL",
                "timestamp": int(datetime.utcnow().timestamp() * 1000),
            },
            "associations": {
                "dealIds": [int(seq_email.hubspot_deal_id)],
            },
            "metadata": {
                "from": {"email": SMTP_FROM, "firstName": "Camelot", "lastName": "Deal Bot"},
                "to": [{"email": seq_email.prospect_email}],
                "subject": seq_email.subject,
                "text": seq_email.body[:5000],  # HubSpot limit
                "status": "SENT",
            },
        }
        try:
            resp = SESSION.post(
                f"{self.BASE_URL}/engagements/v1/engagements",
                headers=headers,
                json=payload,
                timeout=15,
            )
            resp.raise_for_status()
            logger.info(
                "HubSpot activity logged for deal %s, step %d",
                seq_email.hubspot_deal_id,
                seq_email.step_number,
            )
            return True
        except requests.RequestException as exc:
            logger.warning("HubSpot activity log failed: %s", exc)
            return False


# ---------------------------------------------------------------------------
# Sequence runner — processes all pending emails due today
# ---------------------------------------------------------------------------

class SequenceRunner:
    """Processes pending sequence emails and sends them."""

    def __init__(self) -> None:
        self.store = SequenceStore()
        self.sender = EmailSender()
        self.hs_logger = HubSpotActivityLogger()

    def run(self) -> dict[str, int]:
        """
        Send all pending emails due today.

        Returns:
            {'sent': N, 'failed': N, 'skipped': N}
        """
        pending = self.store.load_pending()
        counts = {"sent": 0, "failed": 0, "skipped": 0}

        if not pending:
            logger.info("No pending emails due today")
            return counts

        logger.info("Processing %d pending emails", len(pending))

        for seq_email in pending:
            if not seq_email.prospect_email or "@" not in seq_email.prospect_email:
                logger.warning(
                    "Invalid email address '%s' for sequence %s — skipping",
                    seq_email.prospect_email,
                    seq_email.sequence_id,
                )
                if seq_email.supabase_id:
                    self.store.update_status(seq_email.supabase_id, "skipped")
                counts["skipped"] += 1
                continue

            success = self.sender.send(seq_email)

            if success:
                if seq_email.supabase_id:
                    self.store.update_status(
                        seq_email.supabase_id, "sent", timestamp_field="sent_at"
                    )
                # Log to HubSpot
                self.hs_logger.log_email_activity(seq_email)
                counts["sent"] += 1
            else:
                if seq_email.supabase_id:
                    self.store.update_status(seq_email.supabase_id, "failed")
                counts["failed"] += 1

        logger.info(
            "Sequence run complete: sent=%d, failed=%d, skipped=%d",
            counts["sent"],
            counts["failed"],
            counts["skipped"],
        )
        return counts


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_and_enqueue_sequence(
    profile: ProspectProfile,
    prospect_email: str,
    hubspot_deal_id: str = "",
    angle: Optional[str] = None,
    structure: Optional[str] = None,
    start_date: Optional[date] = None,
) -> EmailSequence:
    """
    Build a 5-email sequence and persist it to Supabase for scheduled delivery.

    Args:
        profile:         ProspectProfile for personalization.
        prospect_email:  Recipient email address.
        hubspot_deal_id: HubSpot deal ID to associate emails with.
        angle:           Outreach angle override.
        structure:       Deal structure override.
        start_date:      Sequence start date (defaults to today).

    Returns:
        The created EmailSequence.
    """
    builder = SequenceBuilder()
    sequence = builder.build(
        profile=profile,
        prospect_email=prospect_email,
        hubspot_deal_id=hubspot_deal_id,
        angle=angle,
        structure=structure,
        start_date=start_date,
    )

    store = SequenceStore()
    store.save_sequence(sequence)

    logger.info(
        "Sequence enqueued: %s — %d emails starting %s",
        sequence.sequence_id,
        len(sequence.emails),
        sequence.start_date,
    )
    return sequence


def run_pending_sequences() -> dict[str, int]:
    """Process all pending emails due today. Called by the scheduler."""
    return SequenceRunner().run()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="Camelot Email Sequence Manager")
    subparsers = parser.add_subparsers(dest="cmd")

    # Create sequence from prospect JSON
    p_create = subparsers.add_parser("create", help="Create sequence for a prospect")
    p_create.add_argument("--profile", required=True, help="Path to prospect JSON")
    p_create.add_argument("--email", required=True, help="Recipient email address")
    p_create.add_argument("--deal-id", default="", help="HubSpot deal ID")
    p_create.add_argument("--angle", help="Outreach angle override")
    p_create.add_argument("--structure", help="Structure override")

    # Run pending emails
    subparsers.add_parser("run", help="Send all pending emails due today")

    # Preview sequence
    p_preview = subparsers.add_parser("preview", help="Preview sequence without sending")
    p_preview.add_argument("--profile", required=True)
    p_preview.add_argument("--email", required=True)

    args = parser.parse_args()

    if args.cmd == "run":
        counts = run_pending_sequences()
        print(f"Sent: {counts['sent']}  Failed: {counts['failed']}  Skipped: {counts['skipped']}")

    elif args.cmd in ("create", "preview"):
        with open(args.profile) as f:
            data = json.load(f)
        from dataclasses import fields as dc_fields
        valid_keys = {f.name for f in dc_fields(ProspectProfile)}
        profile = ProspectProfile(**{k: v for k, v in data.items() if k in valid_keys})

        if args.cmd == "create":
            seq = create_and_enqueue_sequence(
                profile=profile,
                prospect_email=args.email,
                hubspot_deal_id=getattr(args, "deal_id", ""),
                angle=getattr(args, "angle", None),
                structure=getattr(args, "structure", None),
            )
            print(f"Created sequence {seq.sequence_id} with {len(seq.emails)} emails")
            for e in seq.emails:
                print(f"  Step {e.step_number} — Day {e.day_offset} — {e.scheduled_date}: {e.subject}")
        else:  # preview
            builder = SequenceBuilder()
            seq = builder.build(profile=profile, prospect_email=args.email)
            for e in seq.emails:
                print(f"\n{'='*60}")
                print(f"STEP {e.step_number} — Day {e.day_offset} — Scheduled: {e.scheduled_date}")
                print(f"{'='*60}")
                print(f"Subject: {e.subject}\n")
                print(e.body)
    else:
        parser.print_help()
        sys.exit(1)
