"""
utils/emailer.py
-----------------
SMTP email sender for Scout Bot daily lead reports.

Sends a styled HTML email with:
  - Summary statistics table
  - Top leads table (score-sorted)
  - PDF report attached as "Scout_Daily_Report_YYYY-MM-DD.pdf"
  - Optional enriched CSV attached as "Scout_Leads_YYYY-MM-DD.csv"

Configuration (environment variables):
  SMTP_HOST      — SMTP server hostname (required)
  SMTP_PORT      — SMTP port, default 587
  SMTP_USER      — SMTP auth username (required)
  SMTP_PASSWORD  — SMTP auth password (required)
  SMTP_FROM      — Sender address (default: leads-bot@camelot.nyc)
  SMTP_USE_TLS   — "true" / "false" (default: true, uses STARTTLS on port 587)
  SMTP_USE_SSL   — "true" / "false" (default: false, SSL/TLS direct on port 465)

Default recipients:
  dgoldoff@camelot.nyc, slodge@camelot.nyc,
  luigi@camelot.nyc, charkien@camelot.nyc
"""

import logging
import os
import smtplib
import ssl
from datetime import date
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_FROM = "leads-bot@camelot.nyc"
DEFAULT_RECIPIENTS = [
    "dgoldoff@camelot.nyc",
    "slodge@camelot.nyc",
    "luigi@camelot.nyc",
    "charkien@camelot.nyc",
]

# Camelot brand colours
NAVY = "#1A2645"
GOLD = "#C9A84C"
LIGHT_GREY = "#F5F5F5"
WHITE = "#FFFFFF"
TEXT = "#222222"
MID_GREY = "#888888"

# ---------------------------------------------------------------------------
# SMTP config loader
# ---------------------------------------------------------------------------

def _get_smtp_config() -> Dict[str, Any]:
    """Load SMTP settings from environment variables.

    Returns:
        Dict with keys: host, port, user, password, from_addr, use_tls, use_ssl.

    Raises:
        ValueError: If required SMTP_HOST, SMTP_USER, or SMTP_PASSWORD are missing.
    """
    host = os.environ.get("SMTP_HOST", "").strip()
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()

    if not host:
        raise ValueError("SMTP_HOST environment variable is not set.")
    if not user:
        raise ValueError("SMTP_USER environment variable is not set.")
    if not password:
        raise ValueError("SMTP_PASSWORD environment variable is not set.")

    use_ssl = os.environ.get("SMTP_USE_SSL", "false").strip().lower() == "true"
    use_tls = os.environ.get("SMTP_USE_TLS", "true").strip().lower() == "true"
    default_port = 465 if use_ssl else 587
    port = int(os.environ.get("SMTP_PORT", str(default_port)))
    from_addr = os.environ.get("SMTP_FROM", DEFAULT_FROM).strip()

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "from_addr": from_addr,
        "use_tls": use_tls,
        "use_ssl": use_ssl,
    }


# ---------------------------------------------------------------------------
# HTML builders
# ---------------------------------------------------------------------------

def _score_colour(score: int) -> str:
    """Return a hex colour for a given lead score."""
    if score >= 70:
        return "#1A7A1A"
    if score >= 40:
        return GOLD
    return "#CC3300"


def _build_html_body(
    leads: List[Dict[str, Any]],
    run_date: str,
) -> str:
    """Build the full HTML email body.

    Args:
        leads: List of Scout lead dicts (score-sorted descending).
        run_date: Human-readable date string for the header.

    Returns:
        HTML string.
    """
    total = len(leads)
    acquisitions = sum(1 for l in leads if l.get("lead_type") == "Acquisition")
    mandates = sum(1 for l in leads if l.get("lead_type") == "Management mandate")
    rfps = sum(1 for l in leads if l.get("category") == "RFP")
    hiring = sum(1 for l in leads if l.get("lead_type") == "Hiring signal")
    unmanaged = sum(1 for l in leads if l.get("lead_type") == "Unmanaged building")
    top_score = max((l.get("score", 0) for l in leads), default=0)

    def _stat_cell(label: str, value: str, colour: str = NAVY) -> str:
        return f"""
        <td style="padding:12px 18px; text-align:center; border-right:1px solid #ddd;">
          <div style="font-size:22px; font-weight:700; color:{colour};">{value}</div>
          <div style="font-size:11px; color:{MID_GREY}; margin-top:3px;">{label}</div>
        </td>"""

    stat_cells = (
        _stat_cell("TOTAL LEADS", str(total))
        + _stat_cell("ACQUISITIONS", str(acquisitions), GOLD)
        + _stat_cell("MANDATES", str(mandates))
        + _stat_cell("RFPs", str(rfps))
        + _stat_cell("HIRING SIGNALS", str(hiring))
        + _stat_cell("UNMANAGED BLDGS", str(unmanaged))
        + _stat_cell("TOP SCORE", str(top_score), GOLD)
    )

    # Build lead rows (top 30)
    lead_rows_html = ""
    for i, lead in enumerate(leads[:30], start=1):
        score = lead.get("score", 0)
        sc = _score_colour(score)
        company = (lead.get("company_name") or lead.get("title") or "—")[:55]
        region = lead.get("region", "—")
        lead_type = lead.get("lead_type") or lead.get("category") or "—"
        source = lead.get("source_site", "—")[:22]

        # Best contact line
        contacts = lead.get("contacts") or []
        if contacts:
            c = contacts[0]
            contact_html = (
                f"<b>{c.get('name','')}</b><br>"
                f"<span style='color:{MID_GREY};'>{c.get('title','')}</span><br>"
                f"<a href='mailto:{c.get('email','')}' style='color:{NAVY};'>"
                f"{c.get('email','')}</a>"
            )
        else:
            emails = lead.get("email") or []
            contact_html = (
                f"<a href='mailto:{emails[0]}' style='color:{NAVY};'>{emails[0]}</a>"
                if emails else "—"
            )

        link = lead.get("link", "")
        company_linked = (
            f"<a href='{link}' style='color:{NAVY}; text-decoration:none;'>{company}</a>"
            if link else company
        )

        row_bg = WHITE if i % 2 != 0 else LIGHT_GREY
        lead_rows_html += f"""
        <tr style="background:{row_bg};">
          <td style="padding:7px 8px; color:{MID_GREY}; font-size:12px; text-align:center;">{i}</td>
          <td style="padding:7px 8px; font-weight:600; font-size:13px;">{company_linked}</td>
          <td style="padding:7px 8px; font-size:12px;">{region}</td>
          <td style="padding:7px 8px; font-size:12px;">{lead_type}</td>
          <td style="padding:7px 8px; font-size:12px;">{source}</td>
          <td style="padding:7px 8px; text-align:center;">
            <span style="font-weight:700; font-size:14px; color:{sc};">{score}</span>
          </td>
          <td style="padding:7px 8px; font-size:12px; line-height:1.4;">{contact_html}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scout Bot Daily Digest — {run_date}</title>
</head>
<body style="margin:0; padding:0; background:#ECECEC; font-family:Arial,Helvetica,sans-serif;">

<!-- Outer wrapper -->
<table width="100%" cellpadding="0" cellspacing="0" style="background:#ECECEC;">
<tr><td align="center" style="padding:24px 16px;">

<!-- Card -->
<table width="680" cellpadding="0" cellspacing="0"
       style="background:#fff; border-radius:6px; overflow:hidden;
              box-shadow:0 2px 12px rgba(0,0,0,0.08); max-width:100%;">

  <!-- Header -->
  <tr>
    <td style="background:{NAVY}; padding:18px 28px;">
      <table width="100%" cellpadding="0" cellspacing="0">
        <tr>
          <td>
            <span style="font-size:28px; font-weight:900; color:{GOLD};">C</span>
            <span style="font-size:16px; font-weight:700; color:{WHITE};">amelot</span>
            <div style="font-size:11px; color:{GOLD}; margin-top:2px;">
              Property Management Services Corp.
            </div>
          </td>
          <td align="right">
            <div style="font-size:18px; font-weight:700; color:{WHITE};">
              Scout Bot — Daily Lead Digest
            </div>
            <div style="font-size:12px; color:{GOLD}; margin-top:3px;">{run_date}</div>
          </td>
        </tr>
      </table>
    </td>
  </tr>

  <!-- Gold accent bar -->
  <tr><td style="height:3px; background:{GOLD};"></td></tr>

  <!-- Stats bar -->
  <tr>
    <td style="padding:0;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border-bottom:1px solid #ddd;">
        <tr>
          {stat_cells}
        </tr>
      </table>
    </td>
  </tr>

  <!-- Section header -->
  <tr>
    <td style="padding:16px 24px 8px;">
      <div style="font-size:13px; font-weight:700; color:{NAVY};
                  text-transform:uppercase; letter-spacing:0.06em;">
        Top Leads This Run
      </div>
    </td>
  </tr>

  <!-- Lead table -->
  <tr>
    <td style="padding:0 16px 16px;">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="border:1px solid #ddd; border-radius:4px; overflow:hidden;
                    font-size:13px;">
        <thead>
          <tr style="background:{NAVY}; color:{WHITE};">
            <th style="padding:8px 8px; width:28px;">#</th>
            <th style="padding:8px 8px; text-align:left;">COMPANY / TITLE</th>
            <th style="padding:8px 8px; text-align:left; width:50px;">REGION</th>
            <th style="padding:8px 8px; text-align:left; width:110px;">TYPE</th>
            <th style="padding:8px 8px; text-align:left; width:100px;">SOURCE</th>
            <th style="padding:8px 8px; text-align:center; width:50px;">SCORE</th>
            <th style="padding:8px 8px; text-align:left;">CONTACT</th>
          </tr>
        </thead>
        <tbody>
          {lead_rows_html}
        </tbody>
      </table>
    </td>
  </tr>

  <!-- Footer note -->
  <tr>
    <td style="background:{LIGHT_GREY}; padding:14px 24px;
               border-top:1px solid #ddd; text-align:center;">
      <p style="margin:0; font-size:11px; color:{MID_GREY};">
        Full report and lead data attached. This message is confidential and
        intended only for Camelot Property Management Services Corp. internal use.
      </p>
      <p style="margin:6px 0 0; font-size:11px; color:{MID_GREY};">
        © {date.today().year} Camelot Property Management Services Corp. — Scout Bot v1.0
      </p>
    </td>
  </tr>

</table>
<!-- /Card -->

</td></tr></table>
<!-- /Outer wrapper -->

</body>
</html>"""
    return html


# ---------------------------------------------------------------------------
# Core send function
# ---------------------------------------------------------------------------

def send_daily_report(
    to_emails: Optional[List[str]] = None,
    leads_df: Any = None,
    pdf_bytes: Optional[bytes] = None,
    csv_bytes: Optional[bytes] = None,
    enriched_csv_bytes: Optional[bytes] = None,
) -> bool:
    """Send the Scout Bot daily digest email.

    Sends an HTML email with an optional PDF attachment and optional CSV
    attachments.

    Args:
        to_emails: List of recipient addresses. Defaults to
                   ``DEFAULT_RECIPIENTS``.
        leads_df: Pandas DataFrame or list of Scout lead dicts for the
                  HTML summary table. May be ``None`` (empty table shown).
        pdf_bytes: Raw PDF bytes to attach as
                   ``Scout_Daily_Report_YYYY-MM-DD.pdf``. May be ``None``.
        csv_bytes: Raw leads CSV bytes to attach. May be ``None``.
        enriched_csv_bytes: Raw enriched CSV bytes to attach. May be ``None``.

    Returns:
        ``True`` if the email was sent successfully, ``False`` otherwise.
    """
    recipients = to_emails or DEFAULT_RECIPIENTS

    # Normalise leads to list
    if leads_df is None:
        leads: List[Dict[str, Any]] = []
    elif hasattr(leads_df, "to_dict"):
        leads = leads_df.to_dict(orient="records")
    else:
        leads = list(leads_df)

    # Sort by score descending for the table
    leads_sorted = sorted(leads, key=lambda l: l.get("score", 0), reverse=True)

    run_date = date.today().strftime("%A, %B %d, %Y")
    subject = f"Scout Bot Daily Digest — {date.today().strftime('%Y-%m-%d')} — {len(leads)} leads"

    html_body = _build_html_body(leads_sorted, run_date)
    plain_body = (
        f"Scout Bot Daily Lead Digest — {run_date}\n"
        f"Total leads: {len(leads)}\n"
        f"Please view this email in an HTML-capable client.\n"
        f"Full report attached."
    )

    # Build MIME message
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SMTP_FROM", DEFAULT_FROM)
    msg["To"] = ", ".join(recipients)
    msg["X-Mailer"] = "Camelot OS Scout Bot"

    # Attach both plain and HTML as alternatives
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(plain_body, "plain", "utf-8"))
    alt_part.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt_part)

    # Attach PDF
    if pdf_bytes:
        pdf_name = f"Scout_Daily_Report_{date.today().isoformat()}.pdf"
        pdf_part = MIMEBase("application", "pdf")
        pdf_part.set_payload(pdf_bytes)
        encoders.encode_base64(pdf_part)
        pdf_part.add_header("Content-Disposition", "attachment", filename=pdf_name)
        msg.attach(pdf_part)
        logger.debug("Attached PDF: %s (%d bytes)", pdf_name, len(pdf_bytes))

    # Attach leads CSV
    if csv_bytes:
        csv_name = f"Scout_Leads_{date.today().isoformat()}.csv"
        csv_part = MIMEBase("text", "csv")
        csv_part.set_payload(csv_bytes)
        encoders.encode_base64(csv_part)
        csv_part.add_header("Content-Disposition", "attachment", filename=csv_name)
        msg.attach(csv_part)
        logger.debug("Attached CSV: %s (%d bytes)", csv_name, len(csv_bytes))

    # Attach enriched CSV
    if enriched_csv_bytes:
        enriched_name = f"Scout_Leads_Enriched_{date.today().isoformat()}.csv"
        enc_part = MIMEBase("text", "csv")
        enc_part.set_payload(enriched_csv_bytes)
        encoders.encode_base64(enc_part)
        enc_part.add_header("Content-Disposition", "attachment", filename=enriched_name)
        msg.attach(enc_part)
        logger.debug("Attached enriched CSV: %s (%d bytes)", enriched_name, len(enriched_csv_bytes))

    # Send
    try:
        cfg = _get_smtp_config()
    except ValueError as exc:
        logger.error("SMTP config error: %s", exc)
        return False

    logger.info(
        "Sending daily report to %d recipients via %s:%d …",
        len(recipients), cfg["host"], cfg["port"],
    )

    try:
        context = ssl.create_default_context()

        if cfg["use_ssl"]:
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], context=context) as server:
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["from_addr"], recipients, msg.as_string())
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
                server.ehlo()
                if cfg["use_tls"]:
                    server.starttls(context=context)
                    server.ehlo()
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["from_addr"], recipients, msg.as_string())

        logger.info("Daily report sent successfully to: %s", ", ".join(recipients))
        return True

    except smtplib.SMTPAuthenticationError as exc:
        logger.error("SMTP authentication failed: %s", exc)
    except smtplib.SMTPRecipientsRefused as exc:
        logger.error("SMTP recipients refused: %s", exc)
    except smtplib.SMTPException as exc:
        logger.error("SMTP error sending report: %s", exc)
    except OSError as exc:
        logger.error("Network error contacting SMTP server: %s", exc)

    return False


# ---------------------------------------------------------------------------
# Convenience: send with just a message body (for alerts / test pings)
# ---------------------------------------------------------------------------

def send_alert(
    subject: str,
    body: str,
    to_emails: Optional[List[str]] = None,
) -> bool:
    """Send a plain-text alert email to the Camelot team.

    Args:
        subject: Email subject line.
        body: Plain-text email body.
        to_emails: Override recipient list.

    Returns:
        True on success.
    """
    recipients = to_emails or DEFAULT_RECIPIENTS

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = os.environ.get("SMTP_FROM", DEFAULT_FROM)
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        cfg = _get_smtp_config()
    except ValueError as exc:
        logger.error("SMTP config error: %s", exc)
        return False

    try:
        context = ssl.create_default_context()
        if cfg["use_ssl"]:
            with smtplib.SMTP_SSL(cfg["host"], cfg["port"], context=context) as server:
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["from_addr"], recipients, msg.as_string())
        else:
            with smtplib.SMTP(cfg["host"], cfg["port"]) as server:
                server.ehlo()
                if cfg["use_tls"]:
                    server.starttls(context=context)
                    server.ehlo()
                server.login(cfg["user"], cfg["password"])
                server.sendmail(cfg["from_addr"], recipients, msg.as_string())
        logger.info("Alert sent: %s", subject)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to send alert '%s': %s", subject, exc)
        return False
