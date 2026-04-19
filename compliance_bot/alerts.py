"""
alerts.py — Compliance Alert Engine
Camelot Property Management Services Corp / Compliance Bot

Runs full compliance scans across the portfolio, generates alert digests,
and sends email notifications to the operations team.

Recipients: dgoldoff@camelot.nyc, charkien@camelot.nyc

Author: Camelot OS
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Default alert recipients
DEFAULT_RECIPIENTS = [
    "dgoldoff@camelot.nyc",
    "charkien@camelot.nyc",
]

# SMTP config from environment
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "compliance@camelot.nyc")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", "Camelot Compliance Bot <compliance@camelot.nyc>")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ComplianceIssue:
    building_address: str
    building_id: Optional[str]               # BBL or BIN
    issue_type: str                          # HPD_VIOLATION, DOB_VIOLATION, LL97, RENT_STAB, PERMIT, ECB, SWO
    severity: str                            # CRITICAL / WARNING / INFO
    title: str
    description: str
    recommended_action: str
    source_url: Optional[str] = None
    days_to_deadline: Optional[int] = None
    violation_id: Optional[str] = None
    detected_at: str = ""

    def __post_init__(self):
        if not self.detected_at:
            self.detected_at = datetime.utcnow().isoformat() + "Z"


@dataclass
class ComplianceScanResult:
    portfolio_name: str
    scan_timestamp: str
    buildings_scanned: int
    critical_count: int
    warning_count: int
    info_count: int
    issues: list[ComplianceIssue] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Portfolio scan
# ---------------------------------------------------------------------------

def run_compliance_scan(
    portfolio: list[dict],
    include_hpd: bool = True,
    include_dob: bool = True,
    include_ll97: bool = True,
    include_rent_stab: bool = True,
) -> ComplianceScanResult:
    """
    Run full compliance scan across all buildings in a portfolio.

    Args:
        portfolio: List of building dicts. Each dict must have:
            - address (str)
            - bbl (str, preferred)
            - bin (str, optional) — for DOB lookups
            - gross_sq_ft (float, optional) — for LL97
            - asset_type (str, optional)
            - electricity_kwh (float, optional) — for LL97
            - natural_gas_kbtu (float, optional) — for LL97
            - building_id (str, optional) — internal Camelot ID
        include_hpd / include_dob / include_ll97 / include_rent_stab:
            Toggle individual check modules.

    Returns:
        ComplianceScanResult with all issues collected.
    """
    import time

    # Import check modules (deferred to avoid circular imports)
    from compliance_bot import hpd_violations, dob_violations, ll97_monitor, rent_stab_checker

    scan_start = time.time()
    issues: list[ComplianceIssue] = []
    errors: list[str] = []

    scan_ts = datetime.utcnow().isoformat() + "Z"
    logger.info(f"Starting compliance scan for {len(portfolio)} buildings at {scan_ts}")

    for building in portfolio:
        address = building.get("address", "Unknown")
        bbl = building.get("bbl")
        bin_number = building.get("bin")
        building_id = building.get("building_id") or bbl or bin_number

        logger.info(f"Scanning: {address} (BBL: {bbl})")

        # --- HPD Violations ---
        if include_hpd:
            try:
                violations = hpd_violations.get_open_violations(bbl=bbl, address=address)
                violations = hpd_violations.check_violation_deadlines(violations)
                for v in violations:
                    severity = _hpd_severity_to_alert(v.class_code, v.deadline_status)
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="HPD_VIOLATION",
                        severity=severity,
                        title=f"HPD Class {v.class_code} Violation — {v.violation_type}",
                        description=(
                            f"Violation ID: {v.violation_id} | Apt: {v.apartment or 'N/A'}\n"
                            f"Description: {v.novdescription[:200]}\n"
                            f"Deadline: {v.original_correct_by_date or 'Unknown'} | "
                            f"Days remaining: {v.days_to_deadline if v.days_to_deadline is not None else 'Unknown'}"
                        ),
                        recommended_action=v.recommended_action,
                        days_to_deadline=v.days_to_deadline,
                        violation_id=v.violation_id,
                    ))
            except Exception as e:
                msg = f"HPD scan error for {address}: {e}"
                logger.error(msg)
                errors.append(msg)

        # --- DOB Violations & Permits ---
        if include_dob and (bbl or bin_number or address):
            try:
                dob_summary = dob_violations.get_building_dob_summary(
                    bin_number=bin_number, address=address
                )
                for v in dob_summary.stop_work_orders:
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="SWO",
                        severity="CRITICAL",
                        title="STOP WORK ORDER — All construction must cease",
                        description=(
                            f"Violation: {v.violation_type_code} | "
                            f"Issued: {v.issue_date or 'Unknown'} | "
                            f"Days open: {v.days_open or 'Unknown'}\n"
                            f"{v.description or ''}"
                        ),
                        recommended_action=v.recommended_action,
                        violation_id=v.isn_dob_bis_viol,
                    ))
                for v in dob_summary.ecb_violations:
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="ECB",
                        severity="CRITICAL",
                        title=f"ECB Violation — {v.violation_type[:60]}",
                        description=(
                            f"ECB #: {v.ecb_number or v.isn_dob_bis_viol} | "
                            f"Issued: {v.issue_date or 'Unknown'}\n"
                            f"{v.description or ''}"
                        ),
                        recommended_action=v.recommended_action,
                        violation_id=v.ecb_number or v.isn_dob_bis_viol,
                    ))
                for v in dob_summary.open_violations:
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="DOB_VIOLATION",
                        severity="WARNING",
                        title=f"DOB Violation — {v.violation_type_code}: {v.violation_type[:50]}",
                        description=(
                            f"Issued: {v.issue_date or 'Unknown'} | "
                            f"Days open: {v.days_open or 'Unknown'}\n"
                            f"{(v.description or '')[:200]}"
                        ),
                        recommended_action=v.recommended_action,
                        violation_id=v.isn_dob_bis_viol,
                    ))
                for p in dob_summary.expired_permits:
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="PERMIT",
                        severity="WARNING",
                        title=f"Expired DOB Permit — {p.permit_type} #{p.permit_number}",
                        description=(
                            f"Expired: {p.expiration_date or 'Unknown'} | "
                            f"Work type: {p.work_type or 'N/A'}"
                        ),
                        recommended_action="Renew expired permit with DOB NOW. Unpermitted work creates liability.",
                    ))
            except Exception as e:
                msg = f"DOB scan error for {address}: {e}"
                logger.error(msg)
                errors.append(msg)

        # --- LL97 ---
        if include_ll97 and building.get("gross_sq_ft"):
            try:
                ll97_result = ll97_monitor.calculate_ll97_exposure(building)
                if ll97_result.phase_1_status == "NON-COMPLIANT":
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="LL97",
                        severity="CRITICAL",
                        title=f"LL97 NON-COMPLIANT — Est. Phase 1 Penalty: ${ll97_result.phase_1_annual_penalty:,.0f}/yr",
                        description=(
                            f"Carbon intensity: {ll97_result.actual_carbon_intensity:.5f} kgCO₂e/sqft/yr "
                            f"(limit: {ll97_result.phase_1_limit:.5f})\n"
                            f"Excess: {ll97_result.phase_1_excess_tons:.2f} metric tons\n"
                            f"Phase 2 est. penalty: ${ll97_result.phase_2_annual_penalty:,.0f}/yr"
                        ),
                        recommended_action="\n".join(ll97_result.recommended_actions[:3]),
                    ))
                elif ll97_result.phase_1_status == "MARGINAL":
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="LL97",
                        severity="WARNING",
                        title="LL97 MARGINAL — Within 10% of Phase 1 limit",
                        description=(
                            f"Carbon intensity: {ll97_result.actual_carbon_intensity:.5f} kgCO₂e/sqft/yr "
                            f"(limit: {ll97_result.phase_1_limit:.5f})"
                        ),
                        recommended_action="Begin energy efficiency planning to avoid future penalties.",
                    ))
            except Exception as e:
                msg = f"LL97 scan error for {address}: {e}"
                logger.error(msg)
                errors.append(msg)

        # --- Rent Stabilization ---
        if include_rent_stab and bbl:
            try:
                rs_result = rent_stab_checker.check_rent_stab_status(address, bbl=bbl)
                if rs_result.status == "FLAGGED":
                    issues.append(ComplianceIssue(
                        building_address=address,
                        building_id=building_id,
                        issue_type="RENT_STAB",
                        severity="WARNING",
                        title="Rent Stabilization Registration Issue",
                        description="\n".join(rs_result.flags[:3]),
                        recommended_action="\n".join(rs_result.recommended_actions[:2]),
                    ))
            except Exception as e:
                msg = f"Rent stab scan error for {address}: {e}"
                logger.error(msg)
                errors.append(msg)

    duration = time.time() - scan_start

    critical = [i for i in issues if i.severity == "CRITICAL"]
    warnings = [i for i in issues if i.severity == "WARNING"]
    infos = [i for i in issues if i.severity == "INFO"]

    result = ComplianceScanResult(
        portfolio_name="Camelot Portfolio",
        scan_timestamp=scan_ts,
        buildings_scanned=len(portfolio),
        critical_count=len(critical),
        warning_count=len(warnings),
        info_count=len(infos),
        issues=sorted(issues, key=lambda x: {"CRITICAL": 0, "WARNING": 1, "INFO": 2}.get(x.severity, 3)),
        errors=errors,
        duration_seconds=round(duration, 2),
    )

    logger.info(
        f"Scan complete: {len(portfolio)} buildings, "
        f"{len(critical)} critical, {len(warnings)} warnings, "
        f"{len(infos)} info, {len(errors)} errors in {duration:.1f}s"
    )
    return result


# ---------------------------------------------------------------------------
# Digest generation
# ---------------------------------------------------------------------------

def generate_alert_digest(scan_result: ComplianceScanResult) -> str:
    """
    Format compliance issues into a structured Markdown/text digest.

    Returns:
        Formatted digest string grouped by severity.
    """
    ts = scan_result.scan_timestamp
    lines = [
        "=" * 72,
        "CAMELOT PROPERTY MANAGEMENT — COMPLIANCE ALERT DIGEST",
        "=" * 72,
        f"Scan Date: {ts}",
        f"Buildings Scanned: {scan_result.buildings_scanned}",
        f"Duration: {scan_result.duration_seconds:.1f}s",
        "",
        f"SUMMARY: {scan_result.critical_count} CRITICAL | "
        f"{scan_result.warning_count} WARNING | "
        f"{scan_result.info_count} INFO",
        "=" * 72,
        "",
    ]

    # Group by severity
    for severity_label in ("CRITICAL", "WARNING", "INFO"):
        severity_issues = [i for i in scan_result.issues if i.severity == severity_label]
        if not severity_issues:
            continue

        sep = "🚨" if severity_label == "CRITICAL" else ("⚠️" if severity_label == "WARNING" else "ℹ️")
        lines.append(f"{'─' * 72}")
        lines.append(f"{sep}  {severity_label} — {len(severity_issues)} Issue(s)")
        lines.append(f"{'─' * 72}")
        lines.append("")

        # Group by building
        by_building: dict[str, list[ComplianceIssue]] = {}
        for issue in severity_issues:
            by_building.setdefault(issue.building_address, []).append(issue)

        for building_addr, b_issues in by_building.items():
            lines.append(f"📍 {building_addr}")
            for issue in b_issues:
                lines.append(f"   [{issue.issue_type}] {issue.title}")
                for desc_line in issue.description.splitlines():
                    lines.append(f"      {desc_line.strip()}")
                lines.append(f"   → ACTION: {issue.recommended_action[:200]}")
                if issue.days_to_deadline is not None:
                    lines.append(f"   ⏰ Days to deadline: {issue.days_to_deadline}")
                lines.append("")
        lines.append("")

    if scan_result.errors:
        lines.append("─" * 72)
        lines.append(f"SCAN ERRORS ({len(scan_result.errors)})")
        for err in scan_result.errors:
            lines.append(f"  ! {err}")
        lines.append("")

    lines.append("=" * 72)
    lines.append("Generated by Camelot Compliance Bot")
    lines.append("Questions: dgoldoff@camelot.nyc | charkien@camelot.nyc")
    lines.append("=" * 72)

    return "\n".join(lines)


def generate_html_digest(scan_result: ComplianceScanResult) -> str:
    """Generate an HTML-formatted email digest."""
    GOLD = "#C9A84C"
    NAVY = "#1A2645"
    RED = "#DC2626"
    AMBER = "#D97706"
    BLUE = "#2563EB"

    severity_colors = {"CRITICAL": RED, "WARNING": AMBER, "INFO": BLUE}
    severity_icons = {"CRITICAL": "🚨", "WARNING": "⚠️", "INFO": "ℹ️"}

    html_parts = [f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; font-size: 13px; color: #333; margin: 0; padding: 0; }}
  .header {{ background: {NAVY}; color: white; padding: 20px 24px; }}
  .header h1 {{ margin: 0; font-size: 18px; color: {GOLD}; }}
  .header p {{ margin: 4px 0 0; font-size: 12px; opacity: 0.8; }}
  .summary-bar {{ background: #f5f5f5; padding: 12px 24px; border-bottom: 2px solid {GOLD}; display: flex; gap: 24px; }}
  .summary-item {{ font-size: 14px; font-weight: bold; }}
  .summary-item.critical {{ color: {RED}; }}
  .summary-item.warning {{ color: {AMBER}; }}
  .summary-item.info {{ color: {BLUE}; }}
  .section {{ margin: 0; padding: 16px 24px; border-bottom: 1px solid #eee; }}
  .section-header {{ font-size: 14px; font-weight: bold; padding: 8px 12px; border-radius: 4px; color: white; margin-bottom: 12px; }}
  .building {{ margin-bottom: 16px; }}
  .building-name {{ font-weight: bold; font-size: 13px; color: {NAVY}; margin-bottom: 6px; }}
  .issue {{ background: #fafafa; border-left: 3px solid #ddd; padding: 8px 12px; margin-bottom: 6px; border-radius: 2px; }}
  .issue-title {{ font-weight: bold; font-size: 13px; }}
  .issue-desc {{ color: #666; font-size: 12px; margin: 4px 0; white-space: pre-line; }}
  .issue-action {{ color: {NAVY}; font-size: 12px; font-style: italic; }}
  .footer {{ background: #f5f5f5; padding: 12px 24px; font-size: 11px; color: #999; }}
</style>
</head>
<body>
<div class="header">
  <h1>Camelot Compliance Alert Digest</h1>
  <p>Scan: {scan_result.scan_timestamp} &nbsp;|&nbsp; Buildings: {scan_result.buildings_scanned}</p>
</div>
<div class="summary-bar">
  <span class="summary-item critical">🚨 {scan_result.critical_count} Critical</span>
  <span class="summary-item warning">⚠️ {scan_result.warning_count} Warning</span>
  <span class="summary-item info">ℹ️ {scan_result.info_count} Info</span>
</div>
"""]

    for severity_label in ("CRITICAL", "WARNING", "INFO"):
        severity_issues = [i for i in scan_result.issues if i.severity == severity_label]
        if not severity_issues:
            continue

        color = severity_colors[severity_label]
        icon = severity_icons[severity_label]

        html_parts.append(f'<div class="section">')
        html_parts.append(
            f'<div class="section-header" style="background:{color}">'
            f'{icon} {severity_label} — {len(severity_issues)} Issue(s)</div>'
        )

        by_building: dict[str, list[ComplianceIssue]] = {}
        for issue in severity_issues:
            by_building.setdefault(issue.building_address, []).append(issue)

        for building_addr, b_issues in by_building.items():
            html_parts.append(f'<div class="building">')
            html_parts.append(f'<div class="building-name">📍 {_html_escape(building_addr)}</div>')
            for issue in b_issues:
                deadline_str = (
                    f'<br><small>⏰ <strong>{issue.days_to_deadline} days</strong> to deadline</small>'
                    if issue.days_to_deadline is not None else ""
                )
                html_parts.append(f"""<div class="issue" style="border-left-color:{color}">
  <div class="issue-title">[{_html_escape(issue.issue_type)}] {_html_escape(issue.title)}</div>
  <div class="issue-desc">{_html_escape(issue.description[:300])}</div>
  <div class="issue-action">→ {_html_escape(issue.recommended_action[:250])}</div>
  {deadline_str}
</div>""")
            html_parts.append('</div>')
        html_parts.append('</div>')

    html_parts.append(f"""<div class="footer">
  Generated by Camelot Compliance Bot &nbsp;|&nbsp;
  Questions: <a href="mailto:dgoldoff@camelot.nyc">dgoldoff@camelot.nyc</a> &nbsp;|&nbsp;
  <a href="mailto:charkien@camelot.nyc">charkien@camelot.nyc</a>
</div>
</body>
</html>""")

    return "\n".join(html_parts)


# ---------------------------------------------------------------------------
# Email dispatch
# ---------------------------------------------------------------------------

def send_compliance_alert(
    scan_result: ComplianceScanResult,
    recipients: Optional[list[str]] = None,
    subject_prefix: str = "[Camelot Compliance]",
    send_only_if_issues: bool = True,
) -> bool:
    """
    Send compliance alert digest via email.

    Args:
        scan_result:        Output of run_compliance_scan()
        recipients:         List of email addresses. Defaults to DEFAULT_RECIPIENTS.
        subject_prefix:     Email subject prefix.
        send_only_if_issues: If True, skips sending when no issues found.

    Returns:
        True if email sent successfully, False otherwise.
    """
    if recipients is None:
        recipients = DEFAULT_RECIPIENTS

    total_issues = scan_result.critical_count + scan_result.warning_count + scan_result.info_count

    if send_only_if_issues and total_issues == 0:
        logger.info("No compliance issues found — skipping email alert")
        return True

    # Subject line
    if scan_result.critical_count > 0:
        subject = (
            f"{subject_prefix} 🚨 {scan_result.critical_count} CRITICAL ISSUES — "
            f"{datetime.now().strftime('%Y-%m-%d')}"
        )
    elif scan_result.warning_count > 0:
        subject = (
            f"{subject_prefix} ⚠️ {scan_result.warning_count} Warnings — "
            f"{datetime.now().strftime('%Y-%m-%d')}"
        )
    else:
        subject = f"{subject_prefix} Portfolio Compliance Digest — {datetime.now().strftime('%Y-%m-%d')}"

    plain_body = generate_alert_digest(scan_result)
    html_body = generate_html_digest(scan_result)

    if not SMTP_PASSWORD:
        logger.warning("SMTP_PASSWORD not set — cannot send email. Printing digest to stdout.")
        print(plain_body)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = SMTP_FROM
        msg["To"] = ", ".join(recipients)

        msg.attach(MIMEText(plain_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())

        logger.info(
            f"Compliance alert sent to {recipients} — "
            f"{scan_result.critical_count} critical, {scan_result.warning_count} warnings"
        )
        return True

    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending compliance alert: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error sending alert: {e}")
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hpd_severity_to_alert(class_code: str, deadline_status: str) -> str:
    if class_code.upper() == "C":
        return "CRITICAL"
    if class_code.upper() == "B" or deadline_status in ("OVERDUE", "CRITICAL", "WARNING"):
        return "WARNING"
    return "INFO"


def _html_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    # Demo: run scan on a sample portfolio
    sample_portfolio = [
        {
            "address": "123 Main Street, Bronx, NY",
            "bbl": "2025010012",
            "bin": "2000001",
            "gross_sq_ft": 25000,
            "asset_type": "multifamily",
            "electricity_kwh": 150000,
            "natural_gas_kbtu": 1000000,
            "building_id": "CAM-001",
        },
    ]

    result = run_compliance_scan(
        sample_portfolio,
        include_hpd=True,
        include_dob=True,
        include_ll97=True,
        include_rent_stab=True,
    )

    digest = generate_alert_digest(result)
    print(digest)

    # Optionally send email
    if "--send" in sys.argv:
        send_compliance_alert(result)
