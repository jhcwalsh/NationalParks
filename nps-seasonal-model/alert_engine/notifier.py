"""SMS + email alert dispatch.

Sends campsite availability notifications via Twilio — SMS through
the Twilio REST API and email through Twilio SendGrid's SMTP relay.
Enriches the message with conditions data (crowd score, AQI) when
available.
"""

from __future__ import annotations

import logging
import os
from email.message import EmailMessage
from typing import Any

from alert_engine import db
from alert_engine.enricher import get_conditions
from alert_engine.models import AvailabilityEvent

logger = logging.getLogger(__name__)

# ── Env config (all Twilio) ──────────────────────────────────────────────────

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "")
TWILIO_SENDGRID_API_KEY = os.getenv("TWILIO_SENDGRID_API_KEY", "")
TWILIO_FROM_EMAIL = os.getenv("TWILIO_FROM_EMAIL", "alerts@parkpulse.app")


def _build_message(
    scan: dict[str, Any],
    event: AvailabilityEvent,
    conditions: dict[str, Any] | None = None,
) -> str:
    """Build the human-readable alert message body."""
    park_name = scan.get("park_name", "Unknown Park")
    site_id = event.site_id
    loop = event.loop_name or "—"
    site_type = (event.site_type or "standard").capitalize()
    arrival = event.available_date.isoformat()
    num_nights = scan.get("num_nights", 1)

    lines = [
        f"ParkPulse alert: A campsite just opened at {park_name}!",
        "",
        f"Site {site_id} | {loop} | {site_type}",
        f"Dates: {arrival} for {num_nights} night{'s' if num_nights != 1 else ''}",
    ]

    # Conditions block (best-effort)
    if conditions:
        cond_lines: list[str] = []
        if conditions.get("crowd_label"):
            score = conditions.get("crowd_score")
            score_str = f" ({score * 100:.0f}/100)" if score is not None else ""
            cond_lines.append(f"  Crowd level: {conditions['crowd_label']}{score_str}")
        if conditions.get("aqi") is not None:
            cat = conditions.get("aqi_category", "")
            cond_lines.append(f"  Air quality: {cat} (AQI {conditions['aqi']})")
        if cond_lines:
            lines.append("")
            lines.append("Conditions at time of visit:")
            lines.extend(cond_lines)

    lines.append("")
    lines.append(f"Book now (act fast):")
    lines.append(f"https://www.recreation.gov/camping/campsites/{site_id}")
    lines.append("")
    lines.append("Reply STOP to unsubscribe.")

    return "\n".join(lines)


def _send_sms(to: str, body: str) -> str:
    """Send an SMS via Twilio. Returns the message SID."""
    from twilio.rest import Client

    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    message = client.messages.create(
        body=body,
        from_=TWILIO_FROM_NUMBER,
        to=to,
    )
    return message.sid


async def _send_email(to: str, subject: str, body: str) -> bool:
    """Send a plain-text email via Twilio SendGrid SMTP relay."""
    import aiosmtplib

    msg = EmailMessage()
    msg["From"] = TWILIO_FROM_EMAIL
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    await aiosmtplib.send(
        msg,
        hostname="smtp.sendgrid.net",
        port=587,
        username="apikey",
        password=TWILIO_SENDGRID_API_KEY,
        start_tls=True,
    )
    return True


async def send_alert(scan: dict[str, Any], event: AvailabilityEvent) -> None:
    """Enrich, build message, and dispatch via SMS and/or email."""
    # Enrich with conditions (best-effort, never blocks)
    conditions: dict[str, Any] | None = None
    try:
        conditions = await get_conditions(event.facility_id, event.available_date)
    except Exception as exc:
        logger.debug("Enrichment failed: %s", exc)

    body = _build_message(scan, event, conditions)
    park_name = scan.get("park_name", "Unknown Park")
    arrival = event.available_date.isoformat()

    # We need an event_id for logging — look up the most recent event matching
    # this facility+site+date. In practice the event was just inserted by the
    # poller, so it's the latest row.
    event_id = 0  # fallback

    # ── SMS ───────────────────────────────────────────────────────────────
    if scan.get("notify_sms") and TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
        dest = scan["notify_sms"]
        status = "sent"
        try:
            _send_sms(dest, body)
            logger.info("SMS sent to %s for scan %d", dest, scan["id"])
        except Exception as exc:
            status = "failed"
            logger.warning("SMS failed for scan %d: %s: %s", scan["id"], type(exc).__name__, exc)
        await db.insert_alert_log(scan["id"], event_id, "sms", dest, body, status)

    # ── Email ─────────────────────────────────────────────────────────────
    if scan.get("notify_email") and TWILIO_SENDGRID_API_KEY:
        dest = scan["notify_email"]
        subject = f"Campsite available at {park_name} — {arrival}"
        status = "sent"
        try:
            await _send_email(dest, subject, body)
            logger.info("Email sent to %s for scan %d", dest, scan["id"])
        except Exception as exc:
            status = "failed"
            logger.warning("Email failed for scan %d: %s: %s", scan["id"], type(exc).__name__, exc)
        await db.insert_alert_log(scan["id"], event_id, "email", dest, body, status)
