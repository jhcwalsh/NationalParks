"""Match newly-detected availability events against active user scans.

For each event, find all scans whose criteria match, then trigger an
alert for each matched scan via the notifier.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any

from alert_engine import db
from alert_engine.models import AvailabilityEvent

logger = logging.getLogger(__name__)


async def match_and_alert(events: list[AvailabilityEvent]) -> None:
    """For each new availability event, find matching scans and trigger alerts."""
    # Import here to avoid circular import at module level
    from alert_engine.notifier import send_alert

    for event in events:
        scans = await db.get_active_scans_for_facility(event.facility_id)
        for scan in scans:
            if _matches(scan, event):
                logger.info(
                    "Match: scan %d ↔ site %s on %s",
                    scan["id"], event.site_id, event.available_date,
                )
                try:
                    await send_alert(scan, event)
                except Exception as exc:
                    logger.warning(
                        "Alert dispatch failed for scan %d: %s: %s",
                        scan["id"], type(exc).__name__, exc,
                    )


def _matches(scan: dict[str, Any], event: AvailabilityEvent) -> bool:
    """Apply all matching rules. Short-circuit on first failure."""

    # 1. Facility match
    if scan["facility_id"] != event.facility_id:
        return False

    # 2. Date match (exact or flexible ±2 days)
    scan_arrival = date.fromisoformat(str(scan["arrival_date"]))
    event_date = event.available_date

    if scan.get("flexible_arrival"):
        if not (scan_arrival - timedelta(days=2) <= event_date <= scan_arrival + timedelta(days=2)):
            return False
    else:
        if event_date != scan_arrival:
            return False

    # 3. Site type
    scan_type = (scan.get("site_type") or "any").lower()
    if scan_type != "any":
        event_type = (event.site_type or "").lower()
        if event_type and event_type != scan_type:
            return False

    # 4. Vehicle length
    scan_max_vl = scan.get("vehicle_length_max")
    event_vl = event.vehicle_length
    if scan_max_vl is not None and event_vl is not None:
        if event_vl > scan_max_vl:
            return False

    # 5. Specific site IDs
    specific = scan.get("specific_site_ids")
    if specific:
        if isinstance(specific, str):
            try:
                specific = json.loads(specific)
            except (json.JSONDecodeError, TypeError):
                specific = None
        if specific and event.site_id not in specific:
            return False

    # 6. Scan is active
    if not scan.get("active", False):
        return False

    return True
