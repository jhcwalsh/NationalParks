"""Recreation.gov availability poller.

Polls the Recreation.gov campground availability API for all facilities
that have active scans, detects newly-available dates by diffing against
the stored snapshot, and passes new events to the matcher.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from alert_engine import db
from alert_engine.matcher import match_and_alert
from alert_engine.models import AvailabilityEvent

logger = logging.getLogger(__name__)

RECGOV_BASE = "https://www.recreation.gov/api/camps/availability/campground"
CANCELLATION_WINDOW_DAYS = int(os.getenv("CANCELLATION_WINDOW_DAYS", "14"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "120"))

# Recreation.gov expects these headers
RECGOV_HEADERS = {
    "User-Agent": "ParkPulse/1.0 (campsite-alert-engine)",
}

# Shared client — created lazily on first poll
_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        api_key = os.getenv("RIDB_API_KEY", "")
        headers = {**RECGOV_HEADERS}
        if api_key:
            headers["apikey"] = api_key
        _client = httpx.AsyncClient(headers=headers, timeout=30.0)
    return _client


def _months_in_window(start: date, days: int) -> list[str]:
    """Return the YYYY-MM-01 strings covering [start, start + days)."""
    end = start + timedelta(days=days)
    months: list[str] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        months.append(cursor.strftime("%Y-%m-01T00:00:00.000Z"))
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return months


async def _fetch_facility_month(
    client: httpx.AsyncClient,
    facility_id: str,
    month_str: str,
) -> dict[str, Any]:
    """Fetch one month of availability for a facility with retry on 429/503."""
    url = f"{RECGOV_BASE}/{facility_id}/month"
    params = {"start_date": month_str}

    for attempt in range(3):
        try:
            resp = await client.get(url, params=params)
            if resp.status_code in (429, 503):
                wait = (2 ** attempt)
                logger.warning(
                    "Facility %s month %s: %d, retrying in %ds",
                    facility_id, month_str, resp.status_code, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Facility %s month %s: HTTP %d",
                facility_id, month_str, exc.response.status_code,
            )
            return {}
        except Exception as exc:
            logger.warning(
                "Facility %s month %s: %s: %s",
                facility_id, month_str, type(exc).__name__, exc,
            )
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                return {}
    return {}


async def _poll_facility(facility_id: str) -> list[AvailabilityEvent]:
    """Poll a single facility and return newly-available events."""
    client = _get_client()
    today = date.today()
    months = _months_in_window(today, CANCELLATION_WINDOW_DAYS)
    new_events: list[AvailabilityEvent] = []

    # Fetch all months for this facility
    all_campsites: dict[str, dict[str, Any]] = {}
    for month_str in months:
        data = await _fetch_facility_month(client, facility_id, month_str)
        campsites = data.get("campsites", {})
        for site_id, site_data in campsites.items():
            if site_id not in all_campsites:
                all_campsites[site_id] = {
                    "availabilities": {},
                    "campsite_type": site_data.get("campsite_type"),
                    "max_vehicle_length": site_data.get("max_vehicle_length"),
                    "loop": site_data.get("loop"),
                }
            all_campsites[site_id]["availabilities"].update(
                site_data.get("availabilities", {})
            )
        await asyncio.sleep(0.5)  # rate-limit between month requests

    # Diff against snapshot for each site
    window_end = today + timedelta(days=CANCELLATION_WINDOW_DAYS)
    for site_id, site_data in all_campsites.items():
        availabilities = site_data.get("availabilities", {})
        current_available: list[str] = []

        for date_str, status in availabilities.items():
            if status != "Available":
                continue
            try:
                avail_date = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError):
                continue
            if today <= avail_date <= window_end:
                current_available.append(avail_date.isoformat())

        current_available.sort()

        # Get previous snapshot
        prev_available = await db.get_snapshot(facility_id, site_id)
        prev_set = set(prev_available)
        new_dates = [d for d in current_available if d not in prev_set]

        # Update snapshot
        await db.update_snapshot(facility_id, site_id, current_available)

        # Create events for truly new availabilities
        for date_iso in new_dates:
            campsite_type = site_data.get("campsite_type")
            site_type = None
            if campsite_type:
                ct = campsite_type.upper()
                if "TENT" in ct:
                    site_type = "tent"
                elif "RV" in ct or "TRAILER" in ct:
                    site_type = "rv"
                elif "GROUP" in ct:
                    site_type = "group"

            vehicle_length = None
            max_vl = site_data.get("max_vehicle_length")
            if max_vl is not None:
                try:
                    vehicle_length = int(max_vl)
                except (TypeError, ValueError):
                    pass

            event = AvailabilityEvent(
                facility_id=facility_id,
                site_id=site_id,
                available_date=date.fromisoformat(date_iso),
                site_type=site_type,
                vehicle_length=vehicle_length,
                loop_name=site_data.get("loop"),
            )
            event_id = await db.insert_availability_event(event.model_dump())
            new_events.append(event)

    return new_events


async def poll_all_facilities() -> None:
    """Poll all facilities that have active scans, then run the matcher."""
    facility_ids = await db.get_active_facility_ids()
    if not facility_ids:
        logger.info("No active scans — skipping poll cycle")
        return

    logger.info("Polling %d facilities", len(facility_ids))
    all_events: list[AvailabilityEvent] = []

    for fid in facility_ids:
        try:
            events = await _poll_facility(fid)
            if events:
                logger.info("Facility %s: %d new availability events", fid, len(events))
                all_events.extend(events)
            else:
                logger.info("Facility %s: no new availability", fid)
        except Exception as exc:
            logger.warning("Facility %s poll failed: %s: %s", fid, type(exc).__name__, exc)
        await asyncio.sleep(0.5)  # rate-limit between facilities

    if all_events:
        logger.info("Total new events: %d — running matcher", len(all_events))
        await match_and_alert(all_events)
    else:
        logger.info("Poll cycle complete — no new availability detected")


async def start_scheduler():
    """Start the APScheduler polling loop. Returns the scheduler instance."""
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        poll_all_facilities,
        trigger=IntervalTrigger(seconds=POLL_INTERVAL_SECONDS),
        id="campsite_poller",
        name="Campsite Availability Poller",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Poller scheduler started (interval=%ds)", POLL_INTERVAL_SECONDS)
    return scheduler
