"""Campsite availability collector.

Continuously polls Recreation.gov for every facility listed in
``data/facilities.json``, records per-site status snapshots into DuckDB,
and detects status transitions (Available <-> Reserved, etc.).

Timing
------
The scheduler sleeps for 300 s +/- 30 s (uniform random jitter) between
cycles.  Exact-interval polling is a strong bot signal; jittered intervals
look like browser traffic.

Concurrency
-----------
HTTP fetches use ``httpx.AsyncClient`` with a semaphore (default 5) so we
hit Recreation.gov at a respectful rate.  DuckDB writes are synchronous —
one persistent read-write connection is held for the process lifetime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from parkpulse import db

logger = logging.getLogger(__name__)

# ── Configuration (env-overridable) ─────────────────────────────────────────

POLL_BASE_INTERVAL = int(os.getenv("POLL_BASE_INTERVAL", "300"))       # seconds
POLL_JITTER        = int(os.getenv("POLL_JITTER", "30"))               # +/- seconds
LOOKAHEAD_DAYS     = int(os.getenv("LOOKAHEAD_DAYS", "14"))            # days ahead
MAX_CONCURRENCY    = int(os.getenv("POLL_MAX_CONCURRENCY", "5"))       # parallel fetches
INTER_REQUEST_DELAY = float(os.getenv("POLL_REQUEST_DELAY", "0.5"))    # seconds

RECGOV_BASE = "https://www.recreation.gov/api/camps/availability/campground"
RECGOV_HEADERS = {
    "User-Agent": "ParkPulse/1.0 (campsite-availability-collector)",
}

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_FACILITIES_JSON = _PROJECT_ROOT / "data" / "facilities.json"

# ── Facility loading ────────────────────────────────────────────────────────


def load_facilities() -> list[dict[str, Any]]:
    """Load the facility catalog from ``data/facilities.json``."""
    if not _FACILITIES_JSON.exists():
        logger.error("facilities.json not found at %s", _FACILITIES_JSON)
        return []
    with open(_FACILITIES_JSON) as f:
        return json.load(f)


# ── Date helpers ────────────────────────────────────────────────────────────


def _months_covering(start: date, days: int) -> list[str]:
    """Return ``YYYY-MM-01T00:00:00.000Z`` strings spanning [start, start+days)."""
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


# ── HTTP layer ──────────────────────────────────────────────────────────────


async def _fetch_month(
    client: httpx.AsyncClient,
    facility_id: str,
    month_str: str,
    semaphore: asyncio.Semaphore,
) -> dict[str, Any]:
    """Fetch one month of availability for a facility, with retry on 429/503."""
    url = f"{RECGOV_BASE}/{facility_id}/month"
    params = {"start_date": month_str}

    async with semaphore:
        for attempt in range(3):
            try:
                resp = await client.get(url, params=params)
                if resp.status_code in (429, 503):
                    wait = 2 ** attempt + random.uniform(0, 1)
                    logger.warning(
                        "Facility %s month %s: HTTP %d, retry in %.1fs",
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
        # Add a small delay between requests to be respectful
        await asyncio.sleep(INTER_REQUEST_DELAY)
    return {}


async def fetch_facility(
    client: httpx.AsyncClient,
    facility_id: str,
    months: list[str],
    semaphore: asyncio.Semaphore,
) -> dict[str, dict[str, Any]]:
    """Fetch all months for a facility, return merged campsite data.

    Returns dict of campsite_id -> {availabilities: {date_str: status}, ...}
    """
    all_sites: dict[str, dict[str, Any]] = {}

    for month_str in months:
        data = await _fetch_month(client, facility_id, month_str, semaphore)
        campsites = data.get("campsites", {})
        for site_id, site_data in campsites.items():
            if site_id not in all_sites:
                all_sites[site_id] = {
                    "availabilities": {},
                    "campsite_type": site_data.get("campsite_type"),
                    "loop": site_data.get("loop"),
                    "max_vehicle_length": site_data.get("max_vehicle_length"),
                }
            all_sites[site_id]["availabilities"].update(
                site_data.get("availabilities", {})
            )

    return all_sites


# ── Core poll cycle ─────────────────────────────────────────────────────────


def _extract_snapshot_rows(
    poll_id: int,
    facility_id: str,
    sites: dict[str, dict[str, Any]],
    window_start: date,
    window_end: date,
    polled_at: datetime,
) -> list[tuple]:
    """Build (poll_id, facility_id, campsite_id, check_date, status, polled_at)
    tuples from raw API response for one facility."""
    rows: list[tuple] = []
    for site_id, site_data in sites.items():
        for date_str, status in site_data.get("availabilities", {}).items():
            try:
                check_date = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).date()
            except (ValueError, TypeError):
                continue
            if window_start <= check_date <= window_end:
                rows.append((
                    poll_id,
                    facility_id,
                    site_id,
                    check_date,
                    status,
                    polled_at,
                ))
    return rows


def _detect_transitions(
    poll_id: int,
    new_rows: list[tuple],
    prev_snapshot: dict[tuple[str, str], str],
    detected_at: datetime,
) -> list[tuple]:
    """Compare new observations against previous snapshot, emit transitions.

    Each transition tuple:
      (poll_id, facility_id, campsite_id, check_date,
       old_status, new_status, detected_at, days_to_arrival)
    """
    transitions: list[tuple] = []
    for row in new_rows:
        _, facility_id, campsite_id, check_date, new_status, _ = row
        key = (campsite_id, check_date.isoformat())
        old_status = prev_snapshot.get(key)
        if old_status is not None and old_status != new_status:
            days_to = (check_date - detected_at.date()).days
            transitions.append((
                poll_id,
                facility_id,
                campsite_id,
                check_date,
                old_status,
                new_status,
                detected_at,
                days_to,
            ))
    return transitions


async def poll_cycle(conn, db_path: str | None = None) -> dict[str, int]:
    """Execute one full poll cycle.

    Returns a stats dict: {n_facilities, n_sites, n_snapshots, n_transitions}.
    """
    facilities = load_facilities()
    if not facilities:
        logger.warning("No facilities to poll — check data/facilities.json")
        return {"n_facilities": 0, "n_sites": 0, "n_snapshots": 0, "n_transitions": 0}

    poll_id = db.start_poll(conn)
    now = datetime.utcnow()
    today = now.date()
    window_end = today + timedelta(days=LOOKAHEAD_DAYS)
    months = _months_covering(today, LOOKAHEAD_DAYS)

    logger.info(
        "Poll %d started: %d facilities, window %s..%s (%d months)",
        poll_id, len(facilities), today, window_end, len(months),
    )

    # Fetch the previous snapshot for diffing
    prev_snapshot = db.get_latest_snapshot(conn)

    # Async fetch all facilities
    api_key = os.getenv("RIDB_API_KEY", "")
    headers = {**RECGOV_HEADERS}
    if api_key:
        headers["apikey"] = api_key
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    all_snapshot_rows: list[tuple] = []
    all_transitions: list[tuple] = []
    unique_sites: set[str] = set()
    n_fac_ok = 0

    async with httpx.AsyncClient(headers=headers, timeout=30.0) as client:
        for fac in facilities:
            fac_id = fac["facility_id"]
            try:
                sites = await fetch_facility(client, fac_id, months, semaphore)
                rows = _extract_snapshot_rows(
                    poll_id, fac_id, sites, today, window_end, now,
                )
                all_snapshot_rows.extend(rows)
                unique_sites.update(site_id for _, _, site_id, *_ in rows)

                transitions = _detect_transitions(
                    poll_id, rows, prev_snapshot, now,
                )
                all_transitions.extend(transitions)
                n_fac_ok += 1
            except Exception:
                logger.exception("Failed to poll facility %s", fac_id)

    # Bulk-write to DuckDB (synchronous, single RW connection)
    n_snap = db.insert_snapshots(conn, all_snapshot_rows)
    n_trans = db.insert_transitions(conn, all_transitions)

    stats = {
        "n_facilities": n_fac_ok,
        "n_sites": len(unique_sites),
        "n_snapshots": n_snap,
        "n_transitions": n_trans,
    }

    status = "success" if n_fac_ok == len(facilities) else "partial"
    db.finish_poll(conn, poll_id, status=status, **stats)

    logger.info(
        "Poll %d finished: %d facilities, %d sites, %d snapshots, %d transitions [%s]",
        poll_id, stats["n_facilities"], stats["n_sites"],
        stats["n_snapshots"], stats["n_transitions"], status,
    )
    return stats


# ── Main loop ───────────────────────────────────────────────────────────────


async def run_forever(db_path: str | None = None) -> None:
    """Run the collector loop indefinitely with jittered sleep."""
    path = db_path or db.DEFAULT_DB_PATH
    conn = db.connect(path)
    db.init_schema(conn)

    logger.info(
        "Collector starting — base interval %ds, jitter +/-%ds, lookahead %dd",
        POLL_BASE_INTERVAL, POLL_JITTER, LOOKAHEAD_DAYS,
    )

    # Graceful shutdown on SIGINT / SIGTERM
    stop = asyncio.Event()

    def _handle_signal(sig, _frame):
        logger.info("Received %s — shutting down", signal.Signals(sig).name)
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_signal)

    while not stop.is_set():
        try:
            await poll_cycle(conn, db_path=path)
        except Exception:
            logger.exception("Poll cycle failed")

        jitter = random.uniform(-POLL_JITTER, POLL_JITTER)
        sleep_s = max(POLL_BASE_INTERVAL + jitter, 10)  # floor at 10s
        logger.info("Sleeping %.0fs until next poll", sleep_s)

        # Use wait() so SIGINT/SIGTERM can interrupt the sleep
        try:
            await asyncio.wait_for(stop.wait(), timeout=sleep_s)
        except asyncio.TimeoutError:
            pass  # normal — timeout means it's time for the next cycle

    conn.close()
    logger.info("Collector stopped")
