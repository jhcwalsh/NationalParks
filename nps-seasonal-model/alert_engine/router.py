"""FastAPI routes for the campsite alert engine.

Mounted at prefix ``/api/alerts`` by main.py.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from alert_engine import db
from alert_engine.models import ScanCreate, ScanResponse, ScanUpdate

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


def _scan_to_response(scan: dict) -> ScanResponse:
    """Convert a DB row dict into a ScanResponse."""
    return ScanResponse(
        id=scan["id"],
        user_id=scan["user_id"],
        facility_id=scan["facility_id"],
        park_name=scan["park_name"],
        arrival_date=scan["arrival_date"],
        flexible_arrival=scan["flexible_arrival"],
        num_nights=scan["num_nights"],
        site_type=scan.get("site_type", "any"),
        vehicle_length_max=scan.get("vehicle_length_max"),
        specific_site_ids=scan.get("specific_site_ids"),
        notify_sms=scan.get("notify_sms"),
        notify_email=scan.get("notify_email"),
        active=scan["active"],
        alert_count=scan.get("alert_count", 0),
        created_at=scan.get("created_at", ""),
    )


# ── POST /api/alerts/scans ───────────────────────────────────────────────────

@router.post("/scans", status_code=201, response_model=ScanResponse)
async def create_scan(body: ScanCreate):
    """Create a new availability scan."""
    scan = await db.create_scan(body.model_dump())
    return _scan_to_response(scan)


# ── GET /api/alerts/scans/{user_id} ──────────────────────────────────────────

@router.get("/scans/user/{user_id}", response_model=list[ScanResponse])
async def list_user_scans(
    user_id: str,
    active: bool = Query(True, description="Filter to active scans only"),
):
    """List all scans for a user."""
    scans = await db.get_scans_by_user(user_id, active_only=active)
    return [_scan_to_response(s) for s in scans]


# ── GET /api/alerts/scans/{scan_id} ──────────────────────────────────────────

@router.get("/scans/{scan_id}", response_model=ScanResponse)
async def get_scan(scan_id: int):
    """Get a single scan by ID."""
    scan = await db.get_scan(scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")
    return _scan_to_response(scan)


# ── PATCH /api/alerts/scans/{scan_id} ────────────────────────────────────────

@router.patch("/scans/{scan_id}", response_model=ScanResponse)
async def update_scan(scan_id: int, body: ScanUpdate):
    """Update an existing scan."""
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    scan = await db.update_scan(scan_id, updates)
    if scan is None:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")
    return _scan_to_response(scan)


# ── DELETE /api/alerts/scans/{scan_id} ────────────────────────────────────────

@router.delete("/scans/{scan_id}")
async def delete_scan(scan_id: int):
    """Soft-delete (deactivate) a scan."""
    success = await db.deactivate_scan(scan_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")
    return {"status": "paused"}


# ── GET /api/alerts/scans/{scan_id}/history ───────────────────────────────────

@router.get("/scans/{scan_id}/history")
async def scan_history(scan_id: int):
    """Get the alert log for a specific scan."""
    scan = await db.get_scan(scan_id)
    if scan is None:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")
    history = await db.get_alert_history(scan_id)
    return history


# ── GET /api/alerts/status ────────────────────────────────────────────────────

@router.get("/status")
async def alert_status():
    """Poller health check: last poll time, active scans, alerts today."""
    import logging
    try:
        status = await db.get_status()
    except Exception as exc:
        logging.getLogger(__name__).warning("Status query failed: %s: %s", type(exc).__name__, exc)
        status = {
            "active_scans": 0,
            "facilities_monitored": 0,
            "alerts_sent_today": 0,
            "last_poll_event": None,
            "_error": f"{type(exc).__name__}: {exc}",
        }
    return status


# ── GET /api/alerts/facilities ────────────────────────────────────────────────

@router.get("/facilities")
async def list_facilities():
    """List known campground facilities (priority parks seed data)."""
    return await db.list_facilities()


# ── POST /api/alerts/poll (manual trigger for testing) ────────────────────────

@router.post("/poll")
async def trigger_poll():
    """Manually trigger a poll cycle (for testing / debugging)."""
    from alert_engine.poller import poll_all_facilities

    await poll_all_facilities()
    return {"status": "poll_complete"}
