"""Unit tests for the Recreation.gov availability poller.

Mocks httpx responses and tests the diff logic — verifying that new
availabilities are detected while unchanged ones are ignored.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alert_engine.models import AvailabilityEvent
from alert_engine.poller import _months_in_window, _poll_facility


# ── Helper: months_in_window ─────────────────────────────────────────────────

class TestMonthsInWindow:
    def test_single_month(self):
        result = _months_in_window(date(2026, 7, 15), 10)
        assert len(result) >= 1
        assert "2026-07-01T00:00:00.000Z" in result

    def test_spans_two_months(self):
        result = _months_in_window(date(2026, 7, 25), 14)
        assert "2026-07-01T00:00:00.000Z" in result
        assert "2026-08-01T00:00:00.000Z" in result

    def test_year_boundary(self):
        result = _months_in_window(date(2026, 12, 20), 20)
        assert "2026-12-01T00:00:00.000Z" in result
        assert "2027-01-01T00:00:00.000Z" in result


# ── Poller diff logic ────────────────────────────────────────────────────────

def _make_recgov_response(sites: dict[str, dict[str, str]]) -> dict:
    """Build a mock Recreation.gov API response."""
    campsites = {}
    for site_id, avails in sites.items():
        campsites[site_id] = {
            "availabilities": avails,
            "campsite_type": "STANDARD NONELECTRIC",
            "max_vehicle_length": 0,
            "loop": "Upper Pines",
        }
    return {"campsites": campsites}


@pytest.mark.asyncio
async def test_new_availability_detected():
    """A date flipping from Reserved → Available should produce an event."""
    tomorrow = date.today() + timedelta(days=1)
    date_str = tomorrow.strftime("%Y-%m-%dT07:00:00Z")

    response_data = _make_recgov_response({
        "SITE-A": {date_str: "Available"},
    })

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = response_data
    mock_resp.raise_for_status = MagicMock()

    with (
        patch("alert_engine.poller._get_client") as mock_client_fn,
        patch("alert_engine.poller.db") as mock_db,
        patch("alert_engine.poller.match_and_alert", new_callable=AsyncMock),
    ):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        mock_client_fn.return_value = mock_client

        # Empty snapshot = everything is new
        mock_db.get_snapshot = AsyncMock(return_value=[])
        mock_db.update_snapshot = AsyncMock()
        mock_db.insert_availability_event = AsyncMock(return_value=1)

        events = await _poll_facility("232447")

    assert len(events) >= 1
    assert events[0].site_id == "SITE-A"
    assert events[0].available_date == tomorrow


@pytest.mark.asyncio
async def test_unchanged_availability_ignored():
    """A date that was already Available in the snapshot should NOT produce an event."""
    tomorrow = date.today() + timedelta(days=1)
    date_str = tomorrow.strftime("%Y-%m-%dT07:00:00Z")

    response_data = _make_recgov_response({
        "SITE-A": {date_str: "Available"},
    })

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = response_data
    mock_resp.raise_for_status = MagicMock()

    with (
        patch("alert_engine.poller._get_client") as mock_client_fn,
        patch("alert_engine.poller.db") as mock_db,
        patch("alert_engine.poller.match_and_alert", new_callable=AsyncMock),
    ):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        mock_client_fn.return_value = mock_client

        # Snapshot already has this date
        mock_db.get_snapshot = AsyncMock(return_value=[tomorrow.isoformat()])
        mock_db.update_snapshot = AsyncMock()
        mock_db.insert_availability_event = AsyncMock(return_value=1)

        events = await _poll_facility("232447")

    assert len(events) == 0


@pytest.mark.asyncio
async def test_reserved_not_detected():
    """A date that is Reserved should not produce an event."""
    tomorrow = date.today() + timedelta(days=1)
    date_str = tomorrow.strftime("%Y-%m-%dT07:00:00Z")

    response_data = _make_recgov_response({
        "SITE-A": {date_str: "Reserved"},
    })

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = response_data
    mock_resp.raise_for_status = MagicMock()

    with (
        patch("alert_engine.poller._get_client") as mock_client_fn,
        patch("alert_engine.poller.db") as mock_db,
        patch("alert_engine.poller.match_and_alert", new_callable=AsyncMock),
    ):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        mock_client_fn.return_value = mock_client

        mock_db.get_snapshot = AsyncMock(return_value=[])
        mock_db.update_snapshot = AsyncMock()
        mock_db.insert_availability_event = AsyncMock(return_value=1)

        events = await _poll_facility("232447")

    assert len(events) == 0


@pytest.mark.asyncio
async def test_multiple_sites_multiple_dates():
    """Multiple sites with mixed availability should only return the new Available ones."""
    d1 = date.today() + timedelta(days=1)
    d2 = date.today() + timedelta(days=2)

    response_data = _make_recgov_response({
        "SITE-A": {
            d1.strftime("%Y-%m-%dT07:00:00Z"): "Available",
            d2.strftime("%Y-%m-%dT07:00:00Z"): "Reserved",
        },
        "SITE-B": {
            d1.strftime("%Y-%m-%dT07:00:00Z"): "Reserved",
            d2.strftime("%Y-%m-%dT07:00:00Z"): "Available",
        },
    })

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = response_data
    mock_resp.raise_for_status = MagicMock()

    with (
        patch("alert_engine.poller._get_client") as mock_client_fn,
        patch("alert_engine.poller.db") as mock_db,
        patch("alert_engine.poller.match_and_alert", new_callable=AsyncMock),
    ):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_resp)
        mock_client.is_closed = False
        mock_client_fn.return_value = mock_client

        mock_db.get_snapshot = AsyncMock(return_value=[])
        mock_db.update_snapshot = AsyncMock()
        mock_db.insert_availability_event = AsyncMock(return_value=1)

        events = await _poll_facility("232447")

    # One Available date per site
    assert len(events) == 2
    site_ids = {e.site_id for e in events}
    assert site_ids == {"SITE-A", "SITE-B"}
