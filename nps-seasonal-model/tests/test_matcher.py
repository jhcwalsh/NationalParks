"""Unit tests for the campsite alert matcher.

Tests each matching rule: facility, date (exact + flexible), site type,
vehicle length, specific sites, and active flag.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from alert_engine.matcher import _matches
from alert_engine.models import AvailabilityEvent


def _base_scan(**overrides) -> dict:
    """Return a baseline scan dict with sensible defaults."""
    scan = {
        "id": 1,
        "user_id": "user-1",
        "facility_id": "232447",
        "park_name": "Yosemite - Upper Pines",
        "arrival_date": date.today() + timedelta(days=5),
        "flexible_arrival": False,
        "num_nights": 2,
        "site_type": "any",
        "vehicle_length_max": None,
        "specific_site_ids": None,
        "notify_sms": "+14155551234",
        "notify_email": None,
        "active": True,
        "alert_count": 0,
    }
    scan.update(overrides)
    return scan


def _base_event(**overrides) -> AvailabilityEvent:
    """Return a baseline availability event."""
    defaults = {
        "facility_id": "232447",
        "site_id": "SITE-001",
        "available_date": date.today() + timedelta(days=5),
        "site_type": "tent",
        "vehicle_length": None,
        "loop_name": "Upper Pines",
    }
    defaults.update(overrides)
    return AvailabilityEvent(**defaults)


# ── Facility match ───────────────────────────────────────────────────────────

class TestFacilityMatch:
    def test_same_facility(self):
        assert _matches(_base_scan(), _base_event()) is True

    def test_different_facility(self):
        event = _base_event(facility_id="999999")
        assert _matches(_base_scan(), event) is False


# ── Date match ───────────────────────────────────────────────────────────────

class TestDateMatch:
    def test_exact_date_match(self):
        d = date.today() + timedelta(days=5)
        assert _matches(_base_scan(arrival_date=d), _base_event(available_date=d)) is True

    def test_exact_date_mismatch(self):
        d1 = date.today() + timedelta(days=5)
        d2 = date.today() + timedelta(days=6)
        assert _matches(_base_scan(arrival_date=d1), _base_event(available_date=d2)) is False

    def test_flexible_within_range(self):
        base = date.today() + timedelta(days=5)
        scan = _base_scan(arrival_date=base, flexible_arrival=True)
        # +2 days is within range
        event = _base_event(available_date=base + timedelta(days=2))
        assert _matches(scan, event) is True

    def test_flexible_at_boundary(self):
        base = date.today() + timedelta(days=5)
        scan = _base_scan(arrival_date=base, flexible_arrival=True)
        # -2 days is at boundary — should match
        event = _base_event(available_date=base - timedelta(days=2))
        assert _matches(scan, event) is True

    def test_flexible_outside_range(self):
        base = date.today() + timedelta(days=5)
        scan = _base_scan(arrival_date=base, flexible_arrival=True)
        # +3 days is outside range
        event = _base_event(available_date=base + timedelta(days=3))
        assert _matches(scan, event) is False

    def test_flexible_negative_boundary(self):
        base = date.today() + timedelta(days=5)
        scan = _base_scan(arrival_date=base, flexible_arrival=True)
        # -3 days is outside range
        event = _base_event(available_date=base - timedelta(days=3))
        assert _matches(scan, event) is False


# ── Site type ────────────────────────────────────────────────────────────────

class TestSiteType:
    def test_any_matches_tent(self):
        assert _matches(_base_scan(site_type="any"), _base_event(site_type="tent")) is True

    def test_any_matches_rv(self):
        assert _matches(_base_scan(site_type="any"), _base_event(site_type="rv")) is True

    def test_tent_matches_tent(self):
        assert _matches(_base_scan(site_type="tent"), _base_event(site_type="tent")) is True

    def test_tent_rejects_rv(self):
        assert _matches(_base_scan(site_type="tent"), _base_event(site_type="rv")) is False

    def test_event_with_no_type_matches_any_scan_type(self):
        # If the event has no type info, don't reject
        assert _matches(_base_scan(site_type="tent"), _base_event(site_type=None)) is True


# ── Vehicle length ───────────────────────────────────────────────────────────

class TestVehicleLength:
    def test_no_scan_limit(self):
        assert _matches(
            _base_scan(vehicle_length_max=None),
            _base_event(vehicle_length=40),
        ) is True

    def test_no_event_length(self):
        assert _matches(
            _base_scan(vehicle_length_max=30),
            _base_event(vehicle_length=None),
        ) is True

    def test_within_limit(self):
        assert _matches(
            _base_scan(vehicle_length_max=30),
            _base_event(vehicle_length=25),
        ) is True

    def test_at_limit(self):
        assert _matches(
            _base_scan(vehicle_length_max=30),
            _base_event(vehicle_length=30),
        ) is True

    def test_exceeds_limit(self):
        assert _matches(
            _base_scan(vehicle_length_max=30),
            _base_event(vehicle_length=35),
        ) is False


# ── Specific site IDs ────────────────────────────────────────────────────────

class TestSpecificSites:
    def test_no_filter_matches_all(self):
        assert _matches(_base_scan(specific_site_ids=None), _base_event()) is True

    def test_matches_listed_site(self):
        assert _matches(
            _base_scan(specific_site_ids=["SITE-001", "SITE-002"]),
            _base_event(site_id="SITE-001"),
        ) is True

    def test_rejects_unlisted_site(self):
        assert _matches(
            _base_scan(specific_site_ids=["SITE-002", "SITE-003"]),
            _base_event(site_id="SITE-001"),
        ) is False

    def test_json_string_parsed(self):
        assert _matches(
            _base_scan(specific_site_ids='["SITE-001", "SITE-002"]'),
            _base_event(site_id="SITE-001"),
        ) is True


# ── Active flag ──────────────────────────────────────────────────────────────

class TestActiveFlag:
    def test_active_scan_matches(self):
        assert _matches(_base_scan(active=True), _base_event()) is True

    def test_inactive_scan_rejected(self):
        assert _matches(_base_scan(active=False), _base_event()) is False
