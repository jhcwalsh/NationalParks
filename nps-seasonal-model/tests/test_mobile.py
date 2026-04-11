"""
Tests for the mobile overview assembler and the /parks/{code}/overview
endpoint.

Run with:  pytest tests/test_mobile.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

import mobile  # noqa: E402
from campsites import NATIONAL_PARKS  # noqa: E402


# ── mobile.assemble_overview ──────────────────────────────────────────────────

def _no_network():
    """Force every external call in mobile to return a safe fallback."""
    return patch.multiple(
        mobile,
        load_weather=lambda *a, **k: {"_error": "disabled"},
        load_aqi=lambda *a, **k: {"_error": "disabled"},
        load_active_fires=lambda *a, **k: [],
        load_nps_alerts=lambda *a, **k: [],
    )


def test_assemble_overview_rejects_non_national_park():
    assert mobile.assemble_overview("LIBI") is None  # Little Bighorn — NM
    assert mobile.assemble_overview("FOO") is None
    assert mobile.assemble_overview("") is None


def test_assemble_overview_returns_full_shape_for_yose():
    with _no_network():
        payload = mobile.assemble_overview("YOSE")

    assert payload is not None
    assert set(payload.keys()) >= {"park", "busyness", "cards", "alerts", "monthly"}

    # Park header
    assert payload["park"]["code"] == "YOSE"
    assert payload["park"]["name"] == "Yosemite National Park"
    assert payload["park"]["state"] == "California"
    assert payload["park"]["reservation_note"] == "No reservation required in 2026"

    # Busyness (YOSE has seed data)
    b = payload["busyness"]
    assert b is not None
    assert 0 <= b["score"] <= 100
    assert isinstance(b["label"], str) and b["label"]
    assert isinstance(b["context"], str) and b["context"]

    # Cards container always present; individual cards may be None
    assert set(payload["cards"].keys()) == {"aqi", "weather", "camping"}
    assert payload["cards"]["aqi"] is None     # network mocked off
    assert payload["cards"]["weather"] is None  # network mocked off
    assert payload["cards"]["camping"] is not None  # comes from local CSV

    # Monthly bars — exactly 12
    assert len(payload["monthly"]) == 12
    months = [m["month"] for m in payload["monthly"]]
    assert months == ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    for m in payload["monthly"]:
        assert 0 <= m["score"] <= 100
        assert m["label"] in {"peak", "shoulder", "quiet"}


def test_assemble_overview_lowercase_code_works():
    with _no_network():
        payload = mobile.assemble_overview("yose")
    assert payload is not None
    assert payload["park"]["code"] == "YOSE"


def test_assemble_overview_for_park_without_model_data():
    """Parks not in the seed DB should still return park/cards/alerts — just
    without busyness or monthly bars."""
    # Pick a park that exists in NATIONAL_PARKS but not in the 30-park seed DB
    code = "DRTO"  # Dry Tortugas — not in seed
    assert code in NATIONAL_PARKS

    with _no_network():
        payload = mobile.assemble_overview(code)

    assert payload is not None
    assert payload["park"]["code"] == code
    assert payload["busyness"] is None
    assert payload["monthly"] == []
    assert payload["cards"]["aqi"] is None


def test_list_parks_returns_all_63():
    parks = mobile.list_parks()
    assert len(parks) == 63
    codes = {p["code"] for p in parks}
    assert "YOSE" in codes
    assert "GRCA" in codes
    assert "NPSA" in codes  # American Samoa is in the list


def test_camping_card_uses_label_threshold():
    # Direct exercises of _camping_label thresholds
    assert mobile._camping_label(60) == "Wide open"
    assert mobile._camping_label(30) == "Good availability"
    assert mobile._camping_label(15) == "Filling up"
    assert mobile._camping_label(7)  == "Tight"
    assert mobile._camping_label(2)  == "Nearly full"
    assert mobile._camping_label(0)  == "Nearly full"


def test_busyness_label_thresholds():
    assert mobile._busyness_label(95) == "Very busy right now"
    assert mobile._busyness_label(70) == "Busy right now"
    assert mobile._busyness_label(50) == "Moderate right now"
    assert mobile._busyness_label(25) == "Quiet right now"
    assert mobile._busyness_label(5)  == "Very quiet right now"


def test_fire_summary_with_distance_and_bearing():
    # Yosemite ~ (37.87, -119.55). Put a fake fire ~20 miles ENE of park centre.
    fire = {
        "IncidentName": "Crane Flat",
        "_lat": 37.95,
        "_lon": -119.20,
        "PercentContained": 25,
    }
    text = mobile._summarise_fire(37.87, -119.55, fire)
    assert text is not None
    assert "Crane Flat fire" in text
    assert "mi" in text
    assert "contained" in text


# ── FastAPI /parks/{code}/overview ────────────────────────────────────────────

@pytest.fixture
def client():
    # Import here so tests still pass when FastAPI isn't installed in some envs
    from fastapi.testclient import TestClient
    import api
    return TestClient(api.app)


def test_endpoint_overview_yose_ok(client):
    with _no_network():
        r = client.get("/parks/YOSE/overview")
    assert r.status_code == 200
    body = r.json()
    assert body["park"]["code"] == "YOSE"
    assert len(body["monthly"]) == 12


def test_endpoint_overview_non_national_park_404(client):
    r = client.get("/parks/LIBI/overview")
    assert r.status_code == 404
    assert "not one of the 63" in r.json()["detail"]


def test_endpoint_parks_list_returns_63(client):
    r = client.get("/parks")
    assert r.status_code == 200
    assert len(r.json()) == 63


def test_endpoint_root_serves_mobile_index(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "National Parks Now" in r.text


def test_endpoint_health_reports_national_park_count(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["national_parks_total"] == 63
    assert body["national_parks_with_model_data"] >= 1
