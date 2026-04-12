"""FastAPI route tests for the alert engine.

Uses httpx AsyncClient with the FastAPI test transport to test all
/api/alerts/* endpoints including validation errors.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# Set DATABASE_URL to a temp file before importing anything
TEST_DB = "/tmp/test_alert_routes.db"
os.environ["DATABASE_URL"] = TEST_DB

from alert_engine.db import init_db
from alert_engine.router import router

from fastapi import FastAPI

# Build a minimal test app with just the alert routes
_test_app = FastAPI()
_test_app.include_router(router)


@pytest_asyncio.fixture(autouse=True)
async def setup_db(tmp_path):
    """Initialise a fresh DB for each test."""
    db_path = str(tmp_path / "test.db")
    os.environ["DATABASE_URL"] = db_path
    await init_db()
    yield
    # Cleanup handled by tmp_path


@pytest_asyncio.fixture
async def client():
    transport = ASGITransport(app=_test_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ── POST /api/alerts/scans ───────────────────────────────────────────────────

class TestCreateScan:
    @pytest.mark.asyncio
    async def test_create_scan_success(self, client):
        body = {
            "user_id": "user-1",
            "facility_id": "232447",
            "park_name": "Yosemite - Upper Pines",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 2,
            "notify_email": "test@example.com",
        }
        resp = await client.post("/api/alerts/scans", json=body)
        assert resp.status_code == 201
        data = resp.json()
        assert data["user_id"] == "user-1"
        assert data["facility_id"] == "232447"
        assert data["active"] is True
        assert data["alert_count"] == 0

    @pytest.mark.asyncio
    async def test_create_scan_no_notification(self, client):
        """Reject if neither SMS nor email is provided."""
        body = {
            "user_id": "user-1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 2,
        }
        resp = await client.post("/api/alerts/scans", json=body)
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_create_scan_past_date(self, client):
        body = {
            "user_id": "user-1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() - timedelta(days=1)).isoformat(),
            "num_nights": 2,
            "notify_email": "test@example.com",
        }
        resp = await client.post("/api/alerts/scans", json=body)
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_create_scan_invalid_sms(self, client):
        body = {
            "user_id": "user-1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 2,
            "notify_sms": "not-a-phone-number",
        }
        resp = await client.post("/api/alerts/scans", json=body)
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_create_scan_invalid_site_type(self, client):
        body = {
            "user_id": "user-1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 2,
            "site_type": "hammock",
            "notify_email": "test@example.com",
        }
        resp = await client.post("/api/alerts/scans", json=body)
        assert resp.status_code == 422


# ── GET /api/alerts/scans/user/{user_id} ─────────────────────────────────────

class TestListScans:
    @pytest.mark.asyncio
    async def test_list_user_scans(self, client):
        # Create two scans
        body = {
            "user_id": "user-2",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        await client.post("/api/alerts/scans", json=body)
        await client.post("/api/alerts/scans", json=body)

        resp = await client.get("/api/alerts/scans/user/user-2")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    @pytest.mark.asyncio
    async def test_list_empty(self, client):
        resp = await client.get("/api/alerts/scans/user/nobody")
        assert resp.status_code == 200
        assert resp.json() == []


# ── GET /api/alerts/scans/{scan_id} ──────────────────────────────────────────

class TestGetScan:
    @pytest.mark.asyncio
    async def test_get_scan(self, client):
        body = {
            "user_id": "u1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        create_resp = await client.post("/api/alerts/scans", json=body)
        scan_id = create_resp.json()["id"]

        resp = await client.get(f"/api/alerts/scans/{scan_id}")
        assert resp.status_code == 200
        assert resp.json()["id"] == scan_id

    @pytest.mark.asyncio
    async def test_get_scan_not_found(self, client):
        resp = await client.get("/api/alerts/scans/99999")
        assert resp.status_code == 404


# ── PATCH /api/alerts/scans/{scan_id} ────────────────────────────────────────

class TestUpdateScan:
    @pytest.mark.asyncio
    async def test_update_scan(self, client):
        body = {
            "user_id": "u1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        create_resp = await client.post("/api/alerts/scans", json=body)
        scan_id = create_resp.json()["id"]

        resp = await client.patch(
            f"/api/alerts/scans/{scan_id}",
            json={"num_nights": 3},
        )
        assert resp.status_code == 200
        assert resp.json()["num_nights"] == 3

    @pytest.mark.asyncio
    async def test_update_not_found(self, client):
        resp = await client.patch(
            "/api/alerts/scans/99999",
            json={"num_nights": 3},
        )
        assert resp.status_code == 404


# ── DELETE /api/alerts/scans/{scan_id} ────────────────────────────────────────

class TestDeleteScan:
    @pytest.mark.asyncio
    async def test_delete_scan(self, client):
        body = {
            "user_id": "u1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        create_resp = await client.post("/api/alerts/scans", json=body)
        scan_id = create_resp.json()["id"]

        resp = await client.delete(f"/api/alerts/scans/{scan_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"

        # Verify it's deactivated
        get_resp = await client.get(f"/api/alerts/scans/{scan_id}")
        assert get_resp.json()["active"] is False

    @pytest.mark.asyncio
    async def test_delete_not_found(self, client):
        resp = await client.delete("/api/alerts/scans/99999")
        assert resp.status_code == 404


# ── GET /api/alerts/scans/{scan_id}/history ───────────────────────────────────

class TestScanHistory:
    @pytest.mark.asyncio
    async def test_history_empty(self, client):
        body = {
            "user_id": "u1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        create_resp = await client.post("/api/alerts/scans", json=body)
        scan_id = create_resp.json()["id"]

        resp = await client.get(f"/api/alerts/scans/{scan_id}/history")
        assert resp.status_code == 200
        assert resp.json() == []


# ── GET /api/alerts/status ────────────────────────────────────────────────────

class TestAlertStatus:
    @pytest.mark.asyncio
    async def test_status_endpoint(self, client):
        resp = await client.get("/api/alerts/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "active_scans" in data
        assert "facilities_monitored" in data
        assert "alerts_sent_today" in data
        assert "last_poll_event" in data

    @pytest.mark.asyncio
    async def test_status_reflects_scans(self, client):
        # Create a scan first
        body = {
            "user_id": "u1",
            "facility_id": "232447",
            "park_name": "Test",
            "arrival_date": (date.today() + timedelta(days=5)).isoformat(),
            "num_nights": 1,
            "notify_email": "a@b.com",
        }
        await client.post("/api/alerts/scans", json=body)

        resp = await client.get("/api/alerts/status")
        data = resp.json()
        assert data["active_scans"] == 1
        assert data["facilities_monitored"] == 1


# ── GET /api/alerts/facilities ────────────────────────────────────────────────

class TestFacilities:
    @pytest.mark.asyncio
    async def test_list_facilities(self, client):
        resp = await client.get("/api/alerts/facilities")
        assert resp.status_code == 200
        data = resp.json()
        # Should have the seeded priority facilities
        assert len(data) == 8
        facility_ids = {f["facility_id"] for f in data}
        assert "232447" in facility_ids  # Upper Pines
        assert "234869" in facility_ids  # Mather
