"""Unit tests for the alert notifier.

Mocks Twilio and SendGrid clients; verifies message body construction
with and without conditions enrichment.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alert_engine.models import AvailabilityEvent
from alert_engine.notifier import _build_message, send_alert


def _scan(**overrides) -> dict:
    d = {
        "id": 42,
        "user_id": "u1",
        "facility_id": "232447",
        "park_name": "Yosemite - Upper Pines",
        "arrival_date": str(date.today() + timedelta(days=5)),
        "num_nights": 3,
        "notify_sms": "+14155551234",
        "notify_email": "user@example.com",
        "active": True,
        "alert_count": 0,
    }
    d.update(overrides)
    return d


def _event(**overrides) -> AvailabilityEvent:
    defaults = {
        "facility_id": "232447",
        "site_id": "SITE-042",
        "available_date": date.today() + timedelta(days=5),
        "site_type": "tent",
        "vehicle_length": None,
        "loop_name": "Upper Pines",
    }
    defaults.update(overrides)
    return AvailabilityEvent(**defaults)


# ── Message construction ─────────────────────────────────────────────────────

class TestBuildMessage:
    def test_basic_message_includes_park(self):
        msg = _build_message(_scan(), _event())
        assert "Yosemite - Upper Pines" in msg

    def test_basic_message_includes_site_id(self):
        msg = _build_message(_scan(), _event())
        assert "SITE-042" in msg

    def test_basic_message_includes_loop(self):
        msg = _build_message(_scan(), _event())
        assert "Upper Pines" in msg

    def test_basic_message_includes_booking_url(self):
        msg = _build_message(_scan(), _event())
        assert "recreation.gov/camping/campsites/SITE-042" in msg

    def test_basic_message_includes_num_nights(self):
        msg = _build_message(_scan(num_nights=3), _event())
        assert "3 nights" in msg

    def test_single_night(self):
        msg = _build_message(_scan(num_nights=1), _event())
        assert "1 night" in msg
        assert "1 nights" not in msg

    def test_includes_unsubscribe(self):
        msg = _build_message(_scan(), _event())
        assert "STOP" in msg

    def test_with_conditions(self):
        conditions = {
            "crowd_score": 0.82,
            "crowd_label": "Very Busy",
            "aqi": 142,
            "aqi_category": "Unhealthy for Sensitive Groups",
        }
        msg = _build_message(_scan(), _event(), conditions)
        assert "Very Busy" in msg
        assert "82/100" in msg
        assert "AQI 142" in msg
        assert "Unhealthy for Sensitive Groups" in msg

    def test_without_conditions(self):
        msg = _build_message(_scan(), _event(), conditions=None)
        assert "Conditions at time of visit" not in msg

    def test_partial_conditions_crowd_only(self):
        conditions = {"crowd_score": 0.5, "crowd_label": "Moderate"}
        msg = _build_message(_scan(), _event(), conditions)
        assert "Moderate" in msg
        assert "AQI" not in msg

    def test_partial_conditions_aqi_only(self):
        conditions = {"aqi": 50, "aqi_category": "Good"}
        msg = _build_message(_scan(), _event(), conditions)
        assert "Good" in msg
        assert "AQI 50" in msg
        assert "Crowd level" not in msg


# ── Alert dispatch ───────────────────────────────────────────────────────────

class TestSendAlert:
    @pytest.mark.asyncio
    async def test_sms_sent(self):
        scan = _scan(notify_email=None)
        event = _event()

        with (
            patch("alert_engine.notifier.get_conditions", new_callable=AsyncMock, return_value={}),
            patch("alert_engine.notifier._send_sms", return_value="SM123") as mock_sms,
            patch("alert_engine.notifier.db") as mock_db,
            patch("alert_engine.notifier.TWILIO_ACCOUNT_SID", "test_sid"),
            patch("alert_engine.notifier.TWILIO_AUTH_TOKEN", "test_token"),
        ):
            mock_db.insert_alert_log = AsyncMock()
            await send_alert(scan, event)

        mock_sms.assert_called_once()
        assert "+14155551234" in mock_sms.call_args[0]

    @pytest.mark.asyncio
    async def test_email_sent(self):
        scan = _scan(notify_sms=None)
        event = _event()

        with (
            patch("alert_engine.notifier.get_conditions", new_callable=AsyncMock, return_value={}),
            patch("alert_engine.notifier._send_email", new_callable=AsyncMock, return_value=True) as mock_email,
            patch("alert_engine.notifier.db") as mock_db,
            patch("alert_engine.notifier.TWILIO_SENDGRID_API_KEY", "test_key"),
        ):
            mock_db.insert_alert_log = AsyncMock()
            await send_alert(scan, event)

        mock_email.assert_called_once()
        assert "user@example.com" in mock_email.call_args[0]

    @pytest.mark.asyncio
    async def test_both_channels(self):
        scan = _scan()
        event = _event()

        with (
            patch("alert_engine.notifier.get_conditions", new_callable=AsyncMock, return_value={}),
            patch("alert_engine.notifier._send_sms", return_value="SM123") as mock_sms,
            patch("alert_engine.notifier._send_email", new_callable=AsyncMock, return_value=True) as mock_email,
            patch("alert_engine.notifier.db") as mock_db,
            patch("alert_engine.notifier.TWILIO_ACCOUNT_SID", "test_sid"),
            patch("alert_engine.notifier.TWILIO_AUTH_TOKEN", "test_token"),
            patch("alert_engine.notifier.TWILIO_SENDGRID_API_KEY", "test_key"),
        ):
            mock_db.insert_alert_log = AsyncMock()
            await send_alert(scan, event)

        mock_sms.assert_called_once()
        mock_email.assert_called_once()

    @pytest.mark.asyncio
    async def test_sms_failure_logged(self):
        scan = _scan(notify_email=None)
        event = _event()

        with (
            patch("alert_engine.notifier.get_conditions", new_callable=AsyncMock, return_value={}),
            patch("alert_engine.notifier._send_sms", side_effect=Exception("Twilio down")),
            patch("alert_engine.notifier.db") as mock_db,
            patch("alert_engine.notifier.TWILIO_ACCOUNT_SID", "test_sid"),
            patch("alert_engine.notifier.TWILIO_AUTH_TOKEN", "test_token"),
        ):
            mock_db.insert_alert_log = AsyncMock()
            await send_alert(scan, event)

        # Alert log should still be written with status=failed
        mock_db.insert_alert_log.assert_called_once()
        call_args = mock_db.insert_alert_log.call_args
        assert call_args[1].get("status", call_args[0][5] if len(call_args[0]) > 5 else "") == "failed"
