"""Tests for the parkpulse DuckDB polling system.

Covers:
- Schema creation and connection management
- Poll lifecycle (start / finish)
- Snapshot insertion and get_latest_snapshot hot-path query
- Transition detection logic
- Analysis queries on synthetic data
- Collector helpers (month calculation, row extraction)
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest

from parkpulse import db
from parkpulse.collector import (
    _detect_transitions,
    _extract_snapshot_rows,
    _months_covering,
    fetch_facility,
    load_facilities,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mem_conn():
    """In-memory DuckDB connection with schema initialised."""
    conn = duckdb.connect(":memory:")
    db.init_schema(conn)
    return conn


# ── Schema & connection ─────────────────────────────────────────────────────


def test_init_schema_creates_tables(mem_conn):
    tables = mem_conn.execute(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name"
    ).fetchall()
    names = {t[0] for t in tables}
    assert "poll_log" in names
    assert "availability_snapshots" in names
    assert "status_transitions" in names


def test_init_schema_idempotent(mem_conn):
    """Running init_schema twice doesn't raise."""
    db.init_schema(mem_conn)
    tables = mem_conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables"
    ).fetchone()
    assert tables[0] >= 3


# ── Poll lifecycle ──────────────────────────────────────────────────────────


def test_start_poll_returns_incrementing_ids(mem_conn):
    id1 = db.start_poll(mem_conn)
    id2 = db.start_poll(mem_conn)
    assert id2 > id1


def test_finish_poll_updates_row(mem_conn):
    pid = db.start_poll(mem_conn)
    db.finish_poll(
        mem_conn, pid,
        n_facilities=10, n_sites=100, n_snapshots=1000,
        n_transitions=5, status="success",
    )
    row = mem_conn.execute(
        "SELECT * FROM poll_log WHERE poll_id = ?", [pid]
    ).fetchone()
    assert row is not None
    # Columns: poll_id, started_at, finished_at, n_facilities, n_sites,
    #          n_snapshots, n_transitions, status, error_message
    assert row[3] == 10   # n_facilities
    assert row[5] == 1000 # n_snapshots
    assert row[7] == "success"


# ── Snapshot insert & query ─────────────────────────────────────────────────


def _make_snapshot_rows(poll_id, polled_at, overrides=None):
    """Helper to build snapshot tuples."""
    defaults = [
        (poll_id, "FAC1", "SITE_A", date(2026, 5, 1), "Available", polled_at),
        (poll_id, "FAC1", "SITE_A", date(2026, 5, 2), "Reserved", polled_at),
        (poll_id, "FAC1", "SITE_B", date(2026, 5, 1), "Reserved", polled_at),
    ]
    if overrides:
        defaults.extend(overrides)
    return defaults


def test_insert_snapshots(mem_conn):
    pid = db.start_poll(mem_conn)
    now = datetime.utcnow()
    rows = _make_snapshot_rows(pid, now)
    count = db.insert_snapshots(mem_conn, rows)
    assert count == 3


def test_insert_snapshots_empty(mem_conn):
    assert db.insert_snapshots(mem_conn, []) == 0


def test_get_latest_snapshot_returns_most_recent(mem_conn):
    now = datetime.utcnow()

    # First poll: SITE_A on May 1 is Available
    p1 = db.start_poll(mem_conn)
    db.insert_snapshots(mem_conn, [
        (p1, "FAC1", "SITE_A", date(2026, 5, 1), "Available", now),
    ])

    # Second poll: SITE_A on May 1 is now Reserved
    p2 = db.start_poll(mem_conn)
    later = now + timedelta(minutes=5)
    db.insert_snapshots(mem_conn, [
        (p2, "FAC1", "SITE_A", date(2026, 5, 1), "Reserved", later),
    ])

    snap = db.get_latest_snapshot(mem_conn)
    assert snap[("SITE_A", "2026-05-01")] == "Reserved"


def test_get_latest_snapshot_handles_multiple_sites(mem_conn):
    now = datetime.utcnow()
    pid = db.start_poll(mem_conn)
    db.insert_snapshots(mem_conn, [
        (pid, "FAC1", "SITE_A", date(2026, 5, 1), "Available", now),
        (pid, "FAC1", "SITE_B", date(2026, 5, 1), "Reserved", now),
        (pid, "FAC2", "SITE_C", date(2026, 5, 2), "Not Available", now),
    ])
    snap = db.get_latest_snapshot(mem_conn)
    assert len(snap) == 3
    assert snap[("SITE_A", "2026-05-01")] == "Available"
    assert snap[("SITE_B", "2026-05-01")] == "Reserved"
    assert snap[("SITE_C", "2026-05-02")] == "Not Available"


# ── Transition detection ────────────────────────────────────────────────────


def test_detect_transitions_finds_changes():
    now = datetime.utcnow()
    pid = 1
    prev = {("SITE_A", "2026-05-01"): "Reserved"}
    new_rows = [
        (pid, "FAC1", "SITE_A", date(2026, 5, 1), "Available", now),
    ]
    transitions = _detect_transitions(pid, new_rows, prev, now)
    assert len(transitions) == 1
    # (poll_id, facility_id, campsite_id, check_date,
    #  old_status, new_status, detected_at, days_to_arrival)
    t = transitions[0]
    assert t[4] == "Reserved"     # old_status
    assert t[5] == "Available"    # new_status


def test_detect_transitions_ignores_unchanged():
    now = datetime.utcnow()
    prev = {("SITE_A", "2026-05-01"): "Available"}
    new_rows = [
        (1, "FAC1", "SITE_A", date(2026, 5, 1), "Available", now),
    ]
    transitions = _detect_transitions(1, new_rows, prev, now)
    assert len(transitions) == 0


def test_detect_transitions_ignores_new_sites():
    now = datetime.utcnow()
    prev = {}  # no previous data
    new_rows = [
        (1, "FAC1", "SITE_NEW", date(2026, 5, 1), "Available", now),
    ]
    transitions = _detect_transitions(1, new_rows, prev, now)
    assert len(transitions) == 0


def test_detect_transitions_days_to_arrival():
    detected = datetime(2026, 4, 28, 12, 0)
    prev = {("SITE_A", "2026-05-01"): "Reserved"}
    new_rows = [
        (1, "FAC1", "SITE_A", date(2026, 5, 1), "Available", detected),
    ]
    transitions = _detect_transitions(1, new_rows, prev, detected)
    assert transitions[0][7] == 3  # May 1 - Apr 28 = 3 days


# ── Transition insert ──────────────────────────────────────────────────────


def test_insert_transitions(mem_conn):
    now = datetime.utcnow()
    rows = [
        (1, "FAC1", "SITE_A", date(2026, 5, 1),
         "Reserved", "Available", now, 3),
    ]
    count = db.insert_transitions(mem_conn, rows)
    assert count == 1
    stored = mem_conn.execute("SELECT * FROM status_transitions").fetchall()
    assert len(stored) == 1


# ── Collector helpers ───────────────────────────────────────────────────────


def test_months_covering_single_month():
    months = _months_covering(date(2026, 5, 10), 14)
    # May 10 + 14 days = May 24, both in May
    assert len(months) == 1
    assert "2026-05-01" in months[0]


def test_months_covering_spans_two_months():
    months = _months_covering(date(2026, 5, 25), 14)
    # May 25 + 14 = Jun 8 → spans May and June
    assert len(months) == 2


def test_extract_snapshot_rows():
    sites = {
        "SITE_1": {
            "availabilities": {
                "2026-05-01T00:00:00Z": "Available",
                "2026-05-02T00:00:00Z": "Reserved",
                "2026-04-01T00:00:00Z": "Available",  # outside window
            },
            "campsite_type": "STANDARD",
            "loop": "A",
            "max_vehicle_length": 30,
        }
    }
    now = datetime.utcnow()
    rows = _extract_snapshot_rows(
        poll_id=1,
        facility_id="FAC1",
        sites=sites,
        window_start=date(2026, 5, 1),
        window_end=date(2026, 5, 14),
        polled_at=now,
    )
    # Only May 1 and May 2 should be included (Apr 1 is outside window)
    assert len(rows) == 2
    dates = {r[3] for r in rows}
    assert date(2026, 5, 1) in dates
    assert date(2026, 5, 2) in dates


def test_load_facilities():
    """Facilities should load from the JSON file in the repo."""
    facs = load_facilities()
    assert isinstance(facs, list)
    assert len(facs) > 0
    assert "facility_id" in facs[0]


# ── Analysis queries (on synthetic data) ────────────────────────────────────


def _seed_transitions(conn, transitions):
    """Insert synthetic transition rows for analysis testing."""
    conn.executemany(
        """INSERT INTO status_transitions
           (poll_id, facility_id, campsite_id, check_date,
            old_status, new_status, detected_at, days_to_arrival)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        transitions,
    )


def test_cancellation_rate_by_days_to_arrival(mem_conn):
    from parkpulse.analysis import cancellation_rate_by_days_to_arrival

    now = datetime.utcnow()
    # 30 cancellations at days_to=7, 10 bookings at days_to=7
    rows = []
    for i in range(30):
        rows.append((1, "FAC1", f"S{i}", date(2026, 5, 1),
                      "Reserved", "Available", now, 7))
    for i in range(10):
        rows.append((1, "FAC1", f"S{30+i}", date(2026, 5, 1),
                      "Available", "Reserved", now, 7))
    _seed_transitions(mem_conn, rows)

    df = cancellation_rate_by_days_to_arrival(mem_conn, min_observations=5)
    assert len(df) == 1
    assert df.iloc[0]["days_to_arrival"] == 7
    assert df.iloc[0]["cancellations"] == 30
    assert df.iloc[0]["total_transitions"] == 40
    assert abs(df.iloc[0]["cancellation_rate"] - 0.75) < 0.01


def test_facility_churn(mem_conn):
    from parkpulse.analysis import facility_churn

    now = datetime.utcnow()
    rows = [
        (1, "FAC1", "S1", date(2026, 5, 1), "Reserved", "Available", now, 5),
        (1, "FAC1", "S2", date(2026, 5, 2), "Available", "Reserved", now, 4),
        (1, "FAC2", "S3", date(2026, 5, 1), "Reserved", "Available", now, 3),
    ]
    _seed_transitions(mem_conn, rows)

    df = facility_churn(mem_conn)
    assert len(df) == 2
    fac1 = df[df["facility_id"] == "FAC1"]
    assert fac1.iloc[0]["cancellations"] == 1
    assert fac1.iloc[0]["bookings"] == 1


def test_poll_history(mem_conn):
    from parkpulse.analysis import poll_history

    pid = db.start_poll(mem_conn)
    db.finish_poll(mem_conn, pid, n_facilities=5, status="success")

    df = poll_history(mem_conn, limit=10)
    assert len(df) >= 1
    assert df.iloc[0]["status"] == "success"


def test_rebooking_velocity_empty(mem_conn):
    from parkpulse.analysis import rebooking_velocity

    df = rebooking_velocity(mem_conn)
    assert len(df) == 0


def test_rebooking_velocity_with_data(mem_conn):
    from parkpulse.analysis import rebooking_velocity

    t0 = datetime(2026, 5, 1, 10, 0, 0)
    t1 = datetime(2026, 5, 1, 10, 30, 0)  # 30 min later
    rows = [
        # Cancellation: Reserved -> Available
        (1, "FAC1", "SITE_A", date(2026, 6, 1),
         "Reserved", "Available", t0, 31),
        # Rebooking: Available -> Reserved
        (2, "FAC1", "SITE_A", date(2026, 6, 1),
         "Available", "Reserved", t1, 31),
    ]
    _seed_transitions(mem_conn, rows)

    df = rebooking_velocity(mem_conn)
    assert len(df) == 1
    assert abs(df.iloc[0]["rebooking_minutes"] - 30.0) < 0.1


def test_hourly_transition_heatmap(mem_conn):
    from parkpulse.analysis import hourly_transition_heatmap

    t = datetime(2026, 5, 4, 14, 30)  # Monday at 2:30 PM
    rows = [
        (1, "FAC1", "S1", date(2026, 6, 1), "Reserved", "Available", t, 28),
    ]
    _seed_transitions(mem_conn, rows)

    df = hourly_transition_heatmap(mem_conn)
    assert len(df) >= 1


# ── Async fetch (mocked) ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_facility_merges_months():
    """Verify that fetch_facility merges campsites across months."""
    sem = asyncio.Semaphore(5)

    month1_response = {
        "campsites": {
            "SITE_1": {
                "campsite_type": "STANDARD",
                "loop": "A",
                "max_vehicle_length": 30,
                "availabilities": {
                    "2026-05-01T00:00:00Z": "Available",
                    "2026-05-02T00:00:00Z": "Reserved",
                },
            }
        }
    }
    month2_response = {
        "campsites": {
            "SITE_1": {
                "campsite_type": "STANDARD",
                "loop": "A",
                "max_vehicle_length": 30,
                "availabilities": {
                    "2026-06-01T00:00:00Z": "Available",
                },
            }
        }
    }

    mock_response_1 = MagicMock()
    mock_response_1.status_code = 200
    mock_response_1.json.return_value = month1_response
    mock_response_1.raise_for_status = MagicMock()

    mock_response_2 = MagicMock()
    mock_response_2.status_code = 200
    mock_response_2.json.return_value = month2_response
    mock_response_2.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=[mock_response_1, mock_response_2])

    sites = await fetch_facility(mock_client, "FAC1", ["m1", "m2"], sem)

    assert "SITE_1" in sites
    avail = sites["SITE_1"]["availabilities"]
    assert len(avail) == 3  # 2 from month1 + 1 from month2
