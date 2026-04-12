"""SQLite database layer for the campsite alert engine.

Uses aiosqlite for async access. Call ``init_db()`` once at startup to
create the tables and seed the priority facility reference data.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import aiosqlite

# Resolve DB path relative to the project root (nps-seasonal-model/)
# so it's consistent regardless of working directory.
_PROJECT_ROOT = Path(__file__).parent.parent
_DEFAULT_DB = str(_PROJECT_ROOT / "parkpulse.db")

# ── Priority facilities (seed data) ─────────────────────────────────────────

PRIORITY_FACILITIES: list[dict[str, Any]] = [
    {"facility_id": "232447", "park_code": "YOSE", "facility_name": "Upper Pines", "lat": 37.7456, "lon": -119.5936},
    {"facility_id": "232450", "park_code": "YOSE", "facility_name": "Lower Pines", "lat": 37.7399, "lon": -119.5652},
    {"facility_id": "232449", "park_code": "YOSE", "facility_name": "North Pines", "lat": 37.7440, "lon": -119.5652},
    {"facility_id": "234869", "park_code": "GRCA", "facility_name": "Mather Campground", "lat": 36.0561, "lon": -112.1220},
    {"facility_id": "272265", "park_code": "ZION", "facility_name": "Watchman Campground", "lat": 37.2090, "lon": -112.9801},
    {"facility_id": "272267", "park_code": "ZION", "facility_name": "South Campground", "lat": 37.2050, "lon": -112.9800},
    {"facility_id": "251869", "park_code": "GLAC", "facility_name": "Apgar Campground", "lat": 48.7596, "lon": -113.7870},
    {"facility_id": "232493", "park_code": "GLAC", "facility_name": "Fish Creek", "lat": 48.5000, "lon": -113.9800},
]


def _db_path() -> str:
    p = os.getenv("DATABASE_URL", "")
    if p and os.path.isabs(p):
        return p
    return _DEFAULT_DB


async def get_connection() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(_db_path())
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    return conn


async def init_db() -> None:
    """Create all tables and seed priority facilities."""
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")

        await conn.executescript("""
            CREATE TABLE IF NOT EXISTS scans (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         TEXT NOT NULL,
                facility_id     TEXT NOT NULL,
                park_name       TEXT NOT NULL,
                arrival_date    DATE NOT NULL,
                flexible_arrival INTEGER DEFAULT 0,
                num_nights      INTEGER NOT NULL,
                site_type       TEXT DEFAULT 'any',
                vehicle_length_max INTEGER,
                specific_site_ids TEXT,
                notify_sms      TEXT,
                notify_email    TEXT,
                active          INTEGER DEFAULT 1,
                alert_count     INTEGER DEFAULT 0,
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS availability_events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                facility_id     TEXT NOT NULL,
                site_id         TEXT NOT NULL,
                available_date  DATE NOT NULL,
                site_type       TEXT,
                vehicle_length  INTEGER,
                loop_name       TEXT,
                detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS alert_log (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id         INTEGER REFERENCES scans(id),
                availability_event_id INTEGER REFERENCES availability_events(id),
                channel         TEXT NOT NULL,
                destination     TEXT NOT NULL,
                message_body    TEXT NOT NULL,
                sent_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status          TEXT DEFAULT 'sent'
            );

            CREATE TABLE IF NOT EXISTS availability_snapshot (
                facility_id     TEXT NOT NULL,
                site_id         TEXT NOT NULL,
                available_dates TEXT NOT NULL,
                last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (facility_id, site_id)
            );

            CREATE TABLE IF NOT EXISTS park_facilities (
                facility_id     TEXT PRIMARY KEY,
                park_code       TEXT NOT NULL,
                facility_name   TEXT NOT NULL,
                lat             REAL,
                lon             REAL
            );

            CREATE INDEX IF NOT EXISTS idx_scans_facility ON scans(facility_id);
            CREATE INDEX IF NOT EXISTS idx_scans_user ON scans(user_id);
            CREATE INDEX IF NOT EXISTS idx_scans_active ON scans(active);
            CREATE INDEX IF NOT EXISTS idx_events_facility ON availability_events(facility_id);
            CREATE INDEX IF NOT EXISTS idx_alert_log_scan ON alert_log(scan_id);
        """)

        # Seed priority facilities
        for f in PRIORITY_FACILITIES:
            await conn.execute(
                """INSERT OR IGNORE INTO park_facilities
                   (facility_id, park_code, facility_name, lat, lon)
                   VALUES (?, ?, ?, ?, ?)""",
                (f["facility_id"], f["park_code"], f["facility_name"], f["lat"], f["lon"]),
            )
        await conn.commit()


# ── Scan CRUD ────────────────────────────────────────────────────────────────

async def create_scan(data: dict[str, Any]) -> dict[str, Any]:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        specific = json.dumps(data.get("specific_site_ids")) if data.get("specific_site_ids") else None
        cursor = await conn.execute(
            """INSERT INTO scans
               (user_id, facility_id, park_name, arrival_date, flexible_arrival,
                num_nights, site_type, vehicle_length_max, specific_site_ids,
                notify_sms, notify_email)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                data["user_id"], data["facility_id"], data["park_name"],
                str(data["arrival_date"]), int(data.get("flexible_arrival", False)),
                data["num_nights"], data.get("site_type", "any"),
                data.get("vehicle_length_max"), specific,
                data.get("notify_sms"), data.get("notify_email"),
            ),
        )
        await conn.commit()
        scan_id = cursor.lastrowid
        return await _get_scan_by_id(conn, scan_id)


async def get_scans_by_user(user_id: str, active_only: bool = True) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        if active_only:
            rows = await conn.execute_fetchall(
                "SELECT * FROM scans WHERE user_id = ? AND active = 1 ORDER BY created_at DESC",
                (user_id,),
            )
        else:
            rows = await conn.execute_fetchall(
                "SELECT * FROM scans WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        return [_row_to_scan(r) for r in rows]


async def get_scan(scan_id: int) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        return await _get_scan_by_id(conn, scan_id)


async def update_scan(scan_id: int, data: dict[str, Any]) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        existing = await _get_scan_by_id(conn, scan_id)
        if existing is None:
            return None

        sets: list[str] = []
        vals: list[Any] = []
        for key, val in data.items():
            if val is None:
                continue
            if key == "specific_site_ids":
                sets.append("specific_site_ids = ?")
                vals.append(json.dumps(val) if val else None)
            elif key == "active":
                sets.append("active = ?")
                vals.append(int(val))
            elif key == "flexible_arrival":
                sets.append("flexible_arrival = ?")
                vals.append(int(val))
            elif key == "arrival_date":
                sets.append("arrival_date = ?")
                vals.append(str(val))
            else:
                sets.append(f"{key} = ?")
                vals.append(val)

        if not sets:
            return existing

        sets.append("updated_at = CURRENT_TIMESTAMP")
        vals.append(scan_id)
        await conn.execute(
            f"UPDATE scans SET {', '.join(sets)} WHERE id = ?",
            vals,
        )
        await conn.commit()
        return await _get_scan_by_id(conn, scan_id)


async def deactivate_scan(scan_id: int) -> bool:
    async with aiosqlite.connect(_db_path()) as conn:
        cursor = await conn.execute(
            "UPDATE scans SET active = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (scan_id,),
        )
        await conn.commit()
        return cursor.rowcount > 0


async def delete_scan(scan_id: int) -> bool:
    """Hard-delete a scan and its alert history."""
    async with aiosqlite.connect(_db_path()) as conn:
        await conn.execute("DELETE FROM alert_log WHERE scan_id = ?", (scan_id,))
        cursor = await conn.execute("DELETE FROM scans WHERE id = ?", (scan_id,))
        await conn.commit()
        return cursor.rowcount > 0


# ── Active facility IDs ──────────────────────────────────────────────────────

async def get_active_facility_ids() -> list[str]:
    async with aiosqlite.connect(_db_path()) as conn:
        rows = await conn.execute_fetchall(
            "SELECT DISTINCT facility_id FROM scans WHERE active = 1"
        )
        return [r[0] for r in rows]


async def get_active_scans_for_facility(facility_id: str) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        rows = await conn.execute_fetchall(
            "SELECT * FROM scans WHERE facility_id = ? AND active = 1",
            (facility_id,),
        )
        return [_row_to_scan(r) for r in rows]


# ── Snapshot ─────────────────────────────────────────────────────────────────

async def get_snapshot(facility_id: str, site_id: str) -> list[str]:
    async with aiosqlite.connect(_db_path()) as conn:
        row = await conn.execute_fetchall(
            "SELECT available_dates FROM availability_snapshot WHERE facility_id = ? AND site_id = ?",
            (facility_id, site_id),
        )
        if not row:
            return []
        try:
            return json.loads(row[0][0])
        except (json.JSONDecodeError, IndexError):
            return []


async def update_snapshot(facility_id: str, site_id: str, available_dates: list[str]) -> None:
    async with aiosqlite.connect(_db_path()) as conn:
        await conn.execute(
            """INSERT INTO availability_snapshot (facility_id, site_id, available_dates, last_updated)
               VALUES (?, ?, ?, CURRENT_TIMESTAMP)
               ON CONFLICT(facility_id, site_id)
               DO UPDATE SET available_dates = excluded.available_dates,
                             last_updated = excluded.last_updated""",
            (facility_id, site_id, json.dumps(available_dates)),
        )
        await conn.commit()


# ── Events ───────────────────────────────────────────────────────────────────

async def insert_availability_event(event: dict[str, Any]) -> int:
    async with aiosqlite.connect(_db_path()) as conn:
        cursor = await conn.execute(
            """INSERT INTO availability_events
               (facility_id, site_id, available_date, site_type, vehicle_length, loop_name)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                event["facility_id"], event["site_id"],
                str(event["available_date"]), event.get("site_type"),
                event.get("vehicle_length"), event.get("loop_name"),
            ),
        )
        await conn.commit()
        return cursor.lastrowid


# ── Alert log ────────────────────────────────────────────────────────────────

async def insert_alert_log(
    scan_id: int,
    event_id: int,
    channel: str,
    destination: str,
    message_body: str,
    status: str = "sent",
) -> None:
    async with aiosqlite.connect(_db_path()) as conn:
        await conn.execute(
            """INSERT INTO alert_log
               (scan_id, availability_event_id, channel, destination, message_body, status)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (scan_id, event_id, channel, destination, message_body, status),
        )
        await conn.execute(
            "UPDATE scans SET alert_count = alert_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (scan_id,),
        )
        await conn.commit()


async def get_alert_history(scan_id: int) -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        rows = await conn.execute_fetchall(
            "SELECT * FROM alert_log WHERE scan_id = ? ORDER BY sent_at DESC",
            (scan_id,),
        )
        return [dict(r) for r in rows]


# ── Status / health ──────────────────────────────────────────────────────────

async def get_status() -> dict[str, Any]:
    async with aiosqlite.connect(_db_path()) as conn:
        active_scans = (await conn.execute_fetchall(
            "SELECT COUNT(*) FROM scans WHERE active = 1"
        ))[0][0]
        facilities = (await conn.execute_fetchall(
            "SELECT COUNT(DISTINCT facility_id) FROM scans WHERE active = 1"
        ))[0][0]
        today = date.today().isoformat()
        alerts_today = (await conn.execute_fetchall(
            "SELECT COUNT(*) FROM alert_log WHERE DATE(sent_at) = ?",
            (today,),
        ))[0][0]
        last_event_row = await conn.execute_fetchall(
            "SELECT MAX(detected_at) FROM availability_events"
        )
        last_poll = last_event_row[0][0] if last_event_row and last_event_row[0][0] else None
        return {
            "active_scans": active_scans,
            "facilities_monitored": facilities,
            "alerts_sent_today": alerts_today,
            "last_poll_event": last_poll,
        }


# ── Facility lookup ──────────────────────────────────────────────────────────

async def get_facility(facility_id: str) -> dict[str, Any] | None:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        rows = await conn.execute_fetchall(
            "SELECT * FROM park_facilities WHERE facility_id = ?",
            (facility_id,),
        )
        if not rows:
            return None
        return dict(rows[0])


async def list_facilities() -> list[dict[str, Any]]:
    async with aiosqlite.connect(_db_path()) as conn:
        conn.row_factory = aiosqlite.Row
        rows = await conn.execute_fetchall(
            "SELECT * FROM park_facilities ORDER BY park_code, facility_name"
        )
        return [dict(r) for r in rows]


# ── Helpers ──────────────────────────────────────────────────────────────────

async def _get_scan_by_id(conn: aiosqlite.Connection, scan_id: int) -> dict[str, Any] | None:
    rows = await conn.execute_fetchall(
        "SELECT * FROM scans WHERE id = ?", (scan_id,)
    )
    if not rows:
        return None
    return _row_to_scan(rows[0])


def _row_to_scan(row: Any) -> dict[str, Any]:
    d = dict(row)
    d["active"] = bool(d.get("active", 0))
    d["flexible_arrival"] = bool(d.get("flexible_arrival", 0))
    if d.get("specific_site_ids"):
        try:
            d["specific_site_ids"] = json.loads(d["specific_site_ids"])
        except (json.JSONDecodeError, TypeError):
            d["specific_site_ids"] = None
    else:
        d["specific_site_ids"] = None
    return d
