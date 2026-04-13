"""DuckDB schema, connection management, and hot-path queries.

Design notes
------------
* The collector holds ONE read-write connection for its entire lifetime.
* Analysis notebooks / scripts open *separate* read-only connections:
      duckdb.connect("data/parkpulse.duckdb", read_only=True)
  DuckDB supports one writer + many concurrent readers.
* The index on (campsite_id, check_date) exists specifically for
  ``get_latest_snapshot``, which runs every poll cycle (~5 min).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_PROJECT_ROOT / "data" / "parkpulse.duckdb")

_SCHEMA_DDL = """
-- Poll-cycle IDs
CREATE SEQUENCE IF NOT EXISTS seq_poll_id START 1;

-- One row per poll cycle
CREATE TABLE IF NOT EXISTS poll_log (
    poll_id         BIGINT   DEFAULT nextval('seq_poll_id') PRIMARY KEY,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    n_facilities    INTEGER  DEFAULT 0,
    n_sites         INTEGER  DEFAULT 0,
    n_snapshots     INTEGER  DEFAULT 0,
    n_transitions   INTEGER  DEFAULT 0,
    status          VARCHAR  DEFAULT 'running',
    error_message   VARCHAR
);

-- Every (site, date, status) observation per poll
CREATE TABLE IF NOT EXISTS availability_snapshots (
    poll_id         BIGINT    NOT NULL,
    facility_id     VARCHAR   NOT NULL,
    campsite_id     VARCHAR   NOT NULL,
    check_date      DATE      NOT NULL,
    status          VARCHAR   NOT NULL,
    polled_at       TIMESTAMP NOT NULL
);

-- Hot-path index: get_latest_snapshot scans by (campsite_id, check_date)
CREATE INDEX IF NOT EXISTS idx_snap_site_date
    ON availability_snapshots (campsite_id, check_date);

-- Detected status changes between consecutive polls
CREATE TABLE IF NOT EXISTS status_transitions (
    poll_id         BIGINT    NOT NULL,
    facility_id     VARCHAR   NOT NULL,
    campsite_id     VARCHAR   NOT NULL,
    check_date      DATE      NOT NULL,
    old_status      VARCHAR   NOT NULL,
    new_status      VARCHAR   NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    days_to_arrival INTEGER
);
"""


def connect(path: str | None = None, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection.

    Parameters
    ----------
    path : str or None
        Database file path.  Defaults to ``data/parkpulse.duckdb``.
    read_only : bool
        If True, open for read-only access (safe for concurrent analysis).
    """
    db_path = path or DEFAULT_DB_PATH
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path, read_only=read_only)


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables, indexes, and sequences if they don't already exist."""
    conn.execute(_SCHEMA_DDL)
    logger.info("DuckDB schema initialised at %s", conn)


# ── Poll lifecycle ──────────────────────────────────────────────────────────


def start_poll(conn: duckdb.DuckDBPyConnection) -> int:
    """Insert a new poll_log row and return its poll_id."""
    result = conn.execute(
        "INSERT INTO poll_log (started_at) VALUES (?) RETURNING poll_id",
        [datetime.utcnow()],
    ).fetchone()
    return result[0]


def finish_poll(
    conn: duckdb.DuckDBPyConnection,
    poll_id: int,
    *,
    n_facilities: int = 0,
    n_sites: int = 0,
    n_snapshots: int = 0,
    n_transitions: int = 0,
    status: str = "success",
    error_message: str | None = None,
) -> None:
    """Update a poll_log row with final stats."""
    conn.execute(
        """UPDATE poll_log
           SET finished_at   = ?,
               n_facilities  = ?,
               n_sites       = ?,
               n_snapshots   = ?,
               n_transitions = ?,
               status        = ?,
               error_message = ?
           WHERE poll_id = ?""",
        [
            datetime.utcnow(),
            n_facilities,
            n_sites,
            n_snapshots,
            n_transitions,
            status,
            error_message,
            poll_id,
        ],
    )


# ── Bulk inserts ────────────────────────────────────────────────────────────


def insert_snapshots(
    conn: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> int:
    """Bulk-insert snapshot rows.

    Each tuple: (poll_id, facility_id, campsite_id, check_date, status, polled_at)
    Returns the number of rows inserted.
    """
    if not rows:
        return 0
    conn.executemany(
        """INSERT INTO availability_snapshots
           (poll_id, facility_id, campsite_id, check_date, status, polled_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


def insert_transitions(
    conn: duckdb.DuckDBPyConnection,
    rows: list[tuple],
) -> int:
    """Bulk-insert transition rows.

    Each tuple: (poll_id, facility_id, campsite_id, check_date,
                 old_status, new_status, detected_at, days_to_arrival)
    """
    if not rows:
        return 0
    conn.executemany(
        """INSERT INTO status_transitions
           (poll_id, facility_id, campsite_id, check_date,
            old_status, new_status, detected_at, days_to_arrival)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return len(rows)


# ── Hot-path query ──────────────────────────────────────────────────────────


def get_latest_snapshot(
    conn: duckdb.DuckDBPyConnection,
) -> dict[tuple[str, str], str]:
    """Return the most-recent status for every (campsite_id, check_date) pair.

    This is the *hot path* — it runs every poll cycle to diff against the
    freshly-fetched data.  The index on (campsite_id, check_date) keeps it
    under a second for the expected ~21k active pairs.

    Returns
    -------
    dict mapping (campsite_id, date_iso_str) -> status
    """
    rows = conn.execute(
        """SELECT campsite_id, check_date, status
           FROM availability_snapshots
           QUALIFY ROW_NUMBER() OVER (
               PARTITION BY campsite_id, check_date
               ORDER BY poll_id DESC
           ) = 1"""
    ).fetchall()
    return {
        (row[0], row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])): row[2]
        for row in rows
    }
