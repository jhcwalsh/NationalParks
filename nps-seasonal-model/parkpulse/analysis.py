"""Pre-built analysis queries for the ParkPulse availability dataset.

All functions accept a *read-only* DuckDB connection and return a pandas
DataFrame.  Open one like this:

    import duckdb
    conn = duckdb.connect("data/parkpulse.duckdb", read_only=True)

The collector holds the sole read-write connection; these read-only
connections can run concurrently without contention.

Key outputs for the logistic regression model
----------------------------------------------
* ``cancellation_rate_by_days_to_arrival`` — P(cancel) as f(days_to_arrival)
* ``rebooking_velocity`` — time-to-rebook after a cancellation, bucketed
* ``hourly_transition_heatmap`` — when cancellations cluster by hour-of-day
* ``facility_churn`` — per-facility cancel/rebook rates for feature engineering
"""

from __future__ import annotations

import duckdb
import pandas as pd


# ── Cancellation probability ────────────────────────────────────────────────


def cancellation_rate_by_days_to_arrival(
    conn: duckdb.DuckDBPyConnection,
    min_observations: int = 20,
) -> pd.DataFrame:
    """P(cancellation) as a function of days-to-arrival.

    A "cancellation" is any transition where new_status = 'Available'
    (the reservation was dropped).  Grouped by ``days_to_arrival`` and
    filtered to buckets with at least ``min_observations`` transitions.

    Columns: days_to_arrival, cancellations, total_transitions,
             cancellation_rate
    """
    return conn.execute(
        """
        SELECT
            days_to_arrival,
            COUNT(*) FILTER (WHERE new_status = 'Available')  AS cancellations,
            COUNT(*)                                          AS total_transitions,
            ROUND(
                COUNT(*) FILTER (WHERE new_status = 'Available')
                    * 1.0 / COUNT(*), 4
            ) AS cancellation_rate
        FROM status_transitions
        WHERE days_to_arrival IS NOT NULL
          AND days_to_arrival >= 0
        GROUP BY days_to_arrival
        HAVING COUNT(*) >= ?
        ORDER BY days_to_arrival
        """,
        [min_observations],
    ).fetchdf()


# ── Rebooking velocity ──────────────────────────────────────────────────────


def rebooking_velocity(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """How quickly a site gets rebooked after a cancellation.

    Pairs each cancellation (new_status = 'Available') with the *next*
    booking (new_status != 'Available') for the same (campsite_id, check_date).
    The delta is the rebooking time in minutes.

    Columns: campsite_id, check_date, cancelled_at, rebooked_at,
             rebooking_minutes, days_to_arrival
    """
    return conn.execute(
        """
        WITH cancellations AS (
            SELECT
                campsite_id,
                check_date,
                detected_at AS cancelled_at,
                days_to_arrival,
                ROW_NUMBER() OVER (
                    PARTITION BY campsite_id, check_date
                    ORDER BY detected_at
                ) AS cancel_seq
            FROM status_transitions
            WHERE new_status = 'Available'
        ),
        rebookings AS (
            SELECT
                campsite_id,
                check_date,
                detected_at AS rebooked_at,
                ROW_NUMBER() OVER (
                    PARTITION BY campsite_id, check_date
                    ORDER BY detected_at
                ) AS book_seq
            FROM status_transitions
            WHERE old_status = 'Available'
              AND new_status != 'Available'
        )
        SELECT
            c.campsite_id,
            c.check_date,
            c.cancelled_at,
            r.rebooked_at,
            EXTRACT(EPOCH FROM (r.rebooked_at - c.cancelled_at)) / 60.0
                AS rebooking_minutes,
            c.days_to_arrival
        FROM cancellations c
        JOIN rebookings r
          ON  c.campsite_id = r.campsite_id
          AND c.check_date  = r.check_date
          AND r.rebooked_at > c.cancelled_at
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY c.campsite_id, c.check_date, c.cancel_seq
            ORDER BY r.rebooked_at
        ) = 1
        ORDER BY c.cancelled_at
        """
    ).fetchdf()


def rebooking_velocity_summary(
    conn: duckdb.DuckDBPyConnection,
    bucket_minutes: list[int] | None = None,
) -> pd.DataFrame:
    """Histogram of rebooking times, bucketed.

    Default buckets: <5 min, 5-15 min, 15-60 min, 1-6 hr, 6-24 hr, >24 hr.

    Columns: bucket, count, pct
    """
    raw = rebooking_velocity(conn)
    if raw.empty:
        return pd.DataFrame(columns=["bucket", "count", "pct"])

    if bucket_minutes is None:
        bucket_minutes = [5, 15, 60, 360, 1440]

    labels = []
    prev = 0
    for m in bucket_minutes:
        if prev == 0:
            labels.append(f"<{m}m")
        else:
            labels.append(f"{prev}-{m}m" if m <= 60 else f"{prev // 60}-{m // 60}h")
        prev = m
    labels.append(f">{bucket_minutes[-1] // 60}h")

    mins = raw["rebooking_minutes"]
    counts = []
    prev_val = 0
    for m in bucket_minutes:
        counts.append(int(((mins >= prev_val) & (mins < m)).sum()))
        prev_val = m
    counts.append(int((mins >= prev_val).sum()))

    total = sum(counts)
    return pd.DataFrame({
        "bucket": labels,
        "count": counts,
        "pct": [round(c / total * 100, 1) if total else 0 for c in counts],
    })


# ── Temporal patterns ───────────────────────────────────────────────────────


def hourly_transition_heatmap(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Cancellation and booking counts by hour-of-day and day-of-week.

    Useful for discovering when cancellations cluster (late-night drops,
    morning releases, etc.).

    Columns: dow (0=Mon), hour, cancellations, bookings
    """
    return conn.execute(
        """
        SELECT
            EXTRACT(DOW FROM detected_at)  AS dow,
            EXTRACT(HOUR FROM detected_at) AS hour,
            COUNT(*) FILTER (WHERE new_status = 'Available')      AS cancellations,
            COUNT(*) FILTER (WHERE old_status = 'Available'
                              AND new_status != 'Available')      AS bookings
        FROM status_transitions
        GROUP BY dow, hour
        ORDER BY dow, hour
        """
    ).fetchdf()


# ── Per-facility churn ──────────────────────────────────────────────────────


def facility_churn(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Per-facility cancel and rebook rates.

    Columns: facility_id, cancellations, bookings, total_transitions,
             cancel_rate, book_rate
    """
    return conn.execute(
        """
        SELECT
            facility_id,
            COUNT(*) FILTER (WHERE new_status = 'Available')  AS cancellations,
            COUNT(*) FILTER (WHERE old_status = 'Available'
                              AND new_status != 'Available')  AS bookings,
            COUNT(*)                                          AS total_transitions,
            ROUND(
                COUNT(*) FILTER (WHERE new_status = 'Available')
                    * 1.0 / NULLIF(COUNT(*), 0), 4
            ) AS cancel_rate,
            ROUND(
                COUNT(*) FILTER (WHERE old_status = 'Available'
                                  AND new_status != 'Available')
                    * 1.0 / NULLIF(COUNT(*), 0), 4
            ) AS book_rate
        FROM status_transitions
        GROUP BY facility_id
        ORDER BY cancellations DESC
        """
    ).fetchdf()


# ── Data freshness / health ─────────────────────────────────────────────────


def poll_history(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 50,
) -> pd.DataFrame:
    """Recent poll cycles with stats.

    Columns: poll_id, started_at, finished_at, duration_s,
             n_facilities, n_sites, n_snapshots, n_transitions, status
    """
    return conn.execute(
        """
        SELECT
            poll_id,
            started_at,
            finished_at,
            EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_s,
            n_facilities,
            n_sites,
            n_snapshots,
            n_transitions,
            status
        FROM poll_log
        ORDER BY poll_id DESC
        LIMIT ?
        """,
        [limit],
    ).fetchdf()


def snapshot_coverage(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """How many unique (site, date) pairs are being tracked, grouped by facility.

    Columns: facility_id, n_sites, n_dates, n_pairs, latest_poll
    """
    return conn.execute(
        """
        SELECT
            facility_id,
            COUNT(DISTINCT campsite_id) AS n_sites,
            COUNT(DISTINCT check_date)  AS n_dates,
            COUNT(DISTINCT (campsite_id, check_date)) AS n_pairs,
            MAX(polled_at) AS latest_poll
        FROM availability_snapshots
        GROUP BY facility_id
        ORDER BY n_pairs DESC
        """
    ).fetchdf()
