"""Dataset construction for the cancellation prediction model.

Turns raw ``availability_snapshots`` into labeled training examples.

Observation unit
----------------
One row per (campsite_id, check_date, sampled_at) where:
  * At sampled_at the site was observed as Reserved
  * sampled_at's date < check_date (arrival hasn't happened yet)
  * check_date has since passed (we know the outcome)

Label
-----
``cancelled`` = 1 if at any observation between sampled_at and check_date
the site was seen as 'Available'.  Else 0.

Downsampling
------------
Raw snapshots are taken every ~5 minutes, so each (site, check_date) is
observed ~288 times/day — highly correlated.  ``build_observations``
subsamples to one observation per (site, check_date, local-day, hour_bucket)
by default.  Tune ``hours_per_bucket`` to control density.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import duckdb
import pandas as pd


# What counts as "booked"?  Anything that isn't Available.  Recreation.gov
# uses statuses like 'Reserved', 'Not Reservable', 'Not Available', etc.
# We treat the 'Available' status as the positive-signal target state.
AVAILABLE = "Available"


# ── Raw loaders ─────────────────────────────────────────────────────────────


def load_snapshots(
    conn: duckdb.DuckDBPyConnection,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> pd.DataFrame:
    """Load availability_snapshots into a DataFrame, optionally time-filtered."""
    clauses: list[str] = []
    params: list = []
    if since is not None:
        clauses.append("polled_at >= ?")
        params.append(since)
    if until is not None:
        clauses.append("polled_at < ?")
        params.append(until)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    return conn.execute(
        f"""SELECT poll_id, facility_id, campsite_id, check_date, status, polled_at
            FROM availability_snapshots
            {where}
            ORDER BY campsite_id, check_date, polled_at""",
        params,
    ).fetchdf()


def load_transitions(
    conn: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Load status_transitions into a DataFrame."""
    return conn.execute(
        """SELECT poll_id, facility_id, campsite_id, check_date,
                  old_status, new_status, detected_at, days_to_arrival
           FROM status_transitions
           ORDER BY campsite_id, check_date, detected_at"""
    ).fetchdf()


# ── Observation construction ────────────────────────────────────────────────


@dataclass
class ObservationConfig:
    """Tunable knobs for the training-set construction."""

    hours_per_bucket: int = 6
    """Subsample to one observation per (site, check_date, day, bucket).
    4 buckets/day at the default 6h means ~4 observations/day vs ~288 raw."""

    min_days_to_arrival: int = 0
    """Drop observations with days_to_arrival below this.  0 = include same-day."""

    max_days_to_arrival: int = 180
    """Drop observations with days_to_arrival above this.  Rec.gov caps
    booking at ~6 months ahead so this is a natural upper bound."""

    as_of: Optional[datetime] = None
    """Only include observations where check_date < as_of.date(), i.e. where
    the outcome is known.  Defaults to now (UTC)."""


def build_observations(
    conn: duckdb.DuckDBPyConnection,
    config: Optional[ObservationConfig] = None,
) -> pd.DataFrame:
    """Build labeled training examples from DuckDB snapshots.

    Returns a DataFrame with columns:
        campsite_id, facility_id, check_date, sampled_at,
        days_to_arrival, cancelled

    The ``cancelled`` column is the training label.
    """
    cfg = config or ObservationConfig()
    as_of = cfg.as_of or datetime.utcnow()

    # 1. Pull every (site, check_date) pair whose arrival date has passed
    #    and that was observed as Reserved at least once.
    #
    # 2. For each such pair, compute the label: did it ever appear Available
    #    at any snapshot where polled_at.date() < check_date?
    #
    # 3. Select the Reserved observations that will become training rows,
    #    bucketed to reduce correlation.
    #
    # All of this happens in a single DuckDB query because we don't want to
    # ship 100M raw rows into pandas.

    query = """
    WITH
    -- Outcome known for any (site, date) whose date is in the past
    eligible AS (
        SELECT
            campsite_id,
            facility_id,
            check_date,
            -- Did we ever see it Available before the arrival date?
            MAX(CASE WHEN status = ? AND polled_at::DATE < check_date
                     THEN 1 ELSE 0 END) AS cancelled
        FROM availability_snapshots
        WHERE check_date < ?::DATE
        GROUP BY campsite_id, facility_id, check_date
        HAVING MAX(CASE WHEN status != ? THEN 1 ELSE 0 END) = 1  -- was reserved at some point
    ),
    -- All observations where the site was Reserved before arrival,
    -- bucketed to reduce duplication.
    candidate AS (
        SELECT
            s.campsite_id,
            s.facility_id,
            s.check_date,
            s.polled_at AS sampled_at,
            DATE_DIFF('day', s.polled_at::DATE, s.check_date) AS days_to_arrival,
            DATE_TRUNC('day', s.polled_at) AS day_bucket,
            CAST(EXTRACT(hour FROM s.polled_at) / ? AS INTEGER) AS hour_bucket,
            ROW_NUMBER() OVER (
                PARTITION BY s.campsite_id, s.check_date,
                             DATE_TRUNC('day', s.polled_at),
                             CAST(EXTRACT(hour FROM s.polled_at) / ? AS INTEGER)
                ORDER BY s.polled_at
            ) AS rn
        FROM availability_snapshots s
        JOIN eligible e USING (campsite_id, facility_id, check_date)
        WHERE s.status != ?
          AND s.polled_at::DATE < s.check_date
          AND DATE_DIFF('day', s.polled_at::DATE, s.check_date) BETWEEN ? AND ?
    )
    SELECT
        c.campsite_id,
        c.facility_id,
        c.check_date,
        c.sampled_at,
        c.days_to_arrival,
        e.cancelled
    FROM candidate c
    JOIN eligible e USING (campsite_id, facility_id, check_date)
    WHERE c.rn = 1
    ORDER BY c.sampled_at
    """

    return conn.execute(
        query,
        [
            AVAILABLE, as_of, AVAILABLE,
            cfg.hours_per_bucket, cfg.hours_per_bucket,
            AVAILABLE,
            cfg.min_days_to_arrival, cfg.max_days_to_arrival,
        ],
    ).fetchdf()


# ── Train/test split ────────────────────────────────────────────────────────


def time_based_split(
    observations: pd.DataFrame,
    test_fraction: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split observations by sampled_at — newest ``test_fraction`` go to test.

    Random splits leak future information (the same (site, date) pair appears
    at both early and late sampled_at).  A time-based split is the only
    honest way to evaluate a model that will be deployed forward in time.
    """
    if observations.empty:
        return observations.copy(), observations.copy()
    df = observations.sort_values("sampled_at").reset_index(drop=True)
    cut = int(len(df) * (1 - test_fraction))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()
