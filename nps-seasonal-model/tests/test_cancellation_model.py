"""Tests for the stage 1 cancellation model pipeline.

Uses a synthetic DuckDB with engineered signal so we can verify:
  * dataset.build_observations correctly labels rows
  * features produces a consistent matrix at train and inference time
  * BaselineCancellationModel learns the signal better than chance
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
import pytest

from parkpulse import db as pp_db
from parkpulse.model.baseline import BaselineCancellationModel
from parkpulse.model.dataset import (
    ObservationConfig,
    build_observations,
    load_snapshots,
    time_based_split,
)
from parkpulse.model.features import (
    FeatureSchema,
    build_feature_matrix,
)


# ── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def mem_conn():
    conn = duckdb.connect(":memory:")
    pp_db.init_schema(conn)
    return conn


def _seed_snapshots(conn, rows):
    """Insert (poll_id, facility_id, campsite_id, check_date, status, polled_at)."""
    conn.executemany(
        """INSERT INTO availability_snapshots
           (poll_id, facility_id, campsite_id, check_date, status, polled_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        rows,
    )


def _make_history(
    facility_id: str,
    campsite_id: str,
    check_date: date,
    status_timeline: list[tuple[datetime, str]],
    poll_id_start: int = 1,
) -> list[tuple]:
    """Build snapshot rows for a single (site, check_date) over a timeline."""
    return [
        (poll_id_start + i, facility_id, campsite_id, check_date, status, polled_at)
        for i, (polled_at, status) in enumerate(status_timeline)
    ]


# ── Dataset construction ────────────────────────────────────────────────────


def test_load_snapshots_filters_time_range(mem_conn):
    rows = _make_history(
        "F1", "S1", date(2025, 6, 1),
        [
            (datetime(2025, 5, 1, 10, 0), "Reserved"),
            (datetime(2025, 5, 15, 10, 0), "Available"),
        ],
    )
    _seed_snapshots(mem_conn, rows)

    df = load_snapshots(mem_conn, since=datetime(2025, 5, 10))
    assert len(df) == 1
    assert df.iloc[0]["status"] == "Available"


def test_build_observations_labels_cancellation(mem_conn):
    """A Reserved site that flips to Available before arrival should be labelled 1."""
    check_date = date(2025, 6, 1)
    timeline = []
    for d in range(1, 15):
        timeline.append((datetime(2025, 5, d, 12, 0), "Reserved"))
    timeline.append((datetime(2025, 5, 20, 12, 0), "Available"))
    rows = _make_history("F1", "S1", check_date, timeline)
    _seed_snapshots(mem_conn, rows)

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1), hours_per_bucket=24),
    )
    assert len(obs) > 0
    assert (obs["cancelled"] == 1).all()


def test_build_observations_labels_no_cancellation(mem_conn):
    """A Reserved site that never opens up before arrival should be labelled 0."""
    check_date = date(2025, 6, 1)
    timeline = [
        (datetime(2025, 5, d, 12, 0), "Reserved") for d in range(1, 31)
    ]
    _seed_snapshots(mem_conn, _make_history("F1", "S1", check_date, timeline))

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1), hours_per_bucket=24),
    )
    assert len(obs) > 0
    assert (obs["cancelled"] == 0).all()


def test_build_observations_skips_future_arrivals(mem_conn):
    """Observations for sites whose arrival is still in the future are excluded."""
    check_date = date(2030, 6, 1)  # far future
    timeline = [(datetime(2025, 5, 1, 12, 0), "Reserved")]
    _seed_snapshots(mem_conn, _make_history("F1", "S1", check_date, timeline))

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1)),
    )
    assert obs.empty


def test_build_observations_skips_never_reserved(mem_conn):
    """(Site, date) pairs that were always Available provide no signal to learn from."""
    check_date = date(2025, 6, 1)
    timeline = [
        (datetime(2025, 5, d, 12, 0), "Available") for d in range(1, 15)
    ]
    _seed_snapshots(mem_conn, _make_history("F1", "S1", check_date, timeline))

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1)),
    )
    assert obs.empty


def test_build_observations_days_to_arrival(mem_conn):
    check_date = date(2025, 6, 10)
    timeline = [
        (datetime(2025, 5, 1, 12, 0), "Reserved"),
        (datetime(2025, 5, 20, 12, 0), "Available"),
    ]
    _seed_snapshots(mem_conn, _make_history("F1", "S1", check_date, timeline))

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1), hours_per_bucket=24),
    )
    may_1 = obs[obs["sampled_at"].dt.date == date(2025, 5, 1)]
    assert len(may_1) == 1
    assert may_1.iloc[0]["days_to_arrival"] == 40


def test_build_observations_bucketing_reduces_rows(mem_conn):
    """Many observations in the same hour bucket should collapse to one."""
    check_date = date(2025, 6, 1)
    timeline = [
        (datetime(2025, 5, 15, 10, 5 * i), "Reserved") for i in range(12)
    ]
    timeline.append((datetime(2025, 5, 20, 12, 0), "Available"))
    _seed_snapshots(mem_conn, _make_history("F1", "S1", check_date, timeline))

    obs = build_observations(
        mem_conn,
        ObservationConfig(as_of=datetime(2025, 7, 1), hours_per_bucket=6),
    )
    may_15 = obs[obs["sampled_at"].dt.date == date(2025, 5, 15)]
    assert len(may_15) == 1


# ── Train/test split ────────────────────────────────────────────────────────


def test_time_based_split():
    df = pd.DataFrame({
        "sampled_at": pd.to_datetime([
            "2025-05-01", "2025-05-02", "2025-05-03", "2025-05-04", "2025-05-05",
        ]),
        "cancelled": [0, 1, 0, 1, 0],
    })
    train, test = time_based_split(df, test_fraction=0.4)
    assert len(train) == 3
    assert len(test) == 2
    assert train["sampled_at"].max() < test["sampled_at"].min()


def test_time_based_split_empty():
    df = pd.DataFrame(columns=["sampled_at", "cancelled"])
    train, test = time_based_split(df)
    assert train.empty and test.empty


# ── Feature matrix ──────────────────────────────────────────────────────────


def _synthetic_observations(n: int = 200, seed: int = 0) -> pd.DataFrame:
    """Build a synthetic observation DataFrame with learnable signal.

    Signal: cancellation probability rises with days_to_arrival and differs
    between two facilities.
    """
    rng = np.random.default_rng(seed)
    facs = ["FAC_A", "FAC_B"]
    rows = []
    for _ in range(n):
        fac = rng.choice(facs)
        days = int(rng.integers(0, 60))
        logit = -2 + 0.03 * days + (0.8 if fac == "FAC_A" else -0.3)
        p = 1 / (1 + np.exp(-logit))
        y = int(rng.random() < p)
        sampled_at = datetime(2025, 5, 1) + timedelta(
            days=int(rng.integers(0, 30)),
            hours=int(rng.integers(0, 24)),
        )
        check_date = sampled_at.date() + timedelta(days=days)
        rows.append({
            "campsite_id": f"S{rng.integers(0, 10)}",
            "facility_id": fac,
            "check_date": check_date,
            "sampled_at": sampled_at,
            "days_to_arrival": days,
            "cancelled": y,
        })
    return pd.DataFrame(rows)


def test_build_feature_matrix_shape():
    obs = _synthetic_observations(100)
    X, y, schema = build_feature_matrix(obs)
    assert X.shape[0] == 100
    assert y is not None and y.shape[0] == 100
    assert "days_to_arrival" in X.columns
    assert any(c.startswith("arrival_dow_") for c in X.columns)
    assert any(c.startswith("facility_id_") for c in X.columns)


def test_feature_matrix_is_deterministic_under_schema():
    """At inference time, applying the training schema gives the same columns
    even when the inference observations have different categorical values."""
    train = _synthetic_observations(100, seed=1)
    X_train, _, schema = build_feature_matrix(train)

    test = _synthetic_observations(20, seed=2)
    test = test[test["facility_id"] == "FAC_B"].reset_index(drop=True)

    X_test, _, _ = build_feature_matrix(test, schema=schema)
    assert list(X_test.columns) == list(X_train.columns)


def test_feature_schema_roundtrip():
    schema = FeatureSchema(columns=["a", "b", "c"])
    d = schema.to_dict()
    restored = FeatureSchema.from_dict(d)
    assert restored.columns == schema.columns
    assert restored.target == schema.target


# ── Baseline model ──────────────────────────────────────────────────────────


def test_baseline_fit_predict():
    obs = _synthetic_observations(500, seed=0)
    model = BaselineCancellationModel().fit(obs)
    proba = model.predict_proba(obs)
    assert proba.shape == (500,)
    assert (proba >= 0).all() and (proba <= 1).all()


def test_baseline_learns_signal():
    """AUC on held-out test should beat chance."""
    train = _synthetic_observations(2000, seed=0)
    test = _synthetic_observations(500, seed=1)

    model = BaselineCancellationModel().fit(train)
    metrics = model.evaluate(test)

    assert metrics.roc_auc is not None
    assert metrics.roc_auc > 0.6
    assert 0.0 < metrics.brier < 0.5


def test_baseline_evaluate_includes_calibration():
    train = _synthetic_observations(1000, seed=0)
    model = BaselineCancellationModel().fit(train)
    metrics = model.evaluate(train)
    cal = metrics.calibration_by_days_to_arrival
    assert not cal.empty
    assert {"bucket", "n", "mean_pred", "actual_rate"}.issubset(cal.columns)


def test_baseline_save_load(tmp_path):
    obs = _synthetic_observations(300, seed=0)
    model = BaselineCancellationModel().fit(obs)
    orig = model.predict_proba(obs)

    path = tmp_path / "baseline.joblib"
    model.save(path)
    loaded = BaselineCancellationModel.load(path)

    np.testing.assert_allclose(loaded.predict_proba(obs), orig, rtol=1e-9)


def test_baseline_errors_on_unfitted():
    obs = _synthetic_observations(10)
    model = BaselineCancellationModel()
    with pytest.raises(RuntimeError):
        model.predict_proba(obs)


def test_baseline_errors_on_missing_target():
    obs = _synthetic_observations(10).drop(columns=["cancelled"])
    with pytest.raises(ValueError):
        BaselineCancellationModel().fit(obs)
