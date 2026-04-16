"""Feature engineering for the cancellation prediction model.

Takes the raw observations DataFrame from ``dataset.build_observations``
and produces a numeric feature matrix suitable for sklearn.

Stage 1 features
----------------
Temporal (about the arrival date):
  * ``days_to_arrival``                — integer, 0..180
  * ``arrival_dow_{0..6}``             — one-hot, Mon=0
  * ``arrival_is_weekend``             — bool
  * ``arrival_month_{1..12}``          — one-hot

Temporal (about the observation time):
  * ``obs_hour``                       — 0..23, UTC
  * ``obs_dow_{0..6}``                 — one-hot

Spatial (about where):
  * ``facility_id_<id>``               — one-hot across facilities
  * ``park_code_<code>``               — one-hot across parks (coarser)

The one-hot encodings use pandas ``get_dummies`` via a stored column
schema so that inference can apply the exact same transformation.

Stage 2+ will add: reservation duration, facility historical cancel rate,
weather, holiday proximity.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


DEFAULT_TARGET = "cancelled"


# ── Facility catalog lookup ─────────────────────────────────────────────────


def load_facility_catalog(path: Optional[Path] = None) -> pd.DataFrame:
    """Load facilities.json into a DataFrame for joining with observations."""
    if path is None:
        path = Path(__file__).resolve().parents[2] / "data" / "facilities.json"
    with open(path) as f:
        data = json.load(f)
    return pd.DataFrame(data)[["facility_id", "park_code", "facility_name"]]


# ── Feature extraction ──────────────────────────────────────────────────────


@dataclass
class FeatureSchema:
    """Column list captured at fit-time so inference produces the same shape."""

    columns: list[str]
    target: str = DEFAULT_TARGET

    def to_dict(self) -> dict:
        return {"columns": self.columns, "target": self.target}

    @classmethod
    def from_dict(cls, d: dict) -> "FeatureSchema":
        return cls(columns=list(d["columns"]), target=d.get("target", DEFAULT_TARGET))


def _raw_features(
    observations: pd.DataFrame,
    facilities: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Compute pre-encoding features.  Returns a DataFrame with date/category cols."""
    df = observations.copy()

    # Ensure datetime/date dtypes
    df["sampled_at"] = pd.to_datetime(df["sampled_at"])
    df["check_date"] = pd.to_datetime(df["check_date"]).dt.date

    # Arrival-side features
    arrival = pd.to_datetime(df["check_date"])
    df["arrival_dow"] = arrival.dt.weekday
    df["arrival_is_weekend"] = (df["arrival_dow"] >= 5).astype(int)
    df["arrival_month"] = arrival.dt.month

    # Observation-side features
    df["obs_hour"] = df["sampled_at"].dt.hour
    df["obs_dow"] = df["sampled_at"].dt.weekday

    # Join park_code if a facility catalog is provided
    if facilities is not None:
        df = df.merge(
            facilities[["facility_id", "park_code"]],
            on="facility_id",
            how="left",
        )
        df["park_code"] = df["park_code"].fillna("UNKNOWN")
    else:
        df["park_code"] = "UNKNOWN"

    return df


def build_feature_matrix(
    observations: pd.DataFrame,
    facilities: Optional[pd.DataFrame] = None,
    schema: Optional[FeatureSchema] = None,
) -> tuple[pd.DataFrame, Optional[np.ndarray], FeatureSchema]:
    """Build an (X, y, schema) triple from observation rows.

    If ``schema`` is supplied, reindex X to match exactly — used at
    inference time so the model sees the same columns it trained on.
    Otherwise derive the schema from the one-hot encoding.

    ``y`` is None when the observations have no target column (inference).
    """
    raw = _raw_features(observations, facilities)

    # One-hot encode the categorical features
    categorical = {
        "arrival_dow": "arrival_dow",
        "arrival_month": "arrival_month",
        "obs_dow": "obs_dow",
        "facility_id": "facility_id",
        "park_code": "park_code",
    }
    X = pd.get_dummies(
        raw,
        columns=list(categorical.keys()),
        prefix=list(categorical.values()),
        dtype=int,
    )

    # Keep only numeric features
    drop_cols = {"campsite_id", "check_date", "sampled_at"}
    if DEFAULT_TARGET in X.columns:
        y = X[DEFAULT_TARGET].to_numpy()
        drop_cols.add(DEFAULT_TARGET)
    else:
        y = None
    X = X.drop(columns=[c for c in drop_cols if c in X.columns])

    # Reindex to schema (inference path) or capture schema (training path)
    if schema is not None:
        X = X.reindex(columns=schema.columns, fill_value=0)
        out_schema = schema
    else:
        out_schema = FeatureSchema(columns=list(X.columns))

    return X, y, out_schema
