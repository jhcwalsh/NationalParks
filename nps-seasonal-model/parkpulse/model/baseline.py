"""Baseline cancellation prediction model.

A thin wrapper over sklearn's ``LogisticRegression`` that:
  * Standardises numeric features
  * Persists the feature schema so inference matches training
  * Reports Brier score, log-loss, ROC AUC, and calibration by
    ``days_to_arrival`` bucket (the most important axis for this problem)

Usage
-----
    from parkpulse.model.baseline import BaselineCancellationModel
    from parkpulse.model.dataset import build_observations, time_based_split
    from parkpulse.model.features import load_facility_catalog

    obs = build_observations(conn)
    train, test = time_based_split(obs, test_fraction=0.2)
    facs = load_facility_catalog()

    model = BaselineCancellationModel().fit(train, facs)
    metrics = model.evaluate(test, facs)
    model.save("data/baseline_v1.joblib")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler

from parkpulse.model.features import (
    DEFAULT_TARGET,
    FeatureSchema,
    build_feature_matrix,
)


@dataclass
class ModelMetrics:
    """Evaluation metrics for a fitted baseline model."""

    n: int
    positive_rate: float
    brier: float
    log_loss: float
    roc_auc: Optional[float]
    calibration_by_days_to_arrival: pd.DataFrame = field(repr=False)

    def to_dict(self) -> dict:
        d = {
            "n": int(self.n),
            "positive_rate": float(self.positive_rate),
            "brier": float(self.brier),
            "log_loss": float(self.log_loss),
            "roc_auc": None if self.roc_auc is None else float(self.roc_auc),
        }
        return d


class BaselineCancellationModel:
    """Logistic regression on the stage 1 feature set."""

    def __init__(self, C: float = 1.0, max_iter: int = 1000, random_state: int = 0):
        self.C = C
        self.max_iter = max_iter
        self.random_state = random_state
        self._clf: Optional[LogisticRegression] = None
        self._scaler: Optional[StandardScaler] = None
        self._schema: Optional[FeatureSchema] = None

    # ── Training ────────────────────────────────────────────────────────

    def fit(
        self,
        observations: pd.DataFrame,
        facilities: Optional[pd.DataFrame] = None,
    ) -> "BaselineCancellationModel":
        if DEFAULT_TARGET not in observations.columns:
            raise ValueError(
                f"Training observations must include a '{DEFAULT_TARGET}' column"
            )

        X, y, schema = build_feature_matrix(observations, facilities)
        if X.empty:
            raise ValueError("No training rows — nothing to fit")

        self._scaler = StandardScaler(with_mean=False)  # keep sparse dummies non-negative
        X_scaled = self._scaler.fit_transform(X.to_numpy(dtype=float))

        self._clf = LogisticRegression(
            C=self.C,
            max_iter=self.max_iter,
            random_state=self.random_state,
            solver="lbfgs",
        )
        self._clf.fit(X_scaled, y)
        self._schema = schema
        return self

    # ── Inference ───────────────────────────────────────────────────────

    def predict_proba(
        self,
        observations: pd.DataFrame,
        facilities: Optional[pd.DataFrame] = None,
    ) -> np.ndarray:
        """Return P(cancelled) for each row."""
        if self._clf is None or self._schema is None:
            raise RuntimeError("Model is not fitted")
        X, _, _ = build_feature_matrix(observations, facilities, schema=self._schema)
        X_scaled = self._scaler.transform(X.to_numpy(dtype=float))
        # Column 1 is the positive class (cancelled=1)
        return self._clf.predict_proba(X_scaled)[:, 1]

    # ── Evaluation ──────────────────────────────────────────────────────

    def evaluate(
        self,
        observations: pd.DataFrame,
        facilities: Optional[pd.DataFrame] = None,
    ) -> ModelMetrics:
        if DEFAULT_TARGET not in observations.columns:
            raise ValueError("Evaluation observations must include the target column")

        y_true = observations[DEFAULT_TARGET].to_numpy()
        y_prob = self.predict_proba(observations, facilities)

        # roc_auc_score fails if only one class present
        try:
            auc = float(roc_auc_score(y_true, y_prob))
        except ValueError:
            auc = None

        calibration = _calibration_by_days(
            observations[["days_to_arrival", DEFAULT_TARGET]],
            y_prob,
        )

        return ModelMetrics(
            n=len(y_true),
            positive_rate=float(np.mean(y_true)),
            brier=float(brier_score_loss(y_true, y_prob)),
            log_loss=float(log_loss(y_true, y_prob, labels=[0, 1])),
            roc_auc=auc,
            calibration_by_days_to_arrival=calibration,
        )

    # ── Persistence ─────────────────────────────────────────────────────

    def save(self, path: str | Path) -> None:
        if self._clf is None:
            raise RuntimeError("Cannot save an unfitted model")
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "clf": self._clf,
                "scaler": self._scaler,
                "schema": self._schema.to_dict() if self._schema else None,
                "C": self.C,
                "max_iter": self.max_iter,
                "random_state": self.random_state,
            },
            path,
        )

    @classmethod
    def load(cls, path: str | Path) -> "BaselineCancellationModel":
        blob = joblib.load(path)
        m = cls(
            C=blob.get("C", 1.0),
            max_iter=blob.get("max_iter", 1000),
            random_state=blob.get("random_state", 0),
        )
        m._clf = blob["clf"]
        m._scaler = blob["scaler"]
        schema_d = blob.get("schema")
        m._schema = FeatureSchema.from_dict(schema_d) if schema_d else None
        return m


# ── Helpers ─────────────────────────────────────────────────────────────────


def _calibration_by_days(
    df: pd.DataFrame,
    y_prob: np.ndarray,
    buckets: Optional[list[int]] = None,
) -> pd.DataFrame:
    """Bucket predictions by days_to_arrival and compare predicted vs actual.

    Returns a DataFrame with columns: bucket, n, mean_pred, actual_rate.
    """
    if buckets is None:
        buckets = [0, 1, 3, 7, 14, 30, 60, 120, 180]

    rows = []
    days = df["days_to_arrival"].to_numpy()
    y = df[DEFAULT_TARGET].to_numpy()
    for lo, hi in zip(buckets[:-1], buckets[1:]):
        mask = (days >= lo) & (days < hi)
        n = int(mask.sum())
        if n == 0:
            continue
        rows.append({
            "bucket": f"{lo}-{hi - 1}d",
            "n": n,
            "mean_pred": float(y_prob[mask].mean()),
            "actual_rate": float(y[mask].mean()),
        })
    return pd.DataFrame(rows)
