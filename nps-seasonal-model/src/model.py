"""
Seasonal busyness model for NPS parks.

For each park this module computes:
- monthly_score       0–100 normalised busyness per month (100 = busiest month)
- yoy_trend           year-over-year % change (last-3-yr avg vs prior-3-yr avg)
- peak_months         top 3 months by score
- shoulder_months     months where 20% ≤ score < 50% of peak (i.e. score 20–50)
- quiet_months        months where score < 30
- weekend_multiplier  default 1.4x (no weekly data in IRMA monthly series)
- best_visit_windows  ranked 2-week windows with lowest expected busyness
- low_confidence      True if fewer than 5 years of non-COVID data

Weather hostility heuristics (used to skip best-visit windows)
--------------------------------------------------------------
A window is flagged as "weather-hostile" if the park's state matches a winter
closure pattern AND the window falls in Nov–Mar.  High-desert parks flip this:
summer heat (Jun–Aug) is hostile, not winter.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
import db
from clean import exclude_covid

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

EXCLUDED_YEARS = [2020, 2021]

# Parks that are uncomfortable in summer (desert heat)
HIGH_DESERT_PARKS = {"DEVA", "SAGU", "WHSA", "PEFO", "BADL"}

# Parks where winter access is severely limited (deep snow / road closures)
WINTER_LIMITED_PARKS = {
    "YELL", "GLAC", "ROMO", "GRTE", "NOCA", "MORA", "CRLA", "LAVO",
}

# Parks where fall foliage is the peak (Oct heavy)
FOLIAGE_PARKS = {"GRSM", "SHEN", "ACAD", "BLRI"}


@dataclass
class MonthScore:
    month: int
    month_name: str
    score: float
    label: str       # "peak", "shoulder", "quiet"
    avg_visits: int


@dataclass
class VisitWindow:
    label: str
    start_month: int
    start_week: int   # 1–4 within start_month
    score: float
    notes: str


@dataclass
class BusynessModel:
    unit_code: str
    name: str
    monthly_scores: list[MonthScore]
    peak_months: list[int]
    shoulder_months: list[int]
    quiet_months: list[int]
    best_visit_windows: list[VisitWindow]
    yoy_trend: str
    data_years: list[int]
    excluded_years: list[int]
    low_confidence: bool
    weekend_multiplier: float = 1.4

    def to_dict(self) -> dict[str, Any]:
        return {
            "unit_code": self.unit_code,
            "name": self.name,
            "monthly_scores": [
                {
                    "month": ms.month,
                    "month_name": ms.month_name,
                    "score": round(ms.score, 1),
                    "label": ms.label,
                    "avg_visits": ms.avg_visits,
                }
                for ms in self.monthly_scores
            ],
            "peak_months": self.peak_months,
            "shoulder_months": self.shoulder_months,
            "quiet_months": self.quiet_months,
            "best_visit_windows": [
                {
                    "label": w.label,
                    "start_month": w.start_month,
                    "start_week": w.start_week,
                    "score": round(w.score, 1),
                    "notes": w.notes,
                }
                for w in self.best_visit_windows
            ],
            "yoy_trend": self.yoy_trend,
            "data_years": self.data_years,
            "excluded_years": self.excluded_years,
            "low_confidence": self.low_confidence,
            "weekend_multiplier": self.weekend_multiplier,
        }


# ── Core computation ──────────────────────────────────────────────────────────

def build_busyness_model(
    unit_code: str,
    db_path: Path | str = db.DEFAULT_DB,
) -> BusynessModel | None:
    """Build the full seasonal model for a park. Returns None if no data found."""
    park_info = db.get_park(unit_code, db_path)
    if park_info is None:
        return None

    all_visits = db.get_monthly_visits(unit_code, db_path)
    if all_visits.empty:
        return None

    all_visits["year"] = all_visits["year"].astype(int)
    all_visits["month"] = all_visits["month"].astype(int)
    all_visits["visit_count"] = pd.to_numeric(all_visits["visit_count"], errors="coerce")

    # Baseline: exclude COVID years
    baseline = exclude_covid(all_visits)
    baseline = baseline[baseline["visit_count"].notna()]

    data_years = sorted(all_visits["year"].unique().tolist())
    baseline_years = sorted(baseline["year"].unique().tolist())
    low_confidence = len(baseline_years) < 5

    # ── Monthly averages ────────────────────────────────────────────────────
    monthly_avg = (
        baseline.groupby("month")["visit_count"]
        .mean()
        .reindex(range(1, 13), fill_value=0)
    )

    peak_val = monthly_avg.max()
    if peak_val == 0:
        # All zeros — can't build meaningful model
        return None

    monthly_scores_raw = (monthly_avg / peak_val * 100).round(1)

    # ── Classify months ─────────────────────────────────────────────────────
    peak_months = _top_n_months(monthly_scores_raw, 3)
    shoulder_months = _months_in_range(monthly_scores_raw, 20, 50)
    quiet_months = _months_below(monthly_scores_raw, 30)

    monthly_score_objs = []
    for m in range(1, 13):
        score = float(monthly_scores_raw.get(m, 0))
        label = _classify(score, peak_months, shoulder_months, quiet_months)
        monthly_score_objs.append(
            MonthScore(
                month=m,
                month_name=MONTH_NAMES[m - 1],
                score=score,
                label=label,
                avg_visits=int(round(monthly_avg.get(m, 0))),
            )
        )

    # ── YoY trend ───────────────────────────────────────────────────────────
    yoy_trend = _compute_yoy_trend(baseline)

    # ── Best visit windows ──────────────────────────────────────────────────
    best_windows = _compute_best_windows(
        monthly_scores_raw,
        unit_code,
        park_info.get("state", ""),
    )

    return BusynessModel(
        unit_code=unit_code.upper(),
        name=park_info["name"],
        monthly_scores=monthly_score_objs,
        peak_months=peak_months,
        shoulder_months=shoulder_months,
        quiet_months=quiet_months,
        best_visit_windows=best_windows,
        yoy_trend=yoy_trend,
        data_years=data_years,
        excluded_years=EXCLUDED_YEARS,
        low_confidence=low_confidence,
    )


def build_all_models(
    db_path: Path | str = db.DEFAULT_DB,
) -> dict[str, BusynessModel]:
    parks = db.get_all_parks(db_path)
    models: dict[str, BusynessModel] = {}
    for _, row in parks.iterrows():
        m = build_busyness_model(row["unit_code"], db_path)
        if m is not None:
            models[row["unit_code"]] = m
    return models


def precompute_all_models(db_path: Path | str = db.DEFAULT_DB) -> int:
    """Build every park model and save to the park_models cache table.

    Called automatically at the end of the ingest pipeline so the app never
    has to compute models on the fly.  Returns the number of models saved.
    """
    parks = db.get_all_parks(db_path)
    saved = 0
    with db.get_conn(db_path) as conn:
        for _, row in parks.iterrows():
            m = build_busyness_model(row["unit_code"], db_path)
            if m is not None:
                db.save_park_model(conn, row["unit_code"], m.to_dict())
                saved += 1
    return saved


# ── Helper functions ──────────────────────────────────────────────────────────

def _top_n_months(scores: pd.Series, n: int) -> list[int]:
    return scores.nlargest(n).index.tolist()


def _months_in_range(scores: pd.Series, lo: float, hi: float) -> list[int]:
    return [m for m, s in scores.items() if lo <= s < hi]


def _months_below(scores: pd.Series, threshold: float) -> list[int]:
    return [m for m, s in scores.items() if s < threshold]


def _classify(
    score: float,
    peak_months: list[int],
    shoulder_months: list[int],
    quiet_months: list[int],
) -> str:
    # Re-derive label from score thresholds (ignore pre-computed lists for clarity)
    if score >= 70:
        return "peak"
    if score >= 20:
        return "shoulder"
    return "quiet"


def _compute_yoy_trend(df: pd.DataFrame) -> str:
    """
    Compare average annual visits for the last 3 years vs the prior 3 years.
    Returns a string like "+8.3%" or "-4.1%".
    """
    annual = df.groupby("year")["visit_count"].sum()
    years = sorted(annual.index)
    if len(years) < 6:
        return "N/A"
    last3 = annual[years[-3:]].mean()
    prior3 = annual[years[-6:-3]].mean()
    if prior3 == 0:
        return "N/A"
    pct = (last3 - prior3) / prior3 * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _compute_best_windows(
    scores: pd.Series,
    unit_code: str,
    state: str,
) -> list[VisitWindow]:
    """
    Identify up to 5 ranked 2-week windows with the lowest expected busyness,
    excluding weather-hostile periods.
    """
    hostile = _hostile_months(unit_code, state)
    windows: list[tuple[float, VisitWindow]] = []

    for m in range(1, 13):
        if m in hostile:
            continue
        # First half (weeks 1–2) uses current month score
        # Second half (weeks 3–4) uses average of current + next month
        score_first = float(scores.get(m, 0))
        next_m = (m % 12) + 1
        score_second = (float(scores.get(m, 0)) + float(scores.get(next_m, 0))) / 2

        for (week, score_val) in [(1, score_first), (3, score_second)]:
            if score_val > 55:
                continue  # skip busy windows
            label = _window_label(m, week)
            notes = _window_notes(m, week, unit_code, score_val)
            windows.append(
                (score_val, VisitWindow(
                    label=label,
                    start_month=m,
                    start_week=week,
                    score=round(score_val, 1),
                    notes=notes,
                ))
            )

    # Sort by score ascending and take top 5 unique month coverage
    windows.sort(key=lambda x: x[0])
    seen_months: set[int] = set()
    result: list[VisitWindow] = []
    for _, w in windows:
        if w.start_month not in seen_months:
            result.append(w)
            seen_months.add(w.start_month)
        if len(result) >= 5:
            break

    return result


def _hostile_months(unit_code: str, state: str) -> set[int]:
    """Return months that are weather-hostile for this park."""
    if unit_code in HIGH_DESERT_PARKS:
        return {6, 7, 8}   # summer heat
    if unit_code in WINTER_LIMITED_PARKS:
        return {11, 12, 1, 2}  # deep winter
    # Generic: no hostile months (temperate/year-round parks)
    return set()


def _window_label(month: int, week: int) -> str:
    month_name = MONTH_NAMES[month - 1]
    if week <= 2:
        return f"Early {month_name}"
    return f"Late {month_name}"


_WINDOW_NOTES: dict[tuple[int, int], str] = {
    # (month, approx_week) → canned insight
    (1, 1): "Minimal crowds, cold but passable at lower elevations",
    (2, 1): "Very quiet, check road conditions",
    (2, 3): "Late-winter quiet period, spring shoulder approaching",
    (3, 1): "Pre-spring lull, some facilities not yet open",
    (4, 1): "Before summer build-up, pleasant temperatures",
    (5, 1): "Spring shoulder — good weather, crowds not yet peak",
    (9, 1): "Post-summer drop, excellent weather in most parks",
    (9, 3): "Post-Labor Day drop, good weather",
    (10, 1): "Fall shoulder — cooler temperatures, thinner crowds",
    (10, 3): "Mid-October: foliage at many parks, but manageable",
    (11, 1): "Late-season quiet, reduced services likely",
    (11, 3): "Pre-winter lull, very low crowds",
    (12, 1): "Off-season, check park access status",
}


def _window_notes(month: int, week: int, unit_code: str, score: float) -> str:
    base = _WINDOW_NOTES.get((month, week if week <= 2 else 3))
    if base:
        return base
    # Fallback
    season = _season(month)
    return f"{season} visit window, score {score:.0f}/100"


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Fall"


# ── Convenience accessors ─────────────────────────────────────────────────────

def get_month_busyness(
    unit_code: str,
    month: int,
    db_path: Path | str = db.DEFAULT_DB,
) -> dict | None:
    """Return the MonthScore dict for a single (park, month) combination."""
    model = build_busyness_model(unit_code, db_path)
    if model is None:
        return None
    for ms in model.monthly_scores:
        if ms.month == month:
            return {
                "unit_code": unit_code,
                "name": model.name,
                "month": ms.month,
                "month_name": ms.month_name,
                "score": round(ms.score, 1),
                "label": ms.label,
                "avg_visits": ms.avg_visits,
                "yoy_trend": model.yoy_trend,
                "low_confidence": model.low_confidence,
            }
    return None


def compare_parks(
    unit_codes: list[str],
    month: int | None = None,
    db_path: Path | str = db.DEFAULT_DB,
) -> list[dict]:
    results = []
    for uc in unit_codes:
        model = build_busyness_model(uc, db_path)
        if model is None:
            continue
        if month is not None:
            entry = get_month_busyness(uc, month, db_path) or {}
        else:
            entry = {
                "unit_code": model.unit_code,
                "name": model.name,
                "peak_months": model.peak_months,
                "quiet_months": model.quiet_months,
                "yoy_trend": model.yoy_trend,
                "low_confidence": model.low_confidence,
            }
        results.append(entry)
    return results


def recommend_parks(
    db_path: Path | str = db.DEFAULT_DB,
    state: str | None = None,
    month: int | None = None,
    max_score: float = 50.0,
) -> list[dict]:
    """Return parks below max_score for the given month (and optionally state)."""
    parks = db.get_all_parks(db_path)
    if state:
        parks = parks[parks["state"].str.contains(state.upper(), na=False)]

    results = []
    for _, park in parks.iterrows():
        uc = park["unit_code"]
        if month is not None:
            entry = get_month_busyness(uc, month, db_path)
            if entry and entry["score"] <= max_score:
                results.append(entry)
        else:
            model = build_busyness_model(uc, db_path)
            if model:
                avg_score = np.mean([ms.score for ms in model.monthly_scores])
                if avg_score <= max_score:
                    results.append({
                        "unit_code": model.unit_code,
                        "name": model.name,
                        "avg_score": round(float(avg_score), 1),
                        "state": park.get("state", ""),
                        "yoy_trend": model.yoy_trend,
                    })

    results.sort(key=lambda x: x.get("score", x.get("avg_score", 0)))
    return results
