"""
Data cleaning and normalisation for NPS visitation records.

Key responsibilities
--------------------
- Standardise column names and dtypes
- Reject implausible visit counts (negative, astronomically large)
- Flag COVID years (2020, 2021) so downstream code can exclude them from
  baseline calculations while still retaining them in the raw table
- Detect and optionally cap statistical outliers (IQR method per park/month)
- Fill isolated missing months via linear interpolation (limited)
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

COVID_YEARS = {2020, 2021}
MAX_PLAUSIBLE_MONTHLY = 5_000_000   # no single park month exceeds this


# ── Public API ────────────────────────────────────────────────────────────────

def clean_visits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main entry-point.  Accepts a raw long-format DataFrame and returns a
    cleaned copy with consistent column types and a 'covid_year' flag column.

    Expected columns: unit_code, year, month, visit_count
    Optional columns: name, state, type
    """
    if df.empty:
        return df

    df = df.copy()
    df = _standardise_cols(df)
    df = _coerce_types(df)
    df = _filter_implausible(df)
    df = _deduplicate(df)
    df["covid_year"] = df["year"].isin(COVID_YEARS)
    df = _interpolate_missing(df)
    return df.reset_index(drop=True)


def exclude_covid(df: pd.DataFrame) -> pd.DataFrame:
    """Return a view with 2020 and 2021 removed."""
    return df[~df["year"].isin(COVID_YEARS)].copy()


def flag_outliers(df: pd.DataFrame, k: float = 3.0) -> pd.DataFrame:
    """
    Add a boolean 'outlier' column.  A value is an outlier if it is more than
    k inter-quartile ranges below Q1 or above Q3, computed per (unit_code, month).
    """
    df = df.copy()
    df["outlier"] = False
    for (unit, month), grp in df.groupby(["unit_code", "month"]):
        vals = grp["visit_count"].dropna()
        if len(vals) < 4:
            continue
        q1, q3 = np.percentile(vals, [25, 75])
        iqr = q3 - q1
        lo, hi = q1 - k * iqr, q3 + k * iqr
        mask = (df["unit_code"] == unit) & (df["month"] == month)
        df.loc[mask & ((df["visit_count"] < lo) | (df["visit_count"] > hi)), "outlier"] = True
    return df


# ── Internal helpers ──────────────────────────────────────────────────────────

_COL_ALIASES: dict[str, list[str]] = {
    "unit_code": ["unitcode", "parkcode", "unit_code", "code"],
    "name":      ["parkname", "unitname", "name", "park_name"],
    "year":      ["year", "yr"],
    "month":     ["month", "mo"],
    "visit_count": [
        "recreationvisits", "recvisits", "recreation_visits",
        "visitcount", "visit_count", "visits",
    ],
    "state":     ["state", "states"],
    "type":      ["parktype", "unittype", "type", "designation"],
}


def _standardise_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for canonical, aliases in _COL_ALIASES.items():
        for col in df.columns:
            if col.lower().replace(" ", "").replace("_", "") in [
                a.replace("_", "") for a in aliases
            ]:
                if col != canonical:
                    rename[col] = canonical
                break
    return df.rename(columns=rename)


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    if "month" in df.columns:
        df["month"] = pd.to_numeric(df["month"], errors="coerce").astype("Int64")
    if "visit_count" in df.columns:
        df["visit_count"] = pd.to_numeric(
            df["visit_count"].astype(str).str.replace(",", "", regex=False),
            errors="coerce"
        ).astype("Int64")
    for col in ("unit_code", "name", "state", "type"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df


def _filter_implausible(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    # Remove rows with no visit count that we can't use
    df = df[df["year"].between(1979, 2030, inclusive="both")]
    df = df[df["month"].between(1, 12, inclusive="both")]
    # Negative counts → null
    if "visit_count" in df.columns:
        df.loc[df["visit_count"] < 0, "visit_count"] = pd.NA
        df.loc[df["visit_count"] > MAX_PLAUSIBLE_MONTHLY, "visit_count"] = pd.NA
    removed = before - len(df)
    if removed:
        log.debug("Removed %d implausible rows", removed)
    return df


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    key = ["unit_code", "year", "month"]
    if not all(k in df.columns for k in key):
        return df
    dupes = df.duplicated(subset=key, keep=False).sum()
    if dupes:
        log.debug("Deduplicating %d rows on (unit_code, year, month)", dupes)
        df = (
            df.sort_values("visit_count", ascending=False, na_position="last")
            .drop_duplicates(subset=key, keep="first")
        )
    return df


def _interpolate_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each (unit_code, month) time-series, linearly interpolate up to 2
    consecutive missing years.  Exclude COVID years from interpolation.
    """
    if "visit_count" not in df.columns or df.empty:
        return df

    result_parts = []
    for (unit, month), grp in df.groupby(["unit_code", "month"]):
        grp = grp.sort_values("year").copy()
        non_covid = grp[~grp["year"].isin(COVID_YEARS)]
        # Only interpolate if there are at least 3 non-null, non-covid values
        if non_covid["visit_count"].notna().sum() < 3:
            result_parts.append(grp)
            continue
        # Apply linear interpolation on non-covid rows only, limit=2 gaps
        idx = non_covid.index
        non_covid.loc[idx, "visit_count"] = (
            non_covid["visit_count"]
            .interpolate(method="linear", limit=2, limit_direction="both")
        )
        grp.update(non_covid)
        result_parts.append(grp)

    return pd.concat(result_parts).sort_values(["unit_code", "year", "month"])
