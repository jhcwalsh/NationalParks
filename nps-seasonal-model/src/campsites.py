"""Recreation.gov campsite availability for the 63 US National Parks.

Data sources
------------
- RIDB API  (ridb.recreation.gov/api/v1)          : campground discovery
- Rec.gov availability API (www.recreation.gov)   : per-campground availability

API key
-------
A free RIDB API key is required for campground discovery (build_park_facility_map).
Get one at https://ridb.recreation.gov.

The Recreation.gov availability endpoint is accessed without authentication.

Typical usage
-------------
    fac_map = build_park_facility_map(api_key)          # ~3-5 API calls
    df = fetch_all_parks_stats(api_key, fac_map)        # ~500 availability calls
    save_stats_to_db(df, db_path)
"""

from __future__ import annotations

import logging
import random
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Iterator

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── API constants ──────────────────────────────────────────────────────────────
RIDB_BASE        = "https://ridb.recreation.gov/api/v1"
AVAIL_BASE       = "https://www.recreation.gov/api/camps/availability/campground"
NPS_ORG_ID       = 128
RATE_LIMIT_DELAY = 1.0    # seconds between availability requests
MAX_RETRIES      = 5
_RIDB_PAGE       = 50

# ── The 63 designated National Parks ──────────────────────────────────────────
NATIONAL_PARKS: dict[str, str] = {
    "ACAD": "Acadia National Park",
    "NPSA": "National Park of American Samoa",
    "ARCH": "Arches National Park",
    "BADL": "Badlands National Park",
    "BIBE": "Big Bend National Park",
    "BISC": "Biscayne National Park",
    "BLCA": "Black Canyon of the Gunnison National Park",
    "BRCA": "Bryce Canyon National Park",
    "CANY": "Canyonlands National Park",
    "CARE": "Capitol Reef National Park",
    "CAVE": "Carlsbad Caverns National Park",
    "CHIS": "Channel Islands National Park",
    "CONG": "Congaree National Park",
    "CRLA": "Crater Lake National Park",
    "CUVA": "Cuyahoga Valley National Park",
    "DEVA": "Death Valley National Park",
    "DENA": "Denali National Park and Preserve",
    "DRTO": "Dry Tortugas National Park",
    "EVER": "Everglades National Park",
    "GAAR": "Gates of the Arctic National Park and Preserve",
    "JEFF": "Gateway Arch National Park",
    "GLAC": "Glacier National Park",
    "GLBA": "Glacier Bay National Park and Preserve",
    "GRCA": "Grand Canyon National Park",
    "GRTE": "Grand Teton National Park",
    "GRBA": "Great Basin National Park",
    "GRSA": "Great Sand Dunes National Park and Preserve",
    "GRSM": "Great Smoky Mountains National Park",
    "GUMO": "Guadalupe Mountains National Park",
    "HALE": "Haleakala National Park",
    "HAVO": "Hawaii Volcanoes National Park",
    "HOSP": "Hot Springs National Park",
    "INDU": "Indiana Dunes National Park",
    "ISRO": "Isle Royale National Park",
    "JOTR": "Joshua Tree National Park",
    "KATM": "Katmai National Park and Preserve",
    "KEFJ": "Kenai Fjords National Park",
    "KICA": "Kings Canyon National Park",
    "KOVA": "Kobuk Valley National Park",
    "LACL": "Lake Clark National Park and Preserve",
    "LAVO": "Lassen Volcanic National Park",
    "MACA": "Mammoth Cave National Park",
    "MEVE": "Mesa Verde National Park",
    "MORA": "Mount Rainier National Park",
    "NERI": "New River Gorge National Park and Preserve",
    "NOCA": "North Cascades National Park",
    "OLYM": "Olympic National Park",
    "PEFO": "Petrified Forest National Park",
    "PINN": "Pinnacles National Park",
    "REDW": "Redwood National and State Parks",
    "ROMO": "Rocky Mountain National Park",
    "SAGU": "Saguaro National Park",
    "SEQU": "Sequoia National Park",
    "SHEN": "Shenandoah National Park",
    "THRO": "Theodore Roosevelt National Park",
    "VIIS": "Virgin Islands National Park",
    "VOYA": "Voyageurs National Park",
    "WHSA": "White Sands National Park",
    "WICA": "Wind Cave National Park",
    "WRST": "Wrangell-St. Elias National Park and Preserve",
    "YELL": "Yellowstone National Park",
    "YOSE": "Yosemite National Park",
    "ZION": "Zion National Park",
}

# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class FacilityStats:
    """Campsite counts and availability for one Recreation.gov campground facility."""

    facility_id: str
    facility_name: str
    n_reservable: int = 0         # distinct reservable sites
    n_fcfs: int = 0               # distinct first-come-first-served sites
    available_nights: int = 0     # "Available" site×night slots in the window
    total_reservable_nights: int = 0  # n_reservable × window_days
    weekend_available: int = 0    # available slots on Sat/Sun nights
    weekend_total: int = 0        # n_reservable × n_weekend_nights_in_window
    weekday_available: int = 0
    weekday_total: int = 0


@dataclass
class ParkCampsiteStats:
    """Aggregated campsite statistics for one national park over a date window."""

    unit_code: str
    park_name: str
    facilities: list[FacilityStats] = field(default_factory=list)
    window_start: date = field(default_factory=date.today)
    window_end: date = field(default_factory=date.today)
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def n_reservable_sites(self) -> int:
        return sum(f.n_reservable for f in self.facilities)

    @property
    def n_fcfs_sites(self) -> int:
        return sum(f.n_fcfs for f in self.facilities)

    @property
    def avail_nights(self) -> int:
        return sum(f.available_nights for f in self.facilities)

    @property
    def total_nights(self) -> int:
        return sum(f.total_reservable_nights for f in self.facilities)

    @property
    def pct_available(self) -> float | None:
        if self.total_nights == 0:
            return None
        return round(100.0 * self.avail_nights / self.total_nights, 1)

    @property
    def weekend_pct(self) -> float | None:
        total = sum(f.weekend_total for f in self.facilities)
        if total == 0:
            return None
        avail = sum(f.weekend_available for f in self.facilities)
        return round(100.0 * avail / total, 1)

    @property
    def weekday_pct(self) -> float | None:
        total = sum(f.weekday_total for f in self.facilities)
        if total == 0:
            return None
        avail = sum(f.weekday_available for f in self.facilities)
        return round(100.0 * avail / total, 1)


# ── Private helpers ────────────────────────────────────────────────────────────

def _normalize_name(name: str) -> str:
    """Strip designation suffixes and normalise for fuzzy park name matching."""
    name = name.lower()
    for token in (
        "national park and preserve",
        "national park & preserve",
        "national park",
        "national monument",
        "national preserve",
        "and state parks",
        "& preserve",
        "and preserve",
    ):
        name = name.replace(token, "")
    name = "".join(c if (c.isalnum() or c.isspace()) else " " for c in name)
    return " ".join(name.split())


def _date_range(start: date, end: date) -> Iterator[date]:
    """Yield each date in [start, end) (end-exclusive)."""
    cur = start
    while cur < end:
        yield cur
        cur += timedelta(days=1)


def _months_in_window(start: date, days: int) -> list[date]:
    """Return 1 or 2 first-of-month dates that the window [start, start+days) spans."""
    end = start + timedelta(days=days - 1)
    months: list[date] = []
    cur = start.replace(day=1)
    while cur <= end.replace(day=1):
        months.append(cur)
        cur = cur.replace(month=cur.month + 1) if cur.month < 12 else cur.replace(year=cur.year + 1, month=1)
    return months


def _is_reservable(reserve_type: str) -> bool:
    """True for site-specific or lottery reservable sites (not FCFS or management)."""
    rt = reserve_type.lower()
    return "first-come" not in rt and "first come" not in rt and "management" not in rt


def _is_available(status: str) -> bool:
    return status.lower() in ("available", "open")


# ── RIDB API ───────────────────────────────────────────────────────────────────

def _ridb_get(endpoint: str, api_key: str, params: dict | None = None) -> list[dict]:
    """Fetch all records from a paginated RIDB v1 endpoint."""
    p: dict = dict(params or {})
    p["apikey"] = api_key
    p["limit"] = _RIDB_PAGE
    p.setdefault("offset", 0)
    all_data: list[dict] = []
    while True:
        try:
            r = requests.get(f"{RIDB_BASE}{endpoint}", params=p, timeout=20)
            r.raise_for_status()
            body = r.json()
        except requests.exceptions.RequestException as exc:
            logger.warning("RIDB error (%s): %s", endpoint, exc)
            break
        data = body.get("RECDATA", [])
        all_data.extend(data)
        total = int((body.get("METADATA") or {}).get("RESULTS", {}).get("TOTAL_COUNT", 0))
        if len(all_data) >= total or not data:
            break
        p["offset"] = len(all_data)
    return all_data


def build_park_facility_map(api_key: str) -> tuple[dict[str, list[str]], dict[str, str]]:
    """
    Query RIDB to map the 63 national park unit codes to Recreation.gov
    campground facility IDs.

    Makes 2–4 paginated API calls total (rec areas + facilities).

    Returns
    -------
    park_map      : unit_code → [facility_id, ...]   (only parks with campgrounds)
    facility_names: facility_id → facility_name
    """
    # 1. Fetch all NPS rec areas and build normalised-name → rec_area_id lookup
    logger.info("Fetching NPS rec areas from RIDB…")
    rec_areas_raw = _ridb_get(f"/organizations/{NPS_ORG_ID}/recareas", api_key)
    norm_to_ra: dict[str, str] = {}
    for ra in rec_areas_raw:
        ra_id   = str(ra.get("RecAreaID", ""))
        ra_name = str(ra.get("RecAreaName", ""))
        if ra_id and ra_name:
            norm = _normalize_name(ra_name)
            if norm and norm not in norm_to_ra:
                norm_to_ra[norm] = ra_id

    # 2. Match each national park to a rec area
    target_norm: dict[str, str] = {
        _normalize_name(name): code for code, name in NATIONAL_PARKS.items()
    }
    unit_to_ra: dict[str, str] = {}
    for norm_ra, ra_id in norm_to_ra.items():
        if norm_ra in target_norm:
            unit_to_ra[target_norm[norm_ra]] = ra_id
            continue
        # Substring fallback: target name ⊆ rec area name
        for norm_park, unit_code in target_norm.items():
            if norm_park and norm_park in norm_ra and unit_code not in unit_to_ra:
                unit_to_ra[unit_code] = ra_id
                break

    logger.info("Matched %d / %d parks to RIDB rec areas", len(unit_to_ra), len(NATIONAL_PARKS))

    # 3. Fetch all NPS campground facilities (activity 9 = camping)
    logger.info("Fetching NPS campground facilities from RIDB…")
    facilities_raw = _ridb_get(
        f"/organizations/{NPS_ORG_ID}/facilities",
        api_key,
        params={"activity": 9},
    )

    # 4. Index: rec_area_id → [facility_id]; facility_id → name
    ra_to_fac: dict[str, list[str]] = {}
    facility_names: dict[str, str] = {}
    for fac in facilities_raw:
        fac_id   = str(fac.get("FacilityID", ""))
        fac_name = str(fac.get("FacilityName", "")).title()
        parent   = str(fac.get("ParentRecAreaID", ""))
        fac_type = str(fac.get("FacilityTypeDescription", "")).lower()
        if not fac_id or not parent:
            continue
        if "campground" not in fac_type and "camping" not in fac_type:
            continue
        ra_to_fac.setdefault(parent, []).append(fac_id)
        if fac_name:
            facility_names[fac_id] = fac_name

    # 5. Assemble final park → facility list
    park_map: dict[str, list[str]] = {}
    for unit_code, ra_id in unit_to_ra.items():
        ids = ra_to_fac.get(ra_id, [])
        if ids:
            park_map[unit_code] = ids

    logger.info(
        "Found campground facilities for %d / %d parks", len(park_map), len(NATIONAL_PARKS)
    )
    return park_map, facility_names


# ── Availability API ───────────────────────────────────────────────────────────

def fetch_month_availability(
    facility_id: str,
    month_start: date,
) -> dict:
    """
    Fetch campsite availability from Recreation.gov for one campground, one month.

    The endpoint returns the entire calendar month beginning at month_start
    (must be the first of the month).

    Returns the raw 'campsites' dict keyed by site ID, or {} on failure.
    Retries up to MAX_RETRIES on 429 rate-limit responses.
    """
    start_str = month_start.strftime("%Y-%m-01T00:00:00.000Z")
    url = f"{AVAIL_BASE}/{facility_id}/month"
    headers = {
        "User-Agent": "NPS-Dashboard/1.0 (github.com/jhcwalsh/nationalparks)",
        "Accept": "application/json",
    }
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(
                url,
                params={"start_date": start_str},
                headers=headers,
                timeout=30,
            )
            if r.status_code == 429:
                # Honour Retry-After if present, otherwise exponential backoff
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else min(60 * 2 ** attempt, 300)
                jitter = random.uniform(0, wait * 0.25)
                logger.warning(
                    "Rate-limited on facility %s; retrying in %.0fs", facility_id, wait + jitter
                )
                time.sleep(wait + jitter)
                continue
            r.raise_for_status()
            return r.json().get("campsites", {})
        except requests.exceptions.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = min(10 * 2 ** attempt, 120)
                time.sleep(wait + random.uniform(0, wait * 0.25))
            else:
                logger.warning("Availability fetch failed for facility %s: %s", facility_id, exc)
    return {}


def aggregate_facility_availability(
    campsites_raw: dict,
    window_start: date,
    window_end: date,
    facility_id: str = "",
    facility_name: str = "",
) -> FacilityStats:
    """
    Aggregate raw campsite availability data into a FacilityStats.

    Args
    ----
    campsites_raw : dict of site_id → site data from fetch_month_availability
    window_start  : first date of the analysis window (inclusive)
    window_end    : last date of the analysis window (exclusive)
    """
    stats = FacilityStats(facility_id=facility_id, facility_name=facility_name)

    window_dates = list(_date_range(window_start, window_end))
    # Map ISO date string (as returned by Rec.gov) → weekday flag
    window_date_map: dict[str, bool] = {}
    for d in window_dates:
        key = d.strftime("%Y-%m-%dT00:00:00Z")
        window_date_map[key] = d.weekday() >= 5  # True = weekend (Sat/Sun)

    n_total   = len(window_dates)
    n_weekend = sum(1 for v in window_date_map.values() if v)
    n_weekday = n_total - n_weekend

    for _site_id, site in campsites_raw.items():
        reserve_type  = str(site.get("campsite_reserve_type", ""))
        availabilities: dict[str, str] = site.get("availabilities", {})

        if _is_reservable(reserve_type):
            stats.n_reservable          += 1
            stats.total_reservable_nights += n_total
            stats.weekend_total          += n_weekend
            stats.weekday_total          += n_weekday

            for date_str, status in availabilities.items():
                is_wknd = window_date_map.get(date_str)
                if is_wknd is None:
                    continue  # outside window
                if _is_available(status):
                    stats.available_nights += 1
                    if is_wknd:
                        stats.weekend_available += 1
                    else:
                        stats.weekday_available += 1
        else:
            stats.n_fcfs += 1

    return stats


# ── Park-level aggregation ─────────────────────────────────────────────────────

def fetch_park_campsite_stats(
    unit_code: str,
    facility_ids: list[str],
    facility_names: dict[str, str],
    window_start: date | None = None,
    window_days: int = 30,
) -> ParkCampsiteStats:
    """
    Fetch and aggregate campsite availability for one national park.

    Iterates over all Recreation.gov campground facilities for the park,
    fetching 1-2 months of availability data per facility (covering the window),
    then aggregates into FacilityStats and ParkCampsiteStats.

    Applies RATE_LIMIT_DELAY between each facility's availability calls.
    """
    if window_start is None:
        window_start = date.today()
    window_end = window_start + timedelta(days=window_days)

    park_stats = ParkCampsiteStats(
        unit_code=unit_code,
        park_name=NATIONAL_PARKS.get(unit_code, unit_code),
        window_start=window_start,
        window_end=window_end,
        fetched_at=datetime.utcnow(),
    )

    months = _months_in_window(window_start, window_days)

    for fac_id in facility_ids:
        # Fetch and merge availability across all months the window spans
        combined: dict[str, dict] = {}
        for month_start in months:
            raw = fetch_month_availability(fac_id, month_start)
            for site_id, site_data in raw.items():
                if site_id not in combined:
                    combined[site_id] = {**site_data, "availabilities": {}}
                combined[site_id]["availabilities"].update(
                    site_data.get("availabilities", {})
                )
            time.sleep(RATE_LIMIT_DELAY + random.uniform(0, RATE_LIMIT_DELAY * 0.5))

        fac_name = facility_names.get(fac_id, f"Campground {fac_id}")
        fac_stats = aggregate_facility_availability(
            combined, window_start, window_end,
            facility_id=fac_id, facility_name=fac_name,
        )
        park_stats.facilities.append(fac_stats)

    return park_stats


def fetch_all_parks_stats(
    park_facility_map: dict[str, list[str]],
    facility_names: dict[str, str],
    window_start: date | None = None,
    window_days: int = 30,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> pd.DataFrame:
    """
    Fetch campsite availability for all 63 national parks.

    Parks not present in park_facility_map get a row with has_campgrounds=False
    and zero counts — these are parks with no Recreation.gov campgrounds.

    Args
    ----
    park_facility_map : unit_code → [facility_id, ...]
    facility_names    : facility_id → facility_name
    window_start      : first date of window (default: today)
    window_days       : length of window (default: 30)
    progress_callback : optional fn(current_index, total, park_name) for UI feedback

    Returns
    -------
    DataFrame with columns:
        unit_code, park_name, n_reservable_sites, n_fcfs_sites,
        avail_nights, total_nights, pct_available, weekend_pct,
        weekday_pct, has_campgrounds, n_facilities, fetched_at
    """
    if window_start is None:
        window_start = date.today()

    rows: list[dict] = []
    all_codes = list(NATIONAL_PARKS.keys())

    for idx, unit_code in enumerate(all_codes):
        park_name = NATIONAL_PARKS[unit_code]
        if progress_callback:
            progress_callback(idx, len(all_codes), park_name)

        facility_ids = park_facility_map.get(unit_code, [])
        if facility_ids:
            ps = fetch_park_campsite_stats(
                unit_code, facility_ids, facility_names,
                window_start=window_start, window_days=window_days,
            )
            rows.append({
                "unit_code":         unit_code,
                "park_name":         park_name,
                "n_reservable_sites": ps.n_reservable_sites,
                "n_fcfs_sites":       ps.n_fcfs_sites,
                "avail_nights":       ps.avail_nights,
                "total_nights":       ps.total_nights,
                "pct_available":      ps.pct_available,
                "weekend_pct":        ps.weekend_pct,
                "weekday_pct":        ps.weekday_pct,
                "has_campgrounds":    True,
                "n_facilities":       len(ps.facilities),
                "fetched_at":         ps.fetched_at.isoformat(),
                "window_start":       window_start.isoformat(),
                "window_end":         (window_start + timedelta(days=window_days)).isoformat(),
            })
        else:
            rows.append({
                "unit_code":         unit_code,
                "park_name":         park_name,
                "n_reservable_sites": 0,
                "n_fcfs_sites":       0,
                "avail_nights":       0,
                "total_nights":       0,
                "pct_available":      None,
                "weekend_pct":        None,
                "weekday_pct":        None,
                "has_campgrounds":    False,
                "n_facilities":       0,
                "fetched_at":         datetime.utcnow().isoformat(),
                "window_start":       window_start.isoformat(),
                "window_end":         (window_start + timedelta(days=window_days)).isoformat(),
            })

    if progress_callback:
        progress_callback(len(all_codes), len(all_codes), "Complete")

    return pd.DataFrame(rows)


# ── SQLite cache ───────────────────────────────────────────────────────────────

_CAMPSITE_DDL = """
CREATE TABLE IF NOT EXISTS campsite_snapshots (
    unit_code     TEXT NOT NULL,
    fetched_at    TEXT NOT NULL,
    window_start  TEXT NOT NULL,
    window_end    TEXT NOT NULL,
    n_reservable  INTEGER,
    n_fcfs        INTEGER,
    avail_nights  INTEGER,
    total_nights  INTEGER,
    pct_available REAL,
    weekend_pct   REAL,
    weekday_pct   REAL,
    n_facilities  INTEGER DEFAULT 0,
    PRIMARY KEY (unit_code, window_start)
);
CREATE TABLE IF NOT EXISTS campsite_facilities (
    facility_id   TEXT PRIMARY KEY,
    unit_code     TEXT NOT NULL,
    facility_name TEXT,
    fetched_at    TEXT NOT NULL
);
"""


def init_campsite_tables(db_path: Path | str) -> None:
    """Create campsite cache tables in the SQLite database if they don't exist."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_CAMPSITE_DDL)
        conn.commit()
    finally:
        conn.close()


def get_cached_stats(
    db_path: Path | str,
    window_start_str: str | None = None,
    max_age_seconds: int = 3600,
) -> pd.DataFrame | None:
    """
    Return cached campsite stats if a complete run exists that is:
      - Younger than max_age_seconds, AND
      - Matches window_start_str (if provided)

    Returns None when no valid cache is available.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(
                "SELECT * FROM campsite_snapshots ORDER BY unit_code", conn
            )
        finally:
            conn.close()
    except Exception:
        return None

    if df.empty:
        return None

    # Age check
    try:
        most_recent = pd.to_datetime(df["fetched_at"]).max()
        age = (datetime.utcnow() - most_recent.to_pydatetime().replace(tzinfo=None)).total_seconds()
        if age > max_age_seconds:
            return None
    except Exception:
        return None

    # Window match check
    if window_start_str and "window_start" in df.columns:
        if df["window_start"].iloc[0] != window_start_str:
            return None

    # Rename DB column names → DataFrame column names expected by the UI
    df = df.rename(columns={"n_reservable": "n_reservable_sites", "n_fcfs": "n_fcfs_sites"})
    if "park_name" not in df.columns:
        df["park_name"] = df["unit_code"].map(NATIONAL_PARKS)
    if "has_campgrounds" not in df.columns:
        df["has_campgrounds"] = df["n_reservable_sites"] > 0

    return df


def save_stats_to_db(df: pd.DataFrame, db_path: Path | str) -> None:
    """Upsert campsite snapshot rows into SQLite for later re-use."""
    if df.empty:
        return
    db_path = Path(db_path)
    init_campsite_tables(db_path)
    conn = sqlite3.connect(db_path)
    try:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT OR REPLACE INTO campsite_snapshots
                    (unit_code, fetched_at, window_start, window_end,
                     n_reservable, n_fcfs, avail_nights, total_nights,
                     pct_available, weekend_pct, weekday_pct, n_facilities)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("unit_code"),
                    row.get("fetched_at"),
                    str(row.get("window_start", "")),
                    str(row.get("window_end", "")),
                    int(row.get("n_reservable_sites") or 0),
                    int(row.get("n_fcfs_sites") or 0),
                    int(row.get("avail_nights") or 0),
                    int(row.get("total_nights") or 0),
                    row.get("pct_available"),
                    row.get("weekend_pct"),
                    row.get("weekday_pct"),
                    int(row.get("n_facilities") or 0),
                ),
            )
        conn.commit()
    finally:
        conn.close()
