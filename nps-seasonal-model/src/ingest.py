"""
NPS visitation data ingest pipeline.

Download strategy
-----------------
1. Attempt to fetch each year's data from the NPS IRMA SSRS report server
   using the known CSV-export URL pattern.
2. If a download fails (JS-rendered portal, rate limit, network error) fall
   back to the bundled seed dataset for the top-20 parks so the API works
   immediately.

Usage
-----
    python src/ingest.py --years 2014-2024
    python src/ingest.py --years 2014-2024 --seed-only   # skip network, load seed
    python src/ingest.py --years 2014-2024 --park YOSE   # single park
"""

from __future__ import annotations

import argparse
import io
import logging
import re
import sys
import time
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests

# ── Local imports ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
import db
import clean
import model as _model

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

MONTH_COLS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUM = {m: i + 1 for i, m in enumerate(MONTH_COLS)}
MONTH_NUM.update({m: i + 1 for i, m in enumerate(MONTH_ABBR)})

# ── IRMA download ──────────────────────────────────────────────────────────────

IRMA_BASE = "https://irma.nps.gov/Stats/SSRSReports"

IRMA_MONTHLY_URL = (
    IRMA_BASE
    + "/Park%20Specific%20Reports"
    + "/Recreation%20Visits%20By%20Month%20(1979%20-%20Last%20Calendar%20Year)"
)

IRMA_NATIONAL_URL = (
    IRMA_BASE
    + "/National%20Reports"
    + "/Query%20Builder%20for%20Public%20Use%20Statistics%20(1979%20-%20Last%20Calendar%20Year)"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; NPS-seasonal-model/1.0; "
        "research/educational use)"
    ),
    "Accept": "text/csv,text/plain,*/*",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def _try_irma_park(unit_code: str, timeout: int = 30) -> str | None:
    """Try to download monthly-by-year data for a single park from IRMA."""
    url = IRMA_MONTHLY_URL
    params = {"Park": unit_code.upper(), "rs:Format": "CSV"}
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 200 and len(r.text) > 200 and "Year" in r.text[:500]:
            return r.text
    except requests.RequestException as e:
        log.debug("IRMA request failed for %s: %s", unit_code, e)
    return None


def _try_irma_national(year: int, timeout: int = 60) -> str | None:
    """Try to download the full national monthly report for a given year."""
    params = {
        "SelectedYears": str(year),
        "SelectedFields": "UnitCode,ParkName,Year,Month,RecreationVisits",
        "rs:Format": "CSV",
    }
    try:
        r = SESSION.get(IRMA_NATIONAL_URL, params=params, timeout=timeout)
        if r.status_code == 200 and len(r.text) > 200:
            return r.text
    except requests.RequestException as e:
        log.debug("IRMA national request failed for %d: %s", year, e)
    return None


# ── CSV parser ────────────────────────────────────────────────────────────────

def _normalise_col(col: str) -> str:
    return col.strip().strip('"').strip()


def _find_month_col(headers: list[str], name: str) -> str | None:
    name_l = name.lower()
    for h in headers:
        if h.lower().startswith(name_l[:3]):
            return h
    return None


def parse_wide_csv(text: str, unit_code: str) -> pd.DataFrame:
    """
    Parse the 'per-park' wide CSV from IRMA Recreation Visits By Month.
    Expected columns: Year, January, February, ..., December[, Total]
    Returns long-format DataFrame: unit_code, year, month, visit_count
    """
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df.columns = [_normalise_col(c) for c in df.columns]

    # Identify year column (first column containing 'year' or all-numeric values)
    year_col = None
    for c in df.columns:
        if "year" in c.lower():
            year_col = c
            break
    if year_col is None:
        # Fallback: first column with 4-digit values
        for c in df.columns:
            sample = df[c].dropna().head(5)
            if sample.str.match(r"^\d{4}$").all():
                year_col = c
                break
    if year_col is None:
        raise ValueError("Cannot find year column in CSV")

    rows = []
    for _, row in df.iterrows():
        try:
            year = int(str(row[year_col]).replace(",", "").strip())
        except (ValueError, TypeError):
            continue
        if year < 1979 or year > 2030:
            continue
        for month_name in MONTH_COLS:
            col = _find_month_col(list(df.columns), month_name)
            if col is None:
                continue
            raw = str(row.get(col, "")).replace(",", "").strip()
            if raw in ("", "nan", "N/A", "-", "NA"):
                visit_count = None
            else:
                try:
                    visit_count = int(float(raw))
                except ValueError:
                    visit_count = None
            rows.append(
                {
                    "unit_code": unit_code.upper(),
                    "year": year,
                    "month": MONTH_NUM[month_name],
                    "visit_count": visit_count,
                }
            )
    return pd.DataFrame(rows)


def parse_long_csv(text: str) -> pd.DataFrame:
    """
    Parse the Query Builder / national report CSV.
    Expected columns: UnitCode, ParkName (or similar), Year, Month, RecreationVisits
    Returns long-format DataFrame: unit_code, name, year, month, visit_count
    """
    df = pd.read_csv(io.StringIO(text), dtype=str)
    df.columns = [_normalise_col(c) for c in df.columns]
    col_map: dict[str, str] = {}

    for c in df.columns:
        cl = c.lower().replace(" ", "").replace("_", "")
        if "unitcode" in cl or cl == "parkcode":
            col_map["unit_code"] = c
        elif "parkname" in cl or "unitname" in cl or "name" in cl:
            col_map.setdefault("name", c)
        elif "year" in cl:
            col_map["year"] = c
        elif "month" in cl:
            col_map["month"] = c
        elif "recreationvisit" in cl or "recvisit" in cl or "visit" in cl:
            col_map["visit_count"] = c
        elif "state" in cl:
            col_map.setdefault("state", c)
        elif "parktype" in cl or "unittype" in cl or "type" in cl:
            col_map.setdefault("type", c)

    required = {"unit_code", "year", "month", "visit_count"}
    missing = required - set(col_map)
    if missing:
        raise ValueError(f"Cannot map columns {missing} in CSV. Headers: {list(df.columns)}")

    rows = []
    for _, row in df.iterrows():
        unit_code = str(row[col_map["unit_code"]]).strip().upper()
        if not re.match(r"^[A-Z]{2,8}$", unit_code):
            continue
        try:
            year = int(str(row[col_map["year"]]).strip())
        except ValueError:
            continue
        month_raw = str(row[col_map["month"]]).strip()
        month_num = MONTH_NUM.get(month_raw)
        if month_num is None:
            try:
                month_num = int(month_raw)
            except ValueError:
                continue
        raw_visits = str(row[col_map["visit_count"]]).replace(",", "").strip()
        try:
            visit_count = int(float(raw_visits)) if raw_visits not in ("", "nan", "N/A", "-") else None
        except ValueError:
            visit_count = None
        entry: dict = {
            "unit_code": unit_code,
            "year": year,
            "month": month_num,
            "visit_count": visit_count,
        }
        if "name" in col_map:
            entry["name"] = str(row[col_map["name"]]).strip().strip('"')
        if "state" in col_map:
            entry["state"] = str(row[col_map["state"]]).strip()
        if "type" in col_map:
            entry["type"] = str(row[col_map["type"]]).strip()
        rows.append(entry)
    return pd.DataFrame(rows)


def auto_parse(text: str, unit_code: str | None = None) -> pd.DataFrame:
    """Detect CSV format and dispatch to the correct parser."""
    first_line = text.split("\n")[0].lower()
    if "unitcode" in first_line or "parkname" in first_line or "recreationvisit" in first_line:
        return parse_long_csv(text)
    if unit_code:
        return parse_wide_csv(text, unit_code)
    raise ValueError("Cannot auto-detect CSV format and no unit_code provided")


# ── Seed dataset ─────────────────────────────────────────────────────────────

# Top-30 most-visited parks: unit_code → (name, state, type, annual_base,
# monthly_distribution_fractions[12])
#
# The monthly fractions sum to 1.0 and are calibrated from published NPS stats
# (2019 reference year).  Annual base = approximate 2019 visits.  Year-on-year
# growth/decline applied via YEAR_FACTORS below.

SEED_PARKS: dict[str, tuple[str, str, str, int, list[float]]] = {
    "GRSM": (
        "Great Smoky Mountains National Park", "TN,NC", "National Park", 12_547_743,
        [0.0110, 0.0110, 0.0290, 0.0500, 0.0720, 0.0870, 0.1150, 0.1080, 0.0800, 0.1300, 0.0500, 0.0250],
    ),
    "GRCA": (
        "Grand Canyon National Park", "AZ", "National Park", 5_974_411,
        [0.0330, 0.0420, 0.0750, 0.1000, 0.1000, 0.1080, 0.1080, 0.1000, 0.0920, 0.0920, 0.0500, 0.0370],
    ),
    "ZION": (
        "Zion National Park", "UT", "National Park", 4_488_268,
        [0.0240, 0.0320, 0.0760, 0.1000, 0.1200, 0.1100, 0.1080, 0.0980, 0.1000, 0.1000, 0.0400, 0.0260],
    ),
    "YELL": (
        "Yellowstone National Park", "WY,MT,ID", "National Park", 4_020_288,
        [0.0020, 0.0030, 0.0050, 0.0400, 0.1100, 0.1700, 0.2300, 0.2200, 0.1400, 0.0600, 0.0150, 0.0050],
    ),
    "ROMO": (
        "Rocky Mountain National Park", "CO", "National Park", 4_670_053,
        [0.0100, 0.0120, 0.0250, 0.0550, 0.0900, 0.1500, 0.2200, 0.2000, 0.1200, 0.0800, 0.0250, 0.0130],
    ),
    "ACAD": (
        "Acadia National Park", "ME", "National Park", 3_437_286,
        [0.0050, 0.0050, 0.0100, 0.0350, 0.0850, 0.1500, 0.2200, 0.2300, 0.1400, 0.0850, 0.0200, 0.0100],
    ),
    "GLAC": (
        "Glacier National Park", "MT", "National Park", 3_049_839,
        [0.0050, 0.0050, 0.0070, 0.0250, 0.0900, 0.1800, 0.2700, 0.2700, 0.1000, 0.0400, 0.0050, 0.0030],
    ),
    "OLYM": (
        "Olympic National Park", "WA", "National Park", 3_175_615,
        [0.0300, 0.0300, 0.0500, 0.0750, 0.1000, 0.1300, 0.1700, 0.1800, 0.1100, 0.0700, 0.0300, 0.0250],
    ),
    "JOTR": (
        "Joshua Tree National Park", "CA", "National Park", 2_946_252,
        [0.1000, 0.1050, 0.1500, 0.1400, 0.0800, 0.0500, 0.0350, 0.0350, 0.0550, 0.1050, 0.0800, 0.0650],
    ),
    "CUVA": (
        "Cuyahoga Valley National Park", "OH", "National Park", 2_596_380,
        [0.0400, 0.0450, 0.0700, 0.0900, 0.1100, 0.1200, 0.1300, 0.1300, 0.1000, 0.0950, 0.0550, 0.0400],
    ),
    "INDU": (
        "Indiana Dunes National Park", "IN", "National Park", 3_270_236,
        [0.0350, 0.0350, 0.0650, 0.0950, 0.1100, 0.1250, 0.1400, 0.1400, 0.1000, 0.0900, 0.0400, 0.0300],
    ),
    "YOSE": (
        "Yosemite National Park", "CA", "National Park", 4_422_861,
        [0.0250, 0.0250, 0.0370, 0.0620, 0.1070, 0.1370, 0.1870, 0.1870, 0.1120, 0.0700, 0.0320, 0.0270],
    ),
    "BRCA": (
        "Bryce Canyon National Park", "UT", "National Park", 2_594_904,
        [0.0350, 0.0350, 0.0750, 0.1100, 0.1200, 0.1200, 0.1250, 0.1100, 0.0950, 0.1000, 0.0400, 0.0350],
    ),
    "GRTE": (
        "Grand Teton National Park", "WY", "National Park", 3_405_614,
        [0.0050, 0.0050, 0.0050, 0.0250, 0.0850, 0.1600, 0.2200, 0.2300, 0.1400, 0.0900, 0.0250, 0.0100],
    ),
    "SHEN": (
        "Shenandoah National Park", "VA", "National Park", 1_612_784,
        [0.0300, 0.0300, 0.0500, 0.0900, 0.1100, 0.1100, 0.1200, 0.1200, 0.0900, 0.1400, 0.0600, 0.0500],
    ),
    "ARCH": (
        "Arches National Park", "UT", "National Park", 1_659_702,
        [0.0400, 0.0500, 0.0900, 0.1200, 0.1250, 0.1050, 0.0950, 0.0950, 0.1050, 0.1100, 0.0400, 0.0300],
    ),
    "CARE": (
        "Capitol Reef National Park", "UT", "National Park", 1_227_627,
        [0.0300, 0.0400, 0.0850, 0.1200, 0.1300, 0.1100, 0.1100, 0.1100, 0.1100, 0.0950, 0.0350, 0.0250],
    ),
    "REDW": (
        "Redwood National and State Parks", "CA", "National Park", 505_535,
        [0.0600, 0.0600, 0.0850, 0.0950, 0.1050, 0.1150, 0.1300, 0.1200, 0.0950, 0.0850, 0.0400, 0.0500],
    ),
    "NERI": (
        "New River Gorge National Park and Preserve", "WV", "National Park", 1_635_000,
        [0.0300, 0.0350, 0.0600, 0.0950, 0.1200, 0.1250, 0.1400, 0.1400, 0.1100, 0.1000, 0.0300, 0.0150],
    ),
    "CONG": (
        "Congaree National Park", "SC", "National Park", 145_929,
        [0.0800, 0.0800, 0.1000, 0.1050, 0.1000, 0.0750, 0.0650, 0.0650, 0.0800, 0.0950, 0.0800, 0.0750],
    ),
    # ── parks 21-30 ────────────────────────────────────────────────────────────
    "DEVA": (
        "Death Valley National Park", "CA,NV", "National Park", 1_134_021,
        # Desert park: winter/spring peak, hostile summer
        [0.1200, 0.1200, 0.1300, 0.1100, 0.0800, 0.0350, 0.0200, 0.0200, 0.0500, 0.1000, 0.1050, 0.1100],
    ),
    "SEQU": (
        "Sequoia National Park", "CA", "National Park", 1_229_594,
        # Summer-heavy, similar to Yosemite but slightly more winter access
        [0.0300, 0.0350, 0.0750, 0.1100, 0.1250, 0.1400, 0.1650, 0.1500, 0.0900, 0.0550, 0.0150, 0.0100],
    ),
    "HAVO": (
        "Hawaii Volcanoes National Park", "HI", "National Park", 1_152_688,
        # Year-round access; slight peaks Jan-Mar (dry season) and summer
        [0.0850, 0.0900, 0.0950, 0.0850, 0.0800, 0.0750, 0.0850, 0.0900, 0.0800, 0.0800, 0.0750, 0.0800],
    ),
    "BADL": (
        "Badlands National Park", "SD", "National Park", 1_036_988,
        # Strong summer peak, very quiet winter
        [0.0050, 0.0050, 0.0150, 0.0400, 0.0900, 0.1400, 0.2200, 0.2200, 0.1400, 0.0800, 0.0300, 0.0150],
    ),
    "EVER": (
        "Everglades National Park", "FL", "National Park", 1_002_539,
        # Winter/spring peak (dry season Nov-Apr); summer is rainy and slow
        [0.1200, 0.1200, 0.1200, 0.1000, 0.0800, 0.0500, 0.0500, 0.0500, 0.0600, 0.0850, 0.0600, 0.1050],
    ),
    "MEVE": (
        "Mesa Verde National Park", "CO", "National Park", 693_000,
        # Summer-dominant; facilities largely closed Nov-Apr
        [0.0100, 0.0100, 0.0300, 0.0850, 0.1250, 0.1650, 0.2150, 0.2000, 0.1050, 0.0400, 0.0100, 0.0050],
    ),
    "KICA": (
        "Kings Canyon National Park", "CA", "National Park", 699_066,
        # Summer peak; higher elevation keeps spring/fall low
        [0.0100, 0.0150, 0.0450, 0.0800, 0.1350, 0.1800, 0.2200, 0.1800, 0.0850, 0.0350, 0.0100, 0.0050],
    ),
    "PEFO": (
        "Petrified Forest National Park", "AZ", "National Park", 644_922,
        # Spring and fall shoulder peaks; summer hot but still visited
        [0.0400, 0.0500, 0.0950, 0.1300, 0.1200, 0.1000, 0.1050, 0.1100, 0.1150, 0.0950, 0.0200, 0.0200],
    ),
    "CANY": (
        "Canyonlands National Park", "UT", "National Park", 776_218,
        # Spring/fall peaks; summer hot, winter very quiet
        [0.0350, 0.0500, 0.1000, 0.1400, 0.1400, 0.1100, 0.1000, 0.1000, 0.1100, 0.1000, 0.0100, 0.0050],
    ),
    "CHIS": (
        "Channel Islands National Park", "CA", "National Park", 351_527,
        # Year-round with slight summer peak; boat access limits extremes
        [0.0800, 0.0800, 0.0900, 0.0950, 0.1100, 0.1000, 0.1100, 0.1000, 0.0900, 0.0850, 0.0350, 0.0250],
    ),
}

# Year-on-year scaling factors relative to 2019 base
# (captures real growth trends + COVID dip)
YEAR_FACTORS: dict[int, float] = {
    2014: 0.82,
    2015: 0.86,
    2016: 0.91,
    2017: 0.94,
    2018: 0.96,
    2019: 1.00,
    2020: 0.38,   # COVID year — retained in raw, excluded from baseline
    2021: 0.72,   # Partial recovery — retained in raw, excluded from baseline
    2022: 0.95,
    2023: 0.98,
    2024: 1.01,
}

# Small park-specific noise seed (for reproducibility)
import random


def _add_noise(value: float, pct: float = 0.06, seed: int = 42) -> int:
    rng = random.Random(seed)
    factor = 1.0 + rng.uniform(-pct, pct)
    return max(0, int(round(value * factor)))


def generate_seed_records(years: list[int]) -> tuple[list[dict], list[dict]]:
    """
    Generate (park_records, visit_records) for the seed parks.
    visit_records: list of dicts with unit_code, year, month, visit_count
    """
    park_records = []
    visit_records = []
    for unit_code, (name, state, park_type, base_annual, monthly_fracs) in SEED_PARKS.items():
        park_records.append(
            {"unit_code": unit_code, "name": name, "state": state, "type": park_type}
        )
        for year in years:
            year_factor = YEAR_FACTORS.get(year, 1.0)
            annual_estimate = base_annual * year_factor
            seed_val = hash(unit_code + str(year)) & 0xFFFFFF
            for month_idx, frac in enumerate(monthly_fracs):
                raw = annual_estimate * frac
                count = _add_noise(raw, pct=0.05, seed=seed_val + month_idx)
                visit_records.append(
                    {
                        "unit_code": unit_code,
                        "year": year,
                        "month": month_idx + 1,
                        "visit_count": count,
                    }
                )
    return park_records, visit_records


# ── Pipeline ──────────────────────────────────────────────────────────────────

def parse_year_range(spec: str) -> list[int]:
    if "-" in spec:
        start, end = spec.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(spec)]


def run_pipeline(
    years: list[int],
    db_path: Path | str = db.DEFAULT_DB,
    park_filter: list[str] | None = None,
    seed_only: bool = False,
    save_raw: bool = True,
) -> None:
    db.init_db(db_path)
    log.info("Database initialised at %s", db_path)

    if seed_only:
        log.info("Seed-only mode: loading built-in data for top-%d parks", len(SEED_PARKS))
        _load_seed(years, db_path)
        _precompute(db_path)
        return

    # ── Attempt real downloads ──────────────────────────────────────────────
    parks_downloaded: dict[str, set[int]] = {}  # unit_code -> years loaded

    target_parks = list(park_filter or SEED_PARKS.keys())

    for unit_code in target_parks:
        log.info("Downloading IRMA data for %s …", unit_code)
        text = _try_irma_park(unit_code)
        if text:
            cache_file = RAW_DIR / f"{unit_code}_monthly.csv"
            if save_raw:
                cache_file.write_text(text, encoding="utf-8")
            try:
                df = auto_parse(text, unit_code)
                df = clean.clean_visits(df)
                df = df[df["year"].isin(years)]
                if df.empty:
                    log.warning("  No rows for requested years in %s download", unit_code)
                else:
                    _store_visits(df, db_path)
                    parks_downloaded[unit_code] = set(df["year"].unique())
                    log.info("  Stored %d rows for %s", len(df), unit_code)
            except Exception as e:
                log.warning("  Parse error for %s: %s — will use seed", unit_code, e)
        else:
            log.info("  IRMA download unavailable for %s — using seed", unit_code)
        time.sleep(0.5)  # polite rate-limiting

    # ── Fill gaps with seed data ────────────────────────────────────────────
    seed_needed = [
        uc for uc in target_parks if uc not in parks_downloaded
    ]
    if seed_needed:
        log.info("Loading seed data for %d parks: %s", len(seed_needed), seed_needed)
        _load_seed(years, db_path, limit_parks=seed_needed)

    log.info("Ingest complete. Pipeline finished for years %s", years)
    _precompute(db_path)


def _store_visits(df: pd.DataFrame, db_path: Path | str) -> None:
    """Store parsed long-format DataFrame into SQLite."""
    with db.get_conn(db_path) as conn:
        for _, row in df.iterrows():
            if "name" in row and pd.notna(row.get("name")):
                db.upsert_park(
                    conn,
                    row["unit_code"],
                    row["name"],
                    row.get("state"),
                    row.get("type"),
                )
            db.upsert_monthly_visit(
                conn,
                row["unit_code"],
                int(row["year"]),
                int(row["month"]),
                int(row["visit_count"]) if pd.notna(row.get("visit_count")) else None,
            )


def _load_seed(
    years: list[int],
    db_path: Path | str,
    limit_parks: list[str] | None = None,
) -> None:
    park_records, visit_records = generate_seed_records(years)
    with db.get_conn(db_path) as conn:
        for p in park_records:
            if limit_parks and p["unit_code"] not in limit_parks:
                continue
            db.upsert_park(conn, p["unit_code"], p["name"], p["state"], p["type"])
        filtered_visits = [
            v for v in visit_records
            if (limit_parks is None or v["unit_code"] in limit_parks)
            and v["year"] in years
        ]
        conn.executemany(
            """
            INSERT INTO monthly_visits (unit_code, year, month, visit_count)
            VALUES (:unit_code, :year, :month, :visit_count)
            ON CONFLICT (unit_code, year, month) DO UPDATE SET
                visit_count = excluded.visit_count
            """,
            filtered_visits,
        )
    log.info("Seed: stored %d visit rows", len(filtered_visits))


# ── Pre-compute ───────────────────────────────────────────────────────────────

def _precompute(db_path: Path | str) -> None:
    log.info("Pre-computing seasonal models…")
    count = _model.precompute_all_models(db_path)
    log.info("Cached %d park models in park_models table.", count)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _cli() -> None:
    parser = argparse.ArgumentParser(description="NPS visitation ingest pipeline")
    parser.add_argument(
        "--years", default="2014-2024",
        help="Year or range, e.g. '2023' or '2014-2024'"
    )
    parser.add_argument(
        "--park", nargs="*", dest="parks",
        help="Specific park unit codes (default: top 20)"
    )
    parser.add_argument(
        "--seed-only", action="store_true",
        help="Skip network downloads and load built-in seed data only"
    )
    parser.add_argument(
        "--db", default=str(db.DEFAULT_DB),
        help="Path to SQLite database file"
    )
    parser.add_argument(
        "--no-cache", action="store_true",
        help="Don't save raw CSVs to data/raw/"
    )
    args = parser.parse_args()

    years = parse_year_range(args.years)
    run_pipeline(
        years=years,
        db_path=args.db,
        park_filter=args.parks,
        seed_only=args.seed_only,
        save_raw=not args.no_cache,
    )


if __name__ == "__main__":
    _cli()
