"""
Mobile "National Parks Now" overview assembler.

The single public entry point is :func:`assemble_overview`, which returns
the exact JSON shape consumed by `static/app.js` for the phone UI. It
fans out to the seasonal busyness model, the pre-fetched campsite CSV,
live Open-Meteo weather & AQI, live NIFC wildfires, and the NPS
Developer API alerts endpoint. Any sub-fetch that fails degrades to
None so the endpoint always returns a partial response.
"""

from __future__ import annotations

import csv
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

import db as _db  # noqa: E402
import model as _model  # noqa: E402
from campsites import NATIONAL_PARKS  # noqa: E402
from conditions import (  # noqa: E402
    PARK_COORDS,
    aqi_label,
    bearing_to_cardinal,
    describe_weather_code,
    haversine_miles,
    load_active_fires,
    load_aqi,
    load_weather,
)

REPO_ROOT = ROOT.parent
CAMPSITE_PREVIEW_CSV = REPO_ROOT / "campsite_preview.csv"

# ── Static reference data ─────────────────────────────────────────────────────

# Parks with known NPS-managed reservation / day-use ticket systems. Value is
# the *current* human-readable status line for the current calendar year. For
# parks without an entry, the mobile UI simply omits the reservation line.
# Sourced from NPS.gov park announcements and congressional park bulletins.
RESERVATION_STATUS: dict[str, str] = {
    "YOSE": "No reservation required in 2026",
    "ROMO": "Timed-entry permit required May–Oct 2026",
    "ARCH": "Timed-entry ticket required Apr–Oct 2026",
    "GLAC": "Vehicle reservation required Jun–Sep 2026",
    "ZION": "Angels Landing permit required (lottery)",
    "HALE": "Sunrise reservation required for the summit",
    "ACAD": "Cadillac Summit Rd reservation required",
}

US_STATE_NAMES: dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "AS": "American Samoa", "VI": "U.S. Virgin Islands",
}


def _pretty_state(state_field: str | None) -> str:
    """Convert 'CA' → 'California'; 'WY,MT,ID' → 'Wyoming · Montana · Idaho'."""
    if not state_field:
        return ""
    codes = [c.strip().upper() for c in state_field.split(",") if c.strip()]
    names = [US_STATE_NAMES.get(c, c) for c in codes]
    return " · ".join(names)


# ── Busyness label / season copy ──────────────────────────────────────────────

def _busyness_label(score: float) -> str:
    if score >= 80:
        return "Very busy right now"
    if score >= 60:
        return "Busy right now"
    if score >= 40:
        return "Moderate right now"
    if score >= 20:
        return "Quiet right now"
    return "Very quiet right now"


def _season_copy(month: int, month_label: str) -> str:
    """Return e.g. 'Peak summer season · Weekend' / 'Quiet winter · Weekday'."""
    # Map month → season
    if month in (12, 1, 2):
        season = "winter"
    elif month in (3, 4, 5):
        season = "spring"
    elif month in (6, 7, 8):
        season = "summer"
    else:
        season = "autumn"

    # Model label: peak / shoulder / quiet
    adj = {"peak": "Peak", "shoulder": "Shoulder", "quiet": "Quiet"}.get(
        month_label, "Shoulder"
    )

    today = date.today()
    dow = "Weekend" if today.weekday() >= 5 else "Weekday"
    return f"{adj} {season} season · {dow}"


# ── Camping card ──────────────────────────────────────────────────────────────

def _camping_label(pct: float) -> str:
    if pct >= 50:
        return "Wide open"
    if pct >= 25:
        return "Good availability"
    if pct >= 10:
        return "Filling up"
    if pct >= 5:
        return "Tight"
    return "Nearly full"


def load_campsite_pct(unit_code: str) -> dict[str, Any] | None:
    """Read the pre-fetched campsite_preview.csv and return the row for this park."""
    if not CAMPSITE_PREVIEW_CSV.exists():
        return None
    try:
        with CAMPSITE_PREVIEW_CSV.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row.get("unit_code", "").upper() != unit_code.upper():
                    continue
                has_cg = str(row.get("has_campgrounds", "")).strip().lower() == "true"
                if not has_cg:
                    return {
                        "pct_open": None,
                        "label": "No reservable campgrounds",
                    }
                try:
                    pct = float(row.get("pct_available") or 0)
                except ValueError:
                    return None
                return {
                    "pct_open": round(pct),
                    "label": _camping_label(pct),
                }
    except Exception:
        return None
    return None


def load_campsite_detail(unit_code: str) -> dict[str, Any] | None:
    """
    Full camping stats for the Camping tab. Reads campsite_preview.csv
    and returns campground count, reservable sites, FCFS sites,
    availability %, and a recreation.gov search link.
    """
    code = unit_code.upper()
    if code not in NATIONAL_PARKS:
        return None
    park_name = NATIONAL_PARKS[code]

    if not CAMPSITE_PREVIEW_CSV.exists():
        return {
            "has_campgrounds": False,
            "park_name": park_name,
            "rec_gov_url": _rec_gov_url(park_name),
            "stats": None,
            "window": None,
        }

    try:
        with CAMPSITE_PREVIEW_CSV.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row.get("unit_code", "").upper() != code:
                    continue
                has_cg = str(row.get("has_campgrounds", "")).strip().lower() == "true"

                if not has_cg:
                    return {
                        "has_campgrounds": False,
                        "park_name": park_name,
                        "rec_gov_url": _rec_gov_url(park_name),
                        "stats": None,
                        "window": None,
                    }

                def _int(key: str) -> int:
                    try:
                        return int(float(row.get(key) or 0))
                    except (TypeError, ValueError):
                        return 0

                def _float(key: str) -> float:
                    try:
                        return round(float(row.get(key) or 0), 1)
                    except (TypeError, ValueError):
                        return 0.0

                pct = _float("pct_available")
                return {
                    "has_campgrounds": True,
                    "park_name": park_name,
                    "rec_gov_url": _rec_gov_url(park_name),
                    "stats": {
                        "n_campgrounds": _int("n_facilities"),
                        "n_reservable_sites": _int("n_reservable_sites"),
                        "n_fcfs_sites": _int("n_fcfs_sites"),
                        "pct_available": pct,
                        "availability_label": _camping_label(pct),
                        "weekend_pct": _float("weekend_pct"),
                        "weekday_pct": _float("weekday_pct"),
                    },
                    "window": {
                        "start": row.get("window_start", ""),
                        "end": row.get("window_end", ""),
                        "fetched_at": row.get("fetched_at", ""),
                    },
                }
    except Exception:
        pass

    return {
        "has_campgrounds": False,
        "park_name": park_name,
        "rec_gov_url": _rec_gov_url(park_name),
        "stats": None,
        "window": None,
    }


def _rec_gov_url(park_name: str) -> str:
    """Build a recreation.gov search URL for a park's campgrounds."""
    from urllib.parse import quote_plus
    return f"https://www.recreation.gov/search?q={quote_plus(park_name)}&entity_type=campground"


# ── NPS alerts ────────────────────────────────────────────────────────────────

def load_nps_alerts(park_code: str, api_key: str | None) -> list[dict[str, Any]]:
    """
    Fetch active alerts for a park from the NPS Developer API. Returns an
    empty list on any error (missing key, network failure, unexpected
    payload). Each alert is a dict with the raw NPS fields.
    """
    if not api_key:
        return []
    try:
        r = requests.get(
            "https://developer.nps.gov/api/v1/alerts",
            params={"parkCode": park_code.lower(), "api_key": api_key, "limit": 50},
            timeout=15,
        )
        r.raise_for_status()
        return list(r.json().get("data", []))
    except Exception:
        return []


_SMOKE_KEYWORDS = ("smoke", "air quality", "haze", "wildfire smoke")
_FIRE_KEYWORDS = ("fire", "wildfire", "burn")


def _classify_alert_tone(category: str, text: str) -> str:
    text_l = text.lower()
    cat_l = (category or "").lower()
    if any(k in text_l for k in _SMOKE_KEYWORDS):
        return "smoke"
    if any(k in text_l for k in _FIRE_KEYWORDS):
        return "fire"
    if "closure" in cat_l or "closed" in text_l:
        return "closure"
    if "danger" in cat_l or "warning" in cat_l:
        return "warning"
    return "info"


# ── Fire summary line ─────────────────────────────────────────────────────────

def _summarise_fire(park_lat: float, park_lon: float, fire: dict[str, Any]) -> str | None:
    name = (fire.get("IncidentName") or "").strip()
    if not name:
        return None
    flat = fire.get("_lat")
    flon = fire.get("_lon")
    if flat is None or flon is None:
        return f"{name} fire nearby — monitor conditions"
    try:
        dist = round(haversine_miles(park_lat, park_lon, float(flat), float(flon)))
    except (TypeError, ValueError):
        return f"{name} fire nearby — monitor conditions"
    direction = bearing_to_cardinal(park_lat, park_lon, float(flat), float(flon))
    contained = fire.get("PercentContained")
    if contained is None or contained == "":
        tail = "monitor smoke"
    else:
        try:
            c = int(float(contained))
            tail = f"{c}% contained"
        except (TypeError, ValueError):
            tail = "monitor smoke"
    return f"{name} fire {dist} mi {direction} — {tail}"


# ── Main assembler ────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("_safe: %s(%s) raised %s: %s", fn.__name__, args, type(exc).__name__, exc)
        return None


def assemble_overview(
    unit_code: str,
    db_path: Path | None = None,
    nps_api_key: str | None = None,
) -> dict[str, Any] | None:
    """
    Build the full mobile Overview payload for a national park.

    Returns None if the code is not one of the 63 National Parks. Every
    other field degrades to None / [] independently on failure, so the
    caller can always return HTTP 200 with a partial payload.
    """
    code = unit_code.upper()
    if code not in NATIONAL_PARKS:
        return None

    if db_path is None:
        db_path = _db.DEFAULT_DB
    nps_api_key = nps_api_key or os.getenv("NPS_API_KEY", "")

    park_name = NATIONAL_PARKS[code]
    coords = PARK_COORDS.get(code)

    # ── Park header ─────────────────────────────────────────────────────────
    park_info = _safe(_db.get_park, code, db_path) or {}
    state = _pretty_state(park_info.get("state"))
    reservation = RESERVATION_STATUS.get(code)

    # ── Busyness (seasonal model) ───────────────────────────────────────────
    busyness: dict[str, Any] | None = None
    monthly: list[dict[str, Any]] = []
    today = date.today()
    full_model = _safe(_model.build_busyness_model, code, db_path)
    if full_model is not None:
        month_obj = next(
            (ms for ms in full_model.monthly_scores if ms.month == today.month),
            None,
        )
        if month_obj is not None:
            score = int(round(month_obj.score))
            busyness = {
                "score": score,
                "label": _busyness_label(month_obj.score),
                "context": _season_copy(month_obj.month, month_obj.label),
            }
        monthly = [
            {
                "month": ms.month_name[:3],
                "score": int(round(ms.score)),
                "label": ms.label,
            }
            for ms in full_model.monthly_scores
        ]

    # ── Parallel fetch: AQI, weather, fires, NPS alerts ──────────────────────
    # These 4 network calls were sequential (~15-30s worst case). Running
    # them in parallel brings wall-clock time down to the slowest single
    # call (~5-10s), a 3-4x speedup on cold cache.
    aqi_raw = None
    wx_raw = None
    fires_raw: list = []
    nps_alerts_raw: list = []

    if coords is not None:
        lat, lon = coords
        with ThreadPoolExecutor(max_workers=4) as pool:
            fut_aqi = pool.submit(_safe, load_aqi, lat, lon)
            fut_wx = pool.submit(_safe, load_weather, lat, lon)
            fut_fires = pool.submit(_safe, load_active_fires, lat, lon)
            fut_nps = pool.submit(_safe, load_nps_alerts, code, nps_api_key)

            aqi_raw = fut_aqi.result()
            wx_raw = fut_wx.result()
            fires_raw = fut_fires.result() or []
            nps_alerts_raw = fut_nps.result() or []
    else:
        nps_alerts_raw = load_nps_alerts(code, nps_api_key)

    # ── Parse AQI ──────────────────────────────────────────────────────────
    aqi_card: dict[str, Any] | None = None
    if aqi_raw and "_error" not in aqi_raw:
        cur = aqi_raw.get("current") or {}
        us_aqi = cur.get("us_aqi")
        if us_aqi is not None:
            aqi_card = {"value": int(round(us_aqi)), "label": aqi_label(us_aqi)}
    elif aqi_raw and "_error" in aqi_raw:
        logger.warning("load_aqi error for %s: %s", code, aqi_raw["_error"])

    # ── Parse weather ──────────────────────────────────────────────────────
    weather_card: dict[str, Any] | None = None
    if wx_raw and "_error" not in wx_raw:
        cur = wx_raw.get("current") or {}
        temp = cur.get("temperature_2m")
        if temp is not None:
            description = cur.get("short_forecast") or describe_weather_code(cur.get("weather_code"))
            weather_card = {"temp_f": int(round(temp)), "description": description}
    elif wx_raw and "_error" in wx_raw:
        logger.warning("load_weather error for %s: %s", code, wx_raw["_error"])

    camping_card = _safe(load_campsite_pct, code)

    cards = {
        "aqi": aqi_card,
        "weather": weather_card,
        "camping": camping_card,
    }

    # ── Alerts: fires (NIFC) + NPS alerts ──────────────────────────────────
    alerts: list[dict[str, str]] = []

    if coords is not None:
        lat, lon = coords
        enriched: list[tuple[float, dict[str, Any]]] = []
        for f in fires_raw:
            flat, flon = f.get("_lat"), f.get("_lon")
            if flat is None or flon is None:
                continue
            try:
                d = haversine_miles(lat, lon, float(flat), float(flon))
            except (TypeError, ValueError):
                continue
            if d <= 60:
                enriched.append((d, f))
        enriched.sort(key=lambda pair: pair[0])
        for _, f in enriched[:2]:
            text = _summarise_fire(lat, lon, f)
            if text:
                alerts.append({"tone": "fire", "text": text})

    # NPS alerts — surface the most relevant that match smoke/weather/closure
    remaining = 3 - len(alerts)
    if remaining > 0 and nps_alerts_raw:
        picked = 0
        for a in nps_alerts_raw:
            title = (a.get("title") or "").strip()
            if not title:
                continue
            cat = a.get("category") or ""
            tone = _classify_alert_tone(cat, f"{title} {a.get('description', '')}")
            if tone == "info":
                continue
            alerts.append({"tone": tone, "text": title})
            picked += 1
            if picked >= remaining:
                break

    # ── Final payload ───────────────────────────────────────────────────────
    park_block: dict[str, Any] = {
        "code": code,
        "name": park_name,
        "state": state or None,
    }
    if reservation:
        park_block["reservation_note"] = reservation

    return {
        "park": park_block,
        "busyness": busyness,
        "cards": cards,
        "alerts": alerts,
        "monthly": monthly,
        "fetched_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


def load_park_alerts_detail(
    unit_code: str,
    nps_api_key: str | None = None,
) -> dict[str, Any]:
    """
    Full alerts payload for the Alerts tab — includes all NPS alerts (not
    just the filtered top 3 on Overview) plus nearby wildfires, each with
    full title, description, category, and URL.
    """
    code = unit_code.upper()
    nps_api_key = nps_api_key or os.getenv("NPS_API_KEY", "")
    coords = PARK_COORDS.get(code)

    # Parallel fetch: NPS alerts + fires
    nps_alerts_raw: list = []
    fires_raw: list = []

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_nps = pool.submit(load_nps_alerts, code, nps_api_key)
        fut_fires = (
            pool.submit(_safe, load_active_fires, coords[0], coords[1])
            if coords else None
        )
        nps_alerts_raw = fut_nps.result() or []
        if fut_fires:
            fires_raw = fut_fires.result() or []

    # Format NPS alerts with full detail
    nps_formatted: list[dict[str, Any]] = []
    for a in nps_alerts_raw:
        title = (a.get("title") or "").strip()
        if not title:
            continue
        cat = a.get("category") or ""
        desc = (a.get("description") or "").strip()
        url = (a.get("url") or "").strip()
        tone = _classify_alert_tone(cat, f"{title} {desc}")
        nps_formatted.append({
            "tone": tone,
            "category": cat,
            "title": title,
            "description": desc,
            "url": url,
        })

    # Format fires
    fire_formatted: list[dict[str, Any]] = []
    if coords:
        lat, lon = coords
        enriched: list[tuple[float, dict[str, Any]]] = []
        for f in fires_raw:
            flat, flon = f.get("_lat"), f.get("_lon")
            if flat is None or flon is None:
                continue
            try:
                d = haversine_miles(lat, lon, float(flat), float(flon))
            except (TypeError, ValueError):
                continue
            if d <= 80:
                enriched.append((d, f))
        enriched.sort(key=lambda pair: pair[0])
        for dist, f in enriched[:5]:
            name = (f.get("IncidentName") or "").strip()
            if not name:
                continue
            acres = f.get("GISAcres")
            contained = f.get("PercentContained")
            direction = bearing_to_cardinal(lat, lon, float(f["_lat"]), float(f["_lon"]))
            entry: dict[str, Any] = {
                "name": name,
                "distance_mi": round(dist),
                "direction": direction,
                "summary": _summarise_fire(lat, lon, f) or "",
            }
            if acres is not None:
                try:
                    entry["acres"] = int(float(acres))
                except (TypeError, ValueError):
                    pass
            if contained is not None and contained != "":
                try:
                    entry["pct_contained"] = int(float(contained))
                except (TypeError, ValueError):
                    pass
            fire_formatted.append(entry)

    return {
        "park_code": code,
        "nps_alerts": nps_formatted,
        "fires": fire_formatted,
        "total": len(nps_formatted) + len(fire_formatted),
    }


def list_parks() -> list[dict[str, str]]:
    """Return the canonical 63 National Parks for the mobile search bar."""
    return [
        {"code": code, "name": name}
        for code, name in sorted(NATIONAL_PARKS.items(), key=lambda kv: kv[1])
    ]
