"""
NPS Park Dashboard — unified Streamlit app
==========================================
Tabs 1–3: live NPS Developer API  (parks overview, busyness rankings, park detail)
Tabs 4–6: seasonal busyness model (monthly scores, park comparison, recommendations)

Run
---
    streamlit run nps_dashboard.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Optional

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

_ENV_KEY      = os.getenv("NPS_API_KEY",  "")
_ENV_RIDB_KEY = os.getenv("RIDB_API_KEY", "")

# ── Seasonal model imports ────────────────────────────────────────────────────
_SEASONAL_SRC = Path(__file__).parent / "nps-seasonal-model" / "src"
if str(_SEASONAL_SRC) not in sys.path:
    sys.path.insert(0, str(_SEASONAL_SRC))

try:
    import db as _nps_db
    import model as _nps_model
    _SEASONAL_AVAILABLE = True
except ImportError:
    _SEASONAL_AVAILABLE = False

try:
    import campsites as _nps_campsites
    _CAMPSITES_AVAILABLE = True
except ImportError:
    _CAMPSITES_AVAILABLE = False

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NPS Park Dashboard",
    page_icon="🏕️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0f1923; color: #e8edf2; }

    [data-testid="stSidebar"] {
        background-color: #152232;
        border-right: 1px solid #1e3448;
    }

    .metric-card {
        background: #152232;
        border: 1px solid #1e3448;
        border-left: 4px solid #1a6fa8;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 12px;
    }
    .metric-card .label {
        font-size: 12px;
        color: #7a9bbb;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 4px;
    }
    .metric-card .value {
        font-size: 28px;
        font-weight: 700;
        color: #e8edf2;
    }
    .metric-card .sub {
        font-size: 12px;
        color: #7a9bbb;
        margin-top: 2px;
    }

    .section-header {
        font-size: 18px;
        font-weight: 600;
        color: #4da6de;
        border-bottom: 1px solid #1e3448;
        padding-bottom: 8px;
        margin: 24px 0 16px 0;
    }

    .detail-card {
        background: #152232;
        border: 1px solid #1e3448;
        border-radius: 8px;
        padding: 20px 24px;
        margin-bottom: 16px;
    }

    /* Generic blue badge (API tabs) */
    .badge {
        display: inline-block;
        background: #1a6fa8;
        color: #e8edf2;
        font-size: 11px;
        padding: 3px 10px;
        border-radius: 12px;
        margin: 2px 3px;
    }

    /* Seasonal classification badges */
    .badge-peak     { display:inline-block; background:#c0392b; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-shoulder { display:inline-block; background:#e67e22; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-quiet    { display:inline-block; background:#27ae60; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-info     { display:inline-block; background:#1a6fa8; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }

    .window-card {
        background: #0f1923;
        border: 1px solid #1e3448;
        border-left: 4px solid #27ae60;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .window-card .wlabel { font-weight:600; color:#e8edf2; font-size:14px; }
    .window-card .wnotes { color:#7a9bbb; font-size:12px; margin-top:4px; }

    [data-testid="stDataFrame"] { border: 1px solid #1e3448; border-radius: 6px; }

    .stButton > button {
        background: #1a6fa8; color: white; border: none;
        border-radius: 6px; font-weight: 500;
    }
    .stButton > button:hover { background: #1d82c7; }

    .stTextInput input {
        background: #0f1923;
        border-color: #1e3448;
        color: #e8edf2;
    }
    .stSelectbox > div, .stMultiSelect > div {
        background: #152232 !important;
    }

    div[data-testid="stTabs"] button { color: #7a9bbb; }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #4da6de;
        border-bottom-color: #4da6de;
    }

    #MainMenu { visibility: hidden; }
    footer    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://developer.nps.gov/api/v1"
LIMIT    = 500

DB_PATH      = Path(__file__).parent / "nps-seasonal-model" / "data" / "nps.db"
PREVIEW_CSV  = Path(__file__).parent / "campsite_preview.csv"

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0f1923",
    plot_bgcolor="#0f1923",
    font=dict(color="#e8edf2"),
    margin=dict(l=10, r=10, t=30, b=10),
)

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]

# ── Static lat/lon for all 63 national parks ──────────────────────────────────
PARK_COORDS: dict[str, tuple[float, float]] = {
    "ACAD": (44.35, -68.21),  "NPSA": (-14.25, -170.68), "ARCH": (38.68, -109.57),
    "BADL": (43.85, -102.34), "BIBE": (29.25, -103.25),  "BISC": (25.48,  -80.43),
    "BLCA": (38.57, -107.72), "BRCA": (37.57, -112.18),  "CANY": (38.20, -109.93),
    "CARE": (38.20, -111.17), "CAVE": (32.18, -104.44),  "CHIS": (34.01, -119.42),
    "CONG": (33.78,  -80.78), "CRLA": (42.94, -122.10),  "CUVA": (41.24,  -81.55),
    "DEVA": (36.24, -116.82), "DENA": (63.73, -152.49),  "DRTO": (24.63,  -82.87),
    "EVER": (25.39,  -80.93), "GAAR": (67.78, -153.30),  "JEFF": (38.62,  -90.18),
    "GLAC": (48.70, -113.72), "GLBA": (58.50, -136.90),  "GRCA": (36.10, -112.10),
    "GRTE": (43.73, -110.80), "GRBA": (39.00, -114.30),  "GRSA": (37.73, -105.51),
    "GRSM": (35.68,  -83.53), "GUMO": (31.92, -104.87),  "HALE": (20.72, -156.17),
    "HAVO": (19.43, -155.26), "HOSP": (34.51,  -93.05),  "INDU": (41.65,  -87.05),
    "ISRO": (47.99,  -88.91), "JOTR": (33.87, -115.90),  "KATM": (58.50, -154.97),
    "KEFJ": (59.92, -149.65), "KICA": (36.79, -118.56),  "KOVA": (67.33, -159.12),
    "LACL": (60.97, -153.42), "LAVO": (40.49, -121.51),  "MACA": (37.19,  -86.10),
    "MEVE": (37.18, -108.49), "MORA": (46.85, -121.74),  "NERI": (37.94,  -81.07),
    "NOCA": (48.49, -121.20), "OLYM": (47.97, -123.50),  "PEFO": (34.98, -109.78),
    "PINN": (36.49, -121.16), "REDW": (41.30, -124.00),  "ROMO": (40.40, -105.58),
    "SAGU": (32.25, -110.50), "SEQU": (36.43, -118.68),  "SHEN": (38.53,  -78.35),
    "THRO": (46.97, -103.45), "VIIS": (18.33,  -64.73),  "VOYA": (48.49,  -92.84),
    "WHSA": (32.78, -106.17), "WICA": (43.57, -103.48),  "WRST": (61.00, -142.00),
    "YELL": (44.60, -110.50), "YOSE": (37.87, -119.55),  "ZION": (37.30, -113.05),
}

# WMO weather-code → (emoji, description)
WMO: dict[int, tuple[str, str]] = {
    0:  ("☀️",  "Clear sky"),       1:  ("🌤️", "Mainly clear"),
    2:  ("⛅",  "Partly cloudy"),   3:  ("☁️",  "Overcast"),
    45: ("🌫️", "Fog"),             48: ("🌫️", "Icy fog"),
    51: ("🌦️", "Light drizzle"),  53: ("🌦️", "Drizzle"),       55: ("🌧️", "Heavy drizzle"),
    61: ("🌧️", "Slight rain"),    63: ("🌧️", "Moderate rain"),  65: ("🌧️", "Heavy rain"),
    71: ("🌨️", "Slight snow"),    73: ("🌨️", "Moderate snow"),  75: ("🌨️", "Heavy snow"),
    77: ("🌨️", "Snow grains"),
    80: ("🌦️", "Slight showers"), 81: ("🌧️", "Showers"),        82: ("⛈️",  "Heavy showers"),
    85: ("🌨️", "Snow showers"),   86: ("🌨️", "Heavy snow showers"),
    95: ("⛈️",  "Thunderstorm"),  96: ("⛈️",  "Thunderstorm + hail"), 99: ("⛈️",  "Thunderstorm + hail"),
}


# ══════════════════════════════════════════════════════════════════════════════
# NPS API helpers  (tabs 1–3)
# ══════════════════════════════════════════════════════════════════════════════

def nps_get(endpoint: str, api_key: str, params: Optional[dict] = None) -> list:
    """Fetch all records from a paginated NPS Developer API endpoint."""
    params = params or {}
    params.update({"api_key": api_key, "limit": LIMIT, "start": 0})
    all_data: list = []
    while True:
        try:
            r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=20)
            r.raise_for_status()
            body = r.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API error ({endpoint}): {e}")
            return []
        data = body.get("data", [])
        all_data.extend(data)
        if len(all_data) >= int(body.get("total", 0)) or not data:
            break
        params["start"] = len(all_data)
    return all_data


@st.cache_data(ttl=600, show_spinner=False)
def load_api_parks(api_key: str) -> pd.DataFrame:
    raw = nps_get("/parks", api_key)
    if not raw:
        return pd.DataFrame()
    rows = []
    for p in raw:
        fees = p.get("entranceFees", [])
        fee_str = ", ".join(
            f"${f.get('cost','?')} ({f.get('title','')})" for f in fees[:3]
        ) if fees else "No fee info"
        hours_raw = p.get("operatingHours", [])
        hours_str = ""
        if hours_raw:
            std = hours_raw[0].get("standardHours", {})
            days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            hours_str = "  \n".join(f"{d.capitalize()}: {std.get(d,'N/A')}" for d in days)
        contacts  = p.get("contacts", {})
        phones    = contacts.get("phoneNumbers", [])
        emails    = contacts.get("emailAddresses", [])
        amenities = p.get("amenities", [])
        rows.append({
            "park_code":        p.get("parkCode", ""),
            "name":             p.get("fullName", ""),
            "short_name":       p.get("name", ""),
            "states":           p.get("states", ""),
            "designation":      p.get("designation", ""),
            "description":      p.get("description", ""),
            "lat":              float(p.get("latitude")  or 0) or None,
            "lon":              float(p.get("longitude") or 0) or None,
            "url":              p.get("url", ""),
            "fees":             fee_str,
            "hours":            hours_str,
            "phone":            phones[0].get("phoneNumber","") if phones else "",
            "email":            emails[0].get("emailAddress","") if emails else "",
            "amenities":        ", ".join(
                                    a.get("name","") if isinstance(a, dict) else str(a)
                                    for a in amenities
                                ),
            "n_visitor_centers": len(p.get("visitorCenters", [])),
            "n_topics":          len(p.get("topics", [])),
            "n_activities":      len(p.get("activities", [])),
            "images":            p.get("images", []),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=3600, show_spinner=False)
def load_nps_campgrounds(api_key: str) -> pd.DataFrame:
    """
    Fetch all NPS campgrounds and return per-park FCFS + reservable site counts.

    Uses the NPS Developer API /campgrounds endpoint which includes
    numberOfSitesFirstComeFirstServe and numberOfSitesReservable fields.
    Filtered to the 63 national park unit codes.
    """
    if not api_key or not _CAMPSITES_AVAILABLE:
        return pd.DataFrame(columns=["unit_code", "nps_fcfs_sites", "nps_reservable_sites"])

    np_codes = {c.lower() for c in _nps_campsites.NATIONAL_PARKS}
    raw = nps_get("/campgrounds", api_key)
    if not raw:
        return pd.DataFrame(columns=["unit_code", "nps_fcfs_sites", "nps_reservable_sites"])

    rows: list[dict] = []
    for cg in raw:
        park_code = str(cg.get("parkCode", "")).lower()
        if park_code not in np_codes:
            continue
        def _int(val: str) -> int:
            try:
                return int(val) if val else 0
            except (ValueError, TypeError):
                return 0
        rows.append({
            "unit_code":    park_code.upper(),
            "cg_name":      cg.get("name", ""),
            "fcfs":         _int(cg.get("numberOfSitesFirstComeFirstServe", 0)),
            "reservable":   _int(cg.get("numberOfSitesReservable", 0)),
        })

    if not rows:
        return pd.DataFrame(columns=["unit_code", "nps_fcfs_sites", "nps_reservable_sites"])

    df = pd.DataFrame(rows)
    return (
        df.groupby("unit_code")
          .agg(nps_fcfs_sites=("fcfs", "sum"), nps_reservable_sites=("reservable", "sum"))
          .reset_index()
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_alerts(api_key: str) -> pd.DataFrame:
    raw = nps_get("/alerts", api_key)
    if not raw:
        return pd.DataFrame(columns=["parkCode", "category"])
    return pd.DataFrame([
        {"parkCode": a.get("parkCode",""), "category": a.get("category","")}
        for a in raw
    ])


@st.cache_data(ttl=600, show_spinner=False)
def load_activities_parks(api_key: str) -> pd.DataFrame:
    raw = nps_get("/activities/parks", api_key)
    if not raw:
        return pd.DataFrame(columns=["park_code","n_activities_catalog"])
    rows = [
        {"park_code": pk.get("parkCode",""), "activity": act.get("name","")}
        for act in raw
        for pk in act.get("parks", [])
    ]
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["park_code","activity"])
    if not df.empty:
        df = df.groupby("park_code").size().reset_index(name="n_activities_catalog")
    return df


def compute_busyness(
    parks_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    acts_df: pd.DataFrame,
) -> pd.DataFrame:
    """Composite busyness score from weighted NPS API proxy signals."""
    df = parks_df[["park_code","short_name","states","designation",
                   "n_visitor_centers","n_activities","n_topics"]].copy()
    if not alerts_df.empty:
        alert_counts = alerts_df.groupby("parkCode").size().reset_index(name="n_alerts")
        df = df.merge(alert_counts, left_on="park_code", right_on="parkCode",
                      how="left").drop(columns=["parkCode"], errors="ignore")
    else:
        df["n_alerts"] = 0
    if not acts_df.empty and "park_code" in acts_df.columns:
        df = df.merge(acts_df, on="park_code", how="left")
    else:
        df["n_activities_catalog"] = 0
    df = df.fillna(0)

    def norm(col: str) -> pd.Series:
        mn, mx = df[col].min(), df[col].max()
        return (df[col] - mn) / (mx - mn) if mx > mn else pd.Series(0.0, index=df.index)

    df["score"] = (
        norm("n_alerts")             * 0.30 +
        norm("n_activities_catalog") * 0.25 +
        norm("n_activities")         * 0.20 +
        norm("n_visitor_centers")    * 0.15 +
        norm("n_topics")             * 0.10
    )
    df["busyness_score"] = (df["score"] * 100).round(1)
    return df.sort_values("busyness_score", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Seasonal model helpers  (tabs 4–6)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def load_seasonal_parks() -> pd.DataFrame:
    if not _SEASONAL_AVAILABLE or not DB_PATH.exists():
        return pd.DataFrame()
    return _nps_db.get_all_parks(DB_PATH)


@st.cache_data(ttl=300, show_spinner=False)
def load_model(unit_code: str) -> dict | None:
    if not _SEASONAL_AVAILABLE:
        return None
    m = _nps_model.build_busyness_model(unit_code, DB_PATH)
    return m.to_dict() if m else None


def score_color(score: float) -> str:
    if score >= 70: return "#c0392b"
    if score >= 50: return "#e67e22"
    if score >= 20: return "#f39c12"
    return "#27ae60"


# ══════════════════════════════════════════════════════════════════════════════
# Park Conditions helpers  (tab 8)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=1800, show_spinner=False)
def load_weather(lat: float, lon: float) -> dict:
    """Current conditions + 7-day forecast from Open-Meteo (no API key needed)."""
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,apparent_temperature,precipitation,"
                           "wind_speed_10m,wind_direction_10m,weather_code,uv_index",
                "daily": "weather_code,temperature_2m_max,temperature_2m_min,"
                         "precipitation_sum,uv_index_max,sunrise,sunset",
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch",
                "timezone": "auto",
                "forecast_days": 7,
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=1800, show_spinner=False)
def load_aqi(lat: float, lon: float) -> dict:
    """Current AQI + pollutant breakdown from Open-Meteo Air Quality (no key needed)."""
    try:
        r = requests.get(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params={
                "latitude": lat, "longitude": lon,
                "current": "us_aqi,pm10,pm2_5,ozone,carbon_monoxide",
                "timezone": "auto",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


@st.cache_data(ttl=600, show_spinner=False)
def load_park_events(park_code: str, api_key: str) -> list:
    if not api_key:
        return []
    return nps_get("/events", api_key, {"parkCode": park_code.lower()})


@st.cache_data(ttl=3600, show_spinner=False)
def load_park_webcams(park_code: str, api_key: str) -> list:
    if not api_key:
        return []
    return nps_get("/webcams", api_key, {"parkCode": park_code.lower()})


def _aqi_color(aqi: float) -> str:
    if aqi <= 50:  return "#27ae60"
    if aqi <= 100: return "#f1c40f"
    if aqi <= 150: return "#e67e22"
    if aqi <= 200: return "#c0392b"
    if aqi <= 300: return "#8e44ad"
    return "#7f0000"

def _aqi_label(aqi: float) -> str:
    if aqi <= 50:  return "Good"
    if aqi <= 100: return "Moderate"
    if aqi <= 150: return "Unhealthy for Sensitive Groups"
    if aqi <= 200: return "Unhealthy"
    if aqi <= 300: return "Very Unhealthy"
    return "Hazardous"

def _wind_dir(deg: float) -> str:
    return ["N","NE","E","SE","S","SW","W","NW"][round(float(deg) / 45) % 8]


# Coastal parks → nearest NOAA tide station (id, description)
TIDE_STATIONS: dict[str, tuple[str, str]] = {
    "ACAD": ("8413320", "Bar Harbor, ME"),
    "BISC": ("8723214", "Virginia Key, FL"),
    "CHIS": ("9411340", "Santa Barbara, CA"),
    "DRTO": ("8724580", "Key West, FL"),
    "EVER": ("8723970", "Flamingo, FL"),
    "GLBA": ("9452210", "Juneau, AK"),
    "KATM": ("9457292", "Kodiak, AK"),
    "KEFJ": ("9455920", "Seward, AK"),
    "OLYM": ("9444090", "Port Angeles, WA"),
    "REDW": ("9419750", "Crescent City, CA"),
    "VIIS": ("9751401", "Charlotte Amalie, USVI"),
    "NPSA": ("1770000", "Pago Pago, American Samoa"),
}


@st.cache_data(ttl=900, show_spinner=False)
def load_usgs_gauges(lat: float, lon: float) -> list:
    """Active USGS stream gauges within ~40 miles of the park (no key needed)."""
    try:
        r = requests.get(
            "https://waterservices.usgs.gov/nwis/iv/",
            params={
                "format": "json",
                "bBox": f"{lon-0.6:.4f},{lat-0.6:.4f},{lon+0.6:.4f},{lat+0.6:.4f}",
                "parameterCd": "00060,00065",
                "siteType": "ST",
                "siteStatus": "active",
            },
            timeout=15,
        )
        r.raise_for_status()
        ts_list = r.json().get("value", {}).get("timeSeries", [])
        gauges: dict[str, dict] = {}
        for ts in ts_list:
            site      = ts.get("sourceInfo", {})
            site_code = (site.get("siteCode") or [{}])[0].get("value", "")
            site_name = site.get("siteName", "Unknown gauge")
            var_code  = (ts.get("variable", {}).get("variableCode") or [{}])[0].get("value", "")
            values    = (ts.get("values") or [{}])[0].get("value", [])
            latest    = values[-1].get("value", "") if values else ""
            unit      = ts.get("variable", {}).get("unit", {}).get("unitAbbreviation", "")
            if site_code not in gauges:
                gauges[site_code] = {"name": site_name.title(), "code": site_code}
            try:
                fval = float(latest)
                if fval > -999000:
                    if var_code == "00060":
                        gauges[site_code]["discharge"] = f"{fval:,.0f} {unit}"
                    elif var_code == "00065":
                        gauges[site_code]["gage_height"] = f"{fval:.1f} {unit}"
            except (ValueError, TypeError):
                pass
        return list(gauges.values())[:5]
    except Exception:
        return []


@st.cache_data(ttl=3600, show_spinner=False)
def load_noaa_tides(station_id: str) -> list:
    """Next 48 h high/low tide predictions from NOAA (no key needed)."""
    from datetime import datetime as _dt
    try:
        r = requests.get(
            "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
            params={
                "product":    "predictions",
                "station":    station_id,
                "datum":      "MLLW",
                "time_zone":  "lst_ldt",
                "interval":   "hilo",
                "units":      "english",
                "format":     "json",
                "begin_date": _dt.utcnow().strftime("%Y%m%d"),
                "range":      48,
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("predictions", [])
    except Exception:
        return []


@st.cache_data(ttl=1800, show_spinner=False)
def load_active_fires(lat: float, lon: float) -> list:
    """Active wildfire perimeters within ~70 miles from NIFC (no key needed)."""
    try:
        r = requests.get(
            "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
            "Active_Fires/FeatureServer/0/query",
            params={
                "where":          "1=1",
                "outFields":      "IncidentName,GISAcres,PercentContained,ModifiedOnDateTime_dt",
                "geometry":       f"{lon-1:.4f},{lat-1:.4f},{lon+1:.4f},{lat+1:.4f}",
                "geometryType":   "esriGeometryEnvelope",
                "spatialRel":     "esriSpatialRelIntersects",
                "f":              "json",
                "resultRecordCount": 10,
            },
            timeout=15,
        )
        r.raise_for_status()
        return [f.get("attributes", {}) for f in r.json().get("features", [])]
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Campsite availability helpers  (tab 7)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, show_spinner=False)
def load_campsite_preview_csv() -> pd.DataFrame:
    """Load the pre-fetched campsite_preview.csv committed to the repository."""
    if not PREVIEW_CSV.exists():
        return pd.DataFrame()
    df = pd.read_csv(PREVIEW_CSV)
    if "has_campgrounds" in df.columns:
        df["has_campgrounds"] = df["has_campgrounds"].astype(bool)
    return df


@st.cache_data(ttl=86400, show_spinner=False)
def load_park_facility_map(ridb_api_key: str) -> tuple[dict, dict, dict]:
    """Fetch NPS campground facility IDs and site counts from RIDB (cached 24 h)."""
    if not _CAMPSITES_AVAILABLE or not ridb_api_key:
        return {}, {}, {}
    return _nps_campsites.build_park_facility_map(ridb_api_key)


@st.cache_data(ttl=3600, show_spinner=False)
def load_campsite_availability(
    ridb_api_key: str,
    window_start_str: str,
    window_days: int,
    db_path_str: str,
) -> pd.DataFrame:
    """
    Return campsite availability DataFrame (cached 1 h).
    Tries the SQLite snapshot cache first; falls back to live RIDB + Rec.gov calls.
    """
    if not _CAMPSITES_AVAILABLE or not ridb_api_key:
        return pd.DataFrame()

    db_path = Path(db_path_str)
    cached = _nps_campsites.get_cached_stats(
        db_path, window_start_str=window_start_str, max_age_seconds=3600
    )
    if cached is not None and not cached.empty:
        return cached

    park_map, fac_names, site_counts = _nps_campsites.build_park_facility_map(ridb_api_key)
    if not park_map:
        return pd.DataFrame()

    df = _nps_campsites.fetch_all_parks_stats(
        park_map,
        fac_names,
        facility_site_counts=site_counts,
        window_start=date.fromisoformat(window_start_str),
        window_days=window_days,
    )
    if not df.empty:
        _nps_campsites.save_stats_to_db(df, db_path)
    return df


def _avail_color(pct: float | None) -> str:
    """Colour tier for availability percentage."""
    if pct is None:
        return "#4a6680"
    if pct >= 50:
        return "#27ae60"
    if pct >= 20:
        return "#f39c12"
    return "#c0392b"


def badge(text: str, kind: str = "info") -> str:
    return f'<span class="badge-{kind}">{text}</span>'


def month_bar_chart(model: dict, highlight_month: int | None = None) -> go.Figure:
    scores = model["monthly_scores"]
    colors = [
        "#4da6de" if (highlight_month and s["month"] == highlight_month)
        else score_color(s["score"])
        for s in scores
    ]
    fig = go.Figure(go.Bar(
        x=[s["month_name"][:3] for s in scores],
        y=[s["score"] for s in scores],
        marker_color=colors,
        text=[f"{s['score']:.0f}" for s in scores],
        textposition="outside",
        textfont=dict(color="#e8edf2", size=10),
        hovertemplate=(
            "<b>%{x}</b><br>Score: %{y:.1f}<br>"
            "Avg visits: %{customdata:,}<extra></extra>"
        ),
        customdata=[s["avg_visits"] for s in scores],
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(range=[0, 115], gridcolor="#1e3448",
                   title="Busyness Score (0–100)",
                   title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        showlegend=False,
        height=320,
    )
    fig.add_hline(y=70, line_dash="dot", line_color="#c0392b", opacity=0.4,
                  annotation_text="Peak threshold", annotation_font_color="#c0392b",
                  annotation_position="top right")
    fig.add_hline(y=30, line_dash="dot", line_color="#27ae60", opacity=0.4,
                  annotation_text="Quiet threshold", annotation_font_color="#27ae60",
                  annotation_position="bottom right")
    return fig


def comparison_chart(models: dict[str, dict], month: int | None) -> go.Figure:
    fig = go.Figure()
    for uc, m in models.items():
        if month:
            entry = next((s for s in m["monthly_scores"] if s["month"] == month), None)
            if entry:
                fig.add_trace(go.Bar(
                    name=uc,
                    x=[entry["month_name"][:3]],
                    y=[entry["score"]],
                    text=[f"{uc}: {entry['score']:.0f}"],
                    textposition="outside",
                ))
        else:
            fig.add_trace(go.Scatter(
                name=uc,
                x=MONTH_NAMES,
                y=[s["score"] for s in m["monthly_scores"]],
                mode="lines+markers",
                line=dict(width=2),
                marker=dict(size=6),
            ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(range=[0, 115], gridcolor="#1e3448",
                   title="Busyness Score",
                   title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        legend=dict(bgcolor="#152232", bordercolor="#1e3448",
                    font=dict(color="#e8edf2")),
        height=360,
        barmode="group",
    )
    return fig


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════

seasonal_parks_df = load_seasonal_parks()

with st.sidebar:
    st.markdown("## 🏕️ NPS Dashboard")
    st.markdown("---")

    # ── NPS API key ────────────────────────────────────────────────────────────
    if "api_key" not in st.session_state:
        st.session_state.api_key = _ENV_KEY
    api_key_input = st.text_input(
        "NPS API Key",
        value=_ENV_KEY or st.session_state.api_key,
        type="password",
        placeholder="Enter your api.nps.gov key",
        help="Get a free key at https://www.nps.gov/subjects/developer/get-started.htm",
    )
    if api_key_input:
        st.session_state.api_key = api_key_input

    # ── NPS API filters (tabs 1–3) ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**NPS API Filters**")
    state_filter = st.text_input(
        "Filter by state code (e.g. CA  or  CA,TX)", "",
        key="api_state_filter",
    )
    desig_filter = st.text_input(
        "Filter by designation (e.g. National Park)", "",
        key="api_desig_filter",
    )

    # ── Seasonal model controls (tabs 4–6) ────────────────────────────────────
    st.markdown("---")
    st.markdown("**Seasonal Model**")

    selected_uc: str | None = None
    all_states: list[str] = []

    if not _SEASONAL_AVAILABLE:
        st.warning("Seasonal model unavailable — `nps-seasonal-model/src/` not found.")
    elif seasonal_parks_df.empty:
        st.info("No seasonal data. Run:")
        st.code(
            "python nps-seasonal-model/src/ingest.py"
            " --years 2014-2024 --seed-only",
            language="bash",
        )
    else:
        all_states = sorted({
            s.strip()
            for row in seasonal_parks_df["state"].dropna()
            for s in row.split(",")
            if s.strip()
        })
        seas_state = st.selectbox(
            "Filter by state", ["All"] + all_states, key="seas_state"
        )
        all_types = sorted(seasonal_parks_df["type"].dropna().unique().tolist())
        seas_type = st.selectbox(
            "Filter by type", ["All"] + all_types, key="seas_type"
        )
        max_busy = st.slider("Max busyness score", 0, 100, 100, 5)

        # Apply filters to build park selector
        fp = seasonal_parks_df.copy()
        if seas_state != "All":
            fp = fp[fp["state"].str.contains(seas_state, na=False)]
        if seas_type != "All":
            fp = fp[fp["type"].str.contains(seas_type, case=False, na=False)]

        park_options: dict[str, str] = {
            f"{row['name']} ({row['unit_code']})": row["unit_code"]
            for _, row in fp.sort_values("name").iterrows()
        }
        if not park_options:
            st.warning("No parks match filters.")
        else:
            selected_label = st.selectbox(
                "Select park", list(park_options.keys()), key="seas_park"
            )
            selected_uc = park_options[selected_label]

    # ── Recreation.gov API key (tab 7) ────────────────────────────────────────
    st.markdown("---")
    st.markdown("**Recreation.gov**")
    if "ridb_api_key" not in st.session_state:
        st.session_state.ridb_api_key = _ENV_RIDB_KEY
    ridb_key_input = st.text_input(
        "RIDB API Key",
        value=_ENV_RIDB_KEY or st.session_state.ridb_api_key,
        type="password",
        placeholder="Get free key at ridb.recreation.gov",
        help="Required for Tab 7 — Campsite Availability",
        key="ridb_key_widget",
    )
    if ridb_key_input:
        st.session_state.ridb_api_key = ridb_key_input

    st.markdown("---")
    st.caption("NPS API data refreshes every 10 min · Alerts every 5 min.")
    if not seasonal_parks_df.empty:
        st.caption(
            f"{len(seasonal_parks_df)} parks in seasonal DB · "
            "COVID years 2020–21 excluded from baselines."
        )


# ══════════════════════════════════════════════════════════════════════════════
# Header
# ══════════════════════════════════════════════════════════════════════════════

api_key = st.session_state.get("api_key", "")

st.markdown("# 🏕️ NPS Park Dashboard")
st.markdown(
    "Live park data via the **NPS Developer API** (tabs 1–3)  ·  "
    "Historical seasonal busyness model (tabs 4–6)  ·  "
    "Campsite availability via Recreation.gov (tab 7)"
)

# ── Load NPS API data ──────────────────────────────────────────────────────────
if api_key:
    with st.spinner("Loading NPS data…"):
        parks_df   = load_api_parks(api_key)
        alerts_df  = load_alerts(api_key)
        acts_df    = load_activities_parks(api_key)
else:
    parks_df  = pd.DataFrame()
    alerts_df = pd.DataFrame(columns=["parkCode","category"])
    acts_df   = pd.DataFrame(columns=["park_code","n_activities_catalog"])

# Apply API sidebar filters
filtered = parks_df.copy()
if state_filter.strip() and not parks_df.empty:
    wanted_states = {s.strip().upper() for s in state_filter.split(",") if s.strip()}
    def _park_has_state(states_str: str) -> bool:
        park_states = {s.strip().upper() for s in str(states_str).split(",") if s.strip()}
        return bool(wanted_states & park_states)
    filtered = filtered[filtered["states"].apply(_park_has_state)]
if desig_filter.strip() and not parks_df.empty:
    filtered = filtered[
        filtered["designation"].str.contains(desig_filter.strip(), case=False, na=False)
    ]

# ── Top-level metrics (NPS API only) ──────────────────────────────────────────
if not parks_df.empty:
    n_parks  = len(parks_df)
    n_states = parks_df["states"].str.split(",").explode().str.strip().nunique()
    n_alerts = len(alerts_df) if not alerts_df.empty else 0
    n_desig  = parks_df["designation"].nunique()

    mc1, mc2, mc3, mc4 = st.columns(4)
    for col, label, value in [
        (mc1, "Total Parks",          f"{n_parks:,}"),
        (mc2, "States / Territories", f"{n_states:,}"),
        (mc3, "Active Alerts",        f"{n_alerts:,}"),
        (mc4, "Designations",         f"{n_desig:,}"),
    ]:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
            </div>""", unsafe_allow_html=True)
elif not api_key:
    st.info(
        "Enter your NPS API key in the sidebar to load live park data.  "
        "Tabs 4–6 (seasonal model) work without an API key."
    )
    st.markdown(
        "Get a free key at "
        "[developer.nps.gov](https://www.nps.gov/subjects/developer/get-started.htm)"
    )


# ── National park code set (used by tabs 1 & 3) ──────────────────────────────
_np_codes = (
    {c.lower() for c in _nps_campsites.NATIONAL_PARKS}
    if _CAMPSITES_AVAILABLE else set()
)

# ══════════════════════════════════════════════════════════════════════════════
# Tabs
# ══════════════════════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "📋 Parks Overview",
    "📊 Busyness Rankings",
    "🔍 Park Detail",
    "📊 Park Busyness",
    "⚖️ Compare Parks",
    "💡 Recommendations",
    "⛺ Campsite Availability",
    "🌤️ Park Conditions",
])

_NO_API = "Enter your NPS API key in the sidebar to load this tab."


# ────────────────────────────────────────────────────────────────────────────────
# TAB 1 — Parks Overview  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab1:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">National Parks</div>', unsafe_allow_html=True)

        # Restrict to the 63 designated national parks, then apply state filter
        np_df = (
            parks_df[parks_df["park_code"].str.lower().isin(_np_codes)]
            if _np_codes else
            parks_df[parks_df["designation"].str.contains("National Park", case=False, na=False)]
        )
        if state_filter.strip() and not np_df.empty:
            wanted = {s.strip().upper() for s in state_filter.split(",") if s.strip()}
            np_df = np_df[np_df["states"].apply(
                lambda s: bool(wanted & {x.strip().upper() for x in str(s).split(",")})
            )]

        search = st.text_input("Search park name", "", key="search_overview")
        display = np_df.copy()
        if search.strip():
            display = display[display["name"].str.contains(search.strip(), case=False, na=False)]

        show_df = display[["name","states","designation","amenities","url"]].rename(columns={
            "name": "Park Name", "states": "States", "designation": "Designation",
            "amenities": "Amenities", "url": "URL",
        })
        st.dataframe(show_df, use_container_width=True, hide_index=True, height=420)

        st.markdown('<div class="section-header">Park Locations</div>', unsafe_allow_html=True)
        map_df = parks_df.dropna(subset=["lat","lon"])
        map_df = map_df[(map_df["lat"] != 0) & (map_df["lon"] != 0)]
        if not map_df.empty:
            st.map(
                map_df[["lat","lon"]].rename(columns={"lat":"latitude","lon":"longitude"}),
                color="#1a6fa8",
            )


# ────────────────────────────────────────────────────────────────────────────────
# TAB 2 — Busyness Rankings  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab2:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">Composite Busyness Score</div>', unsafe_allow_html=True)
        st.markdown(
            "Busyness is a composite of weighted, normalised proxy signals:  \n"
            "**Active Alerts** (30%) · **Catalog Activities** (25%) · "
            "**Park Activities** (20%) · **Visitor Centers** (15%) · **Topics** (10%)"
        )

        busy_df = compute_busyness(parks_df, alerts_df, acts_df)
        top_n   = st.slider("Show top N parks", min_value=10, max_value=100, value=30, step=5)
        top_busy = busy_df.head(top_n)

        fig_busy = px.bar(
            top_busy, x="busyness_score", y="short_name", orientation="h",
            color="busyness_score",
            color_continuous_scale=[[0,"#1a3a52"],[0.4,"#1a6fa8"],[0.75,"#4da6de"],[1,"#85c9f0"]],
            labels={"busyness_score": "Score", "short_name": "Park"},
            template="plotly_dark",
            hover_data={"states": True, "designation": True, "n_alerts": True,
                        "n_visitor_centers": True, "n_activities_catalog": True},
        )
        fig_busy.update_layout(
            paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
            margin=dict(l=0, r=20, t=10, b=0),
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
            height=max(400, top_n * 22),
        )
        st.plotly_chart(fig_busy, use_container_width=True)

        st.markdown('<div class="section-header">Signal Breakdown (Top 20)</div>', unsafe_allow_html=True)
        top20 = busy_df.head(20)[["short_name","n_alerts","n_activities_catalog",
                                   "n_visitor_centers","n_activities","n_topics","busyness_score"]]
        top20.columns = ["Park","Alerts","Catalog Activities","Visitor Centers",
                         "Park Activities","Topics","Score"]
        st.dataframe(top20, use_container_width=True, hide_index=True)

        st.markdown('<div class="section-header">Activities vs Alerts</div>', unsafe_allow_html=True)
        scatter_df = busy_df[busy_df["n_alerts"] > 0]
        if not scatter_df.empty:
            fig_sc = px.scatter(
                scatter_df, x="n_activities_catalog", y="n_alerts",
                size="busyness_score", color="busyness_score",
                hover_name="short_name",
                color_continuous_scale=[[0,"#1a3a52"],[0.5,"#1a6fa8"],[1,"#4da6de"]],
                labels={"n_activities_catalog":"Catalog Activities","n_alerts":"Active Alerts"},
                template="plotly_dark",
            )
            fig_sc.update_layout(
                paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            st.info("No alert data available for scatter plot.")


# ────────────────────────────────────────────────────────────────────────────────
# TAB 3 — Park Detail  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab3:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">Park Detail View</div>', unsafe_allow_html=True)

        _detail_df = (
            parks_df[parks_df["park_code"].str.lower().isin(_np_codes)]
            if _np_codes else
            parks_df[parks_df["designation"].str.contains("National Park", case=False, na=False)]
        )
        park_names    = sorted(_detail_df["name"].dropna().unique().tolist())
        selected_name = st.selectbox("Select a park", park_names, index=0, key="detail_select")
        park_row      = parks_df[parks_df["name"] == selected_name]

        if park_row.empty:
            st.warning("Park not found.")
        else:
            p = park_row.iloc[0]

            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"## {p['name']}")
                if p["designation"]:
                    st.markdown(f'<span class="badge">{p["designation"]}</span>',
                                unsafe_allow_html=True)
                for s in str(p["states"]).split(","):
                    st.markdown(f'<span class="badge">{s.strip()}</span>',
                                unsafe_allow_html=True)
            with col_b:
                busy_row = compute_busyness(parks_df, alerts_df, acts_df)
                match = busy_row[busy_row["park_code"] == p["park_code"]]
                if not match.empty:
                    score = match.iloc[0]["busyness_score"]
                    rank  = match.index[0] + 1
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">Busyness Score</div>
                        <div class="value">{score}</div>
                    </div>
                    <div class="metric-card">
                        <div class="label">Rank</div>
                        <div class="value">#{rank}</div>
                    </div>""", unsafe_allow_html=True)

            st.markdown("---")

            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**About**")
            st.write(p["description"] or "No description available.")
            st.markdown('</div>', unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Entrance Fees**")
                st.write(p["fees"] or "Not available")
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Contact**")
                if p["phone"]: st.write(f"Phone: {p['phone']}")
                if p["email"]: st.write(f"Email: {p['email']}")
                if p["url"]:   st.markdown(f"[Official Website]({p['url']})")
                if not p["phone"] and not p["email"]:
                    st.write("No contact info available")
                st.markdown('</div>', unsafe_allow_html=True)

            with c2:
                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Operating Hours**")
                st.markdown(p["hours"]) if p["hours"] else st.write("Not available")
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Amenities**")
                if p["amenities"]:
                    for a in p["amenities"].split(", ")[:10]:
                        st.markdown(f'<span class="badge">{a}</span>', unsafe_allow_html=True)
                else:
                    st.write("No amenity data")
                st.markdown('</div>', unsafe_allow_html=True)

            if p["lat"] and p["lon"] and p["lat"] != 0 and p["lon"] != 0:
                st.markdown('<div class="section-header">Location</div>', unsafe_allow_html=True)
                st.map(
                    pd.DataFrame({"latitude": [p["lat"]], "longitude": [p["lon"]]}),
                    zoom=7, color="#1a6fa8",
                )

            if not alerts_df.empty:
                park_alerts = alerts_df[alerts_df["parkCode"] == p["park_code"]]
                if not park_alerts.empty:
                    st.markdown(
                        f'<div class="section-header">Active Alerts ({len(park_alerts)})</div>',
                        unsafe_allow_html=True,
                    )
                    for alert in nps_get("/alerts", api_key, {"parkCode": p["park_code"]})[:10]:
                        category    = alert.get("category","")
                        title       = alert.get("title","")
                        description = alert.get("description","")
                        color = {"Closure":"#c0392b","Danger":"#e67e22"}.get(category,"#1a6fa8")
                        st.markdown(f"""
                        <div class="detail-card" style="border-left-color:{color}">
                            <strong>[{category}] {title}</strong><br>
                            <small>{description}</small>
                        </div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 4 — Park Busyness  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-header">Seasonal Busyness Model</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE:
        st.error(
            "Seasonal model unavailable — `nps-seasonal-model/src/` not found. "
            "Ensure the project structure is intact."
        )
    elif seasonal_parks_df.empty or selected_uc is None:
        st.info(
            "Run the ingest pipeline to load seasonal data, then select a park "
            "from the **Seasonal Model** section in the sidebar.\n\n"
            "```\npython nps-seasonal-model/src/ingest.py --years 2014-2024 --seed-only\n```"
        )
    else:
        with st.spinner(f"Building model for {selected_uc}…"):
            s_model = load_model(selected_uc)

        if s_model is None:
            st.info(
                f"No seasonal data for **{selected_uc}**. "
                f"Run: `python nps-seasonal-model/src/ingest.py --years 2014-2024 --park {selected_uc}`"
            )
        else:
            d = s_model
            peak_score  = max(ms["score"] for ms in d["monthly_scores"])
            quiet_score = min(ms["score"] for ms in d["monthly_scores"])
            peak_str    = ", ".join(MONTH_NAMES[m-1] for m in d["peak_months"])
            quiet_str   = ", ".join(MONTH_NAMES[m-1] for m in d["quiet_months"][:4])
            data_span   = f"{min(d['data_years'])}–{max(d['data_years'])}"

            s_park_row  = seasonal_parks_df[seasonal_parks_df["unit_code"] == selected_uc]
            state_str   = s_park_row.iloc[0]["state"] if not s_park_row.empty else ""

            # Metric cards
            cm1, cm2, cm3, cm4 = st.columns(4)
            for col, label, value, sub in [
                (cm1, "Park",        d["name"][:28] + ("…" if len(d["name"]) > 28 else ""), state_str),
                (cm2, "Peak Score",  f"{peak_score:.0f}/100",  f"Months: {peak_str}"),
                (cm3, "Quiet Score", f"{quiet_score:.0f}/100", f"Months: {quiet_str}"),
                (cm4, "YoY Trend",   d["yoy_trend"],           f"Data: {data_span}"),
            ]:
                with col:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">{label}</div>
                        <div class="value">{value}</div>
                        <div class="sub">{sub}</div>
                    </div>""", unsafe_allow_html=True)

            # Monthly bar chart
            st.markdown('<div class="section-header">Monthly Busyness</div>', unsafe_allow_html=True)
            col_chart, col_ctrl = st.columns([3, 1])
            with col_ctrl:
                hl_opts = ["All months"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
                hl_sel  = st.selectbox("Highlight month", hl_opts, key="t4_highlight")
                hl_m    = None if hl_sel == "All months" else int(hl_sel.split(" –")[0])
            with col_chart:
                st.plotly_chart(month_bar_chart(d, hl_m), use_container_width=True)

            with st.expander("Monthly data table"):
                st.dataframe(
                    pd.DataFrame([{
                        "Month":      ms["month_name"],
                        "Score":      ms["score"],
                        "Label":      ms["label"].capitalize(),
                        "Avg Visits": f"{ms['avg_visits']:,}",
                    } for ms in d["monthly_scores"]]),
                    use_container_width=True, hide_index=True,
                )

            # Classification badges
            st.markdown('<div class="section-header">Seasonal Classification</div>', unsafe_allow_html=True)
            bp, bs, bq = st.columns(3)
            with bp:
                st.markdown("**Peak months** *(score ≥ 70)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "peak") for m in d["peak_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)
            with bs:
                st.markdown("**Shoulder months** *(score 20–70)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "shoulder") for m in d["shoulder_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)
            with bq:
                st.markdown("**Quiet months** *(score < 30)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "quiet") for m in d["quiet_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)

            # Best visit windows
            if d["best_visit_windows"]:
                st.markdown('<div class="section-header">Best Visit Windows</div>', unsafe_allow_html=True)
                st.caption("Ranked 2-week windows · weather-hostile periods excluded.")
                for w in d["best_visit_windows"]:
                    c = score_color(w["score"])
                    st.markdown(f"""
                    <div class="window-card">
                        <div class="wlabel">{w['label']}
                            <span style="color:{c}; font-size:13px; margin-left:8px;">
                                Score: {w['score']:.0f}/100
                            </span>
                        </div>
                        <div class="wnotes">{w['notes']}</div>
                    </div>""", unsafe_allow_html=True)

            # Footer
            st.markdown("---")
            if d["low_confidence"]:
                st.warning(
                    f"Low confidence: only {len(d['data_years'])} years of data. "
                    "Scores may not reflect long-term patterns."
                )
            excluded_str = ", ".join(str(y) for y in d["excluded_years"])
            st.caption(
                f"Baseline excludes COVID years ({excluded_str}). "
                f"Weekend multiplier: {d['weekend_multiplier']}× (estimated). "
                "Data: NPS IRMA / seed dataset."
            )


# ────────────────────────────────────────────────────────────────────────────────
# TAB 5 — Compare Parks  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-header">Park Comparison</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE or seasonal_parks_df.empty:
        st.info("Seasonal data not available. Run the ingest pipeline first.")
    else:
        col_left, col_right = st.columns([3, 1])
        with col_left:
            cmp_park_opts: dict[str, str] = {
                f"{row['name']} ({row['unit_code']})": row["unit_code"]
                for _, row in seasonal_parks_df.sort_values("name").iterrows()
            }
            cmp_labels = st.multiselect(
                "Select parks to compare (2–8)",
                list(cmp_park_opts.keys()),
                default=list(cmp_park_opts.keys())[:3],
                key="t5_compare_parks",
            )
        with col_right:
            cmp_month_opts = ["All months"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
            cmp_month_sel  = st.selectbox("Month", cmp_month_opts, key="t5_month")
            cmp_month = None if cmp_month_sel == "All months" \
                             else int(cmp_month_sel.split(" –")[0])

        if not cmp_labels:
            st.info("Select at least one park to compare.")
        else:
            cmp_ucs = [cmp_park_opts[l] for l in cmp_labels]
            with st.spinner("Loading models…"):
                cmp_models = {
                    uc: m
                    for uc in cmp_ucs
                    if (m := load_model(uc)) is not None
                }
            if not cmp_models:
                st.error("No seasonal data for the selected parks.")
            else:
                st.plotly_chart(
                    comparison_chart(cmp_models, cmp_month),
                    use_container_width=True,
                )

                st.markdown('<div class="section-header">Summary Table</div>', unsafe_allow_html=True)
                rows: list[dict] = []
                for uc, m in cmp_models.items():
                    if cmp_month:
                        entry = next(
                            (s for s in m["monthly_scores"] if s["month"] == cmp_month), None
                        )
                        if entry:
                            rows.append({
                                "Park": m["name"], "Code": uc,
                                "Month": entry["month_name"],
                                "Score": entry["score"],
                                "Label": entry["label"].capitalize(),
                                "Avg Visits": f"{entry['avg_visits']:,}",
                                "YoY Trend": m["yoy_trend"],
                            })
                    else:
                        avg    = sum(s["score"] for s in m["monthly_scores"]) / 12
                        peak_m = ", ".join(MONTH_NAMES[x-1] for x in m["peak_months"])
                        rows.append({
                            "Park": m["name"], "Code": uc,
                            "Avg Score": round(avg, 1),
                            "Peak Months": peak_m,
                            "Quiet Months": len(m["quiet_months"]),
                            "YoY Trend": m["yoy_trend"],
                            "Low Confidence": "⚠️" if m["low_confidence"] else "✓",
                        })
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                # Heat-map (all-months view only)
                if not cmp_month and len(cmp_models) > 1:
                    st.markdown('<div class="section-header">Busyness Heat-Map</div>',
                                unsafe_allow_html=True)
                    heat_df = pd.DataFrame(
                        {uc: [s["score"] for s in m["monthly_scores"]]
                         for uc, m in cmp_models.items()},
                        index=MONTH_NAMES,
                    ).T
                    fig_heat = px.imshow(
                        heat_df,
                        color_continuous_scale=[
                            [0,"#27ae60"],[0.3,"#f39c12"],[0.6,"#e67e22"],[1.0,"#c0392b"]
                        ],
                        zmin=0, zmax=100,
                        text_auto=".0f",
                        template="plotly_dark",
                        labels=dict(color="Score"),
                    )
                    fig_heat.update_layout(
                        **PLOTLY_LAYOUT,
                        height=max(200, len(cmp_models) * 50 + 60),
                        coloraxis_colorbar=dict(
                            tickfont=dict(color="#e8edf2"),
                            title=dict(text="Score", font=dict(color="#e8edf2")),
                        ),
                    )
                    st.plotly_chart(fig_heat, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 6 — Recommendations  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab6:
    st.markdown('<div class="section-header">Find Low-Busyness Parks</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE:
        st.info("Seasonal model unavailable.")
    elif seasonal_parks_df.empty:
        st.info("Run the ingest pipeline first.")
    else:
        st.markdown("Filter by state, month, and maximum busyness score.")
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            rec_state = st.selectbox("State", ["Any"] + all_states, key="t6_state")
        with rc2:
            rec_month_opts = ["Any month"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
            rec_month_sel  = st.selectbox("Month", rec_month_opts, key="t6_month")
            rec_month = None if rec_month_sel == "Any month" \
                             else int(rec_month_sel.split(" –")[0])
        with rc3:
            rec_max = st.slider("Max busyness score", 0, 100, 50, 5, key="t6_max")

        with st.spinner("Searching…"):
            recs = _nps_model.recommend_parks(
                db_path=DB_PATH,
                state=rec_state if rec_state != "Any" else None,
                month=rec_month,
                max_score=rec_max,
            )

        if not recs:
            st.info("No parks match. Try raising the max score or changing filters.")
        else:
            st.success(f"Found **{len(recs)}** parks matching criteria.")
            rec_df    = pd.DataFrame(recs)
            score_col = "score" if "score" in rec_df.columns else "avg_score"

            fig_rec = px.bar(
                rec_df.sort_values(score_col).head(20),
                x=score_col, y="name", orientation="h",
                color=score_col,
                color_continuous_scale=[[0,"#27ae60"],[0.5,"#f39c12"],[1.0,"#e67e22"]],
                template="plotly_dark",
                labels={score_col: "Busyness Score", "name": "Park"},
            )
            fig_rec.update_layout(
                **PLOTLY_LAYOUT,
                height=max(300, min(len(recs), 20) * 28 + 60),
                yaxis=dict(autorange="reversed", tickfont=dict(color="#7a9bbb")),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_rec, use_container_width=True)

            display_cols = [c for c in ["name","unit_code","state",score_col,
                                        "month_name","yoy_trend"] if c in rec_df.columns]
            st.dataframe(
                rec_df[display_cols].rename(columns={
                    "name": "Park", "unit_code": "Code", "state": "State",
                    "score": "Score", "avg_score": "Avg Score",
                    "month_name": "Month", "yoy_trend": "YoY Trend",
                }),
                use_container_width=True, hide_index=True,
            )

            # Deep-dive
            st.markdown("---")
            st.markdown("**Deep-dive into a recommended park:**")
            deep_opts  = {r["name"]: r["unit_code"] for r in recs}
            deep_label = st.selectbox("Choose park", list(deep_opts.keys()), key="t6_deep")
            deep_uc    = deep_opts[deep_label]
            if st.button("Load busyness model →"):
                with st.spinner("Loading…"):
                    deep_model = load_model(deep_uc)
                if deep_model:
                    st.markdown(f"### {deep_model['name']}")
                    st.plotly_chart(
                        month_bar_chart(deep_model, rec_month),
                        use_container_width=True,
                    )
                    for w in deep_model["best_visit_windows"][:3]:
                        st.markdown(f"""
                        <div class="window-card">
                            <div class="wlabel">{w['label']} — Score: {w['score']:.0f}/100</div>
                            <div class="wnotes">{w['notes']}</div>
                        </div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 7 — Campsite Availability  (Recreation.gov)
# ────────────────────────────────────────────────────────────────────────────────
with tab7:
    st.markdown('<div class="section-header">Campsite Availability — Next 30 Days</div>',
                unsafe_allow_html=True)

    if not _CAMPSITES_AVAILABLE:
        st.error(
            "Campsite module unavailable.  "
            "Ensure `campsites.py` is present in `nps-seasonal-model/src/`."
        )
        st.stop()

    # ── Determine data source ──────────────────────────────────────────────────
    # Priority: 1) live refresh via RIDB key  2) pre-fetched CSV in repo
    ridb_key   = st.session_state.get("ridb_api_key", "")
    preview_df = load_campsite_preview_csv()
    has_preview = not preview_df.empty

    # ── Controls row ──────────────────────────────────────────────────────────
    col_info, col_btn = st.columns([5, 1])
    with col_info:
        if has_preview and "fetched_at" in preview_df.columns:
            fetched_raw = preview_df["fetched_at"].max()
            window_raw  = preview_df.get("window_start", pd.Series()).iloc[0] if "window_start" in preview_df.columns else "—"
            st.caption(
                f"Showing pre-fetched data · fetched {str(fetched_raw)[:19]} UTC · "
                f"window start {window_raw}  ·  "
                "To refresh: enter your RIDB API key in the sidebar and click **Refresh Live Data**."
            )
        elif not has_preview and not ridb_key:
            st.info(
                "No pre-fetched data found.  \n\n"
                "**Option A** — Run `fetch_campsite_preview.py` locally, commit "
                "`campsite_preview.csv`, and push.  \n"
                "**Option B** — Enter your [RIDB API key](https://ridb.recreation.gov) "
                "in the sidebar to fetch live data now."
            )
    with col_btn:
        if ridb_key:
            if st.button("Refresh Live Data", key="t7_refresh"):
                st.session_state["t7_live_fetch"] = True
                load_campsite_availability.clear()
                load_park_facility_map.clear()
                load_campsite_preview_csv.clear()

    # ── Load data: CSV by default; live fetch only on explicit Refresh ─────────
    # The RIDB key may be set in the environment (e.g. on Render) but that should
    # not trigger background API calls — only the Refresh button should.
    do_live_fetch = ridb_key and st.session_state.get("t7_live_fetch", False)

    if do_live_fetch:
        t7_days = 30
        with st.spinner(
            "Fetching campsite availability for 63 national parks…  "
            "This may take 1–2 minutes."
        ):
            camp_df = load_campsite_availability(
                ridb_key,
                date.today().isoformat(),
                t7_days,
                str(DB_PATH),
            )
        if camp_df.empty:
            st.warning("No live data returned. Check your RIDB API key.")
            camp_df = preview_df
    else:
        camp_df = preview_df
        t7_days = int(
            (pd.to_datetime(preview_df["window_end"].iloc[0]) -
             pd.to_datetime(preview_df["window_start"].iloc[0])).days
        ) if (has_preview and "window_end" in preview_df.columns
              and "window_start" in preview_df.columns) else 30

    if camp_df.empty:
        st.stop()

    # ── Merge NPS API campground counts (FCFS) ────────────────────────────────
    nps_cg_df = load_nps_campgrounds(api_key) if api_key else pd.DataFrame()
    if not nps_cg_df.empty:
        camp_df = camp_df.merge(nps_cg_df[["unit_code", "nps_fcfs_sites"]],
                                on="unit_code", how="left")
        camp_df["nps_fcfs_sites"] = camp_df["nps_fcfs_sites"].fillna(0).astype(int)
    else:
        camp_df["nps_fcfs_sites"] = 0

    # ── Park filter dropdown ──────────────────────────────────────────────────
    park_options = ["All Parks"] + sorted(
        camp_df["park_name"].dropna().unique().tolist()
    )
    selected_park = st.selectbox("Filter by park", park_options, index=0, key="t7_park_filter")
    if selected_park != "All Parks":
        camp_df = camp_df[camp_df["park_name"] == selected_park]

    # ── Summary metrics ───────────────────────────────────────────────────────
    parks_with_camps = camp_df[camp_df["has_campgrounds"].astype(bool)]
    total_reservable = int(parks_with_camps["n_reservable_sites"].sum())
    total_fcfs       = int(camp_df["nps_fcfs_sites"].sum())
    total_avail      = int(parks_with_camps["avail_nights"].sum())
    avg_pct          = parks_with_camps["pct_available"].dropna().mean()
    fetched_ts       = camp_df["fetched_at"].max() if "fetched_at" in camp_df.columns else "—"

    mc1, mc2, mc3, mc4 = st.columns(4)
    for col, label, value, sub in [
        (mc1, "Total Reservable Sites",   f"{total_reservable:,}",
         "Recreation.gov reservable"),
        (mc2, "Total FCFS Sites",
         f"{total_fcfs:,}" if total_fcfs > 0 else "—",
         "NPS API · walk-in / first-come" if total_fcfs > 0 else "Enter NPS key to load"),
        (mc3, f"Available Site-Nights ({t7_days}d)", f"{total_avail:,}",
         "reservable slots open"),
        (mc4, "Avg % Available",
         f"{avg_pct:.1f}%" if not pd.isna(avg_pct) else "—",
         "parks with campgrounds"),
    ]:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
                <div class="sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    # ── Availability bar chart ─────────────────────────────────────────────────
    st.markdown('<div class="section-header">Availability by Park</div>',
                unsafe_allow_html=True)

    chart_df = parks_with_camps.copy()
    chart_df["pct_display"] = chart_df["pct_available"].fillna(0)
    chart_df = chart_df.sort_values("pct_display", ascending=True)

    fig_avail = px.bar(
        chart_df,
        x="pct_display",
        y="park_name",
        orientation="h",
        color="pct_display",
        color_continuous_scale=[
            [0.0, "#c0392b"],
            [0.2, "#f39c12"],
            [0.5, "#27ae60"],
            [1.0, "#1abc9c"],
        ],
        range_color=[0, 100],
        labels={"pct_display": "% Available", "park_name": "Park"},
        template="plotly_dark",
    )
    fig_avail.update_layout(
        **PLOTLY_LAYOUT,
        height=max(400, len(chart_df) * 20 + 80),
        xaxis=dict(range=[0, 100], title="% Available",
                   ticksuffix="%", gridcolor="#1e3448",
                   tickfont=dict(color="#7a9bbb")),
        yaxis=dict(tickfont=dict(color="#7a9bbb"), title=""),
        coloraxis_showscale=False,
    )
    st.plotly_chart(fig_avail, use_container_width=True)

    # ── Data table ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Park Breakdown</div>',
                unsafe_allow_html=True)

    display_df = parks_with_camps[
        ["park_name", "n_reservable_sites", "nps_fcfs_sites",
         "avail_nights", "pct_available", "weekend_pct", "weekday_pct",
         "n_facilities"]
    ].copy()

    def fmt_pct(v):
        return f"{v:.1f}%" if pd.notna(v) else "—"

    display_df["pct_available"] = display_df["pct_available"].apply(fmt_pct)
    display_df["weekend_pct"]   = display_df["weekend_pct"].apply(fmt_pct)
    display_df["weekday_pct"]   = display_df["weekday_pct"].apply(fmt_pct)
    # Show "—" for FCFS when NPS key not provided
    display_df["nps_fcfs_sites"] = display_df["nps_fcfs_sites"].apply(
        lambda v: str(int(v)) if total_fcfs > 0 else "—"
    )

    st.dataframe(
        display_df.rename(columns={
            "park_name":          "Park",
            "n_reservable_sites": "Reservable (Rec.gov)",
            "nps_fcfs_sites":     "FCFS (NPS)",
            "avail_nights":       f"Avail Nights ({t7_days}d)",
            "pct_available":      "% Available",
            "weekend_pct":        "Wknd %",
            "weekday_pct":        "Wkday %",
            "n_facilities":       "Campgrounds",
        }),
        use_container_width=True,
        hide_index=True,
        height=420,
    )

    # ── Parks not on Recreation.gov ───────────────────────────────────────────
    no_camp = camp_df[~camp_df["has_campgrounds"].astype(bool)]
    if not no_camp.empty:
        with st.expander(
            f"{len(no_camp)} parks with no Recreation.gov campgrounds"
        ):
            st.markdown(
                "These parks either have no reservable camping, use a "
                "different booking system (lottery, permit, walk-in), or "
                "were not matched in the RIDB facility discovery.  "
                "Check the park's website for current camping options."
            )
            st.dataframe(
                no_camp[["park_name"]].rename(columns={"park_name": "Park"}),
                use_container_width=True,
                hide_index=True,
            )

    # ── Per-campground drill-down (requires live RIDB key) ────────────────────
    if ridb_key:
        st.markdown("---")
        st.markdown("**Per-campground detail**")
        drill_options = {
            row["park_name"]: row["unit_code"]
            for _, row in parks_with_camps.sort_values("park_name").iterrows()
        }
        if drill_options:
            drill_label = st.selectbox(
                "Select park", list(drill_options.keys()), key="t7_drill"
            )
            drill_uc = drill_options[drill_label]
            if st.button("Load campground detail →", key="t7_drill_btn"):
                fac_map, fac_names, fac_site_counts = load_park_facility_map(ridb_key)
                fac_ids = fac_map.get(drill_uc, [])
                if not fac_ids:
                    st.info("No facility IDs found for this park.")
                else:
                    with st.spinner(f"Fetching per-campground data for {drill_label}…"):
                        ps = _nps_campsites.fetch_park_campsite_stats(
                            drill_uc, fac_ids, fac_names, fac_site_counts,
                            window_start=date.today(), window_days=t7_days,
                        )
                    fac_rows = []
                    for f in ps.facilities:
                        tot = f.total_reservable_nights
                        pct = round(100 * f.available_nights / tot, 1) if tot else None
                        fac_rows.append({
                            "Campground":       f.facility_name,
                            "Reservable Sites": f.n_reservable,
                            "Avail Nights":     f.available_nights,
                            "% Available":      f"{pct:.1f}%" if pct is not None else "—",
                            "Wknd %": (
                                f"{100*f.weekend_available/f.weekend_total:.1f}%"
                                if f.weekend_total else "—"
                            ),
                            "Wkday %": (
                                f"{100*f.weekday_available/f.weekday_total:.1f}%"
                                if f.weekday_total else "—"
                            ),
                        })
                    st.dataframe(
                        pd.DataFrame(fac_rows),
                        use_container_width=True, hide_index=True,
                    )

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.caption(
        f"Availability fetched: {str(fetched_ts)[:19] if fetched_ts else '—'} UTC  ·  "
        f"30-day window  ·  "
        "Reservable site data: [Recreation.gov](https://www.recreation.gov) / [RIDB](https://ridb.recreation.gov)  ·  "
        "FCFS site counts: [NPS Developer API](https://developer.nps.gov/api/v1) `/campgrounds` "
        "(requires NPS API key in sidebar)"
    )


# ────────────────────────────────────────────────────────────────────────────────
# TAB 8 — Park Conditions  (weather · AQI · alerts · events · webcams)
# ────────────────────────────────────────────────────────────────────────────────
with tab8:
    st.markdown('<div class="section-header">Park Conditions</div>', unsafe_allow_html=True)

    # ── Park selector ─────────────────────────────────────────────────────────
    np_names_sorted = sorted(
        (name, code) for code, name in (
            _nps_campsites.NATIONAL_PARKS.items() if _CAMPSITES_AVAILABLE else []
        )
    )
    t8_park_label = st.selectbox(
        "Select a park",
        [n for n, _ in np_names_sorted],
        key="t8_park",
    )
    t8_uc = next((c for n, c in np_names_sorted if n == t8_park_label), None)

    if not t8_uc:
        st.info("Select a park above.")
        st.stop()

    lat, lon = PARK_COORDS.get(t8_uc, (None, None))
    if lat is None:
        st.warning(f"No coordinates available for {t8_park_label}.")
        st.stop()

    # Fetch all data (cached functions — warm hits are instant)
    with st.spinner("Loading conditions…"):
        wx      = load_weather(lat, lon)
        aqi     = load_aqi(lat, lon)
        fires   = load_active_fires(lat, lon)
        gauges  = load_usgs_gauges(lat, lon)
        tides   = (
            load_noaa_tides(TIDE_STATIONS[t8_uc][0])
            if t8_uc in TIDE_STATIONS else []
        )

    t8_alerts  = (
        alerts_df[alerts_df["parkCode"].str.lower() == t8_uc.lower()]
        if not alerts_df.empty else pd.DataFrame()
    )
    events   = load_park_events(t8_uc, api_key)
    webcams  = load_park_webcams(t8_uc, api_key)

    # ── NPS Alerts ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Active Alerts</div>', unsafe_allow_html=True)
    if not api_key:
        st.caption("Enter NPS API key in the sidebar to load alerts.")
    elif t8_alerts.empty:
        st.success("No active alerts for this park.")
    else:
        full_alerts = [
            a for a in nps_get("/alerts", api_key, {"parkCode": t8_uc.lower()})
        ]
        if not full_alerts:
            st.success("No active alerts.")
        for alert in full_alerts:
            cat   = alert.get("category", "")
            color = {"Closure": "#c0392b", "Danger": "#e67e22",
                     "Caution": "#f39c12"}.get(cat, "#1a6fa8")
            st.markdown(f"""
            <div class="detail-card" style="border-left-color:{color}; margin-bottom:8px;">
                <strong>[{cat}] {alert.get("title","")}</strong><br>
                <small style="color:#7a9bbb">{alert.get("description","")}</small>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Weather ───────────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Weather</div>', unsafe_allow_html=True)

    if not wx:
        st.warning("Weather data unavailable — Open-Meteo could not be reached.")
    else:
        cur = wx.get("current", {})
        wmo_code  = int(cur.get("weather_code", 0))
        emoji, desc = WMO.get(wmo_code, ("🌡️", "Unknown"))
        temp      = cur.get("temperature_2m")
        feels     = cur.get("apparent_temperature")
        wind_spd  = cur.get("wind_speed_10m")
        wind_deg  = cur.get("wind_direction_10m", 0)
        precip    = cur.get("precipitation")
        uv        = cur.get("uv_index")

        # Current conditions cards
        wc1, wc2, wc3, wc4, wc5 = st.columns(5)
        for col, label, value in [
            (wc1, "Conditions",   f"{emoji} {desc}"),
            (wc2, "Temperature",  f"{temp:.0f}°F" if temp is not None else "—"),
            (wc3, "Feels Like",   f"{feels:.0f}°F" if feels is not None else "—"),
            (wc4, "Wind",         f"{wind_spd:.0f} mph {_wind_dir(wind_deg)}" if wind_spd is not None else "—"),
            (wc5, "UV Index",     f"{uv:.0f}" if uv is not None else "—"),
        ]:
            with col:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">{label}</div>
                    <div class="value" style="font-size:20px">{value}</div>
                </div>""", unsafe_allow_html=True)

        # 7-day forecast
        st.markdown("**7-Day Forecast**")
        daily = wx.get("daily", {})
        if daily.get("time"):
            cols = st.columns(7)
            for i, (col, day_date) in enumerate(zip(cols, daily["time"])):
                d_code  = int((daily.get("weather_code") or [0]*7)[i])
                d_emoji = WMO.get(d_code, ("🌡️",""))[0]
                d_hi    = (daily.get("temperature_2m_max") or [None]*7)[i]
                d_lo    = (daily.get("temperature_2m_min") or [None]*7)[i]
                d_prec  = (daily.get("precipitation_sum") or [None]*7)[i]
                d_label = pd.to_datetime(day_date).strftime("%a %-d")
                hi_str  = f"{d_hi:.0f}°" if d_hi is not None else "—"
                lo_str  = f"{d_lo:.0f}°" if d_lo is not None else "—"
                pr_str  = f"{d_prec:.2f}\"" if d_prec else ""
                with col:
                    st.markdown(f"""
                    <div class="metric-card" style="text-align:center;padding:10px 6px">
                        <div class="label">{d_label}</div>
                        <div style="font-size:22px">{d_emoji}</div>
                        <div style="font-size:14px;color:#e8edf2">{hi_str} / {lo_str}</div>
                        <div class="sub">{pr_str}</div>
                    </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Air Quality ───────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Air Quality</div>', unsafe_allow_html=True)

    if not aqi:
        st.warning("Air quality data unavailable.")
    else:
        aqi_cur  = aqi.get("current", {})
        aqi_val  = aqi_cur.get("us_aqi")
        pm25     = aqi_cur.get("pm2_5")
        pm10     = aqi_cur.get("pm10")
        ozone    = aqi_cur.get("ozone")

        aq1, aq2, aq3, aq4 = st.columns(4)
        aqi_c    = _aqi_color(aqi_val) if aqi_val is not None else "#4a6680"
        aqi_lbl  = _aqi_label(aqi_val) if aqi_val is not None else "—"
        for col, label, value, border in [
            (aq1, "US AQI",  f"{int(aqi_val)}" if aqi_val is not None else "—", aqi_c),
            (aq2, "PM 2.5",  f"{pm25:.1f} μg/m³" if pm25 is not None else "—",  "#1a6fa8"),
            (aq3, "PM 10",   f"{pm10:.1f} μg/m³" if pm10 is not None else "—",  "#1a6fa8"),
            (aq4, "Ozone",   f"{ozone:.1f} μg/m³" if ozone is not None else "—","#1a6fa8"),
        ]:
            with col:
                st.markdown(f"""
                <div class="metric-card" style="border-left-color:{border}">
                    <div class="label">{label}</div>
                    <div class="value">{value}</div>
                    <div class="sub">{aqi_lbl if label == "US AQI" else ""}</div>
                </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Sunrise / Sunset ─────────────────────────────────────────────────────
    if wx:
        daily = wx.get("daily", {})
        sr_list = daily.get("sunrise") or []
        ss_list = daily.get("sunset")  or []
        if sr_list and ss_list:
            sr_raw = sr_list[0]   # e.g. "2026-04-11T06:23"
            ss_raw = ss_list[0]
            try:
                sr_str = pd.to_datetime(sr_raw).strftime("%-I:%M %p")
                ss_str = pd.to_datetime(ss_raw).strftime("%-I:%M %p")
                # Day length
                diff_m = int((pd.to_datetime(ss_raw) - pd.to_datetime(sr_raw)).total_seconds() / 60)
                dl_str = f"{diff_m // 60}h {diff_m % 60}m"
            except Exception:
                sr_str = ss_str = dl_str = "—"
            sd1, sd2, sd3 = st.columns(3)
            for col, lbl, val in [
                (sd1, "Sunrise",    f"🌅 {sr_str}"),
                (sd2, "Sunset",     f"🌇 {ss_str}"),
                (sd3, "Day Length", f"⏱ {dl_str}"),
            ]:
                with col:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">{lbl}</div>
                        <div class="value" style="font-size:20px">{val}</div>
                    </div>""", unsafe_allow_html=True)
            st.markdown("---")

    # ── Active Wildfires ──────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Active Wildfires (within ~70 miles)</div>', unsafe_allow_html=True)
    if not fires:
        st.success("No active wildfires reported within ~70 miles.")
    else:
        for fire in fires:
            name       = fire.get("IncidentName", "Unknown Fire")
            acres      = fire.get("GISAcres")
            contained  = fire.get("PercentContained")
            updated    = fire.get("ModifiedOnDateTime_dt", "")
            acres_str  = f"{acres:,.0f} acres" if acres else "size unknown"
            cont_str   = f"{contained:.0f}% contained" if contained is not None else "containment unknown"
            upd_str    = ""
            if updated:
                try:
                    upd_str = pd.to_datetime(updated, unit="ms").strftime("%b %-d")
                except Exception:
                    pass
            cont_color = "#27ae60" if (contained or 0) >= 75 else "#e67e22" if (contained or 0) >= 30 else "#c0392b"
            st.markdown(f"""
            <div class="detail-card" style="border-left-color:{cont_color}; margin-bottom:8px;">
                <strong>🔥 {name}</strong>
                <span style="color:#7a9bbb;font-size:12px;margin-left:12px">
                    {acres_str}  ·  {cont_str}{("  ·  Updated " + upd_str) if upd_str else ""}
                </span>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Water Levels ─────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Water Levels</div>', unsafe_allow_html=True)

    if gauges:
        st.markdown("**Stream Gauges (USGS)**")
        g_cols = st.columns(min(len(gauges), 3))
        for i, gauge in enumerate(gauges[:3]):
            with g_cols[i]:
                discharge    = gauge.get("discharge", "")
                gage_height  = gauge.get("gage_height", "")
                reading_str  = discharge or gage_height or "—"
                label_str    = "Flow" if discharge else "Stage"
                st.markdown(f"""
                <div class="metric-card">
                    <div class="label">{gauge["name"][:30]}</div>
                    <div class="value" style="font-size:16px">{reading_str}</div>
                    <div class="sub">{label_str} · USGS {gauge["code"]}</div>
                </div>""", unsafe_allow_html=True)
    else:
        st.caption("No USGS stream gauges found within ~40 miles.")

    # Tides — only for coastal parks
    if t8_uc in TIDE_STATIONS and tides:
        station_id, station_name = TIDE_STATIONS[t8_uc]
        if tides:
            st.markdown(f"**Tides — {station_name} (NOAA)**")
            tide_cols = st.columns(min(len(tides), 4))
            for i, tide in enumerate(tides[:4]):
                t_time = tide.get("t", "")
                t_type = tide.get("type", "")
                t_ht   = tide.get("v", "")
                type_lbl  = "High Tide" if t_type == "H" else "Low Tide"
                type_emoji = "🌊" if t_type == "H" else "〰️"
                try:
                    fmt_time = pd.to_datetime(t_time).strftime("%a %-I:%M %p")
                except Exception:
                    fmt_time = t_time
                with tide_cols[i]:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">{type_emoji} {type_lbl}</div>
                        <div class="value" style="font-size:16px">{t_ht} ft</div>
                        <div class="sub">{fmt_time}</div>
                    </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # ── Webcams ───────────────────────────────────────────────────────────────
    if api_key and webcams:
        st.markdown('<div class="section-header">Webcams</div>', unsafe_allow_html=True)
        for cam in webcams[:6]:
            title    = cam.get("title", "Webcam")
            desc     = cam.get("description", "")
            url      = cam.get("url", "")
            img_url  = (cam.get("images") or [{}])[0].get("url", "")
            c1, c2 = st.columns([1, 3])
            with c1:
                if img_url:
                    st.image(img_url, use_container_width=True)
            with c2:
                st.markdown(f"**{title}**")
                if desc:
                    st.caption(desc[:200])
                if url:
                    st.markdown(f"[View live webcam]({url})")
        st.markdown("---")

    # ── Upcoming Events ───────────────────────────────────────────────────────
    if api_key:
        st.markdown('<div class="section-header">Upcoming Events</div>', unsafe_allow_html=True)
        if not events:
            st.caption("No upcoming events found for this park.")
        else:
            for ev in events[:8]:
                title   = ev.get("title", "")
                dates   = ev.get("dates", "")
                times   = ev.get("times", [{}])
                loc     = ev.get("location", "")
                desc    = ev.get("description", "")
                fee     = ev.get("isfree", "true")
                time_str = times[0].get("timestart", "")[:5] if times else ""
                st.markdown(f"""
                <div class="detail-card" style="margin-bottom:8px">
                    <strong>{title}</strong>
                    <span style="color:#7a9bbb;font-size:12px;margin-left:12px">
                        {dates} {time_str}{"  ·  " + loc if loc else ""}
                        {"  ·  Free" if str(fee).lower()=="true" else ""}
                    </span><br>
                    <small style="color:#7a9bbb">{(desc or "")[:180]}</small>
                </div>""", unsafe_allow_html=True)
    elif not api_key:
        st.caption("Enter NPS API key in the sidebar to load events and webcams.")

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.caption(
        "Weather & AQI: [Open-Meteo](https://open-meteo.com) (30 min cache)  ·  "
        "Stream gauges: [USGS Water Services](https://waterservices.usgs.gov) (15 min cache)  ·  "
        "Tides: [NOAA Tides & Currents](https://tidesandcurrents.noaa.gov) (1 h cache)  ·  "
        "Wildfires: [NIFC ArcGIS](https://www.nifc.gov) (30 min cache)  ·  "
        "Alerts & Events: [NPS Developer API](https://developer.nps.gov/api/v1)  ·  "
        f"Coords: {lat:.2f}°, {lon:.2f}°"
    )
