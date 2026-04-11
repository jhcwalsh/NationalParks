"""
Shared park-conditions helpers: static coordinates + live weather / AQI /
wildfire loaders.

These functions are imported by both the Streamlit dashboard
(`nps_dashboard.py`) and the FastAPI mobile service (`src/api.py`) so
there is one copy of each external-API call.

None of these functions have any Streamlit dependencies — callers that
need caching should wrap them.
"""

from __future__ import annotations

import math
import time
from typing import Any

import requests

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


# ── WMO weather-code → short description (no emoji, for JSON APIs) ────────────
WMO_DESCRIPTION: dict[int, str] = {
    0: "Clear",           1: "Mainly clear",    2: "Partly cloudy",   3: "Overcast",
    45: "Fog",            48: "Icy fog",
    51: "Light drizzle",  53: "Drizzle",        55: "Heavy drizzle",
    61: "Light rain",     63: "Rain",           65: "Heavy rain",
    71: "Light snow",     73: "Snow",           75: "Heavy snow",
    77: "Snow grains",
    80: "Light showers",  81: "Showers",        82: "Heavy showers",
    85: "Snow showers",   86: "Heavy snow showers",
    95: "Thunderstorm",   96: "Thunderstorm + hail", 99: "Thunderstorm + hail",
}


def describe_weather_code(code: int | float | None) -> str:
    if code is None:
        return "Unknown"
    try:
        return WMO_DESCRIPTION.get(int(code), "Unknown")
    except (TypeError, ValueError):
        return "Unknown"


# ── Live data loaders ─────────────────────────────────────────────────────────

def load_weather(lat: float, lon: float) -> dict[str, Any]:
    """Current conditions + 7-day forecast from Open-Meteo (no API key needed)."""
    last_exc = ""
    for attempt in range(3):
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
                timeout=20,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = f"{type(e).__name__}: {e}"
            if attempt < 2:
                time.sleep(2 ** attempt)
    return {"_error": last_exc}


def load_aqi(lat: float, lon: float) -> dict[str, Any]:
    """Current AQI + pollutant breakdown from Open-Meteo Air Quality (no key needed)."""
    last_exc = ""
    for attempt in range(3):
        try:
            r = requests.get(
                "https://air-quality-api.open-meteo.com/v1/air-quality",
                params={
                    "latitude": lat, "longitude": lon,
                    "current": "us_aqi,pm10,pm2_5,ozone,carbon_monoxide",
                    "timezone": "auto",
                },
                timeout=20,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = f"{type(e).__name__}: {e}"
            if attempt < 2:
                time.sleep(2 ** attempt)
    return {"_error": last_exc}


def load_active_fires(lat: float, lon: float) -> list[dict[str, Any]]:
    """Active wildfire incidents within ~70 miles from NIFC (no key needed)."""
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
                "returnGeometry": "true",
                "outSR":          "4326",
                "f":              "json",
                "resultRecordCount": 10,
            },
            timeout=15,
        )
        r.raise_for_status()
        features = r.json().get("features", [])
        out: list[dict[str, Any]] = []
        for f in features:
            attrs = dict(f.get("attributes", {}))
            geom = f.get("geometry") or {}
            # Point features have x/y; polygon features have rings - use centroid
            if "x" in geom and "y" in geom:
                attrs["_lat"] = geom.get("y")
                attrs["_lon"] = geom.get("x")
            elif "rings" in geom and geom["rings"]:
                ring = geom["rings"][0]
                if ring:
                    attrs["_lat"] = sum(p[1] for p in ring) / len(ring)
                    attrs["_lon"] = sum(p[0] for p in ring) / len(ring)
            out.append(attrs)
        return out
    except Exception:
        return []


# ── Geo utilities ─────────────────────────────────────────────────────────────

def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in statute miles."""
    r_miles = 3958.7613
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r_miles * math.asin(math.sqrt(a))


def bearing_to_cardinal(lat1: float, lon1: float, lat2: float, lon2: float) -> str:
    """Compass direction (N, NE, E, …) from point 1 to point 2."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    deg = (math.degrees(math.atan2(x, y)) + 360) % 360
    return ["N", "NE", "E", "SE", "S", "SW", "W", "NW"][round(deg / 45) % 8]


def aqi_label(aqi: float | int | None) -> str:
    if aqi is None:
        return "Unknown"
    try:
        v = float(aqi)
    except (TypeError, ValueError):
        return "Unknown"
    if v <= 50:  return "Good"
    if v <= 100: return "Moderate"
    if v <= 150: return "Unhealthy for Sensitive Groups"
    if v <= 200: return "Unhealthy"
    if v <= 300: return "Very Unhealthy"
    return "Hazardous"
