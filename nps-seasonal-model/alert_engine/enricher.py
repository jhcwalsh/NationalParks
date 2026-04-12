"""Best-effort conditions enrichment for alert messages.

All calls are async with short timeouts. Failures return None — enrichment
must never block or prevent an alert from being sent.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# Facility → (lat, lon) for AQI lookups.
# Seeded with the priority facilities; extend as needed.
FACILITY_COORDS: dict[str, tuple[float, float]] = {
    "232447": (37.7456, -119.5936),   # Yosemite Valley - Upper Pines
    "232450": (37.7399, -119.5652),   # Yosemite Valley - Lower Pines
    "232449": (37.7440, -119.5652),   # Yosemite Valley - North Pines
    "234869": (36.0561, -112.1220),   # Grand Canyon - Mather
    "272265": (37.2090, -112.9801),   # Zion - Watchman
    "272267": (37.2050, -112.9800),   # Zion - South
    "251869": (48.7596, -113.7870),   # Glacier - Apgar
    "232493": (48.5000, -113.9800),   # Glacier - Fish Creek
}

AIRNOW_API_KEY = os.getenv("AIRNOW_API_KEY", "")


async def get_crowd_score(facility_id: str, target_date: date) -> Optional[dict[str, Any]]:
    """Query the local ParkPulse busyness API (best-effort, 3s timeout)."""
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                f"http://localhost:8000/api/busyness/{facility_id}",
                params={"date": target_date.isoformat()},
            )
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "score": data.get("score"),
                    "label": data.get("label"),
                    "park_name": data.get("park_name"),
                }
    except Exception as exc:
        logger.debug("Crowd score lookup failed for %s: %s", facility_id, exc)
    return None


async def get_aqi(lat: float, lon: float) -> Optional[dict[str, Any]]:
    """Query AirNow API for current AQI (best-effort, 3s timeout)."""
    if not AIRNOW_API_KEY:
        return None
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                "https://www.airnowapi.org/aq/observation/latLong/current/",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "distance": 25,
                    "format": "application/json",
                    "API_KEY": AIRNOW_API_KEY,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if data and isinstance(data, list) and len(data) > 0:
                    # Find the primary pollutant entry (highest AQI)
                    best = max(data, key=lambda x: x.get("AQI", 0))
                    return {
                        "aqi": best.get("AQI"),
                        "category": best.get("Category", {}).get("Name"),
                        "pollutant": best.get("ParameterName"),
                    }
    except Exception as exc:
        logger.debug("AQI lookup failed for (%s, %s): %s", lat, lon, exc)
    return None


async def get_conditions(facility_id: str, target_date: date) -> dict[str, Any]:
    """Combine crowd score + AQI into a single conditions dict.

    Returns an empty dict if both sources fail — never raises.
    """
    import asyncio

    coords = FACILITY_COORDS.get(facility_id)

    # Run both lookups concurrently
    crowd_task = get_crowd_score(facility_id, target_date)
    aqi_task = get_aqi(coords[0], coords[1]) if coords else _noop()

    crowd, aqi = await asyncio.gather(crowd_task, aqi_task, return_exceptions=True)

    result: dict[str, Any] = {}

    if isinstance(crowd, dict) and crowd:
        result["crowd_score"] = crowd.get("score")
        result["crowd_label"] = crowd.get("label")

    if isinstance(aqi, dict) and aqi:
        result["aqi"] = aqi.get("aqi")
        result["aqi_category"] = aqi.get("category")

    return result


async def _noop() -> None:
    return None
