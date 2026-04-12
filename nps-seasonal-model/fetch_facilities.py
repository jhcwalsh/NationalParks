"""Populate the alert engine's park_facilities table with all campground
facility IDs from Recreation.gov's RIDB API.

Uses the existing ``campsites.discover_campground_facilities()`` function
which queries RIDB for every national park and returns facility IDs, names,
and site counts.

Usage:
    cd nps-seasonal-model
    python fetch_facilities.py

Requires RECREATION_GOV_API_KEY in .env or environment.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))

from campsites import NATIONAL_PARKS, discover_campground_facilities
from conditions import PARK_COORDS
from alert_engine.db import init_db, _db_path

import aiosqlite


def main() -> None:
    api_key = os.getenv("RECREATION_GOV_API_KEY", "")
    if not api_key:
        print("ERROR: Set RECREATION_GOV_API_KEY in .env or environment")
        sys.exit(1)

    print("Discovering campground facilities from RIDB...")
    print("(This makes several API calls and may take 1-2 minutes)\n")

    park_map, facility_names, facility_site_counts = discover_campground_facilities(api_key)

    # Build the insert list with coordinates from PARK_COORDS
    facilities: list[dict] = []
    for unit_code, fac_ids in sorted(park_map.items()):
        park_name = NATIONAL_PARKS.get(unit_code, unit_code)
        coords = PARK_COORDS.get(unit_code)
        lat, lon = coords if coords else (None, None)

        for fac_id in fac_ids:
            fac_name = facility_names.get(fac_id, f"Campground {fac_id}")
            facilities.append({
                "facility_id": fac_id,
                "park_code": unit_code,
                "facility_name": fac_name,
                "lat": lat,
                "lon": lon,
            })

    print(f"\nFound {len(facilities)} campgrounds across {len(park_map)} parks\n")

    # Insert into database
    asyncio.run(_insert_facilities(facilities))

    # Print summary
    print("\n--- Facilities by park ---")
    for unit_code in sorted(park_map.keys()):
        fac_ids = park_map[unit_code]
        names = [facility_names.get(fid, fid) for fid in fac_ids]
        print(f"  {unit_code}: {', '.join(names)}")

    parks_without = set(NATIONAL_PARKS.keys()) - set(park_map.keys())
    if parks_without:
        print(f"\n{len(parks_without)} parks with no campgrounds on Recreation.gov:")
        for code in sorted(parks_without):
            print(f"  {code}: {NATIONAL_PARKS[code]}")


async def _insert_facilities(facilities: list[dict]) -> None:
    await init_db()
    db_path = _db_path()
    async with aiosqlite.connect(db_path) as conn:
        inserted = 0
        for f in facilities:
            await conn.execute(
                """INSERT OR REPLACE INTO park_facilities
                   (facility_id, park_code, facility_name, lat, lon)
                   VALUES (?, ?, ?, ?, ?)""",
                (f["facility_id"], f["park_code"], f["facility_name"], f["lat"], f["lon"]),
            )
            inserted += 1
        await conn.commit()
        print(f"Inserted/updated {inserted} facilities in {db_path}")


if __name__ == "__main__":
    main()
