"""
Run this locally to fetch campsite availability data and save a preview CSV.

Usage:
    python fetch_campsite_preview.py

Output:
    campsite_preview.csv  — summary table for all 63 national parks
"""

import sys
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import os

sys.path.insert(0, str(Path(__file__).parent / "nps-seasonal-model" / "src"))

from campsites import build_park_facility_map, fetch_all_parks_stats

RIDB_KEY = os.getenv("RIDB_API_KEY", "")
if not RIDB_KEY:
    print("ERROR: RIDB_API_KEY not set. Add it to your .env file or set it as an env var.")
    sys.exit(1)

print("Step 1/2 — Discovering campgrounds via RIDB…")
park_map, fac_names = build_park_facility_map(RIDB_KEY)
print(f"  Found campgrounds for {len(park_map)} parks "
      f"({sum(len(v) for v in park_map.values())} facilities total)\n")

print("Step 2/2 — Fetching 30-day availability from Recreation.gov…")
print("  (this takes ~2 minutes — fetching ~500 API calls with rate limiting)\n")

def progress(i, total, name):
    if i % 5 == 0 or i == total:
        print(f"  [{i}/{total}] {name}")

df = fetch_all_parks_stats(park_map, fac_names, window_days=30, progress_callback=progress)

out = Path("campsite_preview.csv")
df.to_csv(out, index=False)

print(f"\nSaved → {out.resolve()}")
print(f"\n{'='*70}")
print(df[df["has_campgrounds"]].sort_values("pct_available", ascending=False)[[
    "park_name", "n_reservable_sites", "n_fcfs_sites",
    "avail_nights", "pct_available", "weekend_pct", "weekday_pct", "n_facilities"
]].to_string(index=False))
print(f"{'='*70}")
print(f"\nParks with NO Recreation.gov campgrounds ({len(df[~df['has_campgrounds']])} parks):")
print(", ".join(df[~df["has_campgrounds"]]["park_name"].tolist()))
