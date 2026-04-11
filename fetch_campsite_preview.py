"""
Fetch campsite availability for all 63 national parks and save a preview CSV.

Features
--------
- Checkpointing: saves each park as it completes — safe to interrupt and restart
- Resumes automatically from where it left off (skips already-fetched parks)
- Prints a summary table on completion

Usage:
    python fetch_campsite_preview.py           # run / resume
    python fetch_campsite_preview.py --reset   # start fresh, ignoring checkpoint
"""

import sys
import os
from pathlib import Path

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).parent / "nps-seasonal-model" / "src"))

import pandas as pd
from campsites import (
    NATIONAL_PARKS,
    build_park_facility_map,
    fetch_park_campsite_stats,
)

CHECKPOINT = Path("campsite_preview.csv")
OUT        = Path("campsite_preview.csv")

RIDB_KEY = os.getenv("RIDB_API_KEY", "")
if not RIDB_KEY:
    print("ERROR: RIDB_API_KEY not set in .env or environment.")
    sys.exit(1)

reset = "--reset" in sys.argv
if reset and CHECKPOINT.exists():
    CHECKPOINT.unlink()
    print("Checkpoint cleared — starting fresh.\n")

# ── Load checkpoint ────────────────────────────────────────────────────────────
done: set[str] = set()
rows: list[dict] = []

if CHECKPOINT.exists():
    existing = pd.read_csv(CHECKPOINT)
    done = set(existing["unit_code"].tolist())
    rows = existing.to_dict("records")
    print(f"Resuming from checkpoint — {len(done)} / {len(NATIONAL_PARKS)} parks already done.\n")

# ── Discover campground facility IDs ──────────────────────────────────────────
print("Step 1/2 — Discovering campgrounds via RIDB…")
park_map, fac_names = build_park_facility_map(RIDB_KEY)
print(f"  {len(park_map)} parks with campgrounds  "
      f"({sum(len(v) for v in park_map.values())} facilities total)\n")

# ── Fetch availability park-by-park ───────────────────────────────────────────
remaining = [c for c in NATIONAL_PARKS if c not in done]
total     = len(NATIONAL_PARKS)

print(f"Step 2/2 — Fetching 30-day availability ({len(remaining)} parks remaining)…")
print("  Interrupt at any time with Ctrl+C — progress is saved automatically.\n")

for idx, unit_code in enumerate(remaining, start=len(done) + 1):
    park_name    = NATIONAL_PARKS[unit_code]
    facility_ids = park_map.get(unit_code, [])

    print(f"  [{idx}/{total}] {park_name} "
          f"({'no Rec.gov campgrounds' if not facility_ids else f'{len(facility_ids)} campground(s)'})")

    if facility_ids:
        try:
            ps = fetch_park_campsite_stats(unit_code, facility_ids, fac_names, window_days=30)
            rows.append({
                "unit_code":          unit_code,
                "park_name":          park_name,
                "n_reservable_sites": ps.n_reservable_sites,
                "n_fcfs_sites":       ps.n_fcfs_sites,
                "avail_nights":       ps.avail_nights,
                "total_nights":       ps.total_nights,
                "pct_available":      ps.pct_available,
                "weekend_pct":        ps.weekend_pct,
                "weekday_pct":        ps.weekday_pct,
                "has_campgrounds":    True,
                "n_facilities":       len(ps.facilities),
            })
        except KeyboardInterrupt:
            print("\n\nInterrupted — saving checkpoint…")
            pd.DataFrame(rows).to_csv(CHECKPOINT, index=False)
            print(f"Saved {len(rows)} parks to {CHECKPOINT}. Run again to resume.")
            sys.exit(0)
        except Exception as e:
            print(f"    ERROR: {e} — skipping")
            rows.append({
                "unit_code": unit_code, "park_name": park_name,
                "has_campgrounds": True, "n_facilities": len(facility_ids),
                "n_reservable_sites": 0, "n_fcfs_sites": 0,
                "avail_nights": 0, "total_nights": 0,
                "pct_available": None, "weekend_pct": None, "weekday_pct": None,
            })
    else:
        rows.append({
            "unit_code": unit_code, "park_name": park_name,
            "has_campgrounds": False, "n_facilities": 0,
            "n_reservable_sites": 0, "n_fcfs_sites": 0,
            "avail_nights": 0, "total_nights": 0,
            "pct_available": None, "weekend_pct": None, "weekday_pct": None,
        })

    # Save after every park
    pd.DataFrame(rows).to_csv(CHECKPOINT, index=False)

# ── Summary ───────────────────────────────────────────────────────────────────
df = pd.DataFrame(rows)
df.to_csv(OUT, index=False)

print(f"\nComplete — saved {OUT.resolve()}\n")
print("=" * 80)

has_camps = df[df["has_campgrounds"]].sort_values("pct_available", ascending=False)
print(has_camps[[
    "park_name", "n_reservable_sites", "n_fcfs_sites",
    "avail_nights", "pct_available", "weekend_pct", "weekday_pct", "n_facilities",
]].to_string(index=False))

print("\n" + "=" * 80)
no_camps = df[~df["has_campgrounds"]]["park_name"].tolist()
print(f"\n{len(no_camps)} parks with no Recreation.gov campgrounds:")
print(", ".join(no_camps))
