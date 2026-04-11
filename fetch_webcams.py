"""
Fetch webcam data for all 63 national parks from the NPS Developer API,
test every URL, and save the results to webcams.json.

Usage
-----
    python fetch_webcams.py           # fetch + test + save
    python fetch_webcams.py --no-test # skip URL testing (faster)

Requires NPS_API_KEY in .env or environment.

Workflow
--------
1. Run this script locally
2. Commit the resulting webcams.json to git
3. Push — Render will serve the pre-fetched data immediately (no API key needed)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, str(Path(__file__).parent / "nps-seasonal-model" / "src"))

import requests
from campsites import NATIONAL_PARKS

NPS_KEY = os.getenv("NPS_API_KEY", "")
if not NPS_KEY:
    print("ERROR: NPS_API_KEY not set in .env or environment.")
    sys.exit(1)

BASE_URL   = "https://developer.nps.gov/api/v1"
OUT        = Path("webcams.json")
NO_TEST    = "--no-test" in sys.argv
FETCHED_AT = datetime.utcnow().isoformat(timespec="seconds") + "Z"


# ── Helpers ────────────────────────────────────────────────────────────────────

def nps_webcams_for_park(park_code: str) -> list[dict]:
    """Fetch all webcams for one park from the NPS API."""
    params = {"api_key": NPS_KEY, "parkCode": park_code.lower(), "limit": 50, "start": 0}
    results: list[dict] = []
    while True:
        r = requests.get(f"{BASE_URL}/webcams", params=params, timeout=20)
        r.raise_for_status()
        body  = r.json()
        data  = body.get("data", [])
        total = int(body.get("total", 0))
        results.extend(data)
        if len(results) >= total or not data:
            break
        params["start"] = len(results)
    return results


def test_url(url: str, timeout: int = 10) -> dict:
    """
    HEAD-check a URL.  Returns:
        {"ok": True,  "status": 200}
        {"ok": False, "status": 404, "error": "..."}
    """
    if not url:
        return {"ok": False, "status": None, "error": "no url"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        ok = r.status_code < 400
        return {"ok": ok, "status": r.status_code}
    except Exception as e:
        return {"ok": False, "status": None, "error": str(e)[:120]}


# ── Fetch ──────────────────────────────────────────────────────────────────────

print(f"Fetching webcams for {len(NATIONAL_PARKS)} national parks…\n")

all_webcams: list[dict] = []
seen_ids:    set[str]   = set()   # deduplicate (some cams appear in multiple parks)

for idx, (unit_code, park_name) in enumerate(NATIONAL_PARKS.items(), 1):
    print(f"  [{idx:2d}/{len(NATIONAL_PARKS)}] {park_name} ({unit_code})…", end=" ", flush=True)
    try:
        cams = nps_webcams_for_park(unit_code)
        new  = [c for c in cams if c.get("id") not in seen_ids]
        for c in new:
            seen_ids.add(c["id"])
            c["_unit_code"]  = unit_code
            c["_park_name"]  = park_name
            c["_fetched_at"] = FETCHED_AT
        all_webcams.extend(new)
        print(f"{len(new)} webcams")
    except Exception as e:
        print(f"ERROR — {e}")
    time.sleep(0.15)   # be polite to the NPS API

print(f"\nTotal unique webcams fetched: {len(all_webcams)}\n")


# ── Test URLs ──────────────────────────────────────────────────────────────────

if NO_TEST:
    print("Skipping URL tests (--no-test).\n")
    for cam in all_webcams:
        cam["_url_ok"]    = None
        cam["_url_status"] = None
        cam["_url_error"]  = None
else:
    print(f"Testing {len(all_webcams)} webcam URLs…\n")
    ok_count = fail_count = 0
    for i, cam in enumerate(all_webcams, 1):
        url = cam.get("url", "")
        result = test_url(url)
        cam["_url_ok"]    = result["ok"]
        cam["_url_status"] = result.get("status")
        cam["_url_error"]  = result.get("error")
        status_char = "✓" if result["ok"] else "✗"
        if result["ok"]:
            ok_count += 1
        else:
            fail_count += 1
        print(
            f"  [{i:3d}/{len(all_webcams)}] {status_char} "
            f"HTTP {result.get('status','???'):>3}  "
            f"{cam['_park_name'][:30]:<30}  {cam.get('title','')[:40]}"
            + (f"\n           → {result.get('error','')}" if not result["ok"] else "")
        )
        time.sleep(0.1)

    print(f"\nURL test summary: {ok_count} OK  /  {fail_count} failed  "
          f"(out of {len(all_webcams)} total)\n")


# ── Save ───────────────────────────────────────────────────────────────────────

payload = {
    "fetched_at": FETCHED_AT,
    "total":      len(all_webcams),
    "webcams":    all_webcams,
}
OUT.write_text(json.dumps(payload, indent=2))
print(f"Saved → {OUT.resolve()}")
print("\nNext step: commit webcams.json to git, then push to deploy on Render.")
