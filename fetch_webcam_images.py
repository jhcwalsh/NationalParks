"""
Fetch webcam metadata + download latest images for all 63 national parks.

Usage
-----
    python fetch_webcam_images.py              # fetch metadata + images
    python fetch_webcam_images.py --no-images  # metadata only (faster)

Requires NPS_API_KEY in .env or environment.

Output
------
    nps-seasonal-model/static/webcam-images/
        manifest.json       — metadata for every webcam
        YOSE_half-dome.jpg  — downloaded images (one per webcam)
        ...

Workflow
--------
1. Run this script locally
2. Commit the resulting webcam-images/ directory
3. Push — Render will serve the images as static files
4. Re-run periodically to refresh snapshots

Images are point-in-time snapshots from NPS webcams. They get stale —
re-run the script to update them.
"""

from __future__ import annotations

import json
import os
import re
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
OUT_DIR    = Path(__file__).parent / "nps-seasonal-model" / "static" / "webcam-images"
NO_IMAGES  = "--no-images" in sys.argv
FETCHED_AT = datetime.utcnow().isoformat(timespec="seconds") + "Z"


def slugify(text: str) -> str:
    """Convert a webcam title to a safe filename slug."""
    s = text.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = s.strip("-")
    return s[:60] or "webcam"


def fetch_webcams_for_park(park_code: str) -> list[dict]:
    """Fetch all webcams for one park from the NPS API."""
    params = {"api_key": NPS_KEY, "parkCode": park_code.lower(), "limit": 50}
    try:
        r = requests.get(f"{BASE_URL}/webcams", params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        print(f"    API error: {e}")
        return []


def download_image(url: str, dest: Path, timeout: int = 15) -> bool:
    """Download an image URL to a local file. Returns True on success."""
    try:
        r = requests.get(url, timeout=timeout, stream=True)
        r.raise_for_status()
        content_type = r.headers.get("content-type", "")
        if "image" not in content_type and "octet" not in content_type:
            return False
        with dest.open("wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return True
    except Exception:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"Fetching webcams for {len(NATIONAL_PARKS)} national parks…")
print(f"Output: {OUT_DIR.resolve()}\n")

manifest: list[dict] = []
seen_ids: set[str] = set()
image_count = 0
skip_count = 0

for idx, (unit_code, park_name) in enumerate(sorted(NATIONAL_PARKS.items()), 1):
    print(f"  [{idx:2d}/{len(NATIONAL_PARKS)}] {park_name} ({unit_code})…", end=" ", flush=True)
    cams = fetch_webcams_for_park(unit_code)

    park_cam_count = 0
    for cam in cams:
        cam_id = cam.get("id", "")
        if cam_id in seen_ids:
            continue
        seen_ids.add(cam_id)

        title = (cam.get("title") or "Webcam").strip()
        cam_url = cam.get("url", "")
        images = cam.get("images") or []
        img_url = images[0].get("url", "") if images else ""

        # Determine status
        status_obj = cam.get("status") or {}
        if isinstance(status_obj, dict):
            status = status_obj.get("status", "Unknown")
        else:
            status = str(status_obj)

        is_streaming = str(cam.get("isStreaming", "")).lower() == "true"

        # Build local image filename
        slug = slugify(title)
        img_filename = f"{unit_code}_{slug}.jpg"
        img_path = OUT_DIR / img_filename

        # Download image
        got_image = False
        if not NO_IMAGES and img_url:
            got_image = download_image(img_url, img_path)
            if got_image:
                image_count += 1
            else:
                skip_count += 1

        entry = {
            "park_code": unit_code,
            "park_name": park_name,
            "title": title,
            "cam_url": cam_url,
            "image_url": img_url,
            "local_image": img_filename if got_image else None,
            "status": status,
            "is_streaming": is_streaming,
        }
        manifest.append(entry)
        park_cam_count += 1

    print(f"{park_cam_count} webcams")
    time.sleep(0.15)  # rate-limit courtesy

# ── Save manifest ─────────────────────────────────────────────────────────────

manifest_path = OUT_DIR / "manifest.json"
payload = {
    "fetched_at": FETCHED_AT,
    "total_webcams": len(manifest),
    "images_downloaded": image_count,
    "webcams": manifest,
}
manifest_path.write_text(json.dumps(payload, indent=2))

print(f"\n{'='*60}")
print(f"Total webcams:      {len(manifest)}")
print(f"Images downloaded:  {image_count}")
print(f"Images failed:      {skip_count}")
print(f"Manifest saved:     {manifest_path}")
print(f"\nNext steps:")
print(f"  1. git add nps-seasonal-model/static/webcam-images/")
print(f"  2. git commit -m 'Update webcam snapshots ({FETCHED_AT})'")
print(f"  3. git push")
