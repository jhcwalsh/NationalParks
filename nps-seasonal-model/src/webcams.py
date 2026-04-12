"""
Hardcoded webcam catalog for the 63 US National Parks.

To refresh this data
--------------------
1. Set NPS_API_KEY in your environment
2. Run:  python fetch_webcams.py
3. That produces webcams.json (used by the Streamlit dashboard)
4. Then update WEBCAMS below from the live data, or replace this
   module with a loader that reads webcams.json at startup.

Each entry: (title, url)
  - title: human-readable camera name
  - url: direct link to the NPS webcam page or stream

Source: NPS Developer API /webcams endpoint + public NPS webcam pages.
Last manual update: 2026-04-12.
"""

from __future__ import annotations

from pathlib import Path

# park_code → list of (title, url)
WEBCAMS: dict[str, list[tuple[str, str]]] = {
    "ACAD": [
        ("Jordan Pond House", "https://www.nps.gov/acad/learn/photosmultimedia/webcams.htm"),
        ("Cadillac Mountain Summit", "https://www.nps.gov/acad/learn/photosmultimedia/webcams.htm"),
        ("Otter Cove", "https://www.nps.gov/acad/learn/photosmultimedia/webcams.htm"),
    ],
    "ARCH": [
        ("Arches Entrance Station", "https://www.nps.gov/arch/learn/photosmultimedia/webcams.htm"),
    ],
    "BADL": [
        ("Badlands Panorama", "https://www.nps.gov/badl/learn/photosmultimedia/webcams.htm"),
    ],
    "BIBE": [
        ("Chisos Basin", "https://www.nps.gov/bibe/learn/photosmultimedia/webcams.htm"),
    ],
    "BRCA": [
        ("Bryce Amphitheater", "https://www.nps.gov/brca/learn/photosmultimedia/webcams.htm"),
    ],
    "CRLA": [
        ("Crater Lake Rim", "https://www.nps.gov/crla/learn/photosmultimedia/webcams.htm"),
    ],
    "DENA": [
        ("Denali (Mount McKinley)", "https://www.nps.gov/dena/learn/photosmultimedia/webcams.htm"),
        ("Savage River", "https://www.nps.gov/dena/learn/photosmultimedia/webcams.htm"),
    ],
    "DEVA": [
        ("Zabriskie Point", "https://www.nps.gov/deva/learn/photosmultimedia/webcams.htm"),
    ],
    "EVER": [
        ("Flamingo Visitor Center", "https://www.nps.gov/ever/learn/photosmultimedia/webcams.htm"),
        ("Shark Valley", "https://www.nps.gov/ever/learn/photosmultimedia/webcams.htm"),
    ],
    "GLAC": [
        ("St. Mary Entrance", "https://www.nps.gov/glac/learn/photosmultimedia/webcams.htm"),
        ("Apgar Village", "https://www.nps.gov/glac/learn/photosmultimedia/webcams.htm"),
        ("Logan Pass", "https://www.nps.gov/glac/learn/photosmultimedia/webcams.htm"),
        ("Lake McDonald", "https://www.nps.gov/glac/learn/photosmultimedia/webcams.htm"),
    ],
    "GRCA": [
        ("Yavapai Point — South Rim", "https://www.nps.gov/grca/learn/photosmultimedia/webcams.htm"),
        ("Desert View — South Rim", "https://www.nps.gov/grca/learn/photosmultimedia/webcams.htm"),
        ("Bright Angel Trail", "https://www.nps.gov/grca/learn/photosmultimedia/webcams.htm"),
    ],
    "GRSM": [
        ("Purchase Knob", "https://www.nps.gov/grsm/learn/photosmultimedia/webcams.htm"),
        ("Look Rock", "https://www.nps.gov/grsm/learn/photosmultimedia/webcams.htm"),
        ("Clingmans Dome Tower", "https://www.nps.gov/grsm/learn/photosmultimedia/webcams.htm"),
    ],
    "GRTE": [
        ("Jenny Lake", "https://www.nps.gov/grte/learn/photosmultimedia/webcams.htm"),
        ("Flagg Ranch", "https://www.nps.gov/grte/learn/photosmultimedia/webcams.htm"),
    ],
    "HALE": [
        ("Haleakala Summit", "https://www.nps.gov/hale/learn/photosmultimedia/webcams.htm"),
    ],
    "HAVO": [
        ("Kilauea Summit", "https://www.nps.gov/havo/learn/photosmultimedia/webcams.htm"),
        ("Halema'uma'u Crater", "https://www.nps.gov/havo/learn/photosmultimedia/webcams.htm"),
    ],
    "JOTR": [
        ("Keys View", "https://www.nps.gov/jotr/learn/photosmultimedia/webcams.htm"),
    ],
    "LAVO": [
        ("Lassen Peak", "https://www.nps.gov/lavo/learn/photosmultimedia/webcams.htm"),
    ],
    "MACA": [
        ("Mammoth Cave Entrance", "https://www.nps.gov/maca/learn/photosmultimedia/webcams.htm"),
    ],
    "MORA": [
        ("Paradise — Mount Rainier", "https://www.nps.gov/mora/learn/photosmultimedia/webcams.htm"),
        ("Sunrise", "https://www.nps.gov/mora/learn/photosmultimedia/webcams.htm"),
    ],
    "OLYM": [
        ("Hurricane Ridge", "https://www.nps.gov/olym/learn/photosmultimedia/webcams.htm"),
    ],
    "ROMO": [
        ("Longs Peak", "https://www.nps.gov/romo/learn/photosmultimedia/webcams.htm"),
        ("Alpine Visitor Center", "https://www.nps.gov/romo/learn/photosmultimedia/webcams.htm"),
    ],
    "SHEN": [
        ("Big Meadows", "https://www.nps.gov/shen/learn/photosmultimedia/webcams.htm"),
    ],
    "VIIS": [
        ("Cruz Bay", "https://www.nps.gov/viis/learn/photosmultimedia/webcams.htm"),
    ],
    "VOYA": [
        ("Rainy Lake Visitor Center", "https://www.nps.gov/voya/learn/photosmultimedia/webcams.htm"),
    ],
    "YELL": [
        ("Old Faithful Geyser", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
        ("Mount Washburn", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
        ("Mammoth Hot Springs", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
        ("Roosevelt Arch", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
        ("West Entrance", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
        ("North Entrance", "https://www.nps.gov/yell/learn/photosmultimedia/webcams.htm"),
    ],
    "YOSE": [
        ("Half Dome", "https://www.nps.gov/yose/learn/photosmultimedia/webcams.htm"),
        ("Yosemite Falls", "https://www.nps.gov/yose/learn/photosmultimedia/webcams.htm"),
        ("El Capitan", "https://www.nps.gov/yose/learn/photosmultimedia/webcams.htm"),
        ("Yosemite Valley — Turtleback Dome", "https://www.nps.gov/yose/learn/photosmultimedia/webcams.htm"),
    ],
    "ZION": [
        ("Zion Canyon", "https://www.nps.gov/zion/learn/photosmultimedia/webcams.htm"),
    ],
}


# Fallback NPS webcam page URL for any park not in the curated list.
NPS_WEBCAM_PAGE = "https://www.nps.gov/{code}/learn/photosmultimedia/webcams.htm"

# ── Manifest loader (generated by fetch_webcam_images.py) ─────────────────────

MANIFEST_PATH = Path(__file__).parent.parent / "static" / "webcam-images" / "manifest.json"

_manifest_cache: dict | None = None


def _load_manifest() -> dict:
    global _manifest_cache
    if _manifest_cache is not None:
        return _manifest_cache
    if not MANIFEST_PATH.exists():
        _manifest_cache = {}
        return _manifest_cache
    import json
    try:
        data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        # Index by park code for fast lookup
        by_park: dict[str, list[dict]] = {}
        for cam in data.get("webcams", []):
            pc = cam.get("park_code", "").upper()
            by_park.setdefault(pc, []).append(cam)
        _manifest_cache = {
            "fetched_at": data.get("fetched_at", ""),
            "by_park": by_park,
        }
    except Exception:
        _manifest_cache = {}
    return _manifest_cache


def get_webcams(unit_code: str) -> dict:
    """
    Return webcam info for a park. Prefers the downloaded manifest
    (with local images) if available, otherwise falls back to the
    hardcoded WEBCAMS dict.

    Returns:
        {
            "webcams": [ {"title", "url", "image"}, ... ],
            "nps_page": "https://www.nps.gov/.../webcams.htm",
            "fetched_at": "...",
            "note": "..."
        }
    """
    code = unit_code.upper()
    nps_page = NPS_WEBCAM_PAGE.format(code=code.lower())
    manifest = _load_manifest()

    # ── Try manifest first (has images) ─────────────────────────────────────
    if manifest and manifest.get("by_park", {}).get(code):
        cams = manifest["by_park"][code]
        return {
            "webcams": [
                {
                    "title": c.get("title", "Webcam"),
                    "url": c.get("cam_url", ""),
                    "image": f"/webcam-images/{c['local_image']}" if c.get("local_image") else None,
                    "status": c.get("status", ""),
                    "is_streaming": c.get("is_streaming", False),
                }
                for c in cams
            ],
            "nps_page": nps_page,
            "fetched_at": manifest.get("fetched_at", ""),
            "note": (
                "To refresh webcam images, run: "
                "python fetch_webcam_images.py (requires NPS_API_KEY)"
            ),
        }

    # ── Fallback to hardcoded list ──────────────────────────────────────────
    entries = WEBCAMS.get(code, [])
    return {
        "webcams": [
            {"title": t, "url": u, "image": None}
            for t, u in entries
        ],
        "nps_page": nps_page,
        "fetched_at": None,
        "note": (
            "Webcam list is manually curated. To get images, run: "
            "python fetch_webcam_images.py (requires NPS_API_KEY)"
        ),
    }
