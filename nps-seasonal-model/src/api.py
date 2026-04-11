"""
FastAPI backend for the NPS seasonal busyness model and the mobile
"National Parks Now" web app.

Endpoints
---------
GET /parks                             — list the 63 National Parks
GET /parks/{unit_code}/busyness        — full seasonal model
GET /parks/{unit_code}/busyness?month= — single-month snapshot
GET /parks/{unit_code}/overview        — mobile Overview payload
GET /parks/compare?parks=A,B&month=    — multi-park comparison
GET /parks/recommendations?state=&month=&max_score= — filtered recommendations
GET /health                            — health check
GET /                                  — static mobile web app

Run
---
    uvicorn src.api:app --reload --port 8000
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
import db
import mobile
import model as mdl
from campsites import NATIONAL_PARKS

app = FastAPI(
    title="NPS Seasonal Busyness API",
    description="Historical seasonal busyness model and mobile overview for US National Parks",
    version="1.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_PATH = ROOT / "data" / "nps.db"
STATIC_DIR = ROOT / "static"


def _get_db() -> Path:
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Database not initialised. Run: python src/ingest.py --years 2014-2024",
        )
    return DB_PATH


def _require_national_park(unit_code: str) -> str:
    code = unit_code.upper()
    if code not in NATIONAL_PARKS:
        raise HTTPException(
            status_code=404,
            detail=f"{code} is not one of the 63 US National Parks",
        )
    return code


# ── /parks ────────────────────────────────────────────────────────────────────

@app.get("/parks")
def list_parks(
    state: str | None = Query(None, description="Filter by state code, e.g. CA"),
):
    """
    List the canonical 63 US National Parks. Merges the hardcoded
    NATIONAL_PARKS catalog with any state / type info the seasonal
    database already has.
    """
    db_parks: dict[str, dict] = {}
    if DB_PATH.exists():
        df = db.get_all_parks(DB_PATH)
        if not df.empty:
            db_parks = {
                row["unit_code"]: row.to_dict() for _, row in df.iterrows()
            }

    out: list[dict] = []
    for code, name in NATIONAL_PARKS.items():
        row = db_parks.get(code, {})
        park_state = row.get("state") or ""
        if state and state.upper() not in park_state.upper():
            continue
        out.append(
            {
                "unit_code": code,
                "name": name,
                "state": park_state,
                "type": row.get("type") or "National Park",
            }
        )
    out.sort(key=lambda p: p["name"])
    return out


# ── /parks/compare  (must be declared before /{unit_code}) ───────────────────

@app.get("/parks/compare")
def compare_parks(
    parks: Annotated[str, Query(description="Comma-separated unit codes, e.g. YOSE,GRCA,ZION")],
    month: int | None = Query(None, ge=1, le=12, description="Month number 1–12"),
):
    """Compare busyness for multiple parks, optionally for a specific month."""
    dp = _get_db()
    unit_codes = [uc.strip().upper() for uc in parks.split(",") if uc.strip()]
    if not unit_codes:
        raise HTTPException(status_code=400, detail="Provide at least one park code")
    for uc in unit_codes:
        _require_national_park(uc)
    results = mdl.compare_parks(unit_codes, month=month, db_path=dp)
    if not results:
        raise HTTPException(status_code=404, detail="No data found for requested parks")
    return results


# ── /parks/recommendations ────────────────────────────────────────────────────

@app.get("/parks/recommendations")
def recommend_parks(
    state: str | None = Query(None, description="Filter by state code"),
    month: int | None = Query(None, ge=1, le=12),
    max_score: float = Query(50.0, ge=0, le=100, description="Maximum busyness score"),
):
    """Find parks with busyness below max_score for a given month."""
    dp = _get_db()
    results = mdl.recommend_parks(
        db_path=dp, state=state, month=month, max_score=max_score
    )
    # Constrain to national parks only
    return [r for r in results if r.get("unit_code", "").upper() in NATIONAL_PARKS]


# ── /parks/{unit_code}/busyness ───────────────────────────────────────────────

@app.get("/parks/{unit_code}/busyness")
def park_busyness(
    unit_code: str,
    month: int | None = Query(None, ge=1, le=12, description="Month number 1–12"),
):
    """
    Full seasonal model for a park, or a single-month snapshot if ?month= is set.
    """
    dp = _get_db()
    uc = _require_national_park(unit_code)

    if month is not None:
        result = mdl.get_month_busyness(uc, month, db_path=dp)
        if result is None:
            raise HTTPException(
                status_code=404,
                detail=f"No data for park {uc} month {month}",
            )
        return result

    park_model = mdl.build_busyness_model(uc, db_path=dp)
    if park_model is None:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for park unit code: {uc}",
        )
    return park_model.to_dict()


# ── /parks/{unit_code}/overview  (mobile Overview screen) ─────────────────────

@app.get("/parks/{unit_code}/overview")
def park_overview(unit_code: str):
    """
    Aggregated payload for the mobile National Parks Now Overview screen.
    Every inner section can independently be null — the endpoint always
    returns 200 if the unit code is a valid National Park.
    """
    code = _require_national_park(unit_code)
    db_path = DB_PATH if DB_PATH.exists() else None
    payload = mobile.assemble_overview(code, db_path=db_path)
    if payload is None:
        # Shouldn't happen — _require_national_park already validated.
        raise HTTPException(status_code=404, detail=f"Unknown park: {code}")
    return payload


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    db_exists = DB_PATH.exists()
    park_count = 0
    if db_exists:
        parks_df = db.get_all_parks(DB_PATH)
        # Restrict to national parks
        if not parks_df.empty:
            park_count = int(
                parks_df["unit_code"].str.upper().isin(NATIONAL_PARKS.keys()).sum()
            )
    return {
        "status": "ok",
        "db_exists": db_exists,
        "national_parks_total": len(NATIONAL_PARKS),
        "national_parks_with_model_data": park_count,
    }


# ── Static mobile app (mounted last so API routes win) ───────────────────────

if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="mobile")
