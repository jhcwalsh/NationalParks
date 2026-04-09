"""
FastAPI backend for the NPS seasonal busyness model.

Endpoints
---------
GET /parks                             — list all parks
GET /parks/{unit_code}/busyness        — full seasonal model
GET /parks/{unit_code}/busyness?month= — single-month snapshot
GET /parks/compare?parks=A,B&month=    — multi-park comparison
GET /parks/recommendations?state=&month=&max_score= — filtered recommendations

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

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
import db
import model as mdl

app = FastAPI(
    title="NPS Seasonal Busyness API",
    description="Historical seasonal busyness model for US National Parks",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

DB_PATH = ROOT / "data" / "nps.db"


def _get_db() -> Path:
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Database not initialised. Run: python src/ingest.py --years 2014-2024",
        )
    return DB_PATH


# ── /parks ────────────────────────────────────────────────────────────────────

@app.get("/parks")
def list_parks(
    state: str | None = Query(None, description="Filter by state code, e.g. CA"),
    park_type: str | None = Query(None, alias="type", description="Filter by park type"),
):
    """List all parks in the database."""
    dp = _get_db()
    parks_df = db.get_all_parks(dp)
    if parks_df.empty:
        return []
    if state:
        parks_df = parks_df[parks_df["state"].str.contains(state.upper(), na=False)]
    if park_type:
        parks_df = parks_df[parks_df["type"].str.contains(park_type, case=False, na=False)]
    return parks_df.fillna("").to_dict(orient="records")


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
    return results


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
    uc = unit_code.upper()

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


# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    db_exists = DB_PATH.exists()
    park_count = 0
    if db_exists:
        parks_df = db.get_all_parks(DB_PATH)
        park_count = len(parks_df)
    return {
        "status": "ok",
        "db_exists": db_exists,
        "park_count": park_count,
    }
