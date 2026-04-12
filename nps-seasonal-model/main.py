"""ParkPulse main entry point.

Mounts both the existing NPS seasonal-model API and the new campsite
alert engine, then starts the availability poller scheduler.

Run:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Load .env before any modules read os.getenv
load_dotenv()

# Ensure src/ is importable for the existing seasonal model code
ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))

from alert_engine.db import init_db
from alert_engine.poller import start_scheduler
from alert_engine.router import router as alert_router

# Import the existing NPS API app's routes
import src.api as nps_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init alert DB + start poller.  Shutdown: stop scheduler."""
    logger.info("Initialising alert engine database…")
    await init_db()
    logger.info("Starting availability poller scheduler…")
    scheduler = await start_scheduler()
    yield
    logger.info("Shutting down poller scheduler…")
    scheduler.shutdown()


app = FastAPI(
    title="ParkPulse API",
    description="NPS seasonal busyness model + campsite availability alert engine",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Mount alert engine routes ────────────────────────────────────────────────
app.include_router(alert_router)

# ── Re-mount all existing NPS seasonal-model routes ──────────────────────────
for route in nps_api.app.routes:
    if hasattr(route, "path") and route.path != "/":
        app.routes.append(route)

# ── Serve the mobile frontend (must be last so API routes win) ───────────────
STATIC_DIR = ROOT / "static"
if STATIC_DIR.exists():
    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="mobile")
