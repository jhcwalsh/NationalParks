# NPS Seasonal Busyness Model

Historical seasonal busyness model for US National Parks, built from NPS IRMA
monthly visitation data (2014–2024).

## Project layout

```
nps-seasonal-model/
├── data/
│   ├── raw/          # downloaded / cached CSVs
│   └── nps.db        # SQLite database (auto-created)
├── src/
│   ├── db.py         # SQLite read/write helpers
│   ├── ingest.py     # download + parse NPS CSVs, seed fallback
│   ├── clean.py      # normalisation, outlier handling
│   ├── model.py      # busyness score computation
│   └── api.py        # FastAPI backend
├── tests/
│   └── test_model.py
└── requirements.txt
```

## Quick start

```bash
# Install dependencies (from repo root)
pip install -r requirements.txt

# Load data (uses built-in seed for top-20 parks — no network needed)
python nps-seasonal-model/src/ingest.py --years 2014-2024 --seed-only

# Launch unified Streamlit dashboard (tabs 4–6 use this seasonal model)
streamlit run nps_dashboard.py

# (Optional) Launch FastAPI backend
cd nps-seasonal-model && uvicorn src.api:app --reload --port 8000
```

## Ingest pipeline

```bash
# Try live IRMA downloads, fall back to seed where unavailable
python src/ingest.py --years 2014-2024

# Seed-only (offline, instant)
python src/ingest.py --years 2014-2024 --seed-only

# Single park
python src/ingest.py --years 2014-2024 --park YOSE

# Custom DB path
python src/ingest.py --years 2014-2024 --db /tmp/myparks.db
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/parks` | List all parks |
| GET | `/parks/{unit_code}/busyness` | Full seasonal model |
| GET | `/parks/{unit_code}/busyness?month=7` | Single-month snapshot |
| GET | `/parks/compare?parks=YOSE,GRCA&month=9` | Multi-park comparison |
| GET | `/parks/recommendations?state=CA&month=10&max_score=50` | Filtered recommendations |
| GET | `/health` | Health check |

Docs at `http://localhost:8000/docs` when running.

## Model details

For each park the model computes:

- **monthly_score** — normalised 0–100 busyness (100 = busiest month historically)
- **yoy_trend** — last-3-year avg vs prior-3-year avg
- **peak_months** — top 3 months (score ≥ 70)
- **shoulder_months** — months 20–70
- **quiet_months** — months < 30
- **weekend_multiplier** — 1.4× default (no weekly breakdown in IRMA data)
- **best_visit_windows** — up to 5 ranked 2-week windows, weather-hostile periods excluded

COVID years (2020, 2021) are retained in the raw table but **excluded from all
baseline calculations**. Parks with fewer than 5 non-COVID years of data are
flagged `low_confidence: true`.

## Data source

NPS Visitor Use Statistics portal — https://irma.nps.gov/Stats/

Primary dataset: *Recreation Visits by Month and Park* CSVs, 2014–2024.
Park unit codes (4-letter abbreviations) are the primary key.

The ingest script attempts live downloads from IRMA; if the portal is
unavailable (it requires JS rendering) it falls back to a built-in seed
dataset calibrated from published 2019 reference-year statistics for the
20 most-visited parks.

## Running tests

```bash
pytest tests/ -v
```
