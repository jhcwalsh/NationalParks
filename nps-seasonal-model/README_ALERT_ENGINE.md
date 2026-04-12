# ParkPulse — Campsite Alert Engine

Monitors Recreation.gov for campsite cancellations and notifies users via SMS (Twilio) and email (SendGrid) when a matching site opens.

## Architecture

```
alert_engine/
├── __init__.py       # Package marker
├── models.py         # Pydantic schemas (ScanCreate, ScanUpdate, etc.)
├── db.py             # SQLite via aiosqlite — tables, CRUD, snapshots
├── poller.py         # Recreation.gov availability poller + APScheduler
├── matcher.py        # Match availability events → active user scans
├── enricher.py       # Best-effort AQI + crowd score for alert messages
├── notifier.py       # Twilio SMS + SendGrid email dispatch
└── router.py         # FastAPI routes mounted at /api/alerts
```

**Flow:** Scheduler → Poller → Diff against snapshot → New events → Matcher → Notifier → SMS/Email

## Setup

### 1. Install dependencies

```bash
cd nps-seasonal-model
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required for core functionality:
- `RECREATION_GOV_API_KEY` — Recreation.gov API key
- `DATABASE_URL` — SQLite path (default: `parkpulse.db`)

Required for notifications:
- `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER` — SMS via Twilio
- `SENDGRID_API_KEY`, `SENDGRID_FROM_EMAIL` — Email via SendGrid

Optional:
- `AIRNOW_API_KEY` — AQI enrichment in alert messages
- `POLL_INTERVAL_SECONDS` — How often to poll (default: 120)
- `CANCELLATION_WINDOW_DAYS` — How far ahead to check (default: 14)

### 3. Run the server

```bash
uvicorn main:app --reload --port 8000
```

On startup, the server:
1. Creates the SQLite database and tables
2. Seeds the priority facility reference data (8 campgrounds)
3. Starts the APScheduler poller on the configured interval

## API Endpoints

All alert endpoints are under `/api/alerts`.

### Create a scan

```bash
curl -X POST http://localhost:8000/api/alerts/scans \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "john",
    "facility_id": "232447",
    "park_name": "Yosemite - Upper Pines",
    "arrival_date": "2026-07-04",
    "flexible_arrival": true,
    "num_nights": 3,
    "site_type": "tent",
    "notify_sms": "+14155551234",
    "notify_email": "john@example.com"
  }'
```

### List scans for a user

```bash
curl http://localhost:8000/api/alerts/scans/user/john
# Add ?active=false to include paused scans
```

### Get a specific scan

```bash
curl http://localhost:8000/api/alerts/scans/1
```

### Update a scan

```bash
curl -X PATCH http://localhost:8000/api/alerts/scans/1 \
  -H "Content-Type: application/json" \
  -d '{"num_nights": 2, "flexible_arrival": false}'
```

### Pause (soft-delete) a scan

```bash
curl -X DELETE http://localhost:8000/api/alerts/scans/1
# Returns: {"status": "paused"}
```

### View alert history for a scan

```bash
curl http://localhost:8000/api/alerts/scans/1/history
```

### Check poller status

```bash
curl http://localhost:8000/api/alerts/status
```

### List known facilities

```bash
curl http://localhost:8000/api/alerts/facilities
```

### Trigger a manual poll (testing)

```bash
curl -X POST http://localhost:8000/api/alerts/poll
```

## Priority Facilities (Seed Data)

| Park | Campground | Facility ID |
|---|---|---|
| Yosemite | Upper Pines | 232447 |
| Yosemite | Lower Pines | 232450 |
| Yosemite | North Pines | 232449 |
| Grand Canyon | Mather | 234869 |
| Zion | Watchman | 272265 |
| Zion | South | 272267 |
| Glacier | Apgar | 251869 |
| Glacier | Fish Creek | 232493 |

## Running Tests

```bash
cd nps-seasonal-model
python -m pytest tests/test_matcher.py tests/test_poller.py tests/test_notifier.py tests/test_routes.py -v
```

**Test coverage:**
- `test_matcher.py` (24 tests) — All matching rules: facility, date (exact + flexible boundaries), site type, vehicle length, specific sites, active flag
- `test_poller.py` (7 tests) — Month window calculation, diff logic (new vs unchanged vs reserved)
- `test_notifier.py` (15 tests) — Message construction with/without conditions, SMS/email dispatch, failure logging
- `test_routes.py` (17 tests) — All 7+ endpoints including validation errors (past dates, bad phone numbers, missing notification channels)

## Validation Rules

- `arrival_date` must be today or later
- `notify_sms` must be E.164 format (`+14155551234`)
- `site_type` must be one of: `tent`, `rv`, `group`, `any`
- At least one of `notify_sms` or `notify_email` is required
- `num_nights` must be 1–30
