# Campsite Availability Statistics — Definitions

Statistics produced by `fetch_campsite_preview.py` and displayed in the
Campsite Availability tab of the NPS Park Dashboard.

---

## Data Sources

| Source | Purpose |
|--------|---------|
| **RIDB API** (`ridb.recreation.gov/api/v1`) | Discovers campground facility IDs for each park |
| **Recreation.gov availability API** (`www.recreation.gov`) | Returns per-site booking status for each date in the window |

---

## Analysis Window

All availability statistics are calculated over a **30-day window** starting
from the date the data was fetched. Each day in the window contributes one
slot per reservable site (a "site-night").

---

## Column Definitions

### `unit_code`
The NPS four-letter park identifier (e.g. `YOSE` for Yosemite, `GRCA` for
Grand Canyon). Used as the primary key across all NPS data sources.

---

### `park_name`
The full official name of the national park as defined in the NPS designation
list (e.g. `Yosemite National Park`).

---

### `n_reservable_sites`
**Total number of reservable campsites** across all Recreation.gov campgrounds
in the park.

A site is counted as reservable if its `campsite_reserve_type` from the
Recreation.gov API is **not** one of:
- `First-Come First-Served`
- `Management` (internal/staff use)

Includes site-specific reservations and lottery-based reservations.

> This is a count of distinct sites, not nights. A park with 3 campgrounds
> of 50 sites each would show `150`.

---

### `n_fcfs_sites`
**Total number of first-come-first-served (FCFS) sites** across all
Recreation.gov campgrounds in the park.

These sites cannot be reserved in advance — they are assigned on arrival.
They are included for reference but are **excluded from all availability
calculations** since their occupancy cannot be known ahead of time.

> Note: some parks manage FCFS sites outside Recreation.gov entirely,
> so this count may understate the true number of walk-in sites.

---

### `avail_nights`
**Total available site-nights** within the 30-day window.

Calculated by iterating every reservable site at every date in the window
and counting slots where the Recreation.gov status is `"Available"` or
`"Open"`.

> **Example:** A campground with 10 reservable sites, where 4 sites are
> available on each of 30 days, contributes `4 × 30 = 120` available
> site-nights.

Slots with status `"Reserved"` or `"Not Available"` (which includes
seasonal closures and maintenance) are not counted.

---

### `total_nights`
**Maximum possible reservable site-nights** within the 30-day window.

Calculated as `n_reservable_sites × 30`. This is the theoretical ceiling
if every reservable site were open and unbooked for every night.

> Used as the denominator for `pct_available`. A low `total_nights` with
> a high `pct_available` may simply mean most campgrounds are closed for
> the season rather than genuinely open and empty.

---

### `pct_available`
**Percentage of reservable site-nights that are available** within the
30-day window.

```
pct_available = (avail_nights / total_nights) × 100
```

Rounded to one decimal place. Shown as `—` if the park has no reservable
sites (`total_nights = 0`).

| Range | Interpretation |
|-------|---------------|
| ≥ 50% | High availability — easy to find a spot |
| 20–50% | Moderate — some options, book soon |
| < 20% | Low — limited availability, book immediately |

> **Caveats:** A 0% figure can mean the park is fully booked **or** that
> all campgrounds are seasonally closed. A very high figure (near 100%)
> may indicate the booking window for those dates has not yet opened on
> Recreation.gov (reservations typically open 6 months ahead).

---

### `weekend_pct`
**Percentage of reservable site-nights available on weekends** (Saturday
and Sunday nights) within the 30-day window.

```
weekend_pct = (weekend_available_nights / (n_reservable_sites × n_weekend_nights)) × 100
```

Weekend availability is typically significantly lower than the overall
figure, as Fri/Sat nights fill first. Use this metric when planning a
weekend trip.

---

### `weekday_pct`
**Percentage of reservable site-nights available on weekdays** (Monday
through Friday nights) within the 30-day window.

```
weekday_pct = (weekday_available_nights / (n_reservable_sites × n_weekday_nights)) × 100
```

Generally higher than `weekend_pct`. Useful for flexible travellers who
can visit mid-week to find more open sites.

---

### `has_campgrounds`
Boolean (`True` / `False`) indicating whether the park has at least one
Recreation.gov campground facility matched via the RIDB API.

`False` does **not** mean the park has no camping. It means either:
- The park uses a different booking system (e.g. permit lotteries, state
  park systems, or self-registration envelopes)
- The park has walk-in / dispersed camping only
- The RIDB name-matching did not find a matching rec area for this park

Parks with `has_campgrounds = False` are excluded from the availability
chart and table and listed separately.

---

### `n_facilities`
**Number of Recreation.gov campground facilities** found for the park.

Each facility corresponds to a named campground (e.g. Upper Pines,
Watchman Campground). A single park may have many facilities — Yosemite,
for example, has several distinct campgrounds each with its own facility
ID on Recreation.gov.

---

## Important Caveats

1. **Seasonal closures** — A campground that is closed for winter will
   show all dates as `"Not Available"`, contributing 0 to `avail_nights`
   but its full count to `total_nights`. This drives `pct_available`
   toward 0% for that facility regardless of demand.

2. **Booking window not yet open** — Recreation.gov typically opens
   reservations 6 months in advance. Dates beyond that window appear as
   `"Not Available"`, which can artificially inflate unavailability.

3. **FCFS excluded** — Walk-in sites are not reservable and their
   real-time occupancy is unknown, so they are counted separately and
   excluded from availability percentages.

4. **Permit-only areas** — Some high-demand areas (e.g. Half Dome cables,
   Angels Landing) use a separate permit system and do not appear in
   Recreation.gov campground data at all.

5. **Data freshness** — Availability data is cached for 1 hour in the
   dashboard. The `fetched_at` timestamp shows when the data was last
   retrieved.
