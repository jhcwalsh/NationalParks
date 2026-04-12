/* National Parks Now — mobile web app
 *
 * Fetches /parks (search list) and /parks/{code}/overview (current screen).
 * Renders every section of the mockup. Every field independently falls back
 * to "—" when missing so a partial backend response still produces a usable
 * screen. */

const DEFAULT_PARK = "YOSE";

const el = (id) => document.getElementById(id);

const state = {
  parks: [],          // [{unit_code, name, ...}, …]
  currentCode: null,
  suggestIndex: -1,
};

// ── Init ───────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  wireSearch();
  wireTabs();

  try {
    const r = await fetch("/parks");
    if (r.ok) state.parks = await r.json();
  } catch (e) { /* fall through — overview fetch will still work */ }

  const params = new URLSearchParams(window.location.search);
  const initial = (params.get("park") || DEFAULT_PARK).toUpperCase();
  await loadPark(initial);
});

// ── Park loading ───────────────────────────────────────────────────────────
async function loadPark(code) {
  if (!code) return;
  code = code.toUpperCase();
  state.currentCode = code;

  // Sync the URL without pushing history every navigation
  const url = new URL(window.location.href);
  url.searchParams.set("park", code);
  window.history.replaceState({}, "", url);

  // Reset to Overview tab when switching parks
  resetToOverviewTab();
  setSkeleton();

  try {
    const r = await fetch(`/parks/${code}/overview`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    render(data);
  } catch (err) {
    el("park-name").textContent = "Unable to load park";
    el("park-meta").textContent = err.message;
  }
}

function setSkeleton() {
  el("park-name").textContent = "Loading…";
  el("park-meta").textContent = "";
  el("busyness-score").textContent = "—";
  el("busyness-label").textContent = "";
  el("busyness-context").textContent = "";
  el("busyness-circle").className = "busyness-circle busyness-skeleton";
  setCard("aqi-value",     "—", "aqi-label",    "");
  setCard("weather-value", "—", "weather-desc", "");
  setCard("camping-value", "—", "camping-desc", "");
  el("alerts").hidden = true;
  el("alerts").innerHTML = "";
  el("monthly-chart").innerHTML = "";
}

// ── Render ─────────────────────────────────────────────────────────────────
function render(data) {
  renderPark(data.park);
  renderBusyness(data.busyness);
  renderCards(data.cards);
  renderAlerts(data.alerts);
  renderMonthly(data.monthly);
}

function renderPark(park) {
  if (!park) return;
  el("park-name").textContent = park.name || "";
  const meta = [];
  if (park.state) meta.push(park.state);
  if (park.reservation_note) meta.push(park.reservation_note);
  el("park-meta").textContent = meta.join(" · ");

  const input = el("park-search");
  if (document.activeElement !== input) {
    input.value = park.name || "";
  }
}

function renderBusyness(b) {
  const circle = el("busyness-circle");
  const scoreEl = el("busyness-score");
  if (!b) {
    scoreEl.textContent = "—";
    el("busyness-label").textContent = "No data";
    el("busyness-context").textContent = "Busyness model data not available";
    circle.className = "busyness-circle busyness-skeleton";
    return;
  }
  scoreEl.textContent = b.score;
  el("busyness-label").textContent = b.label || "";
  el("busyness-context").textContent = b.context || "";

  let tier = "shoulder";
  if (b.score >= 70) tier = "peak";
  else if (b.score < 30) tier = "quiet";
  circle.className = `busyness-circle ${tier}`;
}

function renderCards(cards) {
  const aqi = cards && cards.aqi;
  setCard(
    "aqi-value",
    aqi ? `AQI ${aqi.value}` : "—",
    "aqi-label",
    aqi ? aqi.label : "Unavailable",
  );

  const wx = cards && cards.weather;
  setCard(
    "weather-value",
    wx ? `${wx.temp_f}°F` : "—",
    "weather-desc",
    wx ? wx.description : "Unavailable",
  );

  const cp = cards && cards.camping;
  let campValue = "—";
  let campDesc = "Unavailable";
  if (cp) {
    if (cp.pct_open !== null && cp.pct_open !== undefined) {
      campValue = `${cp.pct_open}% open`;
    } else {
      campValue = "—";
    }
    campDesc = cp.label || "";
  }
  setCard("camping-value", campValue, "camping-desc", campDesc);
}

function setCard(valueId, value, subId, sub) {
  el(valueId).textContent = value;
  el(subId).textContent = sub;
}

function renderAlerts(alerts) {
  const wrap = el("alerts");
  wrap.innerHTML = "";
  if (!alerts || alerts.length === 0) {
    wrap.hidden = true;
    return;
  }
  wrap.hidden = false;
  for (const a of alerts) {
    const row = document.createElement("div");
    row.className = `alert ${a.tone || "info"}`;
    const text = document.createElement("span");
    text.textContent = a.text;
    row.appendChild(text);
    wrap.appendChild(row);
  }
}

function renderMonthly(monthly) {
  const wrap = el("monthly-chart");
  wrap.innerHTML = "";
  if (!monthly || monthly.length === 0) {
    const p = document.createElement("p");
    p.className = "card-sub";
    p.textContent = "No historical data available";
    wrap.appendChild(p);
    return;
  }
  for (const m of monthly) {
    const row = document.createElement("div");
    row.className = "month-row";

    const name = document.createElement("span");
    name.className = "month-name";
    name.textContent = m.month;

    const track = document.createElement("div");
    track.className = "bar-track";
    const fill = document.createElement("div");
    fill.className = `bar-fill ${m.label || "shoulder"}`;
    fill.style.width = `${Math.max(2, Math.min(100, m.score))}%`;
    track.appendChild(fill);

    const value = document.createElement("div");
    value.className = "month-value";
    const strong = document.createElement("strong");
    strong.textContent = `${m.score}%`;
    value.appendChild(strong);
    if (m.label === "quiet") {
      const tag = document.createElement("span");
      tag.className = "quiet-tag";
      tag.textContent = "quiet";
      value.appendChild(tag);
    }

    row.appendChild(name);
    row.appendChild(track);
    row.appendChild(value);
    wrap.appendChild(row);
  }
}

// ── Search ────────────────────────────────────────────────────────────────
function wireSearch() {
  const input  = el("park-search");
  const list   = el("park-suggest");

  const filter = (q) => {
    if (!q) return state.parks;
    const ql = q.toLowerCase();
    return state.parks.filter(
      (p) => p.name.toLowerCase().includes(ql) ||
             p.unit_code.toLowerCase().startsWith(ql),
    );
  };

  const renderList = (items) => {
    list.innerHTML = "";
    state.suggestIndex = -1;
    if (items.length === 0) {
      list.hidden = true;
      return;
    }
    for (let i = 0; i < items.length; i++) {
      const p = items[i];
      const li = document.createElement("li");
      li.textContent = p.name;
      li.dataset.code = p.unit_code;
      li.addEventListener("mousedown", (ev) => {
        ev.preventDefault();
        pick(p.unit_code, p.name);
      });
      list.appendChild(li);
    }
    list.hidden = false;
  };

  const pick = (code, name) => {
    input.value = name;
    list.hidden = true;
    loadPark(code);
    input.blur();
  };

  input.addEventListener("focus", () => renderList(filter(input.value)));
  input.addEventListener("input", () => renderList(filter(input.value)));
  input.addEventListener("blur", () => {
    setTimeout(() => { list.hidden = true; }, 150);
  });
  input.addEventListener("keydown", (ev) => {
    const items = Array.from(list.querySelectorAll("li"));
    if (ev.key === "ArrowDown") {
      ev.preventDefault();
      state.suggestIndex = Math.min(items.length - 1, state.suggestIndex + 1);
    } else if (ev.key === "ArrowUp") {
      ev.preventDefault();
      state.suggestIndex = Math.max(-1, state.suggestIndex - 1);
    } else if (ev.key === "Enter") {
      ev.preventDefault();
      let picked = null;
      if (state.suggestIndex >= 0 && items[state.suggestIndex]) {
        picked = items[state.suggestIndex];
      } else if (items[0]) {
        picked = items[0];
      }
      if (picked) {
        pick(picked.dataset.code, picked.textContent);
      }
      return;
    } else if (ev.key === "Escape") {
      list.hidden = true;
      return;
    } else {
      return;
    }
    items.forEach((li, i) => li.classList.toggle("is-active", i === state.suggestIndex));
  });
}

function resetToOverviewTab() {
  const tabs = document.querySelectorAll(".tab");
  tabs.forEach((b) => b.classList.toggle("is-active", b.dataset.tab === "overview"));
  document.querySelectorAll(".park-header, .cards, .alerts, .monthly").forEach(
    (s) => (s.style.display = ""),
  );
  el("webcams-tab").hidden = true;
  el("camping-tab").hidden = true;
  el("alerts-tab").hidden = true;
  el("tab-placeholder").hidden = true;
}

// ── Bottom tabs ───────────────────────────────────────────────────────────

// Sections that are visible on each tab
const TAB_SECTIONS = {
  overview: ["park-header", "cards", "alerts", "monthly"],
  webcams:  ["park-header", "webcams-tab"],
};

function wireTabs() {
  const tabs = document.querySelectorAll(".tab");
  const placeholder = el("tab-placeholder");
  const webcamsTab = el("webcams-tab");
  const campingTab = el("camping-tab");
  const overviewSections = document.querySelectorAll(
    ".park-header, .cards, .alerts, .monthly",
  );

  const alertsTab = el("alerts-tab");

  const hideAll = () => {
    overviewSections.forEach((s) => (s.style.display = "none"));
    webcamsTab.hidden = true;
    campingTab.hidden = true;
    alertsTab.hidden = true;
    placeholder.hidden = true;
  };

  tabs.forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.tab;
      tabs.forEach((b) => b.classList.toggle("is-active", b === btn));
      hideAll();

      if (tab === "overview") {
        overviewSections.forEach((s) => (s.style.display = ""));
      } else if (tab === "webcams") {
        webcamsTab.hidden = false;
        loadWebcams();
      } else if (tab === "camping") {
        campingTab.hidden = false;
        loadCamping();
      } else if (tab === "alerts") {
        alertsTab.hidden = false;
        loadAlertsDetail();
      } else {
        placeholder.hidden = false;
      }
    });
  });
}

// ── Webcams ──────────────────────────────────────────────────────────────
let webcamsCache = {};  // park code → data

async function loadWebcams() {
  const code = state.currentCode;
  if (!code) return;

  const wrap = el("webcams-list");
  const link = el("webcams-nps-link");
  const note = el("webcams-note");

  // Use cache if available
  if (webcamsCache[code]) {
    renderWebcams(webcamsCache[code]);
    return;
  }

  wrap.innerHTML = '<p class="webcams-empty">Loading webcams…</p>';
  link.hidden = true;
  note.textContent = "";

  try {
    const r = await fetch(`/parks/${code}/webcams`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    webcamsCache[code] = data;
    renderWebcams(data);
  } catch (err) {
    wrap.innerHTML = `<p class="webcams-empty">Unable to load webcams: ${err.message}</p>`;
  }
}

function renderWebcams(data) {
  const wrap = el("webcams-list");
  const link = el("webcams-nps-link");
  const note = el("webcams-note");

  wrap.innerHTML = "";
  const cams = data.webcams || [];

  if (cams.length === 0) {
    wrap.innerHTML = '<p class="webcams-empty">No webcam data available for this park yet.</p>';
  } else {
    for (const cam of cams) {
      const a = document.createElement("a");
      a.className = "webcam-card";
      a.href = cam.url || data.nps_page || "#";
      a.target = "_blank";
      a.rel = "noopener";

      if (cam.image) {
        const img = document.createElement("img");
        img.className = "wc-image";
        img.src = cam.image;
        img.alt = cam.title;
        img.loading = "lazy";
        a.appendChild(img);
      }

      const info = document.createElement("div");
      info.className = "wc-info";

      const title = document.createElement("p");
      title.className = "wc-title";
      title.textContent = cam.title;
      info.appendChild(title);

      const badges = [];
      if (cam.status && cam.status.toLowerCase() === "active") badges.push("Active");
      if (cam.is_streaming) badges.push("Live");
      const sub = document.createElement("p");
      sub.className = "wc-sub";
      sub.textContent = badges.length > 0
        ? badges.join(" · ") + " — View on NPS.gov →"
        : "View on NPS.gov →";
      info.appendChild(sub);

      a.appendChild(info);
      wrap.appendChild(a);
    }
  }

  if (data.nps_page) {
    link.href = data.nps_page;
    link.hidden = false;
  } else {
    link.hidden = true;
  }

  note.textContent = data.note || "";
}

// ── Camping ──────────────────────────────────────────────────────────────
let campingCache = {};

async function loadCamping() {
  const code = state.currentCode;
  if (!code) return;

  const wrap = el("camping-content");

  if (campingCache[code]) {
    renderCamping(campingCache[code]);
    return;
  }

  wrap.innerHTML = '<p class="camping-empty">Loading campsite data…</p>';

  try {
    const r = await fetch(`/parks/${code}/camping`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    campingCache[code] = data;
    renderCamping(data);
  } catch (err) {
    wrap.innerHTML = `<p class="camping-empty">Unable to load camping data: ${err.message}</p>`;
  }
}

function renderCamping(data) {
  const wrap = el("camping-content");
  wrap.innerHTML = "";

  if (!data.has_campgrounds || !data.stats) {
    wrap.innerHTML = `
      <p class="camping-empty">No reservable campgrounds at ${data.park_name || "this park"}.</p>
      <a class="camping-recgov" href="${data.rec_gov_url || "#"}" target="_blank" rel="noopener">
        Search Recreation.gov →
      </a>`;
    return;
  }

  const s = data.stats;

  // Stat tiles
  const statsGrid = document.createElement("div");
  statsGrid.className = "camping-stats";

  const tiles = [
    { value: s.n_campgrounds, label: "Campgrounds" },
    { value: s.n_reservable_sites.toLocaleString(), label: "Reservable sites" },
    { value: s.n_fcfs_sites.toLocaleString(), label: "First-come, first-served" },
    { value: `${s.pct_available}%`, label: s.availability_label },
  ];

  for (const t of tiles) {
    const tile = document.createElement("div");
    tile.className = "camping-stat";
    tile.innerHTML = `<p class="cs-value">${t.value}</p><p class="cs-label">${t.label}</p>`;
    statsGrid.appendChild(tile);
  }
  wrap.appendChild(statsGrid);

  // Availability detail card
  const avail = document.createElement("div");
  avail.className = "camping-avail";

  const barPct = Math.max(2, Math.min(100, s.pct_available));
  const barClass = s.pct_available >= 25 ? "good" : s.pct_available >= 10 ? "fair" : "";

  avail.innerHTML = `
    <div class="ca-row">
      <span class="ca-label">Overall availability</span>
      <span class="ca-value">${s.pct_available}%</span>
    </div>
    <div class="camping-avail-bar"><div class="fill ${barClass}" style="width:${barPct}%"></div></div>
    <div class="ca-row">
      <span class="ca-label">Weekday availability</span>
      <span class="ca-value">${s.weekday_pct}%</span>
    </div>
    <div class="ca-row">
      <span class="ca-label">Weekend availability</span>
      <span class="ca-value">${s.weekend_pct}%</span>
    </div>`;
  wrap.appendChild(avail);

  // Recreation.gov button
  const recLink = document.createElement("a");
  recLink.className = "camping-recgov";
  recLink.href = data.rec_gov_url;
  recLink.target = "_blank";
  recLink.rel = "noopener";
  recLink.textContent = "Reserve on Recreation.gov →";
  wrap.appendChild(recLink);

  // Data window note
  if (data.window) {
    const note = document.createElement("p");
    note.className = "camping-window";
    note.textContent = `Availability data for ${data.window.start} to ${data.window.end}`;
    wrap.appendChild(note);
  }
}

// ── Alerts detail tab ────────────────────────────────────────────────────
let alertsDetailCache = {};

async function loadAlertsDetail() {
  const code = state.currentCode;
  if (!code) return;

  const wrap = el("alerts-detail-content");

  if (alertsDetailCache[code]) {
    renderAlertsDetail(alertsDetailCache[code]);
    return;
  }

  wrap.innerHTML = '<p class="alerts-empty">Loading alerts…</p>';

  try {
    const r = await fetch(`/parks/${code}/alerts`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    alertsDetailCache[code] = data;
    renderAlertsDetail(data);
  } catch (err) {
    wrap.innerHTML = `<p class="alerts-empty">Unable to load alerts: ${err.message}</p>`;
  }
}

function renderAlertsDetail(data) {
  const wrap = el("alerts-detail-content");
  wrap.innerHTML = "";

  const fires = data.fires || [];
  const npsAlerts = data.nps_alerts || [];

  if (fires.length === 0 && npsAlerts.length === 0) {
    wrap.innerHTML = '<p class="alerts-empty">No active alerts or nearby fires for this park.</p>';
    return;
  }

  // Fires section
  if (fires.length > 0) {
    const label = document.createElement("p");
    label.className = "alerts-section-label";
    label.textContent = `Nearby wildfires (${fires.length})`;
    wrap.appendChild(label);

    for (const f of fires) {
      const card = document.createElement("div");
      card.className = "fire-detail";

      const name = document.createElement("p");
      name.className = "fd-name";
      name.textContent = `${f.name} Fire`;
      card.appendChild(name);

      const parts = [`${f.distance_mi} mi ${f.direction}`];
      if (f.acres) parts.push(`${f.acres.toLocaleString()} acres`);
      if (f.pct_contained !== undefined) parts.push(`${f.pct_contained}% contained`);

      const meta = document.createElement("p");
      meta.className = "fd-meta";
      meta.textContent = parts.join(" · ");
      card.appendChild(meta);

      wrap.appendChild(card);
    }
  }

  // NPS alerts section
  if (npsAlerts.length > 0) {
    const label = document.createElement("p");
    label.className = "alerts-section-label";
    label.textContent = `Park alerts (${npsAlerts.length})`;
    wrap.appendChild(label);

    for (const a of npsAlerts) {
      const card = document.createElement("div");
      card.className = `alert-detail ${a.tone || "info"}`;

      if (a.category) {
        const cat = document.createElement("p");
        cat.className = "ad-category";
        cat.textContent = a.category;
        card.appendChild(cat);
      }

      const title = document.createElement("p");
      title.className = "ad-title";
      title.textContent = a.title;
      card.appendChild(title);

      if (a.description) {
        const desc = document.createElement("p");
        desc.className = "ad-desc";
        // Truncate long descriptions for mobile readability
        const text = a.description.length > 300
          ? a.description.slice(0, 300) + "…"
          : a.description;
        desc.textContent = text;
        card.appendChild(desc);
      }

      if (a.url) {
        const link = document.createElement("a");
        link.className = "ad-link";
        link.href = a.url;
        link.target = "_blank";
        link.rel = "noopener";
        link.textContent = "More info on NPS.gov →";
        card.appendChild(link);
      }

      wrap.appendChild(card);
    }
  }
}
