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
    if (!q) return state.parks.slice(0, 8);
    const ql = q.toLowerCase();
    return state.parks
      .filter((p) => p.name.toLowerCase().includes(ql) ||
                     p.unit_code.toLowerCase().startsWith(ql))
      .slice(0, 8);
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

// ── Bottom tabs ───────────────────────────────────────────────────────────
function wireTabs() {
  const tabs = document.querySelectorAll(".tab");
  const placeholder = el("tab-placeholder");
  tabs.forEach((btn) => {
    btn.addEventListener("click", () => {
      tabs.forEach((b) => b.classList.toggle("is-active", b === btn));
      placeholder.hidden = btn.dataset.tab === "overview";
    });
  });
}
