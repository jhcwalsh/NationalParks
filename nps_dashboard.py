"""
NPS Park Dashboard — unified Streamlit app
==========================================
Tabs 1–3: live NPS Developer API  (parks overview, busyness rankings, park detail)
Tabs 4–6: seasonal busyness model (monthly scores, park comparison, recommendations)

Run
---
    streamlit run nps_dashboard.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

_ENV_KEY = os.getenv("NPS_API_KEY", "")

# ── Seasonal model imports ────────────────────────────────────────────────────
_SEASONAL_SRC = Path(__file__).parent / "nps-seasonal-model" / "src"
if str(_SEASONAL_SRC) not in sys.path:
    sys.path.insert(0, str(_SEASONAL_SRC))

try:
    import db as _nps_db
    import model as _nps_model
    _SEASONAL_AVAILABLE = True
except ImportError:
    _SEASONAL_AVAILABLE = False

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NPS Park Dashboard",
    page_icon="🏕️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0f1923; color: #e8edf2; }

    [data-testid="stSidebar"] {
        background-color: #152232;
        border-right: 1px solid #1e3448;
    }

    .metric-card {
        background: #152232;
        border: 1px solid #1e3448;
        border-left: 4px solid #1a6fa8;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 12px;
    }
    .metric-card .label {
        font-size: 12px;
        color: #7a9bbb;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 4px;
    }
    .metric-card .value {
        font-size: 28px;
        font-weight: 700;
        color: #e8edf2;
    }
    .metric-card .sub {
        font-size: 12px;
        color: #7a9bbb;
        margin-top: 2px;
    }

    .section-header {
        font-size: 18px;
        font-weight: 600;
        color: #4da6de;
        border-bottom: 1px solid #1e3448;
        padding-bottom: 8px;
        margin: 24px 0 16px 0;
    }

    .detail-card {
        background: #152232;
        border: 1px solid #1e3448;
        border-radius: 8px;
        padding: 20px 24px;
        margin-bottom: 16px;
    }

    /* Generic blue badge (API tabs) */
    .badge {
        display: inline-block;
        background: #1a6fa8;
        color: #e8edf2;
        font-size: 11px;
        padding: 3px 10px;
        border-radius: 12px;
        margin: 2px 3px;
    }

    /* Seasonal classification badges */
    .badge-peak     { display:inline-block; background:#c0392b; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-shoulder { display:inline-block; background:#e67e22; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-quiet    { display:inline-block; background:#27ae60; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-info     { display:inline-block; background:#1a6fa8; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }

    .window-card {
        background: #0f1923;
        border: 1px solid #1e3448;
        border-left: 4px solid #27ae60;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .window-card .wlabel { font-weight:600; color:#e8edf2; font-size:14px; }
    .window-card .wnotes { color:#7a9bbb; font-size:12px; margin-top:4px; }

    [data-testid="stDataFrame"] { border: 1px solid #1e3448; border-radius: 6px; }

    .stButton > button {
        background: #1a6fa8; color: white; border: none;
        border-radius: 6px; font-weight: 500;
    }
    .stButton > button:hover { background: #1d82c7; }

    .stTextInput input {
        background: #0f1923;
        border-color: #1e3448;
        color: #e8edf2;
    }
    .stSelectbox > div, .stMultiSelect > div {
        background: #152232 !important;
    }

    div[data-testid="stTabs"] button { color: #7a9bbb; }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #4da6de;
        border-bottom-color: #4da6de;
    }

    #MainMenu { visibility: hidden; }
    footer    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL = "https://developer.nps.gov/api/v1"
LIMIT    = 500

DB_PATH = Path(__file__).parent / "nps-seasonal-model" / "data" / "nps.db"

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0f1923",
    plot_bgcolor="#0f1923",
    font=dict(color="#e8edf2"),
    margin=dict(l=10, r=10, t=30, b=10),
)

MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]


# ══════════════════════════════════════════════════════════════════════════════
# NPS API helpers  (tabs 1–3)
# ══════════════════════════════════════════════════════════════════════════════

def nps_get(endpoint: str, api_key: str, params: Optional[dict] = None) -> list:
    """Fetch all records from a paginated NPS Developer API endpoint."""
    params = params or {}
    params.update({"api_key": api_key, "limit": LIMIT, "start": 0})
    all_data: list = []
    while True:
        try:
            r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=20)
            r.raise_for_status()
            body = r.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API error ({endpoint}): {e}")
            return []
        data = body.get("data", [])
        all_data.extend(data)
        if len(all_data) >= int(body.get("total", 0)) or not data:
            break
        params["start"] = len(all_data)
    return all_data


@st.cache_data(ttl=600, show_spinner=False)
def load_api_parks(api_key: str) -> pd.DataFrame:
    raw = nps_get("/parks", api_key)
    if not raw:
        return pd.DataFrame()
    rows = []
    for p in raw:
        fees = p.get("entranceFees", [])
        fee_str = ", ".join(
            f"${f.get('cost','?')} ({f.get('title','')})" for f in fees[:3]
        ) if fees else "No fee info"
        hours_raw = p.get("operatingHours", [])
        hours_str = ""
        if hours_raw:
            std = hours_raw[0].get("standardHours", {})
            days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            hours_str = "  \n".join(f"{d.capitalize()}: {std.get(d,'N/A')}" for d in days)
        contacts  = p.get("contacts", {})
        phones    = contacts.get("phoneNumbers", [])
        emails    = contacts.get("emailAddresses", [])
        amenities = p.get("amenities", [])
        rows.append({
            "park_code":        p.get("parkCode", ""),
            "name":             p.get("fullName", ""),
            "short_name":       p.get("name", ""),
            "states":           p.get("states", ""),
            "designation":      p.get("designation", ""),
            "description":      p.get("description", ""),
            "lat":              float(p.get("latitude")  or 0) or None,
            "lon":              float(p.get("longitude") or 0) or None,
            "url":              p.get("url", ""),
            "fees":             fee_str,
            "hours":            hours_str,
            "phone":            phones[0].get("phoneNumber","") if phones else "",
            "email":            emails[0].get("emailAddress","") if emails else "",
            "amenities":        ", ".join(
                                    a.get("name","") if isinstance(a, dict) else str(a)
                                    for a in amenities
                                ),
            "n_visitor_centers": len(p.get("visitorCenters", [])),
            "n_topics":          len(p.get("topics", [])),
            "n_activities":      len(p.get("activities", [])),
            "images":            p.get("images", []),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner=False)
def load_alerts(api_key: str) -> pd.DataFrame:
    raw = nps_get("/alerts", api_key)
    if not raw:
        return pd.DataFrame(columns=["parkCode", "category"])
    return pd.DataFrame([
        {"parkCode": a.get("parkCode",""), "category": a.get("category","")}
        for a in raw
    ])


@st.cache_data(ttl=600, show_spinner=False)
def load_activities_parks(api_key: str) -> pd.DataFrame:
    raw = nps_get("/activities/parks", api_key)
    if not raw:
        return pd.DataFrame(columns=["park_code","n_activities_catalog"])
    rows = [
        {"park_code": pk.get("parkCode",""), "activity": act.get("name","")}
        for act in raw
        for pk in act.get("parks", [])
    ]
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["park_code","activity"])
    if not df.empty:
        df = df.groupby("park_code").size().reset_index(name="n_activities_catalog")
    return df


def compute_busyness(
    parks_df: pd.DataFrame,
    alerts_df: pd.DataFrame,
    acts_df: pd.DataFrame,
) -> pd.DataFrame:
    """Composite busyness score from weighted NPS API proxy signals."""
    df = parks_df[["park_code","short_name","states","designation",
                   "n_visitor_centers","n_activities","n_topics"]].copy()
    if not alerts_df.empty:
        alert_counts = alerts_df.groupby("parkCode").size().reset_index(name="n_alerts")
        df = df.merge(alert_counts, left_on="park_code", right_on="parkCode",
                      how="left").drop(columns=["parkCode"], errors="ignore")
    else:
        df["n_alerts"] = 0
    if not acts_df.empty and "park_code" in acts_df.columns:
        df = df.merge(acts_df, on="park_code", how="left")
    else:
        df["n_activities_catalog"] = 0
    df = df.fillna(0)

    def norm(col: str) -> pd.Series:
        mn, mx = df[col].min(), df[col].max()
        return (df[col] - mn) / (mx - mn) if mx > mn else pd.Series(0.0, index=df.index)

    df["score"] = (
        norm("n_alerts")             * 0.30 +
        norm("n_activities_catalog") * 0.25 +
        norm("n_activities")         * 0.20 +
        norm("n_visitor_centers")    * 0.15 +
        norm("n_topics")             * 0.10
    )
    df["busyness_score"] = (df["score"] * 100).round(1)
    return df.sort_values("busyness_score", ascending=False).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# Seasonal model helpers  (tabs 4–6)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner=False)
def load_seasonal_parks() -> pd.DataFrame:
    if not _SEASONAL_AVAILABLE or not DB_PATH.exists():
        return pd.DataFrame()
    return _nps_db.get_all_parks(DB_PATH)


@st.cache_data(ttl=3600, show_spinner=False)
def load_all_cached_models() -> dict[str, dict]:
    """Load all pre-computed models from the DB in one shot at startup."""
    if not _SEASONAL_AVAILABLE or not DB_PATH.exists():
        return {}
    return _nps_db.load_all_park_models(DB_PATH)


def load_model(unit_code: str) -> dict | None:
    """Return a model dict — from the pre-computed cache, or built on the fly."""
    cached = load_all_cached_models()
    if unit_code in cached:
        return cached[unit_code]
    if not _SEASONAL_AVAILABLE:
        return None
    m = _nps_model.build_busyness_model(unit_code, DB_PATH)
    return m.to_dict() if m else None


def score_color(score: float) -> str:
    if score >= 70: return "#c0392b"
    if score >= 50: return "#e67e22"
    if score >= 20: return "#f39c12"
    return "#27ae60"


def badge(text: str, kind: str = "info") -> str:
    return f'<span class="badge-{kind}">{text}</span>'


def month_bar_chart(model: dict, highlight_month: int | None = None) -> go.Figure:
    scores = model["monthly_scores"]
    colors = [
        "#4da6de" if (highlight_month and s["month"] == highlight_month)
        else score_color(s["score"])
        for s in scores
    ]
    fig = go.Figure(go.Bar(
        x=[s["month_name"][:3] for s in scores],
        y=[s["score"] for s in scores],
        marker_color=colors,
        text=[f"{s['score']:.0f}" for s in scores],
        textposition="outside",
        textfont=dict(color="#e8edf2", size=10),
        hovertemplate=(
            "<b>%{x}</b><br>Score: %{y:.1f}<br>"
            "Avg visits: %{customdata:,}<extra></extra>"
        ),
        customdata=[s["avg_visits"] for s in scores],
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(range=[0, 115], gridcolor="#1e3448",
                   title="Busyness Score (0–100)",
                   title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        showlegend=False,
        height=320,
    )
    fig.add_hline(y=70, line_dash="dot", line_color="#c0392b", opacity=0.4,
                  annotation_text="Peak threshold", annotation_font_color="#c0392b",
                  annotation_position="top right")
    fig.add_hline(y=30, line_dash="dot", line_color="#27ae60", opacity=0.4,
                  annotation_text="Quiet threshold", annotation_font_color="#27ae60",
                  annotation_position="bottom right")
    return fig


def comparison_chart(models: dict[str, dict], month: int | None) -> go.Figure:
    fig = go.Figure()
    for uc, m in models.items():
        if month:
            entry = next((s for s in m["monthly_scores"] if s["month"] == month), None)
            if entry:
                fig.add_trace(go.Bar(
                    name=uc,
                    x=[entry["month_name"][:3]],
                    y=[entry["score"]],
                    text=[f"{uc}: {entry['score']:.0f}"],
                    textposition="outside",
                ))
        else:
            fig.add_trace(go.Scatter(
                name=uc,
                x=MONTH_NAMES,
                y=[s["score"] for s in m["monthly_scores"]],
                mode="lines+markers",
                line=dict(width=2),
                marker=dict(size=6),
            ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(range=[0, 115], gridcolor="#1e3448",
                   title="Busyness Score",
                   title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        legend=dict(bgcolor="#152232", bordercolor="#1e3448",
                    font=dict(color="#e8edf2")),
        height=360,
        barmode="group",
    )
    return fig


def _recommend_from_cache(
    all_models: dict[str, dict],
    parks_df: pd.DataFrame,
    state: str | None,
    month: int | None,
    max_score: float,
) -> list[dict]:
    """Filter pre-computed models instead of building them on the fly."""
    df = parks_df.copy()
    if state:
        df = df[df["state"].str.contains(state, na=False)]
    results = []
    for _, park in df.iterrows():
        uc = park["unit_code"]
        m = all_models.get(uc)
        if m is None:
            continue
        if month is not None:
            entry = next((s for s in m["monthly_scores"] if s["month"] == month), None)
            if entry and entry["score"] <= max_score:
                results.append({
                    "unit_code": uc,
                    "name": m["name"],
                    "score": entry["score"],
                    "month_name": entry["month_name"],
                    "state": park.get("state", ""),
                    "yoy_trend": m["yoy_trend"],
                })
        else:
            avg_score = sum(s["score"] for s in m["monthly_scores"]) / 12
            if avg_score <= max_score:
                results.append({
                    "unit_code": uc,
                    "name": m["name"],
                    "avg_score": round(avg_score, 1),
                    "state": park.get("state", ""),
                    "yoy_trend": m["yoy_trend"],
                })
    results.sort(key=lambda x: x.get("score", x.get("avg_score", 0)))
    return results


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════

seasonal_parks_df = load_seasonal_parks()

with st.sidebar:
    st.markdown("## 🏕️ NPS Dashboard")
    st.markdown("---")

    # ── NPS API key ────────────────────────────────────────────────────────────
    if "api_key" not in st.session_state:
        st.session_state.api_key = _ENV_KEY
    api_key_input = st.text_input(
        "NPS API Key",
        value=_ENV_KEY or st.session_state.api_key,
        type="password",
        placeholder="Enter your api.nps.gov key",
        help="Get a free key at https://www.nps.gov/subjects/developer/get-started.htm",
    )
    if api_key_input:
        st.session_state.api_key = api_key_input

    # ── NPS API filters (tabs 1–3) ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("**NPS API Filters**")
    state_filter = st.text_input(
        "Filter by state code (e.g. CA  or  CA,TX)", "",
        key="api_state_filter",
    )
    desig_filter = st.text_input(
        "Filter by designation (e.g. National Park)", "",
        key="api_desig_filter",
    )

    # ── Seasonal model controls (tabs 4–6) ────────────────────────────────────
    st.markdown("---")
    st.markdown("**Seasonal Model**")

    selected_uc: str | None = None
    all_states: list[str] = []

    if not _SEASONAL_AVAILABLE:
        st.warning("Seasonal model unavailable — `nps-seasonal-model/src/` not found.")
    elif seasonal_parks_df.empty:
        st.info("No seasonal data. Run:")
        st.code(
            "python nps-seasonal-model/src/ingest.py"
            " --years 2014-2024 --seed-only",
            language="bash",
        )
    else:
        all_states = sorted({
            s.strip()
            for row in seasonal_parks_df["state"].dropna()
            for s in row.split(",")
            if s.strip()
        })
        seas_state = st.selectbox(
            "Filter by state", ["All"] + all_states, key="seas_state"
        )
        all_types = sorted(seasonal_parks_df["type"].dropna().unique().tolist())
        seas_type = st.selectbox(
            "Filter by type", ["All"] + all_types, key="seas_type"
        )
        max_busy = st.slider("Max busyness score", 0, 100, 100, 5)

        # Apply filters to build park selector
        fp = seasonal_parks_df.copy()
        if seas_state != "All":
            fp = fp[fp["state"].str.contains(seas_state, na=False)]
        if seas_type != "All":
            fp = fp[fp["type"].str.contains(seas_type, case=False, na=False)]

        park_options: dict[str, str] = {
            f"{row['name']} ({row['unit_code']})": row["unit_code"]
            for _, row in fp.sort_values("name").iterrows()
        }
        if not park_options:
            st.warning("No parks match filters.")
        else:
            selected_label = st.selectbox(
                "Select park", list(park_options.keys()), key="seas_park"
            )
            selected_uc = park_options[selected_label]

    st.markdown("---")
    st.caption("NPS API data refreshes every 10 min · Alerts every 5 min.")
    if not seasonal_parks_df.empty:
        st.caption(
            f"{len(seasonal_parks_df)} parks in seasonal DB · "
            "COVID years 2020–21 excluded from baselines."
        )


# ══════════════════════════════════════════════════════════════════════════════
# Header
# ══════════════════════════════════════════════════════════════════════════════

api_key = st.session_state.get("api_key", "")

st.markdown("# 🏕️ NPS Park Dashboard")
st.markdown(
    "Live park data via the **NPS Developer API** (tabs 1–3)  ·  "
    "Historical seasonal busyness model (tabs 4–6)"
)

# ── Load NPS API data ──────────────────────────────────────────────────────────
if api_key:
    with st.spinner("Loading NPS data…"):
        parks_df   = load_api_parks(api_key)
        alerts_df  = load_alerts(api_key)
        acts_df    = load_activities_parks(api_key)
else:
    parks_df  = pd.DataFrame()
    alerts_df = pd.DataFrame(columns=["parkCode","category"])
    acts_df   = pd.DataFrame(columns=["park_code","n_activities_catalog"])

# Apply API sidebar filters
filtered = parks_df.copy()
if state_filter.strip() and not parks_df.empty:
    wanted_states = {s.strip().upper() for s in state_filter.split(",") if s.strip()}
    def _park_has_state(states_str: str) -> bool:
        park_states = {s.strip().upper() for s in str(states_str).split(",") if s.strip()}
        return bool(wanted_states & park_states)
    filtered = filtered[filtered["states"].apply(_park_has_state)]
if desig_filter.strip() and not parks_df.empty:
    filtered = filtered[
        filtered["designation"].str.contains(desig_filter.strip(), case=False, na=False)
    ]

# ── Top-level metrics (NPS API only) ──────────────────────────────────────────
if not parks_df.empty:
    n_parks  = len(parks_df)
    n_states = parks_df["states"].str.split(",").explode().str.strip().nunique()
    n_alerts = len(alerts_df) if not alerts_df.empty else 0
    n_desig  = parks_df["designation"].nunique()

    mc1, mc2, mc3, mc4 = st.columns(4)
    for col, label, value in [
        (mc1, "Total Parks",          f"{n_parks:,}"),
        (mc2, "States / Territories", f"{n_states:,}"),
        (mc3, "Active Alerts",        f"{n_alerts:,}"),
        (mc4, "Designations",         f"{n_desig:,}"),
    ]:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
            </div>""", unsafe_allow_html=True)
elif not api_key:
    st.info(
        "Enter your NPS API key in the sidebar to load live park data.  "
        "Tabs 4–6 (seasonal model) work without an API key."
    )
    st.markdown(
        "Get a free key at "
        "[developer.nps.gov](https://www.nps.gov/subjects/developer/get-started.htm)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Tabs
# ══════════════════════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📋 Parks Overview",
    "📊 Busyness Rankings",
    "🔍 Park Detail",
    "📊 Park Busyness",
    "⚖️ Compare Parks",
    "💡 Recommendations",
])

_NO_API = "Enter your NPS API key in the sidebar to load this tab."


# ────────────────────────────────────────────────────────────────────────────────
# TAB 1 — Parks Overview  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab1:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">All Parks</div>', unsafe_allow_html=True)

        search = st.text_input("Search park name", "", key="search_overview")
        display = filtered.copy()
        if search.strip():
            display = display[display["name"].str.contains(search.strip(), case=False, na=False)]

        show_df = display[["name","states","designation","amenities","url"]].rename(columns={
            "name": "Park Name", "states": "States", "designation": "Designation",
            "amenities": "Amenities", "url": "URL",
        })
        st.dataframe(show_df, use_container_width=True, hide_index=True, height=420)

        st.markdown('<div class="section-header">Parks by Designation</div>', unsafe_allow_html=True)
        desig_counts = (
            filtered["designation"].value_counts()
            .reset_index()
            .rename(columns={"designation": "Designation", "count": "Count"})
            .head(20)
        )
        fig_desig = px.bar(
            desig_counts, x="Count", y="Designation", orientation="h",
            color="Count",
            color_continuous_scale=[[0,"#1a3a52"],[0.5,"#1a6fa8"],[1,"#4da6de"]],
            template="plotly_dark",
        )
        fig_desig.update_layout(
            paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
            margin=dict(l=0, r=0, t=10, b=0),
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
            height=480,
        )
        st.plotly_chart(fig_desig, use_container_width=True)

        st.markdown('<div class="section-header">Park Locations</div>', unsafe_allow_html=True)
        map_df = parks_df.dropna(subset=["lat","lon"])
        map_df = map_df[(map_df["lat"] != 0) & (map_df["lon"] != 0)]
        if not map_df.empty:
            st.map(
                map_df[["lat","lon"]].rename(columns={"lat":"latitude","lon":"longitude"}),
                color="#1a6fa8",
            )


# ────────────────────────────────────────────────────────────────────────────────
# TAB 2 — Busyness Rankings  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab2:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">Composite Busyness Score</div>', unsafe_allow_html=True)
        st.markdown(
            "Busyness is a composite of weighted, normalised proxy signals:  \n"
            "**Active Alerts** (30%) · **Catalog Activities** (25%) · "
            "**Park Activities** (20%) · **Visitor Centers** (15%) · **Topics** (10%)"
        )

        busy_df = compute_busyness(parks_df, alerts_df, acts_df)
        top_n   = st.slider("Show top N parks", min_value=10, max_value=100, value=30, step=5)
        top_busy = busy_df.head(top_n)

        fig_busy = px.bar(
            top_busy, x="busyness_score", y="short_name", orientation="h",
            color="busyness_score",
            color_continuous_scale=[[0,"#1a3a52"],[0.4,"#1a6fa8"],[0.75,"#4da6de"],[1,"#85c9f0"]],
            labels={"busyness_score": "Score", "short_name": "Park"},
            template="plotly_dark",
            hover_data={"states": True, "designation": True, "n_alerts": True,
                        "n_visitor_centers": True, "n_activities_catalog": True},
        )
        fig_busy.update_layout(
            paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
            margin=dict(l=0, r=20, t=10, b=0),
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
            height=max(400, top_n * 22),
        )
        st.plotly_chart(fig_busy, use_container_width=True)

        st.markdown('<div class="section-header">Signal Breakdown (Top 20)</div>', unsafe_allow_html=True)
        top20 = busy_df.head(20)[["short_name","n_alerts","n_activities_catalog",
                                   "n_visitor_centers","n_activities","n_topics","busyness_score"]]
        top20.columns = ["Park","Alerts","Catalog Activities","Visitor Centers",
                         "Park Activities","Topics","Score"]
        st.dataframe(top20, use_container_width=True, hide_index=True)

        st.markdown('<div class="section-header">Activities vs Alerts</div>', unsafe_allow_html=True)
        scatter_df = busy_df[busy_df["n_alerts"] > 0]
        if not scatter_df.empty:
            fig_sc = px.scatter(
                scatter_df, x="n_activities_catalog", y="n_alerts",
                size="busyness_score", color="busyness_score",
                hover_name="short_name",
                color_continuous_scale=[[0,"#1a3a52"],[0.5,"#1a6fa8"],[1,"#4da6de"]],
                labels={"n_activities_catalog":"Catalog Activities","n_alerts":"Active Alerts"},
                template="plotly_dark",
            )
            fig_sc.update_layout(
                paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
                margin=dict(l=0, r=0, t=10, b=0),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            st.info("No alert data available for scatter plot.")


# ────────────────────────────────────────────────────────────────────────────────
# TAB 3 — Park Detail  (NPS API)
# ────────────────────────────────────────────────────────────────────────────────
with tab3:
    if parks_df.empty:
        st.info(_NO_API)
    else:
        st.markdown('<div class="section-header">Park Detail View</div>', unsafe_allow_html=True)

        park_names   = sorted(parks_df["name"].dropna().unique().tolist())
        selected_name = st.selectbox("Select a park", park_names, index=0, key="detail_select")
        park_row      = parks_df[parks_df["name"] == selected_name]

        if park_row.empty:
            st.warning("Park not found.")
        else:
            p = park_row.iloc[0]

            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(f"## {p['name']}")
                if p["designation"]:
                    st.markdown(f'<span class="badge">{p["designation"]}</span>',
                                unsafe_allow_html=True)
                for s in str(p["states"]).split(","):
                    st.markdown(f'<span class="badge">{s.strip()}</span>',
                                unsafe_allow_html=True)
            with col_b:
                busy_row = compute_busyness(parks_df, alerts_df, acts_df)
                match = busy_row[busy_row["park_code"] == p["park_code"]]
                if not match.empty:
                    score = match.iloc[0]["busyness_score"]
                    rank  = match.index[0] + 1
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">Busyness Score</div>
                        <div class="value">{score}</div>
                    </div>
                    <div class="metric-card">
                        <div class="label">Rank</div>
                        <div class="value">#{rank}</div>
                    </div>""", unsafe_allow_html=True)

            st.markdown("---")

            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**About**")
            st.write(p["description"] or "No description available.")
            st.markdown('</div>', unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Entrance Fees**")
                st.write(p["fees"] or "Not available")
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Contact**")
                if p["phone"]: st.write(f"Phone: {p['phone']}")
                if p["email"]: st.write(f"Email: {p['email']}")
                if p["url"]:   st.markdown(f"[Official Website]({p['url']})")
                if not p["phone"] and not p["email"]:
                    st.write("No contact info available")
                st.markdown('</div>', unsafe_allow_html=True)

            with c2:
                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Operating Hours**")
                st.markdown(p["hours"]) if p["hours"] else st.write("Not available")
                st.markdown('</div>', unsafe_allow_html=True)

                st.markdown('<div class="detail-card">', unsafe_allow_html=True)
                st.markdown("**Amenities**")
                if p["amenities"]:
                    for a in p["amenities"].split(", ")[:10]:
                        st.markdown(f'<span class="badge">{a}</span>', unsafe_allow_html=True)
                else:
                    st.write("No amenity data")
                st.markdown('</div>', unsafe_allow_html=True)

            if p["lat"] and p["lon"] and p["lat"] != 0 and p["lon"] != 0:
                st.markdown('<div class="section-header">Location</div>', unsafe_allow_html=True)
                st.map(
                    pd.DataFrame({"latitude": [p["lat"]], "longitude": [p["lon"]]}),
                    zoom=7, color="#1a6fa8",
                )

            if not alerts_df.empty:
                park_alerts = alerts_df[alerts_df["parkCode"] == p["park_code"]]
                if not park_alerts.empty:
                    st.markdown(
                        f'<div class="section-header">Active Alerts ({len(park_alerts)})</div>',
                        unsafe_allow_html=True,
                    )
                    for alert in nps_get("/alerts", api_key, {"parkCode": p["park_code"]})[:10]:
                        category    = alert.get("category","")
                        title       = alert.get("title","")
                        description = alert.get("description","")
                        color = {"Closure":"#c0392b","Danger":"#e67e22"}.get(category,"#1a6fa8")
                        st.markdown(f"""
                        <div class="detail-card" style="border-left-color:{color}">
                            <strong>[{category}] {title}</strong><br>
                            <small>{description}</small>
                        </div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 4 — Park Busyness  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab4:
    st.markdown('<div class="section-header">Seasonal Busyness Model</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE:
        st.error(
            "Seasonal model unavailable — `nps-seasonal-model/src/` not found. "
            "Ensure the project structure is intact."
        )
    elif seasonal_parks_df.empty or selected_uc is None:
        st.info(
            "Run the ingest pipeline to load seasonal data, then select a park "
            "from the **Seasonal Model** section in the sidebar.\n\n"
            "```\npython nps-seasonal-model/src/ingest.py --years 2014-2024 --seed-only\n```"
        )
    else:
        with st.spinner(f"Building model for {selected_uc}…"):
            s_model = load_model(selected_uc)

        if s_model is None:
            st.info(
                f"No seasonal data for **{selected_uc}**. "
                f"Run: `python nps-seasonal-model/src/ingest.py --years 2014-2024 --park {selected_uc}`"
            )
        else:
            d = s_model
            peak_score  = max(ms["score"] for ms in d["monthly_scores"])
            quiet_score = min(ms["score"] for ms in d["monthly_scores"])
            peak_str    = ", ".join(MONTH_NAMES[m-1] for m in d["peak_months"])
            quiet_str   = ", ".join(MONTH_NAMES[m-1] for m in d["quiet_months"][:4])
            data_span   = f"{min(d['data_years'])}–{max(d['data_years'])}"

            s_park_row  = seasonal_parks_df[seasonal_parks_df["unit_code"] == selected_uc]
            state_str   = s_park_row.iloc[0]["state"] if not s_park_row.empty else ""

            # Metric cards
            cm1, cm2, cm3, cm4 = st.columns(4)
            for col, label, value, sub in [
                (cm1, "Park",        d["name"][:28] + ("…" if len(d["name"]) > 28 else ""), state_str),
                (cm2, "Peak Score",  f"{peak_score:.0f}/100",  f"Months: {peak_str}"),
                (cm3, "Quiet Score", f"{quiet_score:.0f}/100", f"Months: {quiet_str}"),
                (cm4, "YoY Trend",   d["yoy_trend"],           f"Data: {data_span}"),
            ]:
                with col:
                    st.markdown(f"""
                    <div class="metric-card">
                        <div class="label">{label}</div>
                        <div class="value">{value}</div>
                        <div class="sub">{sub}</div>
                    </div>""", unsafe_allow_html=True)

            # Monthly bar chart
            st.markdown('<div class="section-header">Monthly Busyness</div>', unsafe_allow_html=True)
            col_chart, col_ctrl = st.columns([3, 1])
            with col_ctrl:
                hl_opts = ["All months"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
                hl_sel  = st.selectbox("Highlight month", hl_opts, key="t4_highlight")
                hl_m    = None if hl_sel == "All months" else int(hl_sel.split(" –")[0])
            with col_chart:
                st.plotly_chart(month_bar_chart(d, hl_m), use_container_width=True)

            with st.expander("Monthly data table"):
                st.dataframe(
                    pd.DataFrame([{
                        "Month":      ms["month_name"],
                        "Score":      ms["score"],
                        "Label":      ms["label"].capitalize(),
                        "Avg Visits": f"{ms['avg_visits']:,}",
                    } for ms in d["monthly_scores"]]),
                    use_container_width=True, hide_index=True,
                )

            # Classification badges
            st.markdown('<div class="section-header">Seasonal Classification</div>', unsafe_allow_html=True)
            bp, bs, bq = st.columns(3)
            with bp:
                st.markdown("**Peak months** *(score ≥ 70)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "peak") for m in d["peak_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)
            with bs:
                st.markdown("**Shoulder months** *(score 20–70)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "shoulder") for m in d["shoulder_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)
            with bq:
                st.markdown("**Quiet months** *(score < 30)*")
                html = " ".join(badge(MONTH_NAMES[m-1], "quiet") for m in d["quiet_months"]) \
                       or "<em style='color:#7a9bbb'>none</em>"
                st.markdown(html, unsafe_allow_html=True)

            # Best visit windows
            if d["best_visit_windows"]:
                st.markdown('<div class="section-header">Best Visit Windows</div>', unsafe_allow_html=True)
                st.caption("Ranked 2-week windows · weather-hostile periods excluded.")
                for w in d["best_visit_windows"]:
                    c = score_color(w["score"])
                    st.markdown(f"""
                    <div class="window-card">
                        <div class="wlabel">{w['label']}
                            <span style="color:{c}; font-size:13px; margin-left:8px;">
                                Score: {w['score']:.0f}/100
                            </span>
                        </div>
                        <div class="wnotes">{w['notes']}</div>
                    </div>""", unsafe_allow_html=True)

            # Footer
            st.markdown("---")
            if d["low_confidence"]:
                st.warning(
                    f"Low confidence: only {len(d['data_years'])} years of data. "
                    "Scores may not reflect long-term patterns."
                )
            excluded_str = ", ".join(str(y) for y in d["excluded_years"])
            st.caption(
                f"Baseline excludes COVID years ({excluded_str}). "
                f"Weekend multiplier: {d['weekend_multiplier']}× (estimated). "
                "Data: NPS IRMA / seed dataset."
            )


# ────────────────────────────────────────────────────────────────────────────────
# TAB 5 — Compare Parks  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab5:
    st.markdown('<div class="section-header">Park Comparison</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE or seasonal_parks_df.empty:
        st.info("Seasonal data not available. Run the ingest pipeline first.")
    else:
        col_left, col_right = st.columns([3, 1])
        with col_left:
            cmp_park_opts: dict[str, str] = {
                f"{row['name']} ({row['unit_code']})": row["unit_code"]
                for _, row in seasonal_parks_df.sort_values("name").iterrows()
            }
            cmp_labels = st.multiselect(
                "Select parks to compare (2–8)",
                list(cmp_park_opts.keys()),
                default=list(cmp_park_opts.keys())[:3],
                key="t5_compare_parks",
            )
        with col_right:
            cmp_month_opts = ["All months"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
            cmp_month_sel  = st.selectbox("Month", cmp_month_opts, key="t5_month")
            cmp_month = None if cmp_month_sel == "All months" \
                             else int(cmp_month_sel.split(" –")[0])

        if not cmp_labels:
            st.info("Select at least one park to compare.")
        else:
            cmp_ucs = [cmp_park_opts[l] for l in cmp_labels]
            with st.spinner("Loading models…"):
                cmp_models = {
                    uc: m
                    for uc in cmp_ucs
                    if (m := load_model(uc)) is not None
                }
            if not cmp_models:
                st.error("No seasonal data for the selected parks.")
            else:
                st.plotly_chart(
                    comparison_chart(cmp_models, cmp_month),
                    use_container_width=True,
                )

                st.markdown('<div class="section-header">Summary Table</div>', unsafe_allow_html=True)
                rows: list[dict] = []
                for uc, m in cmp_models.items():
                    if cmp_month:
                        entry = next(
                            (s for s in m["monthly_scores"] if s["month"] == cmp_month), None
                        )
                        if entry:
                            rows.append({
                                "Park": m["name"], "Code": uc,
                                "Month": entry["month_name"],
                                "Score": entry["score"],
                                "Label": entry["label"].capitalize(),
                                "Avg Visits": f"{entry['avg_visits']:,}",
                                "YoY Trend": m["yoy_trend"],
                            })
                    else:
                        avg    = sum(s["score"] for s in m["monthly_scores"]) / 12
                        peak_m = ", ".join(MONTH_NAMES[x-1] for x in m["peak_months"])
                        rows.append({
                            "Park": m["name"], "Code": uc,
                            "Avg Score": round(avg, 1),
                            "Peak Months": peak_m,
                            "Quiet Months": len(m["quiet_months"]),
                            "YoY Trend": m["yoy_trend"],
                            "Low Confidence": "⚠️" if m["low_confidence"] else "✓",
                        })
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                # Heat-map (all-months view only)
                if not cmp_month and len(cmp_models) > 1:
                    st.markdown('<div class="section-header">Busyness Heat-Map</div>',
                                unsafe_allow_html=True)
                    heat_df = pd.DataFrame(
                        {uc: [s["score"] for s in m["monthly_scores"]]
                         for uc, m in cmp_models.items()},
                        index=MONTH_NAMES,
                    ).T
                    fig_heat = px.imshow(
                        heat_df,
                        color_continuous_scale=[
                            [0,"#27ae60"],[0.3,"#f39c12"],[0.6,"#e67e22"],[1.0,"#c0392b"]
                        ],
                        zmin=0, zmax=100,
                        text_auto=".0f",
                        template="plotly_dark",
                        labels=dict(color="Score"),
                    )
                    fig_heat.update_layout(
                        **PLOTLY_LAYOUT,
                        height=max(200, len(cmp_models) * 50 + 60),
                        coloraxis_colorbar=dict(
                            tickfont=dict(color="#e8edf2"),
                            title=dict(text="Score", font=dict(color="#e8edf2")),
                        ),
                    )
                    st.plotly_chart(fig_heat, use_container_width=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 6 — Recommendations  (seasonal model)
# ────────────────────────────────────────────────────────────────────────────────
with tab6:
    st.markdown('<div class="section-header">Find Low-Busyness Parks</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE:
        st.info("Seasonal model unavailable.")
    elif seasonal_parks_df.empty:
        st.info("Run the ingest pipeline first.")
    else:
        st.markdown("Filter by state, month, and maximum busyness score.")
        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            rec_state = st.selectbox("State", ["Any"] + all_states, key="t6_state")
        with rc2:
            rec_month_opts = ["Any month"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
            rec_month_sel  = st.selectbox("Month", rec_month_opts, key="t6_month")
            rec_month = None if rec_month_sel == "Any month" \
                             else int(rec_month_sel.split(" –")[0])
        with rc3:
            rec_max = st.slider("Max busyness score", 0, 100, 50, 5, key="t6_max")

        recs = _recommend_from_cache(
            load_all_cached_models(),
            seasonal_parks_df,
            state=rec_state if rec_state != "Any" else None,
            month=rec_month,
            max_score=rec_max,
        )

        if not recs:
            st.info("No parks match. Try raising the max score or changing filters.")
        else:
            st.success(f"Found **{len(recs)}** parks matching criteria.")
            rec_df    = pd.DataFrame(recs)
            score_col = "score" if "score" in rec_df.columns else "avg_score"

            fig_rec = px.bar(
                rec_df.sort_values(score_col).head(20),
                x=score_col, y="name", orientation="h",
                color=score_col,
                color_continuous_scale=[[0,"#27ae60"],[0.5,"#f39c12"],[1.0,"#e67e22"]],
                template="plotly_dark",
                labels={score_col: "Busyness Score", "name": "Park"},
            )
            fig_rec.update_layout(
                **PLOTLY_LAYOUT,
                height=max(300, min(len(recs), 20) * 28 + 60),
                yaxis=dict(autorange="reversed", tickfont=dict(color="#7a9bbb")),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_rec, use_container_width=True)

            display_cols = [c for c in ["name","unit_code","state",score_col,
                                        "month_name","yoy_trend"] if c in rec_df.columns]
            st.dataframe(
                rec_df[display_cols].rename(columns={
                    "name": "Park", "unit_code": "Code", "state": "State",
                    "score": "Score", "avg_score": "Avg Score",
                    "month_name": "Month", "yoy_trend": "YoY Trend",
                }),
                use_container_width=True, hide_index=True,
            )

            # Deep-dive
            st.markdown("---")
            st.markdown("**Deep-dive into a recommended park:**")
            deep_opts  = {r["name"]: r["unit_code"] for r in recs}
            deep_label = st.selectbox("Choose park", list(deep_opts.keys()), key="t6_deep")
            deep_uc    = deep_opts[deep_label]
            if st.button("Load busyness model →"):
                with st.spinner("Loading…"):
                    deep_model = load_model(deep_uc)
                if deep_model:
                    st.markdown(f"### {deep_model['name']}")
                    st.plotly_chart(
                        month_bar_chart(deep_model, rec_month),
                        use_container_width=True,
                    )
                    for w in deep_model["best_visit_windows"][:3]:
                        st.markdown(f"""
                        <div class="window-card">
                            <div class="wlabel">{w['label']} — Score: {w['score']:.0f}/100</div>
                            <div class="wnotes">{w['notes']}</div>
                        </div>""", unsafe_allow_html=True)
