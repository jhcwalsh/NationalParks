import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional
import sys
from pathlib import Path

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

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .stApp { background-color: #0f1923; color: #e8edf2; }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #152232;
        border-right: 1px solid #1e3448;
    }

    /* Metric cards */
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

    /* Section headers */
    .section-header {
        font-size: 18px;
        font-weight: 600;
        color: #4da6de;
        border-bottom: 1px solid #1e3448;
        padding-bottom: 8px;
        margin: 24px 0 16px 0;
    }

    /* Park detail card */
    .detail-card {
        background: #152232;
        border: 1px solid #1e3448;
        border-radius: 8px;
        padding: 20px 24px;
        margin-bottom: 16px;
    }

    /* Badges */
    .badge {
        display: inline-block;
        background: #1a6fa8;
        color: #e8edf2;
        font-size: 11px;
        padding: 3px 10px;
        border-radius: 12px;
        margin: 2px 3px;
    }

    /* DataFrames */
    [data-testid="stDataFrame"] { border: 1px solid #1e3448; border-radius: 6px; }

    /* Buttons */
    .stButton > button {
        background: #1a6fa8;
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 500;
    }
    .stButton > button:hover { background: #1d82c7; }

    /* Input widgets */
    .stTextInput input, .stSelectbox div[data-baseweb="select"] {
        background: #0f1923;
        border-color: #1e3448;
        color: #e8edf2;
    }

    /* Hide Streamlit branding */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* Seasonal tab extras */
    .badge-peak     { display:inline-block; background:#c0392b; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-shoulder { display:inline-block; background:#e67e22; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .badge-quiet    { display:inline-block; background:#27ae60; color:#fff;
                      font-size:11px; padding:3px 10px; border-radius:12px; margin:2px 3px; }
    .metric-card .sub {
        font-size: 12px; color: #7a9bbb; margin-top: 2px;
    }
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
</style>
""", unsafe_allow_html=True)

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL = "https://developer.nps.gov/api/v1"
LIMIT = 500  # max records per request


# ── API helpers ────────────────────────────────────────────────────────────────
def nps_get(endpoint: str, api_key: str, params: Optional[dict] = None) -> list:
    """Fetch all records from a paginated NPS endpoint."""
    params = params or {}
    params.update({"api_key": api_key, "limit": LIMIT, "start": 0})
    all_data = []
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
        total = int(body.get("total", 0))
        if len(all_data) >= total or not data:
            break
        params["start"] = len(all_data)
    return all_data


@st.cache_data(ttl=600, show_spinner=False)
def load_parks(api_key: str) -> pd.DataFrame:
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
            h = hours_raw[0]
            std = h.get("standardHours", {})
            days = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            hours_str = "  \n".join(f"{d.capitalize()}: {std.get(d,'N/A')}" for d in days)

        contacts = p.get("contacts", {})
        phones = contacts.get("phoneNumbers", [])
        emails = contacts.get("emailAddresses", [])
        phone_str = phones[0].get("phoneNumber","") if phones else ""
        email_str = emails[0].get("emailAddress","") if emails else ""

        amenities = p.get("amenities", [])
        amenity_names = [a.get("name","") if isinstance(a, dict) else str(a) for a in amenities]

        rows.append({
            "park_code": p.get("parkCode",""),
            "name": p.get("fullName",""),
            "short_name": p.get("name",""),
            "states": p.get("states",""),
            "designation": p.get("designation",""),
            "description": p.get("description",""),
            "lat": float(p.get("latitude") or 0) or None,
            "lon": float(p.get("longitude") or 0) or None,
            "url": p.get("url",""),
            "fees": fee_str,
            "hours": hours_str,
            "phone": phone_str,
            "email": email_str,
            "amenities": ", ".join(amenity_names),
            "n_visitor_centers": len(p.get("visitorCenters", [])),
            "n_topics": len(p.get("topics", [])),
            "n_activities": len(p.get("activities", [])),
            "images": p.get("images", []),
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=300, show_spinner=False)
def load_alerts(api_key: str) -> pd.DataFrame:
    raw = nps_get("/alerts", api_key)
    if not raw:
        return pd.DataFrame(columns=["parkCode","category"])
    rows = [{"parkCode": a.get("parkCode",""), "category": a.get("category","")} for a in raw]
    return pd.DataFrame(rows)


@st.cache_data(ttl=600, show_spinner=False)
def load_activities_parks(api_key: str) -> pd.DataFrame:
    raw = nps_get("/activities/parks", api_key)
    if not raw:
        return pd.DataFrame(columns=["park_code","n_activities_catalog"])
    rows = []
    for activity in raw:
        parks = activity.get("parks", [])
        for pk in parks:
            rows.append({"park_code": pk.get("parkCode",""), "activity": activity.get("name","")})
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["park_code","activity"])
    if not df.empty:
        df = df.groupby("park_code").size().reset_index(name="n_activities_catalog")
    return df


def compute_busyness(parks_df: pd.DataFrame, alerts_df: pd.DataFrame, acts_df: pd.DataFrame) -> pd.DataFrame:
    """Composite busyness score from proxy signals."""
    df = parks_df[["park_code","short_name","states","designation",
                   "n_visitor_centers","n_activities","n_topics"]].copy()

    # alerts per park
    if not alerts_df.empty:
        alert_counts = alerts_df.groupby("parkCode").size().reset_index(name="n_alerts")
        df = df.merge(alert_counts, left_on="park_code", right_on="parkCode", how="left").drop(columns=["parkCode"], errors="ignore")
    else:
        df["n_alerts"] = 0

    # catalog activities per park
    if not acts_df.empty and "park_code" in acts_df.columns:
        df = df.merge(acts_df, on="park_code", how="left")
    else:
        df["n_activities_catalog"] = 0

    df = df.fillna(0)

    # Normalise each signal 0-1 then weight
    def norm(col):
        mn, mx = df[col].min(), df[col].max()
        return (df[col] - mn) / (mx - mn) if mx > mn else pd.Series(0.0, index=df.index)

    df["score"] = (
        norm("n_alerts")          * 0.30 +
        norm("n_activities_catalog") * 0.25 +
        norm("n_activities")      * 0.20 +
        norm("n_visitor_centers") * 0.15 +
        norm("n_topics")          * 0.10
    )
    df["busyness_score"] = (df["score"] * 100).round(1)
    return df.sort_values("busyness_score", ascending=False).reset_index(drop=True)


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏕️ NPS Dashboard")
    st.markdown("---")

    if "api_key" not in st.session_state:
        st.session_state.api_key = ""

    api_key_input = st.text_input(
        "NPS API Key",
        value=st.session_state.api_key,
        type="password",
        placeholder="Enter your api.nps.gov key",
        help="Get a free key at https://www.nps.gov/subjects/developer/get-started.htm",
    )
    if api_key_input:
        st.session_state.api_key = api_key_input

    st.markdown("---")
    st.markdown("**Filters**")
    state_filter = st.text_input("Filter by state code (e.g. CA  or  CA,TX)", "")
    desig_filter = st.text_input("Filter by designation (e.g. National Park)", "")

    st.markdown("---")
    st.caption("Data refreshes every 10 min. Alerts every 5 min.")

api_key = st.session_state.get("api_key", "")

# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("# 🏕️ National Parks Service Dashboard")
st.markdown("Explore park activity, busyness signals, and detail information across the NPS network.")

if not api_key:
    st.warning("Enter your NPS API key in the sidebar to load data.")
    st.markdown("""
    **Get a free API key:**
    Visit the NPS Developer portal, register, and paste your key in the sidebar.
    """)
    st.stop()

# ── Load data ──────────────────────────────────────────────────────────────────
with st.spinner("Loading NPS data…"):
    parks_df = load_parks(api_key)
    alerts_df = load_alerts(api_key)
    acts_df = load_activities_parks(api_key)

if parks_df.empty:
    st.error("Could not load park data. Check your API key and try again.")
    st.stop()

# Apply sidebar filters
filtered = parks_df.copy()
if state_filter.strip():
    # Split the input on commas, trim whitespace, uppercase each code, then
    # keep only parks whose states field (also comma-separated) contains at
    # least one of the requested codes as an exact match.
    wanted_states = {s.strip().upper() for s in state_filter.split(",") if s.strip()}
    def _park_has_state(states_str: str) -> bool:
        park_states = {s.strip().upper() for s in str(states_str).split(",") if s.strip()}
        return bool(wanted_states & park_states)
    filtered = filtered[filtered["states"].apply(_park_has_state)]
if desig_filter.strip():
    filtered = filtered[filtered["designation"].str.contains(desig_filter.strip(), case=False, na=False)]

# ── Top metrics ────────────────────────────────────────────────────────────────
n_parks = len(parks_df)
n_states = parks_df["states"].str.split(",").explode().str.strip().nunique()
n_alerts = len(alerts_df) if not alerts_df.empty else 0
n_desig = parks_df["designation"].nunique()

col1, col2, col3, col4 = st.columns(4)
for col, label, value in [
    (col1, "Total Parks", f"{n_parks:,}"),
    (col2, "States / Territories", f"{n_states:,}"),
    (col3, "Active Alerts", f"{n_alerts:,}"),
    (col4, "Designations", f"{n_desig:,}"),
]:
    with col:
        st.markdown(f"""
        <div class="metric-card">
            <div class="label">{label}</div>
            <div class="value">{value}</div>
        </div>""", unsafe_allow_html=True)

# ── Tab layout ─────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📋 Parks Overview", "📊 Busyness Rankings", "🔍 Park Detail", "📅 Seasonal Busyness"])

# ────────────────────────────────────────────────────────────────────────────────
# TAB 1 — Parks Overview
# ────────────────────────────────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">All Parks</div>', unsafe_allow_html=True)

    search = st.text_input("Search park name", "", key="search_overview")
    display = filtered.copy()
    if search.strip():
        display = display[display["name"].str.contains(search.strip(), case=False, na=False)]

    table_cols = ["name","states","designation","amenities","url"]
    show_df = display[table_cols].rename(columns={
        "name": "Park Name", "states": "States", "designation": "Designation",
        "amenities": "Amenities", "url": "URL",
    })
    st.dataframe(show_df, use_container_width=True, hide_index=True, height=420)

    # Designation breakdown
    st.markdown('<div class="section-header">Parks by Designation</div>', unsafe_allow_html=True)
    desig_counts = (
        filtered["designation"]
        .value_counts()
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

    # State coverage map
    st.markdown('<div class="section-header">Park Locations (Lat/Lon)</div>', unsafe_allow_html=True)
    map_df = parks_df.dropna(subset=["lat","lon"])
    map_df = map_df[(map_df["lat"] != 0) & (map_df["lon"] != 0)]
    if not map_df.empty:
        st.map(map_df[["lat","lon"]].rename(columns={"lat":"latitude","lon":"longitude"}),
               color="#1a6fa8")


# ────────────────────────────────────────────────────────────────────────────────
# TAB 2 — Busyness Rankings
# ────────────────────────────────────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-header">Composite Busyness Score</div>', unsafe_allow_html=True)
    st.markdown("""
    Busyness is a composite of weighted, normalised proxy signals:
    **Active Alerts** (30%) · **Catalog Activities** (25%) · **Park Activities** (20%) · **Visitor Centers** (15%) · **Topics** (10%)
    """)

    busy_df = compute_busyness(parks_df, alerts_df, acts_df)

    top_n = st.slider("Show top N parks", min_value=10, max_value=100, value=30, step=5)
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

    # Scatter: activities vs alerts
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
# TAB 3 — Park Detail
# ────────────────────────────────────────────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-header">Park Detail View</div>', unsafe_allow_html=True)

    park_names = sorted(parks_df["name"].dropna().unique().tolist())
    selected_name = st.selectbox("Select a park", park_names, index=0, key="detail_select")

    park_row = parks_df[parks_df["name"] == selected_name]
    if park_row.empty:
        st.warning("Park not found.")
    else:
        p = park_row.iloc[0]

        # Header row
        col_a, col_b = st.columns([3, 1])
        with col_a:
            st.markdown(f"## {p['name']}")
            if p["designation"]:
                st.markdown(f'<span class="badge">{p["designation"]}</span>', unsafe_allow_html=True)
            for s in str(p["states"]).split(","):
                st.markdown(f'<span class="badge">{s.strip()}</span>', unsafe_allow_html=True)

        with col_b:
            busy_row = compute_busyness(parks_df, alerts_df, acts_df)
            match = busy_row[busy_row["park_code"] == p["park_code"]]
            if not match.empty:
                score = match.iloc[0]["busyness_score"]
                rank = match.index[0] + 1
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

        # Description
        st.markdown('<div class="detail-card">', unsafe_allow_html=True)
        st.markdown("**About**")
        st.write(p["description"] or "No description available.")
        st.markdown('</div>', unsafe_allow_html=True)

        # Info columns
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**Entrance Fees**")
            st.write(p["fees"] or "Not available")
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**Contact**")
            if p["phone"]:
                st.write(f"Phone: {p['phone']}")
            if p["email"]:
                st.write(f"Email: {p['email']}")
            if p["url"]:
                st.markdown(f"[Official Website]({p['url']})")
            if not p["phone"] and not p["email"]:
                st.write("No contact info available")
            st.markdown('</div>', unsafe_allow_html=True)

        with c2:
            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**Operating Hours**")
            if p["hours"]:
                st.markdown(p["hours"])
            else:
                st.write("Not available")
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('<div class="detail-card">', unsafe_allow_html=True)
            st.markdown("**Amenities**")
            if p["amenities"]:
                for a in p["amenities"].split(", ")[:10]:
                    st.markdown(f'<span class="badge">{a}</span>', unsafe_allow_html=True)
            else:
                st.write("No amenity data")
            st.markdown('</div>', unsafe_allow_html=True)

        # Map
        if p["lat"] and p["lon"] and p["lat"] != 0 and p["lon"] != 0:
            st.markdown('<div class="section-header">Location</div>', unsafe_allow_html=True)
            map_point = pd.DataFrame({"latitude": [p["lat"]], "longitude": [p["lon"]]})
            st.map(map_point, zoom=7, color="#1a6fa8")

        # Park alerts
        if not alerts_df.empty:
            park_alerts = alerts_df[alerts_df["parkCode"] == p["park_code"]]
            if not park_alerts.empty:
                st.markdown(f'<div class="section-header">Active Alerts ({len(park_alerts)})</div>',
                            unsafe_allow_html=True)
                raw_alerts = [
                    a for a in nps_get("/alerts", api_key, {"parkCode": p["park_code"]})
                ]
                for alert in raw_alerts[:10]:
                    category = alert.get("category","")
                    title = alert.get("title","")
                    description = alert.get("description","")
                    color = {"Closure":"#c0392b","Danger":"#e67e22"}.get(category,"#1a6fa8")
                    st.markdown(f"""
                    <div class="detail-card" style="border-left-color:{color}">
                        <strong>[{category}] {title}</strong><br>
                        <small>{description}</small>
                    </div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# TAB 4 — Seasonal Busyness
# ────────────────────────────────────────────────────────────────────────────────

_MONTH_NAMES_SHORT = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
_PLOTLY_BG = dict(paper_bgcolor="#0f1923", plot_bgcolor="#0f1923",
                  font=dict(color="#e8edf2"), margin=dict(l=10,r=10,t=30,b=10))


def _score_color(score: float) -> str:
    if score >= 70: return "#c0392b"
    if score >= 50: return "#e67e22"
    if score >= 20: return "#f39c12"
    return "#27ae60"


def _seasonal_bar_chart(model_dict: dict) -> go.Figure:
    scores = model_dict["monthly_scores"]
    fig = go.Figure(go.Bar(
        x=[s["month_name"][:3] for s in scores],
        y=[s["score"] for s in scores],
        marker_color=[_score_color(s["score"]) for s in scores],
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
        **_PLOTLY_BG,
        yaxis=dict(range=[0, 115], gridcolor="#1e3448",
                   title="Busyness Score (0–100)",
                   title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        showlegend=False, height=300,
    )
    fig.add_hline(y=70, line_dash="dot", line_color="#c0392b", opacity=0.4,
                  annotation_text="Peak", annotation_font_color="#c0392b",
                  annotation_position="top right")
    fig.add_hline(y=30, line_dash="dot", line_color="#27ae60", opacity=0.4,
                  annotation_text="Quiet", annotation_font_color="#27ae60",
                  annotation_position="bottom right")
    return fig


with tab4:
    st.markdown('<div class="section-header">Seasonal Busyness Model</div>', unsafe_allow_html=True)

    if not _SEASONAL_AVAILABLE:
        st.error(
            "Seasonal model modules not found. Ensure `nps-seasonal-model/src/` "
            "exists and run:\n\n"
            "```\npython nps-seasonal-model/src/ingest.py --years 2014-2024 --seed-only\n```"
        )
    else:
        # Park selector — mirrors the Tab 3 picker so the two tabs feel linked
        s_park_names = sorted(parks_df["name"].dropna().unique().tolist())
        s_selected_name = st.selectbox(
            "Select a park", s_park_names, index=0, key="seasonal_select"
        )

        s_park_row = parks_df[parks_df["name"] == s_selected_name]
        if s_park_row.empty:
            st.warning("Park not found.")
        else:
            unit_code = s_park_row.iloc[0]["park_code"].upper()

            with st.spinner(f"Building seasonal model for {unit_code}…"):
                s_model = _nps_model.build_busyness_model(unit_code)

            if s_model is None:
                st.info(
                    f"No seasonal data found for **{unit_code}**. "
                    "The built-in dataset covers the 20 most-visited parks. "
                    "To add more parks run:\n\n"
                    f"```\npython nps-seasonal-model/src/ingest.py --years 2014-2024 --park {unit_code}\n```"
                )
            else:
                d = s_model.to_dict()
                peak_score  = max(ms["score"] for ms in d["monthly_scores"])
                quiet_score = min(ms["score"] for ms in d["monthly_scores"])
                peak_month_str = ", ".join(
                    _MONTH_NAMES_SHORT[m - 1] for m in d["peak_months"]
                )
                quiet_month_str = ", ".join(
                    _MONTH_NAMES_SHORT[m - 1] for m in d["quiet_months"][:4]
                )
                data_span = f"{min(d['data_years'])}–{max(d['data_years'])}"

                # ── Top metrics ────────────────────────────────────────────
                mc1, mc2, mc3, mc4 = st.columns(4)
                for col, label, value, sub in [
                    (mc1, "Unit Code",   unit_code,           d["name"][:30]),
                    (mc2, "Peak Score",  f"{peak_score:.0f}/100",  f"Months: {peak_month_str}"),
                    (mc3, "Quiet Score", f"{quiet_score:.0f}/100", f"Months: {quiet_month_str}"),
                    (mc4, "YoY Trend",   d["yoy_trend"],      f"Data: {data_span}"),
                ]:
                    with col:
                        st.markdown(f"""
                        <div class="metric-card">
                            <div class="label">{label}</div>
                            <div class="value">{value}</div>
                            <div class="sub">{sub}</div>
                        </div>""", unsafe_allow_html=True)

                # ── Monthly bar chart ──────────────────────────────────────
                st.markdown('<div class="section-header">Monthly Busyness Scores</div>',
                            unsafe_allow_html=True)
                st.plotly_chart(_seasonal_bar_chart(d), use_container_width=True)

                # ── Peak / Shoulder / Quiet badges ─────────────────────────
                st.markdown('<div class="section-header">Seasonal Classification</div>',
                            unsafe_allow_html=True)
                bc1, bc2, bc3 = st.columns(3)
                with bc1:
                    st.markdown("**Peak months** *(score ≥ 70)*")
                    html = " ".join(
                        f'<span class="badge-peak">{_MONTH_NAMES_SHORT[m-1]}</span>'
                        for m in d["peak_months"]
                    ) or "<em style='color:#7a9bbb'>none</em>"
                    st.markdown(html, unsafe_allow_html=True)
                with bc2:
                    st.markdown("**Shoulder months** *(score 20–70)*")
                    html = " ".join(
                        f'<span class="badge-shoulder">{_MONTH_NAMES_SHORT[m-1]}</span>'
                        for m in d["shoulder_months"]
                    ) or "<em style='color:#7a9bbb'>none</em>"
                    st.markdown(html, unsafe_allow_html=True)
                with bc3:
                    st.markdown("**Quiet months** *(score < 30)*")
                    html = " ".join(
                        f'<span class="badge-quiet">{_MONTH_NAMES_SHORT[m-1]}</span>'
                        for m in d["quiet_months"]
                    ) or "<em style='color:#7a9bbb'>none</em>"
                    st.markdown(html, unsafe_allow_html=True)

                # ── Best visit windows ─────────────────────────────────────
                if d["best_visit_windows"]:
                    st.markdown('<div class="section-header">Best Visit Windows</div>',
                                unsafe_allow_html=True)
                    st.caption(
                        "Ranked 2-week windows with lowest expected busyness, "
                        "weather-hostile periods excluded."
                    )
                    for w in d["best_visit_windows"]:
                        color = _score_color(w["score"])
                        st.markdown(f"""
                        <div class="window-card">
                            <div class="wlabel">
                                {w['label']}
                                <span style="color:{color}; font-size:13px; margin-left:8px;">
                                    Score: {w['score']:.0f}/100
                                </span>
                            </div>
                            <div class="wnotes">{w['notes']}</div>
                        </div>""", unsafe_allow_html=True)

                # ── Data quality footer ────────────────────────────────────
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
                    f"Data: NPS IRMA / seed dataset."
                )
