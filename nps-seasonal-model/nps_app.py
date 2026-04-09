"""
NPS Seasonal Busyness — Streamlit Dashboard
============================================
Primary UI for exploring historical visitation patterns across US National Parks.

Imports model and db directly — no HTTP round-trip needed.

Run
---
    streamlit run nps_app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "src"))
import db
import model as mdl

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="NPS Seasonal Busyness",
    page_icon="🏕️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
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

    .stSelectbox > div, .stMultiSelect > div {
        background: #152232 !important;
    }

    div[data-testid="stTabs"] button {
        color: #7a9bbb;
    }
    div[data-testid="stTabs"] button[aria-selected="true"] {
        color: #4da6de;
        border-bottom-color: #4da6de;
    }

    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── DB path ────────────────────────────────────────────────────────────────────
DB_PATH = ROOT / "data" / "nps.db"

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0f1923",
    plot_bgcolor="#0f1923",
    font=dict(color="#e8edf2"),
    margin=dict(l=10, r=10, t=30, b=10),
)

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

SCORE_COLOR_SCALE = [
    [0.0,  "#27ae60"],
    [0.3,  "#f39c12"],
    [0.7,  "#e67e22"],
    [1.0,  "#c0392b"],
]


# ── Cached data loaders ────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_parks() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    return db.get_all_parks(DB_PATH)


@st.cache_data(ttl=300, show_spinner=False)
def load_model(unit_code: str) -> dict | None:
    m = mdl.build_busyness_model(unit_code, DB_PATH)
    return m.to_dict() if m else None


@st.cache_data(ttl=300, show_spinner=False)
def load_all_models() -> dict[str, dict]:
    models = mdl.build_all_models(DB_PATH)
    return {uc: m.to_dict() for uc, m in models.items()}


# ── Helpers ────────────────────────────────────────────────────────────────────

def score_color(score: float) -> str:
    if score >= 70:
        return "#c0392b"
    if score >= 50:
        return "#e67e22"
    if score >= 20:
        return "#f39c12"
    return "#27ae60"


def score_label(score: float) -> str:
    if score >= 70:
        return "peak"
    if score >= 50:
        return "shoulder"
    if score >= 20:
        return "shoulder"
    return "quiet"


def badge(text: str, kind: str = "info") -> str:
    return f'<span class="badge-{kind}">{text}</span>'


def month_bar_chart(model: dict, highlight_month: int | None = None) -> go.Figure:
    scores = model["monthly_scores"]
    months = [s["month_name"][:3] for s in scores]
    values = [s["score"] for s in scores]
    colors = [score_color(s["score"]) for s in scores]

    if highlight_month:
        colors = [
            "#4da6de" if s["month"] == highlight_month else score_color(s["score"])
            for s in scores
        ]

    fig = go.Figure(
        go.Bar(
            x=months,
            y=values,
            marker_color=colors,
            text=[f"{v:.0f}" for v in values],
            textposition="outside",
            textfont=dict(color="#e8edf2", size=10),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Score: %{y:.1f}<br>"
                "Avg visits: %{customdata:,}<extra></extra>"
            ),
            customdata=[s["avg_visits"] for s in scores],
        )
    )
    fig.update_layout(
        **PLOTLY_LAYOUT,
        yaxis=dict(
            range=[0, 115],
            gridcolor="#1e3448",
            title="Busyness Score (0–100)",
            title_font=dict(color="#7a9bbb"),
            tickfont=dict(color="#7a9bbb"),
        ),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        showlegend=False,
        height=320,
    )
    # Add reference lines
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
                   title="Busyness Score", title_font=dict(color="#7a9bbb"),
                   tickfont=dict(color="#7a9bbb")),
        xaxis=dict(tickfont=dict(color="#7a9bbb")),
        legend=dict(bgcolor="#152232", bordercolor="#1e3448", font=dict(color="#e8edf2")),
        height=360,
        barmode="group",
    )
    return fig


# ── Sidebar ────────────────────────────────────────────────────────────────────

parks_df = load_parks()

with st.sidebar:
    st.markdown("## 🏕️ NPS Seasonal Model")
    st.markdown("---")

    if parks_df.empty:
        st.error("No data loaded. Run the ingest pipeline first:")
        st.code("python src/ingest.py --years 2014-2024 --seed-only")
        st.stop()

    # State filter
    all_states: list[str] = sorted({
        s.strip()
        for states in parks_df["state"].dropna()
        for s in states.split(",")
        if s.strip()
    })
    state_filter = st.selectbox("Filter by state", ["All"] + all_states)

    # Park type filter
    all_types = sorted(parks_df["type"].dropna().unique().tolist())
    type_filter = st.selectbox("Filter by type", ["All"] + all_types)

    # Max busyness filter
    max_busy = st.slider("Max busyness score", 0, 100, 100, 5)

    st.markdown("---")

    # Apply filters for park selector
    filtered_parks = parks_df.copy()
    if state_filter != "All":
        filtered_parks = filtered_parks[
            filtered_parks["state"].str.contains(state_filter, na=False)
        ]
    if type_filter != "All":
        filtered_parks = filtered_parks[
            filtered_parks["type"].str.contains(type_filter, case=False, na=False)
        ]

    park_options = {
        f"{row['name']} ({row['unit_code']})": row["unit_code"]
        for _, row in filtered_parks.sort_values("name").iterrows()
    }
    if not park_options:
        st.warning("No parks match filters.")
        st.stop()

    selected_label = st.selectbox("Select park", list(park_options.keys()))
    selected_uc = park_options[selected_label]

    st.markdown("---")
    st.caption(f"{len(parks_df)} parks in database")
    st.caption("Data: NPS IRMA / seed 2014–2024")
    st.caption("COVID years (2020–21) excluded from baselines")


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown("# 🏕️ NPS Seasonal Busyness Model")
st.markdown(
    "Historical visitation patterns for US National Parks · "
    "Baseline excludes 2020–21 COVID years"
)

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(
    ["📊 Park Busyness", "⚖️ Compare Parks", "💡 Recommendations"]
)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — Park Busyness
# ════════════════════════════════════════════════════════════════════════════════
with tab1:
    with st.spinner("Building model…"):
        model = load_model(selected_uc)

    if model is None:
        st.error(f"No data available for {selected_uc}. Run ingest pipeline.")
        st.stop()

    # ── Top row: metrics ───────────────────────────────────────────────────
    park_row = parks_df[parks_df["unit_code"] == selected_uc].iloc[0]
    peak_score = max(s["score"] for s in model["monthly_scores"])
    quiet_score = min(s["score"] for s in model["monthly_scores"])
    peak_month_names = ", ".join(
        MONTH_NAMES[m - 1] for m in model["peak_months"]
    )
    data_span = f"{min(model['data_years'])}–{max(model['data_years'])}"

    c1, c2, c3, c4 = st.columns(4)
    metrics = [
        (c1, "Park", f"{model['name'][:25]}…" if len(model['name']) > 25 else model['name'],
         park_row.get("state", "")),
        (c2, "Peak Score", f"{peak_score:.0f}/100", f"Months: {peak_month_names}"),
        (c3, "Quiet Score", f"{quiet_score:.0f}/100",
         f"Months: {', '.join(MONTH_NAMES[m-1] for m in model['quiet_months'][:3])}"),
        (c4, "YoY Trend", model["yoy_trend"], f"Data: {data_span}"),
    ]
    for col, label, value, sub in metrics:
        with col:
            st.markdown(f"""
            <div class="metric-card">
                <div class="label">{label}</div>
                <div class="value">{value}</div>
                <div class="sub">{sub}</div>
            </div>""", unsafe_allow_html=True)

    # ── Month highlight filter ─────────────────────────────────────────────
    st.markdown('<div class="section-header">Monthly Busyness</div>', unsafe_allow_html=True)
    col_a, col_b = st.columns([3, 1])
    with col_b:
        highlight_options = ["All months"] + [
            f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)
        ]
        highlight_sel = st.selectbox("Highlight month", highlight_options, key="highlight")
        highlight_m = None
        if highlight_sel != "All months":
            highlight_m = int(highlight_sel.split(" –")[0])

    with col_a:
        fig = month_bar_chart(model, highlight_m)
        st.plotly_chart(fig, use_container_width=True)

    # ── Month table ────────────────────────────────────────────────────────
    with st.expander("Monthly data table"):
        rows = []
        for ms in model["monthly_scores"]:
            rows.append({
                "Month": ms["month_name"],
                "Score": ms["score"],
                "Label": ms["label"].capitalize(),
                "Avg Visits": f"{ms['avg_visits']:,}",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Season badges ──────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Seasonal Classification</div>', unsafe_allow_html=True)
    col_p, col_s, col_q = st.columns(3)

    with col_p:
        st.markdown("**Peak months**")
        badges_html = " ".join(
            badge(MONTH_NAMES[m - 1], "peak") for m in model["peak_months"]
        )
        st.markdown(badges_html + f'<br><small style="color:#7a9bbb">Score ≥ 70</small>',
                    unsafe_allow_html=True)

    with col_s:
        st.markdown("**Shoulder months**")
        badges_html = " ".join(
            badge(MONTH_NAMES[m - 1], "shoulder") for m in model["shoulder_months"]
        )
        st.markdown(badges_html + '<br><small style="color:#7a9bbb">Score 20–70</small>',
                    unsafe_allow_html=True)

    with col_q:
        st.markdown("**Quiet months**")
        badges_html = " ".join(
            badge(MONTH_NAMES[m - 1], "quiet") for m in model["quiet_months"]
        )
        st.markdown(badges_html + '<br><small style="color:#7a9bbb">Score < 30</small>',
                    unsafe_allow_html=True)

    # ── Best visit windows ─────────────────────────────────────────────────
    if model["best_visit_windows"]:
        st.markdown('<div class="section-header">Best Visit Windows</div>', unsafe_allow_html=True)
        st.markdown(
            "Ranked 2-week windows with lowest expected busyness, "
            "excluding weather-hostile periods."
        )
        for w in model["best_visit_windows"]:
            color = score_color(w["score"])
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

    # ── Confidence / data quality note ────────────────────────────────────
    if model["low_confidence"]:
        st.warning(
            f"Low confidence: only {len(model['data_years'])} years of data available. "
            "Scores may not reflect long-term patterns."
        )

    if model.get("weekend_multiplier"):
        st.caption(
            f"Weekend multiplier: {model['weekend_multiplier']}× "
            "(estimated — no weekly breakdown in IRMA monthly data)"
        )


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — Compare Parks
# ════════════════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div class="section-header">Park Comparison</div>', unsafe_allow_html=True)

    col_left, col_right = st.columns([3, 1])
    with col_left:
        compare_park_options = {
            f"{row['name']} ({row['unit_code']})": row["unit_code"]
            for _, row in parks_df.sort_values("name").iterrows()
        }
        compare_labels = st.multiselect(
            "Select parks to compare (2–8)",
            list(compare_park_options.keys()),
            default=list(compare_park_options.keys())[:3],
            key="compare_parks",
        )
    with col_right:
        month_options = ["All months"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
        compare_month_sel = st.selectbox("Month", month_options, key="cmp_month")
        compare_month = None
        if compare_month_sel != "All months":
            compare_month = int(compare_month_sel.split(" –")[0])

    if len(compare_labels) < 1:
        st.info("Select at least one park to compare.")
    else:
        compare_ucs = [compare_park_options[l] for l in compare_labels]
        with st.spinner("Loading models…"):
            compare_models = {
                uc: m
                for uc in compare_ucs
                if (m := load_model(uc)) is not None
            }

        if not compare_models:
            st.error("No data for selected parks.")
        else:
            fig_cmp = comparison_chart(compare_models, compare_month)
            st.plotly_chart(fig_cmp, use_container_width=True)

            # Summary table
            st.markdown('<div class="section-header">Summary Table</div>', unsafe_allow_html=True)
            rows = []
            for uc, m in compare_models.items():
                if compare_month:
                    entry = next(
                        (s for s in m["monthly_scores"] if s["month"] == compare_month),
                        None,
                    )
                    if entry:
                        rows.append({
                            "Park": m["name"],
                            "Code": uc,
                            "Month": entry["month_name"],
                            "Score": entry["score"],
                            "Label": entry["label"].capitalize(),
                            "Avg Visits": f"{entry['avg_visits']:,}",
                            "YoY Trend": m["yoy_trend"],
                        })
                else:
                    avg = sum(s["score"] for s in m["monthly_scores"]) / 12
                    peak_m = ", ".join(MONTH_NAMES[x - 1] for x in m["peak_months"])
                    rows.append({
                        "Park": m["name"],
                        "Code": uc,
                        "Avg Score": round(avg, 1),
                        "Peak Months": peak_m,
                        "Quiet Months": len(m["quiet_months"]),
                        "YoY Trend": m["yoy_trend"],
                        "Low Confidence": "⚠️" if m["low_confidence"] else "✓",
                    })

            if rows:
                df_cmp = pd.DataFrame(rows)
                st.dataframe(df_cmp, use_container_width=True, hide_index=True)

            # Heat-map grid: parks × months
            if not compare_month and len(compare_models) > 1:
                st.markdown('<div class="section-header">Busyness Heat-Map</div>',
                            unsafe_allow_html=True)
                heat_data = {
                    uc: [s["score"] for s in m["monthly_scores"]]
                    for uc, m in compare_models.items()
                }
                heat_df = pd.DataFrame(heat_data, index=MONTH_NAMES).T
                fig_heat = px.imshow(
                    heat_df,
                    color_continuous_scale=[
                        [0, "#27ae60"], [0.3, "#f39c12"],
                        [0.6, "#e67e22"], [1.0, "#c0392b"],
                    ],
                    zmin=0, zmax=100,
                    text_auto=".0f",
                    template="plotly_dark",
                    labels=dict(color="Score"),
                )
                fig_heat.update_layout(
                    **PLOTLY_LAYOUT,
                    height=max(200, len(compare_models) * 50 + 60),
                    coloraxis_colorbar=dict(
                        tickfont=dict(color="#e8edf2"),
                        title=dict(text="Score", font=dict(color="#e8edf2")),
                    ),
                )
                st.plotly_chart(fig_heat, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — Recommendations
# ════════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div class="section-header">Find Low-Busyness Parks</div>', unsafe_allow_html=True)
    st.markdown(
        "Filter by state, month, and maximum busyness score to find quieter parks."
    )

    rc1, rc2, rc3 = st.columns(3)
    with rc1:
        rec_state = st.selectbox("State", ["Any"] + all_states, key="rec_state")
    with rc2:
        rec_month_opts = ["Any month"] + [f"{i+1} – {MONTH_NAMES[i]}" for i in range(12)]
        rec_month_sel = st.selectbox("Month", rec_month_opts, key="rec_month")
        rec_month = None
        if rec_month_sel != "Any month":
            rec_month = int(rec_month_sel.split(" –")[0])
    with rc3:
        rec_max = st.slider("Max busyness score", 0, 100, 50, 5, key="rec_max")

    with st.spinner("Searching…"):
        recs = mdl.recommend_parks(
            db_path=DB_PATH,
            state=rec_state if rec_state != "Any" else None,
            month=rec_month,
            max_score=rec_max,
        )

    if not recs:
        st.info("No parks found matching your criteria. Try increasing the max score or changing filters.")
    else:
        st.success(f"Found **{len(recs)}** parks matching criteria.")

        # Score distribution chart
        rec_df = pd.DataFrame(recs)
        score_col = "score" if "score" in rec_df.columns else "avg_score"

        fig_rec = px.bar(
            rec_df.sort_values(score_col).head(20),
            x=score_col,
            y="name",
            orientation="h",
            color=score_col,
            color_continuous_scale=[
                [0, "#27ae60"], [0.5, "#f39c12"], [1.0, "#e67e22"]
            ],
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

        # Table
        display_cols = ["name", "unit_code", score_col]
        if "month_name" in rec_df.columns:
            display_cols.append("month_name")
        if "yoy_trend" in rec_df.columns:
            display_cols.append("yoy_trend")
        if "state" in rec_df.columns:
            display_cols.insert(2, "state")

        display_cols = [c for c in display_cols if c in rec_df.columns]
        rename_map = {
            "name": "Park", "unit_code": "Code", "score": "Score",
            "avg_score": "Avg Score", "month_name": "Month",
            "yoy_trend": "YoY Trend", "state": "State",
        }
        show_df = rec_df[display_cols].rename(columns=rename_map)
        st.dataframe(show_df, use_container_width=True, hide_index=True)

        # Deep-dive button
        st.markdown("---")
        st.markdown("**Deep-dive into a recommended park:**")
        if recs:
            deep_options = {r["name"]: r["unit_code"] for r in recs}
            deep_label = st.selectbox("Choose park", list(deep_options.keys()), key="deep_dive")
            deep_uc = deep_options[deep_label]
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
