"""
UK Crime Data Dashboard — Streamlit app
Reads directly from the DuckDB warehouse (marts schema).
"""
import os
from pathlib import Path

import duckdb
import folium
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from streamlit_folium import st_folium

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./warehouse/crime.duckdb")

st.set_page_config(
    page_title="UK Crime Analytics | West Yorkshire",
    page_icon="🔵",
    layout="wide",
)


# --------------------------------------------------------------------------- #
# Data loading — cached so re-renders don't re-query DuckDB                   #
# --------------------------------------------------------------------------- #

@st.cache_data(ttl=300)
def load_by_category() -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("SELECT * FROM marts.crime_by_category").df()
    con.close()
    return df


@st.cache_data(ttl=300)
def load_by_month() -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("SELECT * FROM marts.crime_by_month").df()
    con.close()
    return df


@st.cache_data(ttl=300)
def load_by_force() -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("SELECT * FROM marts.crime_by_force").df()
    con.close()
    return df


@st.cache_data(ttl=300)
def load_hotspots() -> pd.DataFrame:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("SELECT * FROM marts.crime_hotspots").df()
    con.close()
    return df


def db_exists() -> bool:
    path = Path(DUCKDB_PATH)
    if not path.exists():
        return False
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.execute("SELECT 1 FROM marts.crime_by_month LIMIT 1")
        con.close()
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Layout                                                                        #
# --------------------------------------------------------------------------- #

st.title("UK Crime Analytics Dashboard")
st.caption("Source: data.police.uk · West Yorkshire Police · Powered by DuckDB + dbt")

if not db_exists():
    st.warning(
        "No warehouse data found. Run the pipeline first:\n\n"
        "```bash\n"
        "python -m warehouse.setup_duckdb --load-local ./2026-02-west-yorkshire-street.csv\n"
        "cd dbt_crime && dbt run --profiles-dir .\n"
        "```"
    )
    st.stop()

df_category = load_by_category()
df_month    = load_by_month()
df_force    = load_by_force()
df_hotspots = load_hotspots()

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    forces_available = sorted(df_month["force"].unique())
    selected_forces = st.multiselect("Police Force", forces_available, default=forces_available)

    years_available = sorted(df_month["year"].unique())
    selected_years = st.multiselect("Year", years_available, default=years_available)

    crime_types = sorted(df_category["crime_type"].unique())
    selected_types = st.multiselect("Crime Type", crime_types, default=crime_types)

# Apply filters
cat_filtered = df_category[
    df_category["force"].isin(selected_forces) &
    df_category["year"].isin(selected_years) &
    df_category["crime_type"].isin(selected_types)
]
month_filtered = df_month[
    df_month["force"].isin(selected_forces) &
    df_month["year"].isin(selected_years)
]
hotspot_filtered = df_hotspots[
    df_hotspots["force"].isin(selected_forces) &
    df_hotspots["year"].isin(selected_years)
]

# --------------------------------------------------------------------------- #
# KPI row                                                                       #
# --------------------------------------------------------------------------- #

total_crimes   = cat_filtered["total_crimes"].sum()
pct_open       = df_force[df_force["force"].isin(selected_forces)]["pct_under_investigation"].mean()
top_crime      = (
    cat_filtered.groupby("crime_type")["total_crimes"].sum()
    .idxmax() if not cat_filtered.empty else "N/A"
)
high_hotspots  = (hotspot_filtered["hotspot_tier"] == "High").sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Crimes", f"{total_crimes:,}")
col2.metric("% Under Investigation", f"{pct_open:.1f}%")
col3.metric("Top Crime Type", top_crime)
col4.metric("High-Risk LSOAs", int(high_hotspots))

st.divider()

# --------------------------------------------------------------------------- #
# Tab layout                                                                    #
# --------------------------------------------------------------------------- #

tab_trend, tab_breakdown, tab_map, tab_force = st.tabs(
    ["Trends", "Category Breakdown", "Crime Map", "Force Comparison"]
)

# --- Trends tab ---
with tab_trend:
    st.subheader("Monthly Crime Trend")
    trend_data = (
        month_filtered
        .groupby(["month", "force"])["total_crimes"]
        .sum()
        .reset_index()
        .sort_values("month")
    )
    fig_trend = px.line(
        trend_data,
        x="month",
        y="total_crimes",
        color="force",
        markers=True,
        labels={"total_crimes": "Total Crimes", "month": "Month"},
        title="Crime Volume Over Time",
    )
    fig_trend.update_layout(hovermode="x unified")
    st.plotly_chart(fig_trend, use_container_width=True)

# --- Category breakdown tab ---
with tab_breakdown:
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Crime by Category")
        cat_totals = (
            cat_filtered.groupby("crime_type")["total_crimes"]
            .sum()
            .reset_index()
            .sort_values("total_crimes", ascending=True)
        )
        fig_bar = px.bar(
            cat_totals,
            x="total_crimes",
            y="crime_type",
            orientation="h",
            color="total_crimes",
            color_continuous_scale="Reds",
            labels={"total_crimes": "Total", "crime_type": ""},
        )
        fig_bar.update_layout(coloraxis_showscale=False, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_right:
        st.subheader("Outcome Distribution")
        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        outcome_data = con.execute("""
            SELECT last_outcome, COUNT(*) as cnt
            FROM staging.stg_crimes
            GROUP BY last_outcome
            ORDER BY cnt DESC
        """).df()
        con.close()
        fig_pie = px.pie(
            outcome_data,
            names="last_outcome",
            values="cnt",
            hole=0.4,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)

    st.subheader("Crime Heatmap: Category × Month")
    pivot = (
        cat_filtered
        .groupby(["month", "crime_type"])["total_crimes"]
        .sum()
        .reset_index()
        .pivot(index="crime_type", columns="month", values="total_crimes")
        .fillna(0)
    )
    if not pivot.empty:
        fig_heat = px.imshow(
            pivot,
            color_continuous_scale="YlOrRd",
            aspect="auto",
            labels=dict(color="Crimes"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)

# --- Map tab ---
with tab_map:
    st.subheader("Crime Hotspot Map")

    month_options = sorted(hotspot_filtered["month"].unique(), reverse=True)
    if not month_options:
        st.info("No hotspot data available for selected filters.")
    else:
        selected_month = st.selectbox("Select Month", month_options)
        map_data = hotspot_filtered[hotspot_filtered["month"] == selected_month]

        # Bradford city centre as default centre
        m = folium.Map(location=[53.795, -1.759], zoom_start=11, tiles="CartoDB positron")

        tier_colours = {"High": "red", "Medium": "orange", "Low": "green"}

        for _, row in map_data.iterrows():
            if pd.isna(row["centroid_lat"]) or pd.isna(row["centroid_lon"]):
                continue
            folium.CircleMarker(
                location=[row["centroid_lat"], row["centroid_lon"]],
                radius=max(4, min(row["total_crimes"] / 3, 20)),
                color=tier_colours.get(row["hotspot_tier"], "blue"),
                fill=True,
                fill_opacity=0.6,
                popup=folium.Popup(
                    f"<b>{row['lsoa_name']}</b><br>"
                    f"Crimes: {int(row['total_crimes'])}<br>"
                    f"Tier: {row['hotspot_tier']}<br>"
                    f"Violence: {int(row['violence_count'])}<br>"
                    f"Burglary: {int(row['burglary_count'])}",
                    max_width=200,
                ),
            ).add_to(m)

        st_folium(m, width=None, height=520, returned_objects=[])

        st.caption(
            "Circle size = crime volume · Red = High · Orange = Medium · Green = Low"
        )

# --- Force comparison tab ---
with tab_force:
    st.subheader("Force Performance Comparison")
    force_data = df_force[
        df_force["force"].isin(selected_forces) &
        df_force["year"].isin(selected_years)
    ]

    col_a, col_b = st.columns(2)
    with col_a:
        fig_resolve = px.bar(
            force_data.groupby("force")[["pct_resolved", "pct_no_suspect", "pct_under_investigation"]]
            .mean()
            .reset_index()
            .melt(id_vars="force", var_name="Outcome", value_name="Percentage"),
            x="force",
            y="Percentage",
            color="Outcome",
            barmode="stack",
            title="Average Outcome Distribution by Force (%)",
            color_discrete_map={
                "pct_resolved": "#2ecc71",
                "pct_no_suspect": "#e74c3c",
                "pct_under_investigation": "#f39c12",
            },
        )
        st.plotly_chart(fig_resolve, use_container_width=True)

    with col_b:
        monthly_total = (
            force_data.groupby(["month", "force"])["total_crimes"]
            .sum()
            .reset_index()
        )
        fig_box = px.box(
            monthly_total,
            x="force",
            y="total_crimes",
            color="force",
            title="Monthly Crime Volume Distribution",
            labels={"total_crimes": "Crimes per Month"},
        )
        st.plotly_chart(fig_box, use_container_width=True)
