"""
User Demographics page — country breakdown and platform distribution.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ANALYTICS_PATH = os.getenv("ANALYTICS_PATH", "/data/lake/analytics")


@st.cache_data(ttl=120)
def _load(table: str) -> pd.DataFrame | None:
    path = Path(ANALYTICS_PATH) / table
    if not path.exists() or not any(path.glob("*.parquet")):
        return None
    return pd.read_parquet(path)


st.set_page_config(page_title="User Demographics", page_icon="🌍", layout="wide")
st.title("🌍 User Demographics")

demo_df = _load("user_demographics")

if demo_df is not None and not demo_df.empty:
    # ── Country map ───────────────────────────────────────────────────────────
    st.subheader("Listeners by country")
    country_agg = (
        demo_df.groupby("country", as_index=False)
        .agg(unique_users=("unique_users", "sum"), total_plays=("total_plays", "sum"))
        .sort_values("unique_users", ascending=False)
    )
    fig_map = px.choropleth(
        country_agg,
        locations="country",
        locationmode="country names",
        color="unique_users",
        color_continuous_scale="Blues",
        labels={"unique_users": "Unique users"},
        height=480,
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # ── Top countries bar ─────────────────────────────────────────────────────
    st.subheader("Top 20 countries by unique listeners")
    fig_bar = px.bar(
        country_agg.head(20),
        x="unique_users",
        y="country",
        orientation="h",
        labels={"unique_users": "Unique users", "country": ""},
        height=500,
    )
    fig_bar.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig_bar, use_container_width=True)

    # ── Platform breakdown ────────────────────────────────────────────────────
    st.subheader("Platform distribution")
    platform_agg = (
        demo_df.groupby("platform", as_index=False)
        .agg(total_plays=("total_plays", "sum"))
    )
    fig_pie = px.pie(
        platform_agg,
        names="platform",
        values="total_plays",
        hole=0.35,
        height=380,
    )
    st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("No demographics data yet.")
