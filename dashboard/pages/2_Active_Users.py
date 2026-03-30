"""
Active Users page — leaderboard and per-user listening stats.
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


st.set_page_config(page_title="Active Users", page_icon="👤", layout="wide")
st.title("👤 Most Active Users")

users_df = _load("active_users")

if users_df is not None and not users_df.empty:
    st.subheader("Listening time leaderboard")
    n = st.slider("Show top N users", min_value=5, max_value=100, value=20, step=5)
    top = users_df.head(n)[
        ["user_id", "songs_played", "unique_songs", "total_hours_played", "avg_ms_played"]
    ].copy()
    top["avg_min_played"] = (top["avg_ms_played"] / 60_000).round(2)
    top.drop(columns=["avg_ms_played"], inplace=True)
    st.dataframe(top, use_container_width=True)

    # Hours played distribution
    st.subheader("Listening hours distribution")
    fig = px.histogram(
        users_df,
        x="total_hours_played",
        nbins=40,
        labels={"total_hours_played": "Hours listened"},
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Songs played vs unique songs scatter
    st.subheader("Songs played vs. unique songs")
    fig2 = px.scatter(
        users_df.head(200),
        x="songs_played",
        y="unique_songs",
        color="total_hours_played",
        color_continuous_scale="Viridis",
        labels={
            "songs_played": "Total plays",
            "unique_songs": "Unique songs",
            "total_hours_played": "Hours",
        },
        height=400,
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.info("No user activity data yet.")
