"""
Music Streaming Analytics Dashboard — main page.

Provides an overview with key metrics across all analytics tables.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ANALYTICS_PATH = os.getenv("ANALYTICS_PATH", "/data/lake/analytics")


# ── Helpers ───────────────────────────────────────────────────────────────────


@st.cache_data(ttl=120)
def _load(table: str) -> pd.DataFrame | None:
    """Return a Pandas DataFrame for *table*, or None if not yet available."""
    path = Path(ANALYTICS_PATH) / table
    if not path.exists() or not any(path.glob("*.parquet")):
        return None
    try:
        return pd.read_parquet(path)
    except Exception:  # noqa: BLE001
        return None


def _metric(label: str, df: pd.DataFrame | None, col: str, fmt: str = "{:,}") -> None:
    if df is not None and not df.empty:
        val = int(df[col].sum()) if df[col].dtype.kind in ("i", "u", "f") else len(df)
        st.metric(label, fmt.format(val))
    else:
        st.metric(label, "–")


# ── Layout ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Music Streaming Analytics",
    page_icon="🎵",
    layout="wide",
)

st.title("🎵 Music Streaming Analytics")
st.caption("Real-time pipeline · data refreshed every ~2 min · analytics updated hourly")

# KPI row
top_songs_df = _load("top_songs")
active_users_df = _load("active_users")
genre_df = _load("genre_popularity")
hourly_df = _load("hourly_activity")

col1, col2, col3, col4 = st.columns(4)
with col1:
    _metric("Total Plays", top_songs_df, "play_count")
with col2:
    total_users = int(active_users_df["songs_played"].count()) if active_users_df is not None else 0
    st.metric("Active Users (tracked)", total_users)
with col3:
    _metric("Total Listening Hours",
            active_users_df, "total_hours_played", fmt="{:,.1f}")
with col4:
    genres = len(genre_df) if genre_df is not None else 0
    st.metric("Genres tracked", genres)

st.divider()

# Top 10 songs bar chart
st.subheader("🏆 Top 10 Songs")
if top_songs_df is not None and not top_songs_df.empty:
    top10 = top_songs_df.head(10).copy()
    top10["label"] = top10["song_name"] + " – " + top10["artist_name"]
    fig = px.bar(
        top10,
        x="play_count",
        y="label",
        orientation="h",
        color="genre",
        labels={"play_count": "Plays", "label": ""},
        height=400,
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, showlegend=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data yet — the pipeline may still be warming up.")

# Genre popularity pie chart
st.subheader("🎸 Genre Popularity")
if genre_df is not None and not genre_df.empty:
    fig = px.pie(
        genre_df,
        names="genre",
        values="play_count",
        hole=0.4,
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)

# Hourly activity line chart
st.subheader("📈 Hourly Activity")
if hourly_df is not None and not hourly_df.empty:
    fig = px.line(
        hourly_df,
        x="hour",
        y=["listen_count", "page_view_count", "auth_count"],
        labels={"value": "Events", "hour": "Hour of day", "variable": "Event type"},
        height=350,
    )
    st.plotly_chart(fig, use_container_width=True)
