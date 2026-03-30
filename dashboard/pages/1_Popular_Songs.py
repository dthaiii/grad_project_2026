"""
Popular Songs page — deep-dive into track and artist analytics.
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


st.set_page_config(page_title="Popular Songs", page_icon="🎵", layout="wide")
st.title("🎵 Popular Songs & Artists")

songs_df = _load("top_songs")
artists_df = _load("top_artists")

# ── Song table ────────────────────────────────────────────────────────────────
st.subheader("Top Songs")
if songs_df is not None and not songs_df.empty:
    n = st.slider("Show top N songs", min_value=5, max_value=100, value=20, step=5)
    display = songs_df.head(n)[
        ["song_name", "artist_name", "genre", "play_count", "unique_listeners", "total_ms_played"]
    ].copy()
    display["total_min_played"] = (display["total_ms_played"] / 60_000).round(1)
    display.drop(columns=["total_ms_played"], inplace=True)
    st.dataframe(display, use_container_width=True)

    st.subheader("Play count by genre")
    genre_songs = (
        songs_df.groupby("genre", as_index=False)["play_count"].sum().sort_values("play_count", ascending=False)
    )
    fig = px.bar(genre_songs, x="genre", y="play_count", color="genre", height=350)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No song data yet.")

# ── Artist table ──────────────────────────────────────────────────────────────
st.subheader("Top Artists")
if artists_df is not None and not artists_df.empty:
    n2 = st.slider("Show top N artists", min_value=5, max_value=50, value=10, step=5)
    fig = px.bar(
        artists_df.head(n2),
        x="play_count",
        y="artist_name",
        orientation="h",
        color="unique_listeners",
        color_continuous_scale="Blues",
        labels={"play_count": "Plays", "artist_name": "Artist"},
        height=450,
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)
