"""
Unit tests for the batch-processing analytics transformations
(batch_processing/batch_job.py).

These tests use PySpark in local mode — no Kafka or HDFS required.
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timezone

import pytest

# Make batch_processing importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "batch_processing"))

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import batch_job


# ── Spark session (module-scoped for speed) ────────────────────────────────────


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("BatchJobTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ── Sample data helpers ───────────────────────────────────────────────────────


def _listen_rows(spark: SparkSession):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return spark.createDataFrame(
        [
            # (event_id, user_id, song_id, song_name, artist_name, genre, ms_played, location_country, platform, timestamp)
            ("e1", "u1", "s1", "Song A", "Artist X", "Pop",  180_000, "USA",     "web",     now),
            ("e2", "u2", "s1", "Song A", "Artist X", "Pop",  200_000, "Canada",  "ios",     now),
            ("e3", "u1", "s2", "Song B", "Artist Y", "Rock",  90_000, "USA",     "android", now),
            ("e4", "u3", "s1", "Song A", "Artist X", "Pop",  210_000, "Germany", "web",     now),
            ("e5", "u1", "s1", "Song A", "Artist X", "Pop",  160_000, "USA",     "web",     now),
        ],
        schema=(
            "event_id STRING, user_id STRING, song_id STRING, "
            "song_name STRING, artist_name STRING, genre STRING, "
            "ms_played LONG, location STRING, platform STRING, timestamp STRING"
        ),
    )


def _listen_df_with_struct(spark: SparkSession):
    """Build a listen DataFrame with a nested location struct (as batch_job expects)."""
    raw = _listen_rows(spark)
    # Simulate the nested struct produced by the streaming job
    return raw.withColumn(
        "location",
        F.struct(F.col("location").alias("country"), F.lit("city").alias("city")),
    )


def _page_view_rows(spark: SparkSession):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return spark.createDataFrame(
        [
            ("pv1", "u1", "home",   "web", now),
            ("pv2", "u2", "search", "ios", now),
            ("pv3", "u1", "album",  "web", now),
        ],
        schema="event_id STRING, user_id STRING, page STRING, platform STRING, timestamp STRING",
    )


def _auth_rows(spark: SparkSession):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return spark.createDataFrame(
        [
            ("a1", "u1", "login",  True,  "email", now),
            ("a2", "u2", "logout", True,  "google", now),
        ],
        schema=(
            "event_id STRING, user_id STRING, action STRING, "
            "success BOOLEAN, method STRING, timestamp STRING"
        ),
    )


# ── top_songs ─────────────────────────────────────────────────────────────────


class TestTopSongs:
    def test_most_played_song_first(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_songs(df).toPandas()
        assert result.iloc[0]["song_id"] == "s1"

    def test_play_count_correct(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_songs(df).toPandas()
        s1_row = result[result["song_id"] == "s1"].iloc[0]
        assert s1_row["play_count"] == 4  # e1, e2, e4, e5

    def test_unique_listeners_correct(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_songs(df).toPandas()
        s1_row = result[result["song_id"] == "s1"].iloc[0]
        # u1, u2, u3 all listened to s1
        assert s1_row["unique_listeners"] == 3

    def test_respects_top_n(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_songs(df, top_n=1).toPandas()
        assert len(result) == 1

    def test_output_columns(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_songs(df)
        expected = {"song_id", "song_name", "artist_name", "genre",
                    "play_count", "unique_listeners", "total_ms_played", "avg_ms_played"}
        assert expected.issubset(set(result.columns))


# ── top_artists ───────────────────────────────────────────────────────────────


class TestTopArtists:
    def test_most_played_artist_first(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_artists(df).toPandas()
        assert result.iloc[0]["artist_name"] == "Artist X"

    def test_output_columns(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.top_artists(df)
        expected = {"artist_name", "play_count", "unique_listeners", "unique_songs", "total_ms_played"}
        assert expected.issubset(set(result.columns))


# ── active_users ──────────────────────────────────────────────────────────────


class TestActiveUsers:
    def test_most_active_user_first(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.active_users(df).toPandas()
        # u1 has 3 events (e1, e3, e5); u2 and u3 have 1 each
        assert result.iloc[0]["user_id"] == "u1"

    def test_total_hours_played_column_present(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.active_users(df)
        assert "total_hours_played" in result.columns

    def test_songs_played_count(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.active_users(df).toPandas()
        u1_row = result[result["user_id"] == "u1"].iloc[0]
        assert u1_row["songs_played"] == 3

    def test_unique_songs_count(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.active_users(df).toPandas()
        u1_row = result[result["user_id"] == "u1"].iloc[0]
        assert u1_row["unique_songs"] == 2  # s1 and s2


# ── user_demographics ─────────────────────────────────────────────────────────


class TestUserDemographics:
    def test_output_columns(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.user_demographics(df)
        expected = {"country", "platform", "unique_users", "total_plays"}
        assert expected.issubset(set(result.columns))

    def test_country_values(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.user_demographics(df).toPandas()
        countries = set(result["country"].tolist())
        assert "USA" in countries


# ── genre_popularity ──────────────────────────────────────────────────────────


class TestGenrePopularity:
    def test_output_columns(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.genre_popularity(df)
        assert set(result.columns) == {"genre", "play_count", "unique_listeners", "unique_songs"}

    def test_pop_most_popular(self, spark: SparkSession) -> None:
        df = _listen_df_with_struct(spark)
        result = batch_job.genre_popularity(df).toPandas()
        assert result.iloc[0]["genre"] == "Pop"


# ── hourly_activity ───────────────────────────────────────────────────────────


class TestHourlyActivity:
    def test_output_has_all_event_type_cols(self, spark: SparkSession) -> None:
        listen_df = _listen_df_with_struct(spark)
        pv_df = _page_view_rows(spark)
        auth_df = _auth_rows(spark)
        result = batch_job.hourly_activity(listen_df, pv_df, auth_df)
        expected = {"hour", "listen_count", "page_view_count", "auth_count"}
        assert expected.issubset(set(result.columns))

    def test_hour_values_in_range(self, spark: SparkSession) -> None:
        listen_df = _listen_df_with_struct(spark)
        pv_df = _page_view_rows(spark)
        auth_df = _auth_rows(spark)
        result = batch_job.hourly_activity(listen_df, pv_df, auth_df).toPandas()
        assert result["hour"].between(0, 23).all()
