"""
Hourly PySpark batch job.

Reads the raw Parquet files that Spark Streaming has deposited in the data
lake and materialises six analytics tables that the dashboard consumes:

  1. top_songs          – songs ranked by total play-count
  2. top_artists        – artists ranked by total play-count
  3. active_users       – users ranked by listening time / song count
  4. user_demographics  – aggregated stats broken down by country and platform
  5. genre_popularity   – play counts per genre
  6. hourly_activity    – event counts bucketed by hour-of-day

All output tables are written as Parquet files to ANALYTICS_PATH, overwriting
the previous run so the dashboard always reads fresh data.

Environment variables
---------------------
DATALAKE_PATH        default: /data/lake
ANALYTICS_PATH       default: /data/lake/analytics
SPARK_MASTER         default: local[*]
BATCH_INTERVAL_SECONDS  default: 3600  (how long to sleep between runs)
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

DATALAKE_PATH: str = os.getenv("DATALAKE_PATH", "/data/lake")
ANALYTICS_PATH: str = os.getenv("ANALYTICS_PATH", "/data/lake/analytics")
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
BATCH_INTERVAL: int = int(os.getenv("BATCH_INTERVAL_SECONDS", "3600"))

RAW_PATH = f"{DATALAKE_PATH}/raw"


# ── Analytics functions ───────────────────────────────────────────────────────


def top_songs(listen_df: DataFrame, top_n: int = 100) -> DataFrame:
    """Return the top-*n* songs ordered by play count."""
    return (
        listen_df.groupBy("song_id", "song_name", "artist_name", "genre")
        .agg(
            F.count("event_id").alias("play_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.sum("ms_played").alias("total_ms_played"),
            F.avg("ms_played").alias("avg_ms_played"),
        )
        .orderBy(F.desc("play_count"))
        .limit(top_n)
    )


def top_artists(listen_df: DataFrame, top_n: int = 50) -> DataFrame:
    """Return the top-*n* artists ordered by play count."""
    return (
        listen_df.groupBy("artist_name")
        .agg(
            F.count("event_id").alias("play_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.countDistinct("song_id").alias("unique_songs"),
            F.sum("ms_played").alias("total_ms_played"),
        )
        .orderBy(F.desc("play_count"))
        .limit(top_n)
    )


def active_users(listen_df: DataFrame, top_n: int = 100) -> DataFrame:
    """Return the top-*n* most active users ordered by total listening time."""
    return (
        listen_df.groupBy("user_id")
        .agg(
            F.count("event_id").alias("songs_played"),
            F.countDistinct("song_id").alias("unique_songs"),
            F.sum("ms_played").alias("total_ms_played"),
            F.avg("ms_played").alias("avg_ms_played"),
        )
        .withColumn("total_hours_played", F.round(F.col("total_ms_played") / 3_600_000, 2))
        .orderBy(F.desc("total_ms_played"))
        .limit(top_n)
    )


def user_demographics(listen_df: DataFrame) -> DataFrame:
    """
    Aggregate listening stats by country and platform (demographics proxy).
    The listen events carry ``location.country`` and ``platform``.
    """
    return (
        listen_df.groupBy(
            F.col("location.country").alias("country"),
            "platform",
        )
        .agg(
            F.countDistinct("user_id").alias("unique_users"),
            F.count("event_id").alias("total_plays"),
            F.sum("ms_played").alias("total_ms_played"),
        )
        .orderBy(F.desc("unique_users"))
    )


def hourly_activity(listen_df: DataFrame, page_view_df: DataFrame, auth_df: DataFrame) -> DataFrame:
    """Return per-hour event counts for all three event types."""
    ts = F.to_timestamp(F.col("timestamp"))

    listen_hourly = (
        listen_df.withColumn("hour", F.hour(ts))
        .groupBy("hour")
        .agg(F.count("event_id").alias("listen_count"))
    )
    pv_hourly = (
        page_view_df.withColumn("hour", F.hour(ts))
        .groupBy("hour")
        .agg(F.count("event_id").alias("page_view_count"))
    )
    auth_hourly = (
        auth_df.withColumn("hour", F.hour(ts))
        .groupBy("hour")
        .agg(F.count("event_id").alias("auth_count"))
    )

    return (
        listen_hourly.join(pv_hourly, on="hour", how="outer")
        .join(auth_hourly, on="hour", how="outer")
        .fillna(0)
        .orderBy("hour")
    )


def genre_popularity(listen_df: DataFrame) -> DataFrame:
    """Aggregate play counts by music genre."""
    return (
        listen_df.groupBy("genre")
        .agg(
            F.count("event_id").alias("play_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
            F.countDistinct("song_id").alias("unique_songs"),
        )
        .orderBy(F.desc("play_count"))
    )


# ── I/O helpers ───────────────────────────────────────────────────────────────


def _read_raw(spark: SparkSession, name: str) -> DataFrame | None:
    path = f"{RAW_PATH}/{name}"
    try:
        df = spark.read.parquet(path)
        count = df.count()
        logger.info("Read %d rows from %s", count, path)
        return df
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read %s (%s) — skipping.", path, exc)
        return None


def _write(df: DataFrame, name: str) -> None:
    path = f"{ANALYTICS_PATH}/{name}"
    df.write.mode("overwrite").parquet(path)
    logger.info("Wrote analytics table → %s", path)


# ── Single batch run ──────────────────────────────────────────────────────────


def run_batch(spark: SparkSession) -> None:
    logger.info("─── Batch run started ───")

    listen_df = _read_raw(spark, "listen_events")
    page_view_df = _read_raw(spark, "page_view_events")
    auth_df = _read_raw(spark, "auth_events")

    if listen_df is not None:
        _write(top_songs(listen_df), "top_songs")
        _write(top_artists(listen_df), "top_artists")
        _write(active_users(listen_df), "active_users")
        _write(user_demographics(listen_df), "user_demographics")
        _write(genre_popularity(listen_df), "genre_popularity")

    if listen_df is not None and page_view_df is not None and auth_df is not None:
        _write(hourly_activity(listen_df, page_view_df, auth_df), "hourly_activity")

    logger.info("─── Batch run complete ───")


# ── Scheduling loop ───────────────────────────────────────────────────────────


def main() -> None:
    spark = (
        SparkSession.builder.appName("MusicStreamingBatch")
        .master(SPARK_MASTER)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    running = True

    def _stop(signum, frame):  # noqa: ANN001
        nonlocal running
        logger.info("Shutdown signal received.")
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while running:
        try:
            run_batch(spark)
        except Exception:  # noqa: BLE001
            logger.exception("Batch run failed — will retry next cycle.")
        if running:
            logger.info("Sleeping %d s until next batch run…", BATCH_INTERVAL)
            time.sleep(BATCH_INTERVAL)

    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
