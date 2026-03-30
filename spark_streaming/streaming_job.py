"""
PySpark Structured Streaming job.

Reads events from three Kafka topics:
  * listen_events
  * page_view_events
  * auth_events

Writes raw Parquet files to the data lake every TRIGGER_INTERVAL (default:
2 minutes), partitioned by event_type / year / month / day / hour so that the
hourly batch job can efficiently read only the relevant partitions.

Data-lake layout
----------------
/data/lake/raw/
  listen_events/
    year=2026/month=03/day=30/hour=09/  ← Parquet part files
  page_view_events/
    ...
  auth_events/
    ...

Environment variables
---------------------
KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
DATALAKE_PATH            default: /data/lake
CHECKPOINT_PATH          default: /data/checkpoints
TRIGGER_INTERVAL         default: 2 minutes
SPARK_MASTER             default: local[*]
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, month, to_timestamp, year, dayofmonth
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATALAKE_PATH: str = os.getenv("DATALAKE_PATH", "/data/lake")
CHECKPOINT_PATH: str = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")
TRIGGER_INTERVAL: str = os.getenv("TRIGGER_INTERVAL", "2 minutes")
SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

RAW_PATH = f"{DATALAKE_PATH}/raw"

# ── Schemas ───────────────────────────────────────────────────────────────────

_LOCATION_SCHEMA = StructType(
    [
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
    ]
)

LISTEN_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", StringType()),
        StructField("song_id", StringType()),
        StructField("song_name", StringType()),
        StructField("artist_name", StringType()),
        StructField("album_name", StringType()),
        StructField("genre", StringType()),
        StructField("duration_ms", IntegerType()),
        StructField("ms_played", IntegerType()),
        StructField("platform", StringType()),
        StructField("location", _LOCATION_SCHEMA),
    ]
)

PAGE_VIEW_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", StringType()),
        StructField("session_id", StringType()),
        StructField("page", StringType()),
        StructField("prev_page", StringType()),
        StructField("platform", StringType()),
    ]
)

AUTH_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("user_id", StringType()),
        StructField("action", StringType()),
        StructField("success", BooleanType()),
        StructField("method", StringType()),
        StructField("platform", StringType()),
    ]
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _add_time_partitions(df, ts_col: str = "timestamp"):
    """Derive year/month/day/hour partition columns from the ISO timestamp."""
    ts = to_timestamp(col(ts_col))
    return (
        df.withColumn("year", year(ts))
        .withColumn("month", month(ts))
        .withColumn("day", dayofmonth(ts))
        .withColumn("hour", hour(ts))
    )


def _build_stream(spark: SparkSession, topic: str, schema: StructType):
    """Read a Kafka topic and parse JSON values according to *schema*."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    return _add_time_partitions(parsed)


def _start_sink(df, name: str, partition_cols: list[str]):
    """Attach a file-sink that writes Parquet files on the configured trigger."""
    output_path = f"{RAW_PATH}/{name}"
    checkpoint = f"{CHECKPOINT_PATH}/{name}"
    return (
        df.writeStream.trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .format("parquet")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint)
        .partitionBy(*partition_cols)
        .start()
    )


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    spark = (
        SparkSession.builder.appName("MusicStreamingIngestion")
        .master(SPARK_MASTER)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    listen_df = _build_stream(spark, "listen_events", LISTEN_SCHEMA)
    page_view_df = _build_stream(spark, "page_view_events", PAGE_VIEW_SCHEMA)
    auth_df = _build_stream(spark, "auth_events", AUTH_SCHEMA)

    q1 = _start_sink(listen_df, "listen_events", ["year", "month", "day", "hour"])
    q2 = _start_sink(page_view_df, "page_view_events", ["year", "month", "day", "hour"])
    q3 = _start_sink(auth_df, "auth_events", ["year", "month", "day", "hour"])

    # Keep the driver alive until all queries terminate
    spark.streams.awaitAnyTermination()

    for q in (q1, q2, q3):
        q.stop()


if __name__ == "__main__":
    main()
