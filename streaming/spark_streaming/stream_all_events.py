# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_all_events.py

import os
import logging
import uuid
from datetime import datetime
# from dotenv import load_dotenv
from streaming_functions import *
from schema import schema

# # Load environment variables from .env if it exists
# load_dotenv()

# Kafka Topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"
SESSION_EVENTS_TOPIC = "session_start_end_events"
STATUS_EVENTS_TOPIC = "status_change_events"
USER_INFO_TOPIC = "user_info"

KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", '10.128.0.8')
SPARK_STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "earliest")
SPARK_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "50000"))
SPARK_TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "60 seconds")
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'data-grad2026')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

# Generate unique consumer group ID to force consuming from EARLIEST
UNIQUE_RUN_ID = os.getenv("SPARK_RUN_ID", f"spark-{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}")

# Reset checkpoints if RESET_OFFSETS is set
RESET_OFFSETS = os.getenv("SPARK_RESET_OFFSETS", "false").lower() == "true"
if RESET_OFFSETS:
    import subprocess
    logging.info(f"Resetting checkpoint offsets...")
    try:
        # Reset both query checkpoints and sink metadata logs.
        # If only checkpoints are removed, Spark may skip writes due to existing
        # _spark_metadata batch ids at the sink path.
        subprocess.run(["gsutil", "rm", "-r", f"{GCS_STORAGE_PATH}/checkpoint/"], check=False)
        for topic in [
            LISTEN_EVENTS_TOPIC,
            PAGE_VIEW_EVENTS_TOPIC,
            AUTH_EVENTS_TOPIC,
            SESSION_EVENTS_TOPIC,
            STATUS_EVENTS_TOPIC,
            USER_INFO_TOPIC,
        ]:
            subprocess.run(["gsutil", "rm", "-r", f"{GCS_STORAGE_PATH}/{topic}/_spark_metadata/"], check=False)
        logging.info("Checkpoint deleted successfully")
    except Exception as e:
        logging.warning(f"Could not delete checkpoint: {e}")

# initialize a spark session
spark = create_or_get_spark_session('Eventsim Stream')
spark.streams.resetTerminated()
# listen events stream
listen_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# page view stream
page_view_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
page_view_events = process_stream(
    page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# auth stream
auth_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# session start end stream
session_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, SESSION_EVENTS_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
session_events = process_stream(
    session_events, schema[SESSION_EVENTS_TOPIC], SESSION_EVENTS_TOPIC)

# status change stream
status_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, STATUS_EVENTS_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
status_events = process_stream(
    status_events, schema[STATUS_EVENTS_TOPIC], STATUS_EVENTS_TOPIC)

# user info stream
user_info_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, USER_INFO_TOPIC, SPARK_STARTING_OFFSETS, SPARK_MAX_OFFSETS_PER_TRIGGER)
user_info_events = process_stream(
    user_info_events, schema[USER_INFO_TOPIC], USER_INFO_TOPIC)

# write a file to storage every 2 minutes in parquet format
listen_events_writer = create_file_write_stream(listen_events,
                                                f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}",
                                                SPARK_TRIGGER_INTERVAL
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                   f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   f"{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   SPARK_TRIGGER_INTERVAL
                                                   )

auth_events_writer = create_file_write_stream(auth_events,
                                              f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
                                              f"{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}",
                                              SPARK_TRIGGER_INTERVAL
                                              )

session_events_writer = create_file_write_stream(session_events,
                                                 f"{GCS_STORAGE_PATH}/{SESSION_EVENTS_TOPIC}",
                                                 f"{GCS_STORAGE_PATH}/checkpoint/{SESSION_EVENTS_TOPIC}",
                                                 SPARK_TRIGGER_INTERVAL
                                                 )

status_events_writer = create_file_write_stream(status_events,
                                                f"{GCS_STORAGE_PATH}/{STATUS_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{STATUS_EVENTS_TOPIC}",
                                                SPARK_TRIGGER_INTERVAL
                                                )

user_info_writer = create_file_write_stream(user_info_events,
                                            f"{GCS_STORAGE_PATH}/{USER_INFO_TOPIC}",
                                            f"{GCS_STORAGE_PATH}/checkpoint/{USER_INFO_TOPIC}",
                                            SPARK_TRIGGER_INTERVAL
                                            )


listen_events_writer.start()
auth_events_writer.start()
page_view_events_writer.start()
session_events_writer.start()
status_events_writer.start()
user_info_writer.start()

spark.streams.awaitAnyTermination()
