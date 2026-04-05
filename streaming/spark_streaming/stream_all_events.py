# Run the script using the following command
# spark-submit \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
# stream_all_events.py

import os
import logging
import uuid
from datetime import datetime
from dotenv import load_dotenv
from streaming_functions import *
from schema import schema

# Load environment variables from .env if it exists
load_dotenv()

# Kafka Topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"
SESSION_EVENTS_TOPIC = "session_start_end_events"
STATUS_EVENTS_TOPIC = "status_change_events"
USER_INFO_TOPIC = "user_info"

KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
GCP_GCS_BUCKET = os.getenv("GCP_GCS_BUCKET", 'raw-data-bucket-2026')
GCS_STORAGE_PATH = f'gs://{GCP_GCS_BUCKET}'

# Generate unique consumer group ID to force consuming from EARLIEST
UNIQUE_RUN_ID = os.getenv("SPARK_RUN_ID", f"spark-{datetime.now().strftime('%Y%m%d_%H%M%S')}-{uuid.uuid4().hex[:8]}")

# Reset checkpoints if RESET_OFFSETS is set
RESET_OFFSETS = os.getenv("SPARK_RESET_OFFSETS", "false").lower() == "true"
if RESET_OFFSETS:
    import subprocess
    logging.info(f"Resetting checkpoint offsets...")
    try:
        subprocess.run(["gsutil", "rm", "-r", f"{GCS_STORAGE_PATH}/checkpoint/"], check=False)
        logging.info("Checkpoint deleted successfully")
    except Exception as e:
        logging.warning(f"Could not delete checkpoint: {e}")

# initialize a spark session
spark = create_or_get_spark_session('Eventsim Stream')
spark.streams.resetTerminated()
# listen events stream
listen_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(
    listen_events, schema[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# page view stream
page_view_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(
    page_view_events, schema[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# auth stream
auth_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(
    auth_events, schema[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# session start end stream
session_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, SESSION_EVENTS_TOPIC)
session_events = process_stream(
    session_events, schema[SESSION_EVENTS_TOPIC], SESSION_EVENTS_TOPIC)

# status change stream
status_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, STATUS_EVENTS_TOPIC)
status_events = process_stream(
    status_events, schema[STATUS_EVENTS_TOPIC], STATUS_EVENTS_TOPIC)

# user info stream
user_info_events = create_kafka_read_stream(
    spark, KAFKA_ADDRESS, KAFKA_PORT, USER_INFO_TOPIC)
user_info_events = process_stream(
    user_info_events, schema[USER_INFO_TOPIC], USER_INFO_TOPIC)

# write a file to storage every 2 minutes in parquet format
listen_events_writer = create_file_write_stream(listen_events,
                                                f"{GCS_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}"
                                                )

page_view_events_writer = create_file_write_stream(page_view_events,
                                                   f"{GCS_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}",
                                                   f"{GCS_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}"
                                                   )

auth_events_writer = create_file_write_stream(auth_events,
                                              f"{GCS_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}",
                                              f"{GCS_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}"
                                              )

session_events_writer = create_file_write_stream(session_events,
                                                 f"{GCS_STORAGE_PATH}/{SESSION_EVENTS_TOPIC}",
                                                 f"{GCS_STORAGE_PATH}/checkpoint/{SESSION_EVENTS_TOPIC}"
                                                 )

status_events_writer = create_file_write_stream(status_events,
                                                f"{GCS_STORAGE_PATH}/{STATUS_EVENTS_TOPIC}",
                                                f"{GCS_STORAGE_PATH}/checkpoint/{STATUS_EVENTS_TOPIC}"
                                                )

user_info_writer = create_file_write_stream(user_info_events,
                                            f"{GCS_STORAGE_PATH}/{USER_INFO_TOPIC}",
                                            f"{GCS_STORAGE_PATH}/checkpoint/{USER_INFO_TOPIC}"
                                            )


listen_events_writer.start()
auth_events_writer.start()
page_view_events_writer.start()
session_events_writer.start()
status_events_writer.start()
user_info_writer.start()

spark.streams.awaitAnyTermination()
