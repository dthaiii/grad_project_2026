import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    "owner": "airflow",
}

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "data_staging")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Allow overriding object paths without changing DAG code.
GCS_SONGS_OBJECT = os.environ.get("GCS_SONGS_OBJECT", "raw/flat_files/songs.parquet")
GCS_LOCATIONS_OBJECT = os.environ.get("GCS_LOCATIONS_OBJECT", "raw/flat_files/locations.parquet")

with DAG(
    dag_id="load_dims_from_gcs_dag",
    default_args=default_args,
    description="Load raw songs/locations parquet files from GCS into BigQuery raw dataset",
    schedule_interval="@once",
    start_date=datetime(2026, 3, 8),
    catchup=False,
    max_active_runs=1,
    tags=["eventsim", "dimensions", "raw"],
) as dag:

    load_songs_raw = GCSToBigQueryOperator(
        task_id="load_songs_raw",
        bucket=GCP_GCS_BUCKET,
        source_objects=[GCS_SONGS_OBJECT],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.songs_ext",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default",
    )

    load_locations_raw = GCSToBigQueryOperator(
        task_id="load_locations_raw",
        bucket=GCP_GCS_BUCKET,
        source_objects=[GCS_LOCATIONS_OBJECT],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.locations_ext",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default",
    )

    [load_songs_raw, load_locations_raw]
