import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = {
    "owner": "airflow",
}

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "eventsim_stgging")
GCP_GCS_BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Allow overriding object paths without changing DAG code.
GCS_SONGS_OBJECT = os.environ.get("GCS_SONGS_OBJECT", "raw/flat_files/songs.parquet")
GCS_LOCATIONS_OBJECT = os.environ.get("GCS_LOCATIONS_OBJECT", "raw/flat_files/locations.parquet")

SONGS_EXTERNAL_SQL = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.songs_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCP_GCS_BUCKET}/{GCS_SONGS_OBJECT}']
)
"""

LOCATIONS_EXTERNAL_SQL = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.locations_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCP_GCS_BUCKET}/{GCS_LOCATIONS_OBJECT}']
)
"""

with DAG(
    dag_id="load_dims_from_gcs_dag",
    default_args=default_args,
    description="Refresh songs_ext/locations_ext and build dim_songs/dim_locations",
    schedule_interval="@once",
    start_date=datetime(2026, 3, 8),
    catchup=False,
    max_active_runs=1,
    tags=["eventsim", "dimensions", "dbt"],
) as dag:

    create_songs_external_table = BigQueryInsertJobOperator(
        task_id="create_songs_external_table",
        configuration={
            "query": {
                "query": SONGS_EXTERNAL_SQL,
                "useLegacySql": False,
            }
        },
        location="us-central1",
    )

    create_locations_external_table = BigQueryInsertJobOperator(
        task_id="create_locations_external_table",
        configuration={
            "query": {
                "query": LOCATIONS_EXTERNAL_SQL,
                "useLegacySql": False,
            }
        },
        location="us-central1",
    )

    run_dim_models = BashOperator(
        task_id="run_dim_songs_locations",
        bash_command="cd /dbt && dbt deps && dbt run --select dim_songs dim_locations --profiles-dir . --target prod",
    )

    [create_songs_external_table, create_locations_external_table] >> run_dim_models
