import os
import logging
import pandas as pd
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from google.cloud import storage
from schema import schema

default_args ={
    'owner' : 'airflow'
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

URL = 'https://raw.githubusercontent.com/dthaiii/grad_project_2026/main/orchestration/dbt/seeds/songs.csv'
CSV_FILENAME = 'songs.csv'
PARQUET_FILENAME = CSV_FILENAME.replace('csv', 'parquet')

CSV_OUTFILE = f'{AIRFLOW_HOME}/{CSV_FILENAME}'
PARQUET_OUTFILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'
TABLE_NAME = 'songs'

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'data_staging')


def convert_to_parquet(csv_file, parquet_file):
    if not csv_file.endswith('csv'):
        raise ValueError('The input file is not in csv format')
    
    df = pd.read_csv(
        csv_file,
        engine='python',
        on_bad_lines='skip',
    )
    df.to_parquet(parquet_file, index=False)


def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)


with DAG(
    dag_id = f'load_songs_dag',
    default_args = default_args,
    description = f'Execute only once to create songs table in bigquery',
    schedule_interval="@once", #At the 5th minute of every hour
    start_date=datetime(2026,3,15),
    end_date=datetime(2026,4,15),
    catchup=True,
    tags=['eventsim']
) as dag:

    download_songs_file_task = BashOperator(
        task_id = "download_songs_file",
        bash_command = f"curl -sSLf {URL} > {CSV_OUTFILE}"
    )

    convert_to_parquet_task = PythonOperator(
        task_id = 'convert_to_parquet',
        python_callable = convert_to_parquet,
        op_kwargs = {
            'csv_file' : CSV_OUTFILE,
            'parquet_file' : PARQUET_OUTFILE
        }
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'file_path' : PARQUET_OUTFILE,
            'bucket_name' : GCP_GCS_BUCKET,
            'blob_name' : f'{TABLE_NAME}/{PARQUET_FILENAME}'
        }
    )

    remove_files_from_local_task=BashOperator(
        task_id='remove_files_from_local',
        bash_command=f'rm {CSV_OUTFILE} {PARQUET_OUTFILE}'
    )

    load_raw_songs_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_raw_songs_to_bigquery',
        bucket=GCP_GCS_BUCKET,
        source_objects=[f'{TABLE_NAME}/*.parquet'],
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{TABLE_NAME}',
        source_format='PARQUET',
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id='google_cloud_default',
    )

    download_songs_file_task >> convert_to_parquet_task >> upload_to_gcs_task >> remove_files_from_local_task >> load_raw_songs_to_bigquery_task
