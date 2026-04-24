import os
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


EVENTS = ['auth_events', 
          'listen_events', 
          'page_view_events', 
          'session_start_end_events', 
          'status_change_events', 
          'user_info'] # we have data coming in from six events


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_RAW_DATASET = os.environ.get('BIGQUERY_DATASET', 'data_staging')

EXECUTION_MONTH = '{{ logical_date.strftime("%-m") }}'
EXECUTION_DAY = '{{ logical_date.strftime("%-d") }}'
EXECUTION_HOUR = '{{ logical_date.strftime("%-H") }}'

default_args = {
    'owner' : 'airflow'
}


def has_event_files(bucket_name, events_data_path, **kwargs):
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    objects = hook.list(bucket_name=bucket_name, prefix=events_data_path)
    return bool(objects)


def create_producer_dag(event):
    dag_id = f'load_{event}_producer'
    
    # Định nghĩa Dataset riêng cho từng event
    event_dataset = Dataset(f'bq://{GCP_PROJECT_ID}/{BIGQUERY_RAW_DATASET}/{event}')
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Hourly data pipeline to load {event} to BigQuery',
        schedule_interval="5 * * * *",
        start_date=datetime(2026, 4, 1),
        end_date=datetime(2026, 4, 30),
        catchup=True,
        max_active_runs=1,
        tags=['eventdata', 'producer', 'backfill']
    )
    
    with dag:
        events_data_path = f'{event}/month={EXECUTION_MONTH}/day={EXECUTION_DAY}/hour={EXECUTION_HOUR}'

        check_event_data_task = ShortCircuitOperator(
            task_id=f'{event}_check_event_data',
            python_callable=has_event_files,
            op_kwargs={
                'bucket_name': GCP_GCS_BUCKET,
                'events_data_path': events_data_path,
            },
        )

        load_raw_to_bigquery_task = GCSToBigQueryOperator(
            task_id=f'{event}_load_raw_to_bigquery',
            bucket=GCP_GCS_BUCKET,
            source_objects=[f'{events_data_path}/*'],
            destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{event}',
            source_format='PARQUET',
            autodetect=True,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            outlets=[event_dataset],
            gcp_conn_id='google_cloud_default',
        )

        check_event_data_task >> load_raw_to_bigquery_task
         
    return dag

# Tạo các DAG thông qua globals()
for event in EVENTS:
    producer_dag = create_producer_dag(event)
    globals()[producer_dag.dag_id] = producer_dag
