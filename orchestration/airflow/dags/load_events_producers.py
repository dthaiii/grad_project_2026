import os
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from schema import schema
from task_templates import (create_external_table, 
                            create_empty_table, 
                            insert_job, 
                            delete_external_table)


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
EXECUTION_DATETIME_STR = '{{ logical_date.strftime("%m%d%H") }}'

TABLE_MAP = { f"{event.upper()}_TABLE" : event for event in EVENTS}

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
              "BIGQUERY_DATASET": BIGQUERY_RAW_DATASET, 
              "EXECUTION_DATETIME_STR": EXECUTION_DATETIME_STR
              }
MACRO_VARS.update(TABLE_MAP)

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
        start_date=datetime(2026, 5, 1),
        end_date=datetime(2026, 5, 12),
        catchup=True,
        max_active_runs=1,
        user_defined_macros=MACRO_VARS,
        tags=['eventdata', 'producer']
    )
    
    with dag:
        staging_table_name = event
        insert_query = f"{{% include 'sql/{event}.sql' %}}" #extra {} for f-strings escape
        external_table_name = f'{staging_table_name}_{EXECUTION_DATETIME_STR}'
        events_data_path = f'{staging_table_name}/month={EXECUTION_MONTH}/day={EXECUTION_DAY}/hour={EXECUTION_HOUR}'
        events_schema = schema[event]

        check_event_data_task = ShortCircuitOperator(
            task_id=f'{event}_check_event_data',
            python_callable=has_event_files,
            op_kwargs={
                'bucket_name': GCP_GCS_BUCKET,
                'events_data_path': events_data_path,
            },
        )

        create_external_table_task = create_external_table(event,
                                                           GCP_PROJECT_ID, 
                                                           BIGQUERY_RAW_DATASET, 
                                                           external_table_name, 
                                                           GCP_GCS_BUCKET, 
                                                           events_data_path)

        create_empty_table_task = create_empty_table(event,
                                                     GCP_PROJECT_ID,
                                                     BIGQUERY_RAW_DATASET,
                                                     staging_table_name,
                                                     events_schema)
                                                
        execute_insert_query_task = insert_job(event,
                                               insert_query,
                                               BIGQUERY_RAW_DATASET,
                                               GCP_PROJECT_ID)

        delete_external_table_task = delete_external_table(event,
                                                           GCP_PROJECT_ID, 
                                                           BIGQUERY_RAW_DATASET, 
                                                           external_table_name)
                    
        # Báo hiệu hoàn thành dataset để trigger dbt_consumer
        delete_external_table_task.outlets = [event_dataset]
        
        check_event_data_task >> create_external_table_task >> create_empty_table_task >> execute_insert_query_task >> delete_external_table_task
         
    return dag

# Tạo các DAG thông qua globals()
for event in EVENTS:
    producer_dag = create_producer_dag(event)
    globals()[producer_dag.dag_id] = producer_dag
