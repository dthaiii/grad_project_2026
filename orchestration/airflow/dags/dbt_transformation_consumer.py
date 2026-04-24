import os
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

EVENTS = ['auth_events', 
          'listen_events', 
          'page_view_events', 
          'session_start_end_events', 
          'status_change_events', 
          'user_info'] 

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "grad2026")
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'data_staging')

# Lên lịch chạy Consumer dựa trên tổng hợp 6 Datasets từ các DAG Producers.
trigger_datasets = [Dataset(f'bq://{GCP_PROJECT_ID}/{BIGQUERY_DATASET}/{event}') for event in EVENTS]

default_args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='dbt_transformation_dag',
    default_args=default_args,
    description='Consumer DAG running dbt dependent on 6 BigQuery Datasets',
    schedule=trigger_datasets, # Kích hoạt khi tất cả outlets cập nhật
    start_date=datetime(2026, 3, 16),
    catchup=False,
    max_active_runs=1, # Tối ưu hóa max_active_runs
    tags=['dbt', 'consumer']
) as dag:

    initiate_dbt_task = BashOperator(
        task_id='dbt_initiate',
        bash_command='cd /dbt && dbt seed --select state_codes --profiles-dir . --target prod',
    )

    execute_dbt_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt && dbt run --profiles-dir . --target prod'
    )

    initiate_dbt_task >> execute_dbt_task
