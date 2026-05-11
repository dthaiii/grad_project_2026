import os
from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

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
    start_date=datetime(2026, 3, 15),
    catchup=True,
    max_active_runs=1, # Tối ưu hóa max_active_runs
    tags=['dbt', 'consumer']
) as dag:

    initiate_dbt_task = BashOperator(
        task_id='dbt_initiate',
        bash_command='cd /dbt && dbt seed --select state_codes --profiles-dir . --target prod',
    )

    run_stg_layer_task = BashOperator(
        task_id='dbt_run_stg_layer',
        bash_command='cd /dbt && dbt run --select tag:layer_stg --profiles-dir . --target prod',
    )

    run_s2_snapshot_task = BashOperator(
        task_id='dbt_snapshot_s2_layer',
        bash_command='cd /dbt && dbt snapshot --select app_user_profile__s2 --profiles-dir . --target prod',
    )

    run_ods_layer_task = BashOperator(
        task_id='dbt_run_ods_layer',
        bash_command='cd /dbt && dbt run --select tag:layer_ods --profiles-dir . --target prod',
    )

    run_rpt_layer_task = BashOperator(
        task_id='dbt_run_rpt_layer',
        bash_command='cd /dbt && dbt run --select tag:layer_rpt --profiles-dir . --target prod',
    )

    initiate_dbt_task >> run_stg_layer_task >> run_s2_snapshot_task >> run_ods_layer_task >> run_rpt_layer_task
