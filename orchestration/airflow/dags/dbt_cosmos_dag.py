import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import BigQueryServiceAccountProfileMapping

# Path to your dbt project
DBT_PROJECT_PATH = Path("/dbt")
DBT_EXECUTABLE_PATH = Path("/usr/local/bin/dbt")

# Define the BigQuery profile using service account
profile_config = ProfileConfig(
    profile_name="default",
    target_name="prod",
    profile_mapping=BigQueryServiceAccountProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": os.environ.get("GCP_PROJECT_ID"),
            "dataset": os.environ.get("BIGQUERY_DATASET"),
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE_PATH),
)

dbt_workflow_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True,
        "full_refresh": False,
    },
    # DAG configurations
    dag_id="dbt_cosmos_dag",
    start_date=datetime(2026, 3, 8),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["dbt", "cosmos"],
)
