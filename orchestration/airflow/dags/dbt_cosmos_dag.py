import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping


DBT_PROJECT_PATH = Path("/dbt")
DBT_EXECUTABLE_PATH = Path("/home/airflow/.local/bin/dbt")


profile_config = ProfileConfig(
    profile_name="default",
    target_name="prod",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": os.environ.get("GCP_PROJECT_ID"),
            "dataset": os.environ.get("DBT_BIGQUERY_DATASET", os.environ.get("BIGQUERY_DATASET")),
        },
    ),
)


execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE_PATH),
    # Run dbt in subprocess mode to avoid dbtRunner invocation edge-cases.
    invocation_mode=InvocationMode.SUBPROCESS,
)


render_config = RenderConfig(
    # Keep model-level Cosmos graph while avoiding problematic seed nodes.
    select=["resource_type:model"],
    # Prefer fresh dbt ls parse over cached parsing to reduce cache-related runtime issues.
    load_method=LoadMode.DBT_LS,
)


dbt_workflow_dag = DbtDag(
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    operator_args={
        "install_deps": True,
        "full_refresh": False,
    },
    dag_id="dbt_cosmos_dag",
    start_date=datetime(2026,3,1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=["dbt", "cosmos"],
)
