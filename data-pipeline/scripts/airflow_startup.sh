#!/bin/bash

# Get the script's directory for relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "Changing permissions for dbt folder..."
cd "$PROJECT_ROOT/orchestration/dbt" && sudo chmod -R 777 .

echo "Setting environment variables for Airflow..."
cat <<EOF > "$PROJECT_ROOT/orchestration/airflow/.env"
AIRFLOW_UID=$(id -u)
GCP_PROJECT_ID=${GCP_PROJECT_ID:-grad-service-music-pipeline}
GCP_GCS_BUCKET=${GCP_GCS_BUCKET:-music-streaming-data-lake}
BIGQUERY_DATASET=${BIGQUERY_DATASET:-eventsim-stgging}
EOF

echo "Building airflow docker images..."
cd "$PROJECT_ROOT/orchestration/airflow"
docker-compose build

echo "Running airflow-init..."
docker-compose up airflow-init

echo "Starting up airflow in detached mode..."
docker-compose up -d

echo "Airflow started successfully."
echo "Airflow is running in detached mode. "
echo "Run 'docker-compose logs --follow' to see the logs."