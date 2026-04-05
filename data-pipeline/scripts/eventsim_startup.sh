# Get the script's directory for relative paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

cd "$PROJECT_ROOT/data-pipeline/ingestion/eventsim"

echo "Building Eventsim Image..."
docker build -t events:1.0 .

echo "Running Eventsim in detached mode..."
docker run -itd \
  --network host \
  --name million_events \
  --memory="8g" \
  --oom-kill-disable \
  events:1.0 \
    -c "examples/example-config.json" \
    --start-time "2026-03-01T00:00:00" \
    --end-time "2026-04-04T23:59:59" \
    --nusers 200000 \
    --growth-rate 5 \
    --userid 1 \
    --kafkaBrokerList localhost:9092 \
    --randomseed 42 \
    --continuous

echo "Started streaming events for 100,000 users..."
echo "Eventsim is running in detached mode. "
echo "Run 'docker logs --follow million_events' to see the logs."