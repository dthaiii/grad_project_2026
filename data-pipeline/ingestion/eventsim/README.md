# Quickstart
1. Set up eventsim and kafka cluster by running the 2 commands:
```sh
# sets up environment variables for eventsim
. ./scripts/setup_environment.sh

# Creates the network and containers and runs them in detached mode
docker compose up -d
```
2. Build the image for kafka connect with gcs sink connector:
```sh
docker build -t connect-gcs:1.0 -f Dockerfile.kafkaConnect .
```
3. (See note below if you are not using a Google Compute VM) Create the container for kafka connect:
```sh
docker run -it \
    --net eventsim_default \
    --name connect-container \
    --env-file ./configs/kafka-connect/connect_setup.env \
    -p 8083:8083
    gcs-connect:2.0
```
4. Wait until you see a heartbeat message being printed every 5 seconds. This indicates that the Kafka Connect REST API endpoint is ready.

5. Open a new terminal in the same machine and navigate to the `config/kafka-connect` subdirectory in this directory. Edit the `connect_sink_users.json` file (e.g. using vim or other text editors) by replacing the `"web-events-bucket"` in `"gcs.bucket.name": "web-events-bucket"` with the name of your GCS bucket.

6. Create the sink task using this command:
```sh
curl -X POST -H "Content-Type: application/json" --data @connector_sink_users.json http://localhost:8083/connectors
```
Kafka Connect will now start writing files to your storage bucket.


Note for step 3: if you are not running the containers on a Google Compute VM, you may need to set the google credentials manually. One way to do this is to add the following to `connect_setup.env`
```sh
GOOGLE_APPLICATION_CREDENTIALS=/.google/credentials/my-creds.json
```
followed by adding `-v path/to/google/credentials.json:/.google/credentials/my-creds.json` to the command in step 3.