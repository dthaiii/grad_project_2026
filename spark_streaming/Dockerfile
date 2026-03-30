FROM bitnami/spark:3.5.1

USER root

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY streaming_job.py /app/streaming_job.py
WORKDIR /app

CMD ["spark-submit", \
     "--master", "${SPARK_MASTER}", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "streaming_job.py"]
