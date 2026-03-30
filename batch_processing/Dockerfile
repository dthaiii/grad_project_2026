FROM bitnami/spark:3.5.1

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY batch_job.py /app/batch_job.py
WORKDIR /app

CMD ["spark-submit", "--master", "${SPARK_MASTER}", "batch_job.py"]
