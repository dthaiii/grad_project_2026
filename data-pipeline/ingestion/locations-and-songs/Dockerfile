FROM python:3.12

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

# Thêm vào cuối file Dockerfile

COPY google_credentials.json /app/google_credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/google_credentials.json
ENTRYPOINT ["python", "-u", "ingest_data.py"]