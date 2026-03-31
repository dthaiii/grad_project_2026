FROM python:3.12

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "-u", "ingest_data.py"]