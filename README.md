# 🎵 Music Streaming Data Pipeline

A real-time + batch data pipeline that simulates a music-streaming service
(à la Spotify), processes user events as they arrive, and surfaces analytics
on a live dashboard.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Event Producer                                  │
│   Generates fake listen / page-view / auth events at configurable rate  │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  publishes JSON to three Kafka topics
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Apache Kafka (KRaft)                              │
│   listen_events  │  page_view_events  │  auth_events                    │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  consumed by Spark Structured Streaming
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               Spark Structured Streaming Job                             │
│   • Triggers every 2 minutes                                             │
│   • Parses JSON, adds partition columns (year/month/day/hour)            │
│   • Writes Parquet files to the Data Lake                                │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  raw Parquet files
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         Data Lake  (/data/lake/raw/)                     │
│   listen_events/  │  page_view_events/  │  auth_events/                 │
│   Partitioned by year / month / day / hour                               │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  read by hourly batch job
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    Hourly PySpark Batch Job                              │
│   Reads raw partitions → computes six analytics tables:                  │
│     top_songs  │  top_artists  │  active_users                           │
│     user_demographics  │  genre_popularity  │  hourly_activity           │
└──────────────────────────┬───────────────────────────────────────────────┘
                           │  analytics Parquet tables
                           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                     Streamlit Dashboard  (:8501)                         │
│   Overview  │  Popular Songs  │  Active Users  │  User Demographics      │
└──────────────────────────────────────────────────────────────────────────┘
```

### Event Types

| Event | Kafka Topic | Key Fields |
|-------|-------------|------------|
| **Listen** | `listen_events` | `user_id`, `song_id`, `artist_name`, `genre`, `ms_played`, `platform`, `location` |
| **Page View** | `page_view_events` | `user_id`, `session_id`, `page`, `prev_page`, `platform` |
| **Auth** | `auth_events` | `user_id`, `action` (login/logout/register), `success`, `method` |

### Analytics Tables (produced by batch job)

| Table | Description |
|-------|-------------|
| `top_songs` | Songs ranked by play count with unique listeners & total play time |
| `top_artists` | Artists ranked by play count |
| `active_users` | Users ranked by total listening hours |
| `user_demographics` | Play stats broken down by country and platform |
| `genre_popularity` | Genre play counts |
| `hourly_activity` | Hourly listen / page-view / auth event counts |

---

## Repository Structure

```
grad_project_2026/
├── docker-compose.yml         ← Orchestrates all services
├── .env.example               ← Environment variable template
├── Makefile                   ← Common commands
├── requirements-dev.txt       ← Local dev / test dependencies
│
├── event_producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── schemas.py             ← Dataclasses for all event types
│   └── producer.py            ← Kafka producer loop
│
├── spark_streaming/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── streaming_job.py       ← PySpark Structured Streaming job
│
├── batch_processing/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── batch_job.py           ← Hourly PySpark batch transformations
│
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                 ← Streamlit main page (overview)
│   └── pages/
│       ├── 1_Popular_Songs.py
│       ├── 2_Active_Users.py
│       └── 3_User_Demographics.py
│
└── tests/
    ├── test_schemas.py              ← Unit tests for event dataclasses
    ├── test_producer.py             ← Unit tests for producer (Kafka mocked)
    └── test_batch_transformations.py ← Unit tests for PySpark analytics
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker | ≥ 24 |
| Docker Compose | ≥ 2.24 |
| Python (local tests only) | ≥ 3.11 |
| Java (local tests only) | ≥ 11 |

---

## Quick Start

```bash
# 1. Copy environment file
cp .env.example .env

# 2. Build all images
make build

# 3. Start all services
make up

# 4. Check status
make status

# 5. Open the dashboard
open http://localhost:8501

# 6. View Spark master UI
open http://localhost:8080
```

The pipeline will start producing events immediately.  
The Spark streaming job writes Parquet files every **2 minutes**.  
The batch job runs every **hour** and refreshes the analytics tables.

---

## Running Tests Locally

```bash
# Install dev dependencies
make install-dev

# Run all tests
make test

# Run individual test modules
make test-schemas
make test-producer
make test-batch
```

> **Note:** The batch-transformation tests spin up a local PySpark session — Java 11+ must be on your `PATH`.

---

## Configuration

All tuneable parameters live in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address (inside Docker) |
| `EVENTS_PER_SECOND` | `10` | How many events the producer emits per second |
| `NUM_USERS` | `1000` | Size of the synthetic user catalogue |
| `NUM_SONGS` | `500` | Size of the synthetic song catalogue |
| `TRIGGER_INTERVAL` | `2 minutes` | Spark Streaming micro-batch trigger |
| `BATCH_INTERVAL_SECONDS` | `3600` | Seconds between batch runs (3 600 = 1 h) |
| `DATALAKE_PATH` | `/data/lake` | Root path for the data lake (inside containers) |
| `ANALYTICS_PATH` | `/data/lake/analytics` | Root path for analytics tables |

---

## Useful Commands

```bash
# Tail all logs
make logs

# Tail logs for a single service
make logs-producer
make logs-streaming
make logs-batch

# List Kafka topics
make kafka-topics

# Stop everything and remove volumes
make clean
```

---

## Dashboard Pages

| Page | URL | Content |
|------|-----|---------|
| Overview | `http://localhost:8501` | KPIs, top-10 songs, genre pie, hourly trend |
| Popular Songs | `http://localhost:8501/Popular_Songs` | Song & artist leaderboards |
| Active Users | `http://localhost:8501/Active_Users` | User leaderboard, listening distribution |
| User Demographics | `http://localhost:8501/User_Demographics` | World map, country bar chart, platform pie |

---

## Data Flow in Detail

```
1. event_producer/producer.py
   └─ generates User & Song catalogues (Faker)
   └─ emits ListenEvent / PageViewEvent / AuthEvent at EVENTS_PER_SECOND
   └─ serialises to JSON and publishes to Kafka

2. spark_streaming/streaming_job.py
   └─ reads all three topics via spark-sql-kafka connector
   └─ parses JSON with predefined StructType schemas
   └─ adds year/month/day/hour partition columns
   └─ fires a micro-batch every TRIGGER_INTERVAL
   └─ appends Parquet files under /data/lake/raw/<topic>/

3. batch_processing/batch_job.py
   └─ wakes up every BATCH_INTERVAL_SECONDS
   └─ reads raw Parquet with spark.read.parquet()
   └─ computes six analytics DataFrames
   └─ overwrites /data/lake/analytics/<table>/

4. dashboard/app.py + pages/
   └─ reads analytics Parquet with pandas.read_parquet()
   └─ renders Plotly charts via Streamlit
   └─ cache TTL = 120 s so the UI refreshes without a page reload
```