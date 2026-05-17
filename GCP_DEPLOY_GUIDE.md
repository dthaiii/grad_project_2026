#!/bin/bash
# Hướng dẫn chạy trên 3 máy ảo GCP

echo "--- STEP 1: VM KAFKA & EVENTSIM (GCP-1) ---"
echo "1. Chạy vm_setup.sh"
echo "2. Nạp .env.gcp (hoặc export KAFKA_ADDRESS=IP_VM) trước khi chạy docker compose"
echo "3. Chạy eventsim_startup.sh"
echo "Lưu ý: External IP của máy này sẽ được dùng cho KAFKA_ADDRESS"

echo -e "\n--- STEP 2: VM SPARK STREAMING (GCP-2) ---"
echo "1. Chạy vm_setup.sh và spark_setup.sh"
echo "2. Nạp .env.gcp (hoặc export KAFKA_ADDRESS=IP_VM-1) trước khi chạy Spark job"
echo "3. Chạy Spark job: spark-submit --packages ... stream_all_events.py"
echo "LOG: Kafka endpoint cần trỏ về IP VM-1:9092"

echo -e "\n--- STEP 3: VM AIRFLOW & DBT (GCP-3) ---"
echo "1. Chạy vm_setup.sh"
echo "2. Copy file .env.gcp vào orchestration/airflow/.env"
echo "3. Chạy airflow_startup.sh"
echo "LOG: Airflow sẽ điều phối dbt run sau khi Spark đẩy dữ liệu"
