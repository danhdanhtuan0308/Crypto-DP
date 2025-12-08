#!/bin/bash
set -e

echo "Initializing Airflow database..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username ${AIRFLOW_USERNAME:-admin} \
    --password ${AIRFLOW_PASSWORD:-admin} \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com 2>&1 || true

echo "Starting scheduler in background..."
airflow scheduler &

echo "Starting webserver..."
exec airflow webserver
