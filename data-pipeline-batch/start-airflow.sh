#!/bin/bash
set -e

echo "=========================================="
echo "Initializing Airflow..."
echo "=========================================="

# Initialize database
echo "Running database migration..."
airflow db migrate

# Create admin user
echo "Creating admin user..."
USERNAME=${AIRFLOW_USERNAME:-admin}
PASSWORD=${AIRFLOW_PASSWORD:-admin}

echo "Using username: $USERNAME"

# Try to create user, ignore if exists
airflow users create \
    --username "$USERNAME" \
    --password "$PASSWORD" \
    --firstname Crypto \
    --lastname Admin \
    --role Admin \
    --email admin@cryptodp.com 2>&1 || echo "User already exists or error occurred"

echo "=========================================="
echo "Starting Airflow Services..."
echo "=========================================="

# Start scheduler in background
echo "Starting scheduler..."
airflow scheduler &

# Start webserver in foreground
echo "Starting webserver on 0.0.0.0:8080..."
exec airflow webserver --port 8080 --host 0.0.0.0
