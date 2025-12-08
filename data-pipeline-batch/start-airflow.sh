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

# Try to create user
airflow users create \
    --username "$USERNAME" \
    --password "$PASSWORD" \
    --firstname Crypto \
    --lastname Admin \
    --role Admin \
    --email admin@cryptodp.com 2>&1 || echo "User may already exist"

echo "=========================================="
echo "Starting Airflow Standalone Mode..."
echo "=========================================="

# Use standalone mode - combines webserver and scheduler
exec airflow standalone
