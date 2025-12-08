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

# Try to create user - must succeed or we'll use existing
echo "Attempting to create user: $USERNAME"
if airflow users create \
    --username "$USERNAME" \
    --password "$PASSWORD" \
    --firstname Crypto \
    --lastname Admin \
    --role Admin \
    --email admin@cryptodp.com 2>&1; then
    echo "✅ User '$USERNAME' created successfully"
else
    echo "⚠️  User creation failed or user already exists - checking if user exists..."
    # List users to verify
    airflow users list | grep "$USERNAME" && echo "✅ User '$USERNAME' exists" || echo "❌ ERROR: User '$USERNAME' does NOT exist!"
fi

echo "=========================================="
echo "Starting Airflow Services..."
echo "=========================================="

# Wait a moment to ensure DB is fully ready
sleep 2

# Start scheduler in background
echo "Starting scheduler..."
airflow scheduler &

# Wait for scheduler to initialize
sleep 3

# Start webserver in foreground
echo "Starting webserver on 0.0.0.0:8080..."
exec airflow webserver --port 8080 --host 0.0.0.0
