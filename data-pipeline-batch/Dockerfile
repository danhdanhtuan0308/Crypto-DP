# Dockerfile for Batch Collector Service
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY raw_webhook_to_gcs.py .

# Environment variables
ENV GCS_BUCKET=batch-btc-1h-east1
ENV PYTHONUNBUFFERED=1
ENV PORT=5000

# Run the batch collector
CMD ["python", "raw_webhook_to_gcs.py"]
