# Railway Dockerfile for Batch Layer
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the necessary files for Railway deployment
COPY raw_webhook_to_gcs.py .
COPY start_railway.sh .

# Make script executable
RUN chmod +x start_railway.sh

# Railway will provide PORT, but we don't need it for this service
ENV GCS_BUCKET=batch-btc-1h-east1

CMD ["./start_railway.sh"]
