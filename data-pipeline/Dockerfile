FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline scripts
COPY data-pipeline/ ./data-pipeline/
COPY start_pipeline_railway.sh .
COPY health_server.py .

# Make script executable
RUN chmod +x start_pipeline_railway.sh

# Set environment
ENV PYTHONUNBUFFERED=1

# Expose port for Railway health check
EXPOSE ${PORT:-8080}

# Run the startup script
CMD ["./start_pipeline_railway.sh"]
