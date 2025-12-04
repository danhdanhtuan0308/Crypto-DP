FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all pipeline files
COPY . .

# Make script executable
RUN chmod +x start_pipeline_railway.sh

# Set environment
ENV PYTHONUNBUFFERED=1

# Expose port for Railway health check
EXPOSE ${PORT:-8080}

# Run the startup script
CMD ["./start_pipeline_railway.sh"]
