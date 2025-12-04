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

# Copy dashboard files
COPY dashboard/ ./dashboard/
COPY start_dashboard.sh .

# Make script executable
RUN chmod +x start_dashboard.sh

# Set environment
ENV PYTHONUNBUFFERED=1

# Expose Streamlit port
EXPOSE ${PORT:-8501}

# Run the dashboard
CMD ["./start_dashboard.sh"]
