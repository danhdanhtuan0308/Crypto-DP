#!/bin/bash
# Crypto Batch Pipeline - Docker Management Script

set -e

CMD=${1:-help}

case "$CMD" in
    build)
        echo "🚀 Building Batch Pipeline Docker Image..."
        docker build -f Dockerfile.airflow -t crypto-batch-airflow:latest .
        echo ""
        echo "✅ Build complete!"
        echo "📦 Image: crypto-batch-airflow:latest"
        echo ""
        echo "Run with: ./docker.sh start"
        ;;
        
    start)
        # Check if .env file exists
        if [ ! -f .env ]; then
            echo "❌ Error: .env file not found!"
            echo "Please create .env file with your GCP credentials."
            exit 1
        fi
        
        # Stop existing container if running
        if [ "$(docker ps -aq -f name=crypto-batch-airflow)" ]; then
            echo "🛑 Stopping existing container..."
            docker stop crypto-batch-airflow 2>/dev/null || true
            docker rm crypto-batch-airflow 2>/dev/null || true
        fi
        
        echo "🚀 Starting Batch Pipeline Container..."
        
        # Create logs directory
        mkdir -p logs
        
        # Check if credentials file exists
        if [ -f "/home/daniellai/confluent-gcs-key.json" ]; then
            echo "✅ Found GCS credentials file"
            CREDS_MOUNT="-v /home/daniellai/confluent-gcs-key.json:/opt/airflow/gcs-key.json:ro"
        else
            echo "⚠️  No credentials file found"
            CREDS_MOUNT=""
        fi
        
        # Run container
        docker run -d \
            --name crypto-batch-airflow \
            -p 8080:8080 \
            --env-file .env \
            -e GCS_CREDENTIALS_PATH=/opt/airflow/gcs-key.json \
            -v "$(pwd)/logs:/opt/airflow/logs" \
            $CREDS_MOUNT \
            --restart unless-stopped \
            crypto-batch-airflow:latest
        
        echo ""
        echo "✅ Container started!"
        echo ""
        echo "📊 Airflow UI: http://localhost:8080"
        echo "   Username: ${AIRFLOW_USERNAME:-admin}"
        echo "   Password: ${AIRFLOW_PASSWORD:-admin}"
        echo ""
        echo "📝 View logs: ./docker.sh logs"
        echo "🛑 Stop: ./docker.sh stop"
        echo ""
        echo "⏳ Waiting for Airflow to start (30-60 seconds)..."
        sleep 10
        docker logs crypto-batch-airflow --tail 20
        ;;
        
    stop)
        echo "🛑 Stopping container..."
        docker stop crypto-batch-airflow 2>/dev/null || echo "Container not running"
        docker rm crypto-batch-airflow 2>/dev/null || echo "Container already removed"
        echo "✅ Container stopped"
        ;;
        
    restart)
        echo "🔄 Restarting container..."
        docker restart crypto-batch-airflow
        echo "✅ Container restarted"
        ;;
        
    logs)
        docker logs -f crypto-batch-airflow
        ;;
        
    status)
        echo "📊 Container Status:"
        docker ps -a | grep crypto-batch-airflow || echo "Container not found"
        ;;
        
    shell)
        echo "🐚 Opening shell in container..."
        docker exec -it crypto-batch-airflow bash
        ;;
        
    clean)
        echo "🗑️  Cleaning up..."
        docker stop crypto-batch-airflow 2>/dev/null || true
        docker rm crypto-batch-airflow 2>/dev/null || true
        docker rmi crypto-batch-airflow:latest 2>/dev/null || true
        echo "✅ Cleanup complete"
        ;;
        
    help|*)
        echo "Crypto Batch Pipeline - Docker Management"
        echo ""
        echo "Usage: ./docker.sh [command]"
        echo ""
        echo "Commands:"
        echo "  build     Build the Docker image"
        echo "  start     Start the container"
        echo "  stop      Stop the container"
        echo "  restart   Restart the container"
        echo "  logs      View container logs (live)"
        echo "  status    Check container status"
        echo "  shell     Open bash shell in container"
        echo "  clean     Remove container and image"
        echo "  help      Show this help message"
        echo ""
        echo "Quick Start:"
        echo "  1. ./docker.sh build"
        echo "  2. ./docker.sh start"
        echo "  3. Open http://localhost:8080"
        ;;
esac
