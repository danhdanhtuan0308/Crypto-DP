#!/bin/bash

# Start all 3 pipeline processes in the background
cd data-pipeline

echo "Starting Coinbase Producer..."
python coinbase_kafka_producer.py &
PID1=$!

echo "Starting 1-Min Aggregator..."
python kafka_1min_aggregator.py &
PID2=$!

echo "Starting GCS Parquet Writer..."
python gcs_kafka_consumer.py &
PID3=$!

echo "All processes started:"
echo "  Producer: $PID1"
echo "  Aggregator: $PID2"
echo "  Writer: $PID3"

# Start health check server for Railway on PORT
cd ..
echo "Starting health check server on port ${PORT:-8080}..."
python health_server.py &
HTTP_PID=$!
echo "  Health Server: $HTTP_PID"

# Wait for all processes
wait $PID1 $PID2 $PID3 $HTTP_PID
