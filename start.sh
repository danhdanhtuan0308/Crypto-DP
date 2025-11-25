#!/bin/bash

# Start all 3 processes in the background
cd data-pipeline

echo "Starting Coinbase Producer..."
python coinbase_kafka_producer.py &
PID1=$!

echo "Starting 1-Min Aggregator..."
python kafka_1min_aggregator.py &
PID2=$!

echo "Starting GCS Parquet Writer..."
python gcs_parquet_writer.py &
PID3=$!

echo "All processes started:"
echo "  Producer: $PID1"
echo "  Aggregator: $PID2"
echo "  Writer: $PID3"

# Wait for all processes
wait $PID1 $PID2 $PID3
