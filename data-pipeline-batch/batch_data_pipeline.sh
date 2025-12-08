#!/bin/bash
# Batch Data Pipeline - Coinbase WebSocket to GCS

echo "==================================="
echo "Batch Data Pipeline Starting"
echo "==================================="
echo "Source: Coinbase WebSocket (BTC-USD)"
echo "Output: GCS batch-btc-1h-east1"
echo "Aggregation: 1-minute Parquet"
echo "==================================="

# Run the batch collector
python raw_webhook_to_gcs.py
