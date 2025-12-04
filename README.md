# Crypto-DP

Real-time BTC data pipeline: **Coinbase WebSocket → Kafka → GCS → Dashboard**

## Architecture

**Data Pipeline** (`data-pipeline/`)
- **Producer**: Streams BTC-USD from Coinbase → Kafka (1 msg/sec, 50+ metrics)
- **Aggregator**: 1-sec → 1-min OHLC aggregation (20+ metrics)
- **Consumer**: Writes Parquet files to GCS (hourly partitions)

**Dashboard** (`dashboard/`)
- Live BTC monitoring with OHLC charts, volume, volatility
- Auto-refresh every 60 seconds

## Deployment (Railway)

Two services with separate Dockerfiles:

**Service 1: Data Pipeline**
- Root Directory: `data-pipeline`
- Dockerfile: `data-pipeline/Dockerfile`
- Runs: Producer + Aggregator + GCS Writer

**Service 2: Dashboard**
- Root Directory: `dashboard`
- Dockerfile: `dashboard/Dockerfile`
- Runs: Streamlit app on Railway's public URL
- Access: Railway will generate a public URL (e.g., `https://your-app.railway.app`)

## Environment Variables

```bash
# Kafka (both services)
CONFLUENT_KAFKA_BOOTSTRAP_SERVERS
CONFLUENT_KAFKA_API_KEY_GCS
CONFLUENT_KAFKA_API_KEY_SECRET_GCS

# GCS (both services)
GCS_BUCKET
GCP_SERVICE_ACCOUNT_JSON
```

## Local Development

```bash
pip install -r requirements.txt
./start_pipeline_railway.sh  # Start pipeline
./start_dashboard.sh         # Start dashboard
``` 
