# Crypto-DP

Real-time BTC data pipeline: **Coinbase WebSocket → Kafka → GCS → Dashboard -> Real time AI analyzer**

## Architecture

**Data Pipeline** (`data-pipeline/`)
- **Producer**: Streams BTC-USD from Coinbase → Kafka (1 msg/sec, 50+ metrics)
- **Aggregator**: 1-sec → 1-min OHLC aggregation (20+ metrics)
- **Consumer**: Writes Parquet files to GCS (hourly partitions)

**Batch Pipeline** (`data-pipeline-batch/`)
- **Airflow DAG**: Runs every 5 minutes (*/5 * * * *) - collects 5 min of data
- **ETL**: Coinbase WebSocket → 1-min aggregation → 5 rows per file → GCS Parquet
- **Output**: Cloud-Storage (`batch-btc-1h-east1`)
- **Schema Parity**: Validated against Kafka pipeline (37 features, all double types)

**Dashboard** (`dashboard/`)
- Live BTC monitoring with OHLC charts, volume, volatility
- Auto-refresh every 60 seconds
- **Danziel-AI**: Grok-powered assistant for real-time market analysis
  - Analyzes latest 6 hours of 1-minute data
  - Floating chat interface (bottom-right corner)
  - Context-aware responses about prices, volumes, volatility, and order flow

## Deployment 
Three services with separate Dockerfiles:

**Service 1: Data Pipeline**
- Root Directory: `data-pipeline`
- Dockerfile: `data-pipeline/Dockerfile`
- Runs: Producer + Aggregator + GCS Writer

**Service 2: Batch Pipeline (Airflow) - Local**
- Root Directory: `data-pipeline-batch`
- Dockerfile: `data-pipeline-batch/Dockerfile.airflow`
- Runs: ./docker.sh build then ./docker.sh start 

**Service 3: Dashboard**
- Root Directory: `dashboard`
- Dockerfile: `dashboard/Dockerfile`
- Runs: Streamlit app on Railway's public URL
- Access: Railway will generate a public URL (e.g., `https://your-app.railway.app`)

## Environment Variables

```bash
# Kafka (data-pipeline service)
CONFLUENT_KAFKA_BOOTSTRAP_SERVERS
CONFLUENT_KAFKA_API_KEY_GCS
CONFLUENT_KAFKA_API_KEY_SECRET_GCS

# GCS (all services)
GCS_BUCKET  # btc-1min-data-est OR batch-btc-1h-east1
GCP_SERVICE_ACCOUNT_JSON

# Airflow (data-pipeline-batch service)
AIRFLOW_USERNAME
AIRFLOW_PASSWORD

# Grok AI (dashboard service)
GROK_API_KEY


