# Crypto-DP

Real-time BTC data pipeline: **Coinbase WebSocket → Kafka → GCS → Dashboard**

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

## Deployment (Railway)

Three services with separate Dockerfiles:

**Service 1: Data Pipeline**
- Root Directory: `data-pipeline`
- Dockerfile: `data-pipeline/Dockerfile`
- Runs: Producer + Aggregator + GCS Writer

**Service 2: Batch Pipeline (Airflow)**
- Root Directory: `data-pipeline-batch`
- Dockerfile: `data-pipeline-batch/Dockerfile.airflow`
- Runs: Airflow webserver + scheduler
- Access: Railway generates public URL (login with AIRFLOW_USERNAME/AIRFLOW_PASSWORD)

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
```
## Local Development

```bash
pip install -r requirements.txt
./start_pipeline_railway.sh  # Start pipeline
./start_dashboard.sh         # Start dashboard

# For batch layer
cd data-pipeline-batch
./start-airflow.sh           # Start Airflow (http://localhost:8080)
```

## Validation

**Schema Parity Check**
```bash
python validate_schema_parity.py
```
Validates that Batch pipeline produces identical schema to Kafka pipeline:
- ✅ All 37 columns present
- ✅ All data types match (double for all metrics)
- ✅ Sample data ranges verifiedtart-airflow.sh           # Start Airflow (http://localhost:8080)
``` 
