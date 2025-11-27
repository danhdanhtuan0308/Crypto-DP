# Railway Deployment Guide - Pipeline + Dashboard

This guide explains how to deploy **both** the data pipeline and Streamlit dashboard on Railway as separate services.

## Architecture Overview

- **Service 1 (Pipeline)**: Runs the Kafka producer, aggregator, and GCS writer (uses `web:` process type)
- **Service 2 (Dashboard)**: Runs the Streamlit web dashboard with public access

**⚠️ IMPORTANT**: Railway only runs `web:` process types. Even though the pipeline doesn't serve HTTP traffic, it must be declared as `web:` in the Procfile, not `worker:`.

## Prerequisites

1. Railway account with a project created
2. GCP Service Account JSON key file with:
   - Cloud Storage Admin permissions
   - Pub/Sub Admin permissions (for Kafka alternative)
3. Access to the GitHub repository

---

## Deployment Steps

### 1. Create Two Railway Services

You'll need to create **two separate services** from the same repository:

#### Service 1: Data Pipeline

1. In Railway, create a new service: **"crypto-pipeline"**
2. Connect to your GitHub repository
3. Configure the service:
   - **Root Directory**: Leave as `/` (root)
   - **Procfile Path**: `Procfile.pipeline` (or leave default if using `Procfile`)

#### Service 2: Streamlit Dashboard

1. In Railway, create another service: **"crypto-dashboard"**
2. Connect to the **same** GitHub repository
3. Configure the service:
   - **Root Directory**: Leave as `/` (root)
   - **Procfile Path**: `Procfile.dashboard`

---

### 2. Configure Procfiles

The repository contains three Procfile variants:

- **`Procfile`**: Default, runs the pipeline (contains `web: bash start.sh`)
- **`Procfile.pipeline`**: For the data pipeline (contains `web: bash start.sh`)
- **`Procfile.dashboard`**: For the Streamlit dashboard (contains `web: bash start_dashboard.sh`)

#### For the Pipeline Service:

In Railway service settings:
- **Procfile Path**: `Procfile.pipeline` (or leave blank to use default `Procfile`)

#### For the Dashboard Service:

In Railway service settings:
- **Procfile Path**: `Procfile.dashboard`

---

### 3. Set Environment Variables

Both services need the GCP credentials, but different additional variables.

#### For BOTH Services:

Add the `GCP_SERVICE_ACCOUNT_JSON` variable:

```bash
# Get your service account JSON as a single-line string
cat your-service-account.json | jq -c
```

Copy the output and add it as:
- **Variable Name**: `GCP_SERVICE_ACCOUNT_JSON`
- **Value**: (paste the single-line JSON)

#### For Pipeline Service Only:

```bash
COINBASE_API_KEY=your_coinbase_api_key
COINBASE_API_SECRET=your_coinbase_api_secret
KAFKA_BOOTSTRAP_SERVERS=your_kafka_broker:9092
```

#### For Dashboard Service Only:

Railway automatically provides:
- `PORT`: The port for the web service (automatically set by Railway)

The dashboard will automatically use this port.

---

### 4. Deploy

Once configured:

1. **Pipeline Service**: 
   - Click "Deploy" 
   - Railway will build and run as a `web` process (but runs background workers internally)
   - Check logs to verify all three processes start:
     ```
     Starting Coinbase Producer...
     Starting 1-Min Aggregator...
     Starting GCS Parquet Writer...
     All processes started:
       Producer: <PID>
       Aggregator: <PID>
       Writer: <PID>
     ```

2. **Dashboard Service**:
   - Click "Deploy"
   - Railway will generate a public URL (e.g., `crypto-dashboard.up.railway.app`)
   - Visit the URL to see your live dashboard

---

## Why `web:` Instead of `worker:`?

Railway's platform only executes process types declared as `web:`. Other process types like `worker:`, `scheduler:`, etc., are **ignored and won't run**.

Even though the pipeline doesn't serve HTTP requests, we must use `web:` in the Procfile. The actual processes (Kafka producer, aggregator, consumer) run as background tasks started by the shell script.

---

## Troubleshooting

### Pipeline Not Running

**Symptom**: Build succeeds but no logs show the pipeline starting

**Solution**: 
- Verify Procfile uses `web:` not `worker:`
- Check you're using the correct Procfile path in Railway settings
- Look for logs showing "Starting Coinbase Producer..."

### Dashboard Error: "Metadata service unavailable"

**Symptom**: 
```
Failed to retrieve from metadata.google.internal
```

**Solution**: Ensure `GCP_SERVICE_ACCOUNT_JSON` is set correctly in the dashboard service environment variables.

### Dashboard Shows "No data available"

**Possible causes**:
1. Pipeline service isn't running or hasn't written data yet
2. GCS bucket is empty
3. Incorrect bucket name in `streamlit_dashboard.py`

**Check**:
- Verify pipeline service logs show successful writes to GCS
- Check GCS bucket in Google Cloud Console
- Verify bucket name matches: `crypto-db-east1`

### Pipeline Not Writing Data

**Check**:
1. Kafka connection (verify `KAFKA_BOOTSTRAP_SERVERS`)
2. Coinbase API credentials
3. GCP permissions (service account needs Storage Admin)

---

## File Structure Reference

```
/
├── Procfile                          # Default (pipeline) - uses web:
├── Procfile.pipeline                 # For pipeline service - uses web:
├── Procfile.dashboard                # For dashboard service - uses web:
├── start.sh                          # Starts all 3 pipeline processes
├── start_dashboard.sh                # Starts Streamlit dashboard
├── requirements.txt                  # All Python dependencies
├── data-pipeline/
│   ├── coinbase_kafka_producer.py
│   ├── kafka_1min_aggregator.py
│   └── gcs_kafka_consumer.py
└── monitoring/
    └── streamlit_dashboard.py
```

---

## Updating Deployments

When you push changes to GitHub:

- **Pipeline Service**: Automatically redeploys if changes affect pipeline code
- **Dashboard Service**: Automatically redeploys if changes affect dashboard code

You can also manually trigger deployments in Railway.

---

## Cost Optimization

Railway charges based on resource usage:

- **Pipeline Service**: Runs continuously, uses modest resources
- **Dashboard Service**: Only active when users are viewing it (web service)

Both services should fit within Railway's free tier limits if you're not running high-frequency production workloads.

---

## Monitoring

### Pipeline Health:
- Check Railway logs for the pipeline service
- Look for successful Kafka messages and GCS writes
- Monitor GCS bucket size in Google Cloud Console

### Dashboard Health:
- Visit the public URL
- Check "Last Data" timestamp in sidebar
- Verify data freshness indicator (should be green/fresh)

---

## Next Steps

1. Set up alerts for service failures in Railway
2. Add monitoring/metrics collection
3. Consider rate limiting for Coinbase API
4. Add authentication to Streamlit dashboard for production use

---

## Support

If you encounter issues:

1. Check Railway service logs (click service → Logs)
2. Verify environment variables are set correctly
3. Ensure GCP service account has proper permissions
4. Check GCS bucket contents and structure
