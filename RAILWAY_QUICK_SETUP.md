# Railway Quick Setup

## ⚠️ CRITICAL: Use `web:` Not `worker:`

Railway **ONLY runs `web:` process types**. All Procfiles must use `web:` even for background processes!

---

## Service 1: Pipeline
- **Name**: crypto-pipeline
- **Procfile Path**: `Procfile.pipeline` (or leave blank for default `Procfile`)
- **Environment Variables**:
  ```
  GCP_SERVICE_ACCOUNT_JSON=<your-service-account-json>
  COINBASE_API_KEY=<your-api-key>
  COINBASE_API_SECRET=<your-api-secret>
  KAFKA_BOOTSTRAP_SERVERS=<your-kafka-broker:9092>
  ```

## Service 2: Dashboard
- **Name**: crypto-dashboard  
- **Procfile Path**: `Procfile.dashboard`
- **Environment Variables**:
  ```
  GCP_SERVICE_ACCOUNT_JSON=<your-service-account-json>
  ```
- **Note**: PORT is automatically provided by Railway

---

## Getting Single-Line JSON for GCP_SERVICE_ACCOUNT_JSON

```bash
cat your-service-account.json | jq -c
```

Or manually remove all newlines and extra spaces from your JSON file.

---

## Verification

### Pipeline Service Logs Should Show:
```
↳ Detected Python
↳ Using pip
↳ Found web command in Procfile    <-- Must say "web" not "worker"!

Deploy
──────────
$ bash start.sh

Starting Coinbase Producer...
Starting 1-Min Aggregator...
Starting GCS Parquet Writer...
All processes started:
  Producer: <PID>
  Aggregator: <PID>
  Writer: <PID>
```

### Dashboard Service:
- Should get a public URL from Railway
- Visit URL to see live dashboard
- Sidebar should show: "🔑 Using GCP_SERVICE_ACCOUNT_JSON env var"

---

## Troubleshooting

**Problem**: Pipeline builds but doesn't run
**Solution**: Check Procfile uses `web:` not `worker:` - Railway ignores `worker:` processes!

**Problem**: Dashboard shows metadata error
**Solution**: Add `GCP_SERVICE_ACCOUNT_JSON` environment variable with your service account JSON
