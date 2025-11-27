# BTC Live Dashboard - Streamlit Deployment

## Overview
Real-time Bitcoin monitoring dashboard with 1-minute data updates from Google Cloud Storage.

## Features
- 📈 Live OHLC candlestick chart
- 📊 Volume analysis (buy vs sell)
- 🔬 Advanced metrics (volatility, order imbalance)
- ⚡ Auto-refresh every 60 seconds
- 📱 Responsive design
- 🌐 Shareable public URL

## Architecture
```
Kafka (btc_1min_agg) → GCS Parquet Files → Streamlit Dashboard
```

## Local Testing

### 1. Install Dependencies
```bash
pip install -r requirements-streamlit.txt
```

### 2. Set up GCP Credentials
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

### 3. Run Streamlit
```bash
streamlit run streamlit_dashboard.py
```

Open browser: http://localhost:8501

## Deploy to Streamlit Cloud (FREE)

### Step 1: Push to GitHub
```bash
git add streamlit_dashboard.py requirements-streamlit.txt
git commit -m "Add Streamlit dashboard"
git push origin main
```

### Step 2: Deploy on Streamlit Cloud

1. Go to https://share.streamlit.io
2. Click "New app"
3. Connect your GitHub account
4. Select:
   - Repository: `danhdanhtuan0308/Crypto-DP`
   - Branch: `main`
   - Main file path: `streamlit_dashboard.py`
5. Click "Advanced settings"
6. Add secrets (GCP credentials):
   ```toml
   # .streamlit/secrets.toml format
   [gcp_service_account]
   type = "service_account"
   project_id = "crypto-dp"
   private_key_id = "your-key-id"
   private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   client_email = "your-service-account@crypto-dp.iam.gserviceaccount.com"
   client_id = "your-client-id"
   auth_uri = "https://accounts.google.com/o/oauth2/auth"
   token_uri = "https://oauth2.googleapis.com/token"
   auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
   client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
   ```
7. Click "Deploy"

### Step 3: Get Your Public URL
After deployment completes, you'll get a URL like:
```
https://your-app-name.streamlit.app
```

Share this URL with anyone!

## Configuration

### Update GCS Bucket
Edit `streamlit_dashboard.py`:
```python
BUCKET_NAME = 'your-bucket-name'
PREFIX = 'your-prefix/'
```

### Customize Refresh Interval
Default: 60 seconds
Adjust in sidebar or change default in code:
```python
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 30, 300, 60)
```

## Keep Dashboard Awake 24/7 (Free)

Use UptimeRobot to ping your dashboard every 5 minutes:

1. Go to https://uptimerobot.com
2. Add new monitor
3. Monitor Type: HTTP(s)
4. URL: Your Streamlit app URL
5. Monitoring Interval: 5 minutes

This prevents Streamlit Cloud from sleeping your app.

## Costs
- Streamlit Cloud: **FREE** (3 apps on free tier)
- GCS Storage: ~$0.25/month
- BigQuery (if used): ~$0.01-0.05/month

**Total: ~$0.30/month**

## Troubleshooting

### No data showing
1. Check GCS bucket has data: `gsutil ls gs://crypto-db-east1/btc_1min_agg/`
2. Verify service account has permissions
3. Check Streamlit logs in the cloud dashboard

### App keeps restarting
- Reduce lookback hours
- Optimize data loading
- Check memory usage (free tier: 1GB RAM)

### Authentication errors
- Verify GCP credentials in Streamlit secrets
- Ensure service account has `Storage Object Viewer` role

## Support
For issues, check:
- Streamlit Logs: https://share.streamlit.io (your app → Manage app → Logs)
- GCS Console: https://console.cloud.google.com/storage
