# Crypto-DP

Overview : Data Pipeline from Coinbase Websocket -> Kafka Topic then fan-out to Cloud-storage ( Offline Training ) , Gafana Websocket for-real time monitoring and Online Serving for real-time prediction dynamic Pricing 

## Data Pipeline

### Completed Steps
**Coinbase WebSocket → Kafka Producer**
   - Streaming BTC/USDT market data from Coinbase WebSocket
   - 54 metrics per message (price, volume, order book, rolling calculations)
   - Real-time price from order book mid-price + trade updates
   - Publishing to Confluent Cloud Kafka topic `BTC-USD` every 1 second
   - Rolling metrics: 1s, 1min, 1h, 24h time windows

**1-Minute Aggregation**
   - Consumes from `BTC-USD` topic (1 msg/sec)
   - Calculates 1-minute OHLC, volatility, order imbalance ratio
   - Produces aggregated data to `btc_1min_agg` topic (1 msg/min)
   - 20+ metrics per aggregated message

**GCS Parquet Writer**
   - Consumes from `btc_1min_agg` topic
   - Writes Parquet files to Google Cloud Storage (production: 1 file/hour with 60 rows)
   - Organized in hourly folders: `year=YYYY/month=MM/day=DD/hour=HH/`
   - Snappy compression for efficient storage

**Streamlit Dashboard**
   - Live BTC monitoring with OHLC charts, volume analysis, volatility tracking
   - Auto-refresh every 60 seconds, displays full day of 1-minute data
   - Deployed on Railway with parallel file loading from GCS

**Railway Deployment**
   - Pipeline service: 3 workers (Producer, Aggregator, GCS Writer) + health server
   - Dashboard service: Streamlit web app
   - Auto-deploys on git push

### Next Steps
- Build ML model consumer for dynamic pricing predictions 
