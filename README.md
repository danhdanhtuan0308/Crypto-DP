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

### Next Steps
- Configure Confluent GCS Sink Connector (10-minute batch uploads) -> Cloud Storage 
- Set up Grafana Websocket Cloud real-time monitoring dashboard
- Build ML model consumer for dynamic pricing predictions 
