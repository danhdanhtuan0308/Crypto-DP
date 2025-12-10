# BTC Live Dashboard - Reflex Frontend

A real-time Bitcoin dashboard built with Reflex (Python) and Plotly for interactive data visualization.

## Features

- **Real-time Data**: 1-minute aggregated BTC data from Google Cloud Storage
- **Interactive Charts**: 9 different Plotly charts showing price, volume, and market microstructure metrics
- **Auto-refresh**: Updates every 60 seconds
- **Timeline Selection**: View data from 5 minutes to 24 hours
- **Eastern Time**: All timestamps displayed in EST/EDT

## Architecture

- **Framework**: Reflex (Python-based reactive web framework)
- **Charts**: Plotly for interactive visualizations
- **Data Source**: Google Cloud Storage (bucket: crypto-db-east1)
- **State Management**: Reflex State with background tasks
- **Data Loading**: Incremental loading with 24-hour cache

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set GCP credentials:
```bash
export GCP_SERVICE_ACCOUNT_JSON='{"type": "service_account", ...}'
```

3. Run the application:
```bash
reflex run
```

Or use the start script:
```bash
chmod +x start_dashboard.sh
./start_dashboard.sh
```

## Project Structure

```
front_end/
├── btc_dashboard/
│   ├── __init__.py          # Package initialization
│   ├── btc_dashboard.py     # Main app and index page
│   ├── state.py             # State management (DashboardState)
│   ├── data_loader.py       # GCS data loading (DataLoader)
│   ├── charts.py            # Plotly chart generation
│   ├── components.py        # UI components (sidebar, metrics, etc.)
│   └── api.py               # API endpoints for charts
├── rxconfig.py              # Reflex configuration
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container configuration
└── start_dashboard.sh       # Start script
```

## Charts Included

1. **Price Chart**: OHLC candlestick chart
2. **Volume Analysis**: Total, buy, and sell volumes
3. **Advanced Metrics**: Volatility and order imbalance
4. **Trade Count**: Number of trades per minute
5. **Bid-Ask Spread & Depth**: Spread and order book depth
6. **Price Comparison**: VWAP, mid-price, and close price
7. **Order Flow**: CVD and OFI
8. **Market Quality**: Kyle's Lambda and liquidity health
9. **Micro-Price Deviation**: Deviation from fair value

## Metrics Displayed

- Current Price
- Volatility (with regime classification)
- Buy/Sell/Total Volume
- Trade Count
- Bid-Ask Spread
- CVD (Cumulative Volume Delta)
- OFI (Order Flow Imbalance)
- Kyle's Lambda (Price Impact)
- Liquidity Health
- VWAP, Mid-Price
- Micro-Price Deviation
- Order Imbalance Ratio

## Configuration

Edit `rxconfig.py` to change:
- App name
- API URL
- Port settings

## Docker

Build and run with Docker:

```bash
docker build -t btc-dashboard .
docker run -p 3000:3000 -p 8000:8000 -e GCP_SERVICE_ACCOUNT_JSON='...' btc-dashboard
```

## Notes

- All times are in Eastern Time (EST/EDT)
- Data is cached for 24 hours with incremental updates
- Full reload occurs every 2 hours to prevent memory leaks
- Memory usage is monitored and cache is cleared if > 100MB
