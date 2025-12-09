"""
Batch Layer: Coinbase WebSocket -> 1-Min Aggregation -> 60 rows per hour -> Parquet to GCS
Exact same ETL logic as data-pipeline/kafka_1min_aggregator.py

Source: Coinbase WebSocket (BTC-USD ticker)
Process: Aggregate every 1 minute (same as kafka_1min_aggregator.py)
Output: gs://batch-btc-1h-east1/{year}/{month}/{day}/btc_1h_{hour}.parquet
Format: 1 Parquet file per hour with 60 rows (1 row = 1 minute of data)

Airflow triggers write to GCS every hour

ALL TIMESTAMPS IN EASTERN TIME (EST/EDT)
"""

import json
import os
import math
import io
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
import logging
import time
import pytz
import asyncio
import websockets
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Eastern Time Zone
EASTERN = pytz.timezone('America/New_York')

# Coinbase WebSocket (same source as data-pipeline speed layer)
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

# GCS Configuration - Output to batch-btc-1h-east1 bucket (us-east1)
GCS_BUCKET = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')


class MinuteAggregator:
    """
    Aggregate Coinbase ticks into 1-minute candles
    EXACT SAME LOGIC as data-pipeline/kafka_1min_aggregator.py
    Stores 60 rows (60 minutes) before writing to GCS
    """
    
    def __init__(self):
        self.window_data = []  # 1-second ticks for current minute
        self.hourly_buffer = []  # 60 rows of 1-minute aggregated data
        self.current_minute = None
        self.current_hour = None
        self.window_start = None
        
    def _get_est_minute(self):
        """Get current minute rounded down in EST"""
        now_est = datetime.now(EASTERN)
        return now_est.replace(second=0, microsecond=0)
    
    def _get_est_hour(self):
        """Get current hour rounded down in EST"""
        now_est = datetime.now(EASTERN)
        return now_est.replace(minute=0, second=0, microsecond=0)
    
    def add_tick(self, tick_data):
        """
        Add a tick to the current 1-minute window
        Returns hourly data (60 rows) if hour is complete, None otherwise
        """
        current_minute = self._get_est_minute()
        current_hour = self._get_est_hour()
        
        # Initialize on first tick
        if self.current_minute is None:
            self.current_minute = current_minute
            self.current_hour = current_hour
            self.window_start = current_minute
            self.window_data = [tick_data]
            return None
        
        # New minute started - aggregate the completed minute
        if current_minute > self.current_minute:
            minute_agg = self._aggregate_minute()
            if minute_agg:
                self.hourly_buffer.append(minute_agg)
            
            # Check if new hour started - flush hourly buffer
            if current_hour > self.current_hour:
                result = self.hourly_buffer.copy() if len(self.hourly_buffer) > 0 else None
                self.hourly_buffer = []
                self.current_hour = current_hour
            else:
                result = None
            
            # Start new minute window
            self.current_minute = current_minute
            self.window_start = current_minute
            self.window_data = [tick_data]
            return result
        
        # Same minute - add to current window
        self.window_data.append(tick_data)
        return None
    
    def _aggregate_minute(self):
        """
        Calculate 1-minute aggregation from collected ticks
        EXACT SAME LOGIC as kafka_1min_aggregator.py
        """
        if not self.window_data:
            return None
        
        prices = [d['price'] for d in self.window_data if d.get('price', 0) > 0]
        if not prices:
            return None
        
        # OHLC
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        
        # Volume aggregation
        total_volume = float(sum(d.get('volume_1s', 0.0) for d in self.window_data))
        total_buy_volume = float(sum(d.get('buy_volume_1s', 0.0) for d in self.window_data))
        total_sell_volume = float(sum(d.get('sell_volume_1s', 0.0) for d in self.window_data))
        
        # Total trades
        total_trades = sum(d.get('trade_count_1s', 0) for d in self.window_data)
        
        # Pass-through (latest value)
        latest_data = self.window_data[-1]
        volume_24h = latest_data.get('volume_24h', 0)
        high_24h = latest_data.get('high_24h', 0)
        low_24h = latest_data.get('low_24h', 0)
        
        # VOLATILITY: Standard deviation of 1-second price returns
        price_returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                ret = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
                price_returns.append(ret)
        
        if price_returns and len(price_returns) > 1:
            mean_return = sum(price_returns) / len(price_returns)
            variance = sum((r - mean_return) ** 2 for r in price_returns) / len(price_returns)
            volatility_1m = math.sqrt(variance)
        else:
            volatility_1m = ((high_price - low_price) / open_price * 100) if open_price > 0 else 0
        
        # ORDER IMBALANCE RATIO: (BuyVol - SellVol) / (BuyVol + SellVol)
        total_volume_traded = total_buy_volume + total_sell_volume
        if total_volume_traded > 0:
            order_imbalance_ratio_1m = (total_buy_volume - total_sell_volume) / total_volume_traded
        else:
            order_imbalance_ratio_1m = 0.0
        
        # Price change
        price_change_1m = close_price - open_price
        price_change_percent_1m = (price_change_1m / open_price * 100) if open_price > 0 else 0.0
        
        # Average buy/sell ratio
        buy_sell_ratios = [d.get('buy_sell_ratio', 0) for d in self.window_data if d.get('buy_sell_ratio', 0) > 0]
        avg_buy_sell_ratio_1m = sum(buy_sell_ratios) / len(buy_sell_ratios) if buy_sell_ratios else 0.0
        
        # MICROSTRUCTURE METRICS (aggregated from 1s data)
        
        # 1. Bid-Ask Spread (average over 1 minute)
        spreads = [d.get('bid_ask_spread_1s', 0) for d in self.window_data if d.get('bid_ask_spread_1s', 0) > 0]
        avg_bid_ask_spread_1m = np.mean(spreads) if spreads else 0.0
        
        # 2. Order Book Depth 2% (average over 1 minute)
        depths = [d.get('depth_2pct_1s', 0) for d in self.window_data if d.get('depth_2pct_1s', 0) > 0]
        avg_depth_2pct_1m = np.mean(depths) if depths else 0.0
        
        bid_depths = [d.get('bid_depth_2pct_1s', 0) for d in self.window_data if d.get('bid_depth_2pct_1s', 0) > 0]
        avg_bid_depth_2pct_1m = np.mean(bid_depths) if bid_depths else 0.0
        
        ask_depths = [d.get('ask_depth_2pct_1s', 0) for d in self.window_data if d.get('ask_depth_2pct_1s', 0) > 0]
        avg_ask_depth_2pct_1m = np.mean(ask_depths) if ask_depths else 0.0
        
        # 3. VWAP (average over 1 minute)
        vwaps = [d.get('vwap_1s', 0) for d in self.window_data if d.get('vwap_1s', 0) > 0]
        avg_vwap_1m = np.mean(vwaps) if vwaps else 0.0
        
        # 4. Micro-Price Deviation (average over 1 minute)
        micro_deviations = [d.get('micro_price_deviation_1s', 0) for d in self.window_data]
        avg_micro_price_deviation_1m = np.mean(micro_deviations) if micro_deviations else 0.0
        
        # 5. CVD (Cumulative Volume Delta) - use latest value as it's cumulative
        cvd_1m = float(latest_data.get('cvd_1s', 0.0))
        
        # 6. Order Flow Imbalance (sum over 1 minute)
        ofi_values = [d.get('ofi_1s', 0.0) for d in self.window_data]
        total_ofi_1m = float(sum(ofi_values))
        avg_ofi_1m = np.mean(ofi_values) if ofi_values else 0.0
        
        # 7. Kyle's Lambda (average over 1 minute)
        lambdas = [d.get('kyles_lambda_1s', 0) for d in self.window_data if d.get('kyles_lambda_1s', 0) != 0]
        avg_kyles_lambda_1m = np.mean(lambdas) if lambdas else 0.0
        
        # 8. Liquidity Health (average over 1 minute)
        health_values = [d.get('liquidity_health_1s', 0) for d in self.window_data if d.get('liquidity_health_1s', 0) > 0]
        avg_liquidity_health_1m = np.mean(health_values) if health_values else 0.0
        
        # 9. Mid-Price (average over 1 minute)
        mid_prices = [d.get('mid_price_1s', 0) for d in self.window_data if d.get('mid_price_1s', 0) > 0]
        avg_mid_price_1m = np.mean(mid_prices) if mid_prices else 0.0
        
        # 10. Best Bid/Ask (latest values)
        latest_best_bid = float(latest_data.get('best_bid_1s', 0.0))
        latest_best_ask = float(latest_data.get('best_ask_1s', 0.0))
        
        # 11. Volatility Regime Classification
        if volatility_1m < 0.1:
            volatility_regime = 'low'
        elif volatility_1m < 0.5:
            volatility_regime = 'medium'
        else:
            volatility_regime = 'high'
        
        # Timestamps
        window_start_ms = int(self.window_start.timestamp() * 1000)
        window_end_ms = window_start_ms + 60000  # +60 seconds in milliseconds
        
        return {
            'symbol': latest_data.get('symbol', 'BTC-USD'),
            'window_start': window_start_ms,
            'window_end': window_end_ms,
            'window_start_est': self.window_start.strftime('%Y-%m-%d %H:%M:%S'),
            
            # OHLC
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            
            # Volume
            'total_volume_1m': total_volume,
            'total_buy_volume_1m': total_buy_volume,
            'total_sell_volume_1m': total_sell_volume,
            
            # Pass-through from source
            'volume_24h': volume_24h,
            'high_24h': high_24h,
            'low_24h': low_24h,
            
            # EXISTING FEATURES
            'volatility_1m': volatility_1m,
            'order_imbalance_ratio_1m': order_imbalance_ratio_1m,
            
            # Additional metrics
            'trade_count_1m': total_trades,
            'avg_buy_sell_ratio_1m': avg_buy_sell_ratio_1m,
            'price_change_1m': price_change_1m,
            'price_change_percent_1m': price_change_percent_1m,
            'num_ticks': len(self.window_data),
            'last_ingestion_time': latest_data.get('ingestion_time', datetime.now(EASTERN).isoformat()),
            
            # MICROSTRUCTURE METRICS (1m aggregated)
            'avg_bid_ask_spread_1m': avg_bid_ask_spread_1m,
            'avg_depth_2pct_1m': avg_depth_2pct_1m,
            'avg_bid_depth_2pct_1m': avg_bid_depth_2pct_1m,
            'avg_ask_depth_2pct_1m': avg_ask_depth_2pct_1m,
            'avg_vwap_1m': avg_vwap_1m,
            'avg_micro_price_deviation_1m': avg_micro_price_deviation_1m,
            'cvd_1m': cvd_1m,
            'total_ofi_1m': total_ofi_1m,
            'avg_ofi_1m': avg_ofi_1m,
            'avg_kyles_lambda_1m': avg_kyles_lambda_1m,
            'avg_liquidity_health_1m': avg_liquidity_health_1m,
            'avg_mid_price_1m': avg_mid_price_1m,
            'latest_best_bid_1m': latest_best_bid,
            'latest_best_ask_1m': latest_best_ask,
            'volatility_regime': volatility_regime,
        }


def write_parquet_to_gcs(hourly_rows: list, bucket_name: str):
    """Write 60 rows of 1-minute data as Parquet to GCS (triggered by Airflow every hour)"""
    if not hourly_rows or len(hourly_rows) == 0:
        logger.warning("No data to write")
        return False
    
    try:
        # Initialize GCS client with credentials from environment
        creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
        if creds_json:
            from google.oauth2 import service_account
            creds_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            client = storage.Client(credentials=credentials, project=creds_dict.get('project_id'))
        else:
            client = storage.Client()
        
        bucket = client.bucket(bucket_name)
        
        # Use first row's timestamp for file path
        first_row = hourly_rows[0]
        window_start_est = datetime.strptime(first_row['window_start_est'], '%Y-%m-%d %H:%M:%S')
        window_start_est = EASTERN.localize(window_start_est)
        
        # Path: {year}/{month}/{day}/btc_1h_{hour}_{timestamp}.parquet
        year = window_start_est.strftime('%Y')
        month = window_start_est.strftime('%m')
        day = window_start_est.strftime('%d')
        hour = window_start_est.strftime('%H')
        timestamp = datetime.now(EASTERN).strftime('%Y%m%d_%H%M%S')
        
        blob_path = f"{year}/{month}/{day}/btc_1h_{hour}_{timestamp}.parquet"
        
        # Convert list of dicts to PyArrow table (60 rows)
        table = pa.Table.from_pylist(hourly_rows)
        
        # Write to buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Upload to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        logger.info(
            f"✅ PARQUET: gs://{bucket_name}/{blob_path} | "
            f"{len(hourly_rows)} rows (1-min each) | "
            f"Hour: {hour}:00 EST"
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to write Parquet: {e}")
        return False


async def coinbase_stream():
    """
    Connect to Coinbase WebSocket and aggregate into 1-minute rows
    NOTE: This is not used directly - Airflow DAG runs the ETL
    """
    aggregator = MinuteAggregator()
    tick_count = 0
    minute_count = 0
    files_written = 0
    last_status = time.time()
    
    while True:
        try:
            logger.info(f"Connecting to Coinbase: {COINBASE_WS_URL}")
            
            async with websockets.connect(
                COINBASE_WS_URL,
                ping_interval=20,
                ping_timeout=20
            ) as ws:
                # Subscribe to ticker
                subscribe_msg = {
                    "type": "subscribe",
                    "product_ids": [PRODUCT_ID],
                    "channels": ["ticker"]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info(f"✅ Connected to Coinbase WebSocket - {PRODUCT_ID}")
                
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        
                        # Only process ticker messages
                        if data.get('type') != 'ticker':
                            continue
                        
                        # Parse Coinbase ticker format (same as data-pipeline)
                        tick = {
                            'price': float(data.get('price', 0)),
                            'volume_1s': float(data.get('last_size', 0)),
                            'buy_volume_1s': 0,  # Not available in ticker
                            'sell_volume_1s': 0,
                            'trade_count_1s': 1,
                            'volume_24h': float(data.get('volume_24h', 0)),
                            'high_24h': float(data.get('high_24h', 0)),
                            'low_24h': float(data.get('low_24h', 0)),
                            'trade_time': data.get('time', ''),
                            'symbol': PRODUCT_ID,
                            'ingestion_time': datetime.now(EASTERN).isoformat(),
                            # Microstructure metrics - set to 0 since not available in ticker
                            'bid_ask_spread_1s': 0,
                            'depth_2pct_1s': 0,
                            'bid_depth_2pct_1s': 0,
                            'ask_depth_2pct_1s': 0,
                            'vwap_1s': 0,
                            'micro_price_deviation_1s': 0,
                            'cvd_1s': 0,
                            'ofi_1s': 0,
                            'kyles_lambda_1s': 0,
                            'liquidity_health_1s': 0,
                            'mid_price_1s': 0,
                            'best_bid_1s': 0,
                            'best_ask_1s': 0,
                            'buy_sell_ratio': 0,
                        }
                        
                        tick_count += 1
                        
                        # Add to aggregator - returns 60 rows when hour is complete
                        hourly_data = aggregator.add_tick(tick)
                        
                        if hourly_data:
                            # Write 60 rows to GCS
                            if write_parquet_to_gcs(hourly_data, GCS_BUCKET):
                                files_written += 1
                                logger.info(f"📦 Wrote {len(hourly_data)} rows to GCS (Hour complete)")
                        
                        # Status log every 30s
                        if time.time() - last_status >= 30:
                            logger.info(
                                f"📊 Ticks: {tick_count} | "
                                f"Minute buffer: {len(aggregator.window_data)} | "
                                f"Hour buffer: {len(aggregator.hourly_buffer)}/60 | "
                                f"Files: {files_written}"
                            )
                            last_status = time.time()
                            
                    except asyncio.TimeoutError:
                        logger.debug("⏱️ Timeout, sending ping...")
                        await ws.ping()
                        
        except Exception as e:
            logger.error(f"❌ WebSocket error: {e}")
            logger.info("🔄 Reconnecting in 5s...")
            await asyncio.sleep(5)


def main():
    """
    This file is imported by Airflow DAG, not run standalone.
    Airflow DAG runs the ETL every hour.
    """
    logger.info("=" * 70)
    logger.info("BATCH LAYER: Coinbase WebSocket -> 1-Min Aggregation -> 60 rows/hour")
    logger.info("=" * 70)
    logger.info(f"Source: {COINBASE_WS_URL} ({PRODUCT_ID})")
    logger.info(f"Output: gs://{GCS_BUCKET}/{{year}}/{{month}}/{{day}}/btc_1h_{{hour}}.parquet")
    logger.info(f"Format: 1 Parquet file per hour with 60 rows (1 row = 1 minute)")
    logger.info(f"Features: Same as data-pipeline/kafka_1min_aggregator.py")
    logger.info(f"Orchestration: Airflow DAG (0 * * * *)")
    logger.info("=" * 70)
    logger.info("Use Airflow to run this ETL, not standalone!")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()
