"""
Batch Layer: Coinbase WebSocket -> 1-Hour Aggregation -> Parquet to GCS
Same ETL logic as data-pipeline (kafka_1min_aggregator.py)

Source: Coinbase WebSocket (BTC-USD ticker)
Output: gs://batch-btc-1h/{year}/{month}/{day}/btc_1h_{hour}.parquet

Production: 1 Parquet file per hour

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


class HourAggregator:
    """
    Aggregate Coinbase trades into 1-hour OHLCV candles
    Same logic as kafka_1min_aggregator.py but for 1-hour windows
    """
    
    def __init__(self):
        self.window_data = []
        self.current_hour = None
        self.window_start = None
        
    def _get_est_hour(self):
        """Get current hour rounded down in EST"""
        now_est = datetime.now(EASTERN)
        return now_est.replace(minute=0, second=0, microsecond=0)
    
    def add_trade(self, trade_data):
        """
        Add a trade to the current window
        Returns aggregated data if hour is complete, None otherwise
        """
        current_hour = self._get_est_hour()
        
        # Initialize on first trade
        if self.current_hour is None:
            self.current_hour = current_hour
            self.window_start = current_hour
            self.window_data = [trade_data]
            return None
        
        # New hour started - flush previous hour and start new window
        if current_hour > self.current_hour:
            result = self._flush()
            self.current_hour = current_hour
            self.window_start = current_hour
            self.window_data = [trade_data]
            return result
        
        # Same hour - add to current window
        self.window_data.append(trade_data)
        return None
    
    def _flush(self):
        """
        Calculate 1-hour aggregation from collected trades
        Same logic as kafka_1min_aggregator.py but for 1-hour windows
        """
        if not self.window_data:
            return None
        
        prices = [t['price'] for t in self.window_data if t.get('price', 0) > 0]
        if not prices:
            return None
        
        # OHLC
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        
        # Volume aggregation
        total_volume = sum(t.get('quantity', 0) for t in self.window_data)
        buy_volume = sum(t.get('quantity', 0) for t in self.window_data if not t.get('is_buyer_maker', True))
        sell_volume = sum(t.get('quantity', 0) for t in self.window_data if t.get('is_buyer_maker', True))
        
        # Trade count
        trade_count = len(self.window_data)
        
        # VOLATILITY: Standard deviation of price returns
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
        
        # ORDER IMBALANCE RATIO
        total_traded = buy_volume + sell_volume
        order_imbalance = (buy_volume - sell_volume) / total_traded if total_traded > 0 else 0
        
        # Price change
        price_change = close_price - open_price
        price_change_pct = (price_change / open_price * 100) if open_price > 0 else 0
        
        # Average buy/sell ratio
        buy_sell_ratios = [t.get('buy_sell_ratio', 0) for t in self.window_data if t.get('buy_sell_ratio', 0) > 0]
        avg_buy_sell_ratio_1m = sum(buy_sell_ratios) / len(buy_sell_ratios) if buy_sell_ratios else 0.0
        
        # VWAP
        total_value = sum(t.get('price', 0) * t.get('quantity', 0) for t in self.window_data)
        vwap = total_value / total_volume if total_volume > 0 else close_price
        
        # Pass-through (latest value)
        latest_data = self.window_data[-1] if self.window_data else {}
        volume_24h = latest_data.get('volume_24h', 0)
        high_24h = latest_data.get('high_24h', 0)
        low_24h = latest_data.get('low_24h', 0)
        
        # MICROSTRUCTURE METRICS (aggregated from 1s data)
        import numpy as np
        
        # 1. Bid-Ask Spread (average over 1 minute)
        spreads = [t.get('bid_ask_spread_1s', 0) for t in self.window_data if t.get('bid_ask_spread_1s', 0) > 0]
        avg_bid_ask_spread_1m = np.mean(spreads) if spreads else 0
        
        # 2. Order Book Depth 2% (average over 1 minute)
        depths = [t.get('depth_2pct_1s', 0) for t in self.window_data if t.get('depth_2pct_1s', 0) > 0]
        avg_depth_2pct_1m = np.mean(depths) if depths else 0
        
        bid_depths = [t.get('bid_depth_2pct_1s', 0) for t in self.window_data if t.get('bid_depth_2pct_1s', 0) > 0]
        avg_bid_depth_2pct_1m = np.mean(bid_depths) if bid_depths else 0
        
        ask_depths = [t.get('ask_depth_2pct_1s', 0) for t in self.window_data if t.get('ask_depth_2pct_1s', 0) > 0]
        avg_ask_depth_2pct_1m = np.mean(ask_depths) if ask_depths else 0
        
        # 3. VWAP (average over 1 minute)
        vwaps = [t.get('vwap_1s', 0) for t in self.window_data if t.get('vwap_1s', 0) > 0]
        avg_vwap_1m = np.mean(vwaps) if vwaps else 0
        
        # 4. Micro-Price Deviation (average over 1 minute)
        micro_deviations = [t.get('micro_price_deviation_1s', 0) for t in self.window_data]
        avg_micro_price_deviation_1m = np.mean(micro_deviations) if micro_deviations else 0
        
        # 5. CVD (Cumulative Volume Delta) - use latest value as it's cumulative
        cvd_1m = latest_data.get('cvd_1s', 0)
        
        # 6. Order Flow Imbalance (sum over 1 minute)
        ofi_values = [t.get('ofi_1s', 0) for t in self.window_data]
        total_ofi_1m = sum(ofi_values)
        avg_ofi_1m = np.mean(ofi_values) if ofi_values else 0
        
        # 7. Kyle's Lambda (average over 1 minute)
        lambdas = [t.get('kyles_lambda_1s', 0) for t in self.window_data if t.get('kyles_lambda_1s', 0) != 0]
        avg_kyles_lambda_1m = np.mean(lambdas) if lambdas else 0
        
        # 8. Liquidity Health (average over 1 minute)
        health_values = [t.get('liquidity_health_1s', 0) for t in self.window_data if t.get('liquidity_health_1s', 0) > 0]
        avg_liquidity_health_1m = np.mean(health_values) if health_values else 0
        
        # 9. Mid-Price (average over 1 minute)
        mid_prices = [t.get('mid_price_1s', 0) for t in self.window_data if t.get('mid_price_1s', 0) > 0]
        avg_mid_price_1m = np.mean(mid_prices) if mid_prices else 0
        
        # 10. Best Bid/Ask (latest values)
        latest_best_bid = latest_data.get('best_bid_1s', 0)
        latest_best_ask = latest_data.get('best_ask_1s', 0)
        
        # 11. Volatility Regime Classification
        if volatility_1m < 0.1:
            volatility_regime = 'low'
        elif volatility_1m < 0.5:
            volatility_regime = 'medium'
        else:
            volatility_regime = 'high'
        
        # Timestamps
        window_start_ms = int(self.window_start.timestamp() * 1000)
        window_end_ms = window_start_ms + 3600000  # +1 hour in milliseconds
        
        return {
            'symbol': 'BTC-USD',
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
            'total_buy_volume_1m': buy_volume,
            'total_sell_volume_1m': sell_volume,
            
            # Pass-through from source
            'volume_24h': volume_24h,
            'high_24h': high_24h,
            'low_24h': low_24h,
            
            # EXISTING FEATURES
            'volatility_1m': volatility_1m,
            'order_imbalance_ratio_1m': order_imbalance,
            
            # Additional metrics
            'trade_count_1m': trade_count,
            'avg_buy_sell_ratio_1m': avg_buy_sell_ratio_1m,
            'price_change_1m': price_change,
            'price_change_percent_1m': price_change_pct,
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
            
            # Metadata
            'batch_processed': True,
            'batch_processing_time': datetime.now(EASTERN).isoformat(),
        }


def write_parquet_to_gcs(data: dict, bucket_name: str):
    """Write aggregated 1-hour data as Parquet to GCS"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # Parse timestamp for path
        window_start_est = datetime.strptime(data['window_start_est'], '%Y-%m-%d %H:%M:%S')
        window_start_est = EASTERN.localize(window_start_est)
        
        # Path: {year}/{month}/{day}/btc_1h_{hour}.parquet
        year = window_start_est.strftime('%Y')
        month = window_start_est.strftime('%m')
        day = window_start_est.strftime('%d')
        hour = window_start_est.strftime('%H')
        
        blob_path = f"{year}/{month}/{day}/btc_1h_{hour}.parquet"
        
        # Convert to PyArrow table
        table = pa.Table.from_pydict({k: [v] for k, v in data.items()})
        
        # Write to buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Upload to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        logger.info(
            f"PARQUET: gs://{bucket_name}/{blob_path} | "
            f"O:{data['open']:.2f} H:{data['high']:.2f} L:{data['low']:.2f} C:{data['close']:.2f} | "
            f"Vol:{data['total_volume_1m']:.4f} | Trades:{data['trade_count_1m']} | "
            f"EST: {data['window_start_est']}"
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to write Parquet: {e}")
        return False


async def coinbase_stream():
    """Connect to Coinbase WebSocket and aggregate trades into 1-hour Parquet files"""
    aggregator = HourAggregator()
    trade_count = 0
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
                logger.info(f"Connected to Coinbase WebSocket - subscribed to {PRODUCT_ID}")
                
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        data = json.loads(msg)
                        
                        # Only process ticker messages
                        if data.get('type') != 'ticker':
                            continue
                        
                        # Parse Coinbase ticker format (same as data-pipeline)
                        trade = {
                            'price': float(data.get('price', 0)),
                            'quantity': float(data.get('volume_24h', 0)),
                            'is_buyer_maker': False,  # Not available in ticker
                            'trade_time': data.get('time', ''),
                        }
                        
                        trade_count += 1
                        
                        # Add to aggregator - returns data when minute is complete
                        agg_result = aggregator.add_trade(trade)
                        
                        if agg_result:
                            if write_parquet_to_gcs(agg_result, GCS_BUCKET):
                                files_written += 1
                        
                        # Status log every 30s
                        if time.time() - last_status >= 30:
                            logger.info(
                                f"Status | Trades: {trade_count} | "
                                f"Buffer: {len(aggregator.window_data)} | "
                                f"Parquet files: {files_written}"
                            )
                            last_status = time.time()
                            
                    except asyncio.TimeoutError:
                        logger.debug("Timeout, sending ping...")
                        await ws.ping()
                        
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            logger.info("Reconnecting in 5s...")
            await asyncio.sleep(5)


def main():
    logger.info("=" * 70)
    logger.info("BATCH LAYER: Coinbase WebSocket -> 1-Hour Aggregation -> Parquet")
    logger.info("=" * 70)
    logger.info(f"Source: Coinbase WebSocket ({COINBASE_WS_URL})")
    logger.info(f"Output: gs://{GCS_BUCKET}/{{year}}/{{month}}/{{day}}/btc_1h_{{hour}}.parquet")
    logger.info(f"Interval: 1 hour (production mode)")
    logger.info("=" * 70)
    
    asyncio.run(coinbase_stream())


if __name__ == '__main__':
    main()
