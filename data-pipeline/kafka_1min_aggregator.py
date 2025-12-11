"""
1-Minute Aggregation Consumer
Reads from BTC-USD topic (1 msg/sec) and aggregates into 1-minute windows
Produces to btc_1min_agg topic (1 msg/min)
ALL TIMESTAMPS IN EASTERN TIME (EST/EDT)
"""

import json
import time
import math
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer
import os
from dotenv import load_dotenv
import logging
import pytz
import numpy as np

# Discord monitoring
try:
    from alerts import get_discord_alert, get_health_monitor
    MONITORING_ENABLED = True
except ImportError:
    MONITORING_ENABLED = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Eastern Time Zone - ALL timestamps will be in EST
EASTERN = pytz.timezone('America/New_York')

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_KAFKA_API_KEY_GCS'),
    'sasl.password': os.getenv('CONFLUENT_KAFKA_API_KEY_SECRET_GCS'),
}

CONSUMER_CONFIG = {
    **KAFKA_CONFIG,
    'group.id': 'btc-1min-aggregator',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
}

PRODUCER_CONFIG = {
    **KAFKA_CONFIG,
    'client.id': 'btc-1min-producer',
}

SOURCE_TOPIC = 'BTC-USD'
OUTPUT_TOPIC = 'btc_1min_agg'


def delivery_callback(err, msg):
    if err:
        logger.error(f'Delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()}')


class MinuteAggregator:
    def __init__(self):
        self.window_data = deque(maxlen=60)  # Store up to 60 seconds
        self.window_start = None  # Will store EST datetime
        self.message_count = 0
    
    def _get_est_minute(self):
        """Get current minute rounded down in EST"""
        now_est = datetime.now(EASTERN)
        # Round down to minute (remove seconds and microseconds)
        return now_est.replace(second=0, microsecond=0)
        
    def add_tick(self, data):
        """Add a 1-second tick to the current window"""
        current_minute_est = self._get_est_minute()
        
        # Initialize window start
        if self.window_start is None:
            self.window_start = current_minute_est
        
        # Check if we need to flush (new minute started)
        if current_minute_est > self.window_start:
            return self.flush()
        
        # Add to current window
        self.window_data.append(data)
        return None
    
    def flush(self):
        """Calculate aggregated metrics for the completed minute"""
        if not self.window_data:
            return None
        
        # Extract price data
        prices = [d['price'] for d in self.window_data if d.get('price', 0) > 0]
        
        if not prices:
            return None
        
        # OHLC
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        
        # Volume aggregation
        total_volume = sum(d.get('volume_1s', 0) for d in self.window_data)
        total_buy_volume = sum(d.get('buy_volume_1s', 0) for d in self.window_data)
        total_sell_volume = sum(d.get('sell_volume_1s', 0) for d in self.window_data)
        
        # Total trades
        total_trades = sum(d.get('trade_count_1s', 0) for d in self.window_data)
        
        # Pass-through (latest value)
        latest_data = self.window_data[-1]
        volume_24h = latest_data.get('volume_24h', 0)
        high_24h = latest_data.get('high_24h', 0)
        low_24h = latest_data.get('low_24h', 0)
        
        # VOLATILITY: Standard deviation of 1-second price returns
        # Calculate price returns (percentage change between consecutive prices)
        price_returns = []
        for i in range(1, len(prices)):
            if prices[i-1] > 0:
                ret = ((prices[i] - prices[i-1]) / prices[i-1]) * 100
                price_returns.append(ret)
        
        # Volatility = standard deviation of returns
        if price_returns and len(price_returns) > 1:
            mean_return = sum(price_returns) / len(price_returns)
            variance = sum((r - mean_return) ** 2 for r in price_returns) / len(price_returns)
            volatility_1m = math.sqrt(variance)
        else:
            # Fallback: use high-low range as volatility proxy
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

        
        # 1. Bid-Ask Spread (average over 1 minute)
        spreads = [d.get('bid_ask_spread_1s', 0) for d in self.window_data if d.get('bid_ask_spread_1s', 0) > 0]
        avg_bid_ask_spread_1m = np.mean(spreads) if spreads else 0
        
        # 2. Order Book Depth 2% (average over 1 minute)
        depths = [d.get('depth_2pct_1s', 0) for d in self.window_data if d.get('depth_2pct_1s', 0) > 0]
        avg_depth_2pct_1m = np.mean(depths) if depths else 0
        
        bid_depths = [d.get('bid_depth_2pct_1s', 0) for d in self.window_data if d.get('bid_depth_2pct_1s', 0) > 0]
        avg_bid_depth_2pct_1m = np.mean(bid_depths) if bid_depths else 0
        
        ask_depths = [d.get('ask_depth_2pct_1s', 0) for d in self.window_data if d.get('ask_depth_2pct_1s', 0) > 0]
        avg_ask_depth_2pct_1m = np.mean(ask_depths) if ask_depths else 0
        
        # 3. VWAP (average over 1 minute)
        vwaps = [d.get('vwap_1s', 0) for d in self.window_data if d.get('vwap_1s', 0) > 0]
        avg_vwap_1m = np.mean(vwaps) if vwaps else 0
        
        # 4. Micro-Price Deviation (average over 1 minute)
        micro_deviations = [d.get('micro_price_deviation_1s', 0) for d in self.window_data]
        avg_micro_price_deviation_1m = np.mean(micro_deviations) if micro_deviations else 0
        
        # 5. CVD (Cumulative Volume Delta) - use latest value as it's cumulative
        cvd_1m = latest_data.get('cvd_1s', 0)
        
        # 6. Order Flow Imbalance (sum over 1 minute)
        ofi_values = [d.get('ofi_1s', 0) for d in self.window_data]
        total_ofi_1m = sum(ofi_values)
        avg_ofi_1m = np.mean(ofi_values) if ofi_values else 0
        
        # 7. Kyle's Lambda (average over 1 minute)
        lambdas = [d.get('kyles_lambda_1s', 0) for d in self.window_data if d.get('kyles_lambda_1s', 0) != 0]
        avg_kyles_lambda_1m = np.mean(lambdas) if lambdas else 0
        
        # 8. Liquidity Health (average over 1 minute)
        health_values = [d.get('liquidity_health_1s', 0) for d in self.window_data if d.get('liquidity_health_1s', 0) > 0]
        avg_liquidity_health_1m = np.mean(health_values) if health_values else 0
        
        # 9. Mid-Price (average over 1 minute)
        mid_prices = [d.get('mid_price_1s', 0) for d in self.window_data if d.get('mid_price_1s', 0) > 0]
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
        
        # Build aggregated message - Convert EST datetime to UTC timestamp
        # self.window_start is already EST-aware, .timestamp() gives us UTC epoch correctly
        window_start_ms = int(self.window_start.timestamp() * 1000)
        window_end_ms = window_start_ms + 60000  # +60 seconds in ms
        
        agg_message = {
            'symbol': latest_data.get('symbol', 'BTC-USD'),
            'window_start': window_start_ms,  # EST timestamp in milliseconds
            'window_end': window_end_ms,
            'window_start_est': self.window_start.strftime('%Y-%m-%d %H:%M:%S'),  # Human-readable EST
            
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
            
            # ===============================
            # NEW MICROSTRUCTURE METRICS (1m aggregated)
            # ===============================
            'avg_bid_ask_spread_1m': avg_bid_ask_spread_1m,
            'avg_depth_2pct_1m': avg_depth_2pct_1m,
            'avg_bid_depth_2pct_1m': avg_bid_depth_2pct_1m,
            'avg_ask_depth_2pct_1m': avg_ask_depth_2pct_1m,
            'avg_vwap_1m': avg_vwap_1m,
            'avg_micro_price_deviation_1m': avg_micro_price_deviation_1m,
            'cvd_1m': cvd_1m,  # Cumulative (latest value)
            'total_ofi_1m': total_ofi_1m,  # Sum of OFI over 1 minute
            'avg_ofi_1m': avg_ofi_1m,  # Average OFI
            'avg_kyles_lambda_1m': avg_kyles_lambda_1m,
            'avg_liquidity_health_1m': avg_liquidity_health_1m,
            'avg_mid_price_1m': avg_mid_price_1m,
            'latest_best_bid_1m': latest_best_bid,
            'latest_best_ask_1m': latest_best_ask,
            'volatility_regime': volatility_regime,
        }
        
        # Reset for next window
        self.window_data.clear()
        self.window_start = self._get_est_minute()
        self.message_count += 1
        
        return agg_message


def main():
    logger.info("Starting 1-minute aggregation consumer")
    logger.info(f"Source: {SOURCE_TOPIC} (1 msg/sec)")
    logger.info(f"Output: {OUTPUT_TOPIC} (1 msg/min)")
    
    # Send startup alert
    if MONITORING_ENABLED:
        alert = get_discord_alert()
        alert.aggregator_started(SOURCE_TOPIC, OUTPUT_TOPIC)
        monitor = get_health_monitor()
    
    consumer = Consumer(CONSUMER_CONFIG)
    producer = Producer(PRODUCER_CONFIG)
    aggregator = MinuteAggregator()
    
    consumer.subscribe([SOURCE_TOPIC])
    logger.info(f"Subscribed to {SOURCE_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Check if we need to flush due to timeout (new minute started)
                current_minute_est = aggregator._get_est_minute()
                if aggregator.window_start and current_minute_est > aggregator.window_start:
                    agg_msg = aggregator.flush()
                    if agg_msg:
                        producer.produce(
                            OUTPUT_TOPIC,
                            key=agg_msg['symbol'],
                            value=json.dumps(agg_msg),
                            callback=delivery_callback
                        )
                        producer.poll(0)
                        logger.info(
                            f"Agg #{aggregator.message_count} (timeout) | "
                            f"EST: {agg_msg['window_start_est']} | "
                            f"OHLC: {agg_msg['open']:.2f}/{agg_msg['high']:.2f}/"
                            f"{agg_msg['low']:.2f}/{agg_msg['close']:.2f} | "
                            f"Vol: {agg_msg['total_volume_1m']:.4f} | "
                            f"Spread: ${agg_msg['avg_bid_ask_spread_1m']:.2f} | "
                            f"Depth: {agg_msg['avg_depth_2pct_1m']:.2f}"
                        )
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Track message for monitoring
                if MONITORING_ENABLED:
                    monitor.track_consumer_message(SOURCE_TOPIC)
                
                # Add to aggregator
                agg_msg = aggregator.add_tick(data)
                
                # If window is complete, send aggregated message
                if agg_msg:
                    producer.produce(
                        OUTPUT_TOPIC,
                        key=agg_msg['symbol'],
                        value=json.dumps(agg_msg),
                        callback=delivery_callback
                    )
                    producer.poll(0)
                    logger.info(
                        f"Agg #{aggregator.message_count} | "
                        f"EST: {agg_msg['window_start_est']} | "
                        f"OHLC: {agg_msg['open']:.2f}/{agg_msg['high']:.2f}/"
                        f"{agg_msg['low']:.2f}/{agg_msg['close']:.2f} | "
                        f"Vol: {agg_msg['total_volume_1m']:.4f} | "
                        f"Spread: ${agg_msg['avg_bid_ask_spread_1m']:.2f} | "
                        f"Depth: {agg_msg['avg_depth_2pct_1m']:.2f} | "
                        f"Vol: {agg_msg['volatility_1m']:.4f}% | "
                        f"λ: {agg_msg['avg_kyles_lambda_1m']:.6f}"
                    )
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                if MONITORING_ENABLED:
                    monitor.report_consumer_error(SOURCE_TOPIC, f"JSON decode error: {str(e)}")
                continue
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if MONITORING_ENABLED:
            alert = get_discord_alert()
            alert.aggregator_failed(str(e))
        raise
    
    finally:
        consumer.close()
        producer.flush()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
