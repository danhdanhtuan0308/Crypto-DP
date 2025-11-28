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
            
            # NEW FEATURES
            'volatility_1m': volatility_1m,
            'order_imbalance_ratio_1m': order_imbalance_ratio_1m,
            
            # Additional metrics
            'trade_count_1m': total_trades,
            'avg_buy_sell_ratio_1m': avg_buy_sell_ratio_1m,
            'price_change_1m': price_change_1m,
            'price_change_percent_1m': price_change_percent_1m,
            'num_ticks': len(self.window_data),
            'last_ingestion_time': latest_data.get('ingestion_time', datetime.now(EASTERN).isoformat())
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
                            f"Aggregated minute {aggregator.message_count} | "
                            f"EST: {agg_msg['window_start_est']} | "
                            f"OHLC: {agg_msg['open']:.2f}/{agg_msg['high']:.2f}/"
                            f"{agg_msg['low']:.2f}/{agg_msg['close']:.2f} | "
                            f"Vol: {agg_msg['total_volume_1m']:.4f} | "
                            f"Volatility: {agg_msg['volatility_1m']:.4f} | "
                            f"OIR: {agg_msg['order_imbalance_ratio_1m']:.4f}"
                        )
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
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
                        f"Aggregated minute {aggregator.message_count} | "
                        f"EST: {agg_msg['window_start_est']} | "
                        f"OHLC: {agg_msg['open']:.2f}/{agg_msg['high']:.2f}/"
                        f"{agg_msg['low']:.2f}/{agg_msg['close']:.2f} | "
                        f"Vol: {agg_msg['total_volume_1m']:.4f} | "
                        f"Volatility: {agg_msg['volatility_1m']:.4f} | "
                        f"OIR: {agg_msg['order_imbalance_ratio_1m']:.4f}"
                    )
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                continue
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    
    finally:
        consumer.close()
        producer.flush()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
