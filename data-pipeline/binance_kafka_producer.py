import asyncio
import json
import logging
import time
from collections import deque
import websockets
from confluent_kafka import Producer
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

#Setting up Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_KAFKA_API_KEY_GCS'),
    'sasl.password': os.getenv('CONFLUENT_KAFKA_API_KEY_SECRET_GCS'),
    'client.id': 'binance-ltc-producer',
}

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'BTC-USD')

# Try Binance.US if you're in the US, otherwise use a VPN or alternative exchange
BINANCE_WS_URL = "wss://stream.binance.us:9443/stream?streams=btcusdt@ticker/btcusdt@depth20@1000ms/btcusdt@trade"


def delivery_callback(err, msg):
    if err:
        logger.error(f'Delivery failed: {err}')

# Calculate order book metrics
def calculate_order_book_metrics(bids, asks):
    total_bid_volume = sum(float(bid[1]) for bid in bids[:10])
    total_ask_volume = sum(float(ask[1]) for ask in asks[:10])
    
    best_bid_price = float(bids[0][0]) if bids else 0
    best_ask_price = float(asks[0][0]) if asks else 0
    
    spread = best_ask_price - best_bid_price if best_bid_price and best_ask_price else 0
    spread_percent = (spread / best_bid_price * 100) if best_bid_price else 0
    buy_sell_ratio = total_bid_volume / total_ask_volume if total_ask_volume > 0 else 0
    
    return {
        'best_bid_price': best_bid_price,
        'best_ask_price': best_ask_price,
        'bid_ask_spread': spread,
        'spread_percent': spread_percent,
        'total_bid_volume': total_bid_volume,
        'total_ask_volume': total_ask_volume,
        'buy_sell_ratio': buy_sell_ratio,
        'top_5_bids': [[float(b[0]), float(b[1])] for b in bids[:5]],
        'top_5_asks': [[float(a[0]), float(a[1])] for a in asks[:5]]
    }

# Calculate rolling metrics over different time windows
def calculate_rolling_metrics(price_history):
    current_time = time.time()
    windows = {'1s': 1, '1min': 60, '1h': 3600, '24h': 86400}
    metrics = {}
    
    for window_name, window_seconds in windows.items():
        window_data = [d for d in price_history if current_time - d['timestamp'] <= window_seconds]
        
        if not window_data:
            metrics[window_name] = {
                'high': 0, 'low': 0, 'price_change': 0, 'price_change_percent': 0,
                'volume': 0, 'buy_volume': 0, 'sell_volume': 0
            }
            continue
        
        prices = [d['price'] for d in window_data]
        first_price = window_data[0]['price']
        last_price = window_data[-1]['price']
        price_change = last_price - first_price
        
        metrics[window_name] = {
            'high': max(prices),
            'low': min(prices),
            'price_change': price_change,
            'price_change_percent': (price_change / first_price * 100) if first_price > 0 else 0,
            'volume': sum(d.get('buy_vol', 0) + d.get('sell_vol', 0) for d in window_data),
            'buy_volume': sum(d.get('buy_vol', 0) for d in window_data),
            'sell_volume': sum(d.get('sell_vol', 0) for d in window_data)
        }
    
    return metrics


async def stream_binance_to_kafka():
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"Starting producer → Topic: {KAFKA_TOPIC}")
    
    latest_ticker = {}
    latest_depth = {}
    latest_price = 0.0
    last_send_time = 0
    buy_volume_1s = 0.0
    sell_volume_1s = 0.0
    trade_count_1s = 0
    price_history = deque(maxlen=86400)
    
    try:
        async with websockets.connect(BINANCE_WS_URL) as websocket:
            message_count = 0
            
            while True:
                raw_message = await websocket.recv()
                message = json.loads(raw_message)
                stream_name = message.get('stream', '')
                data = message.get('data', message)
                
                if 'ticker' in stream_name or data.get('e') == '24hrTicker':
                    latest_ticker = {
                        'symbol': data.get('s'),
                        'price': float(data.get('c', 0)),
                        'timestamp': data.get('E'),
                        'volume_24h': float(data.get('v', 0)),
                        'quote_volume_24h': float(data.get('q', 0)),
                        'high_24h': float(data.get('h', 0)),
                        'low_24h': float(data.get('l', 0)),
                        'price_change_24h': float(data.get('p', 0)),
                        'price_change_percent_24h': float(data.get('P', 0)),
                        'number_of_trades': data.get('n', 0),
                        'buy_volume_24h': float(data.get('v', 0)),
                    }
                
                elif 'depth' in stream_name or 'bids' in data:
                    latest_depth = {
                        'bids': data.get('bids', []),
                        'asks': data.get('asks', []),
                        'last_update_id': data.get('lastUpdateId', 0)
                    }
                
                elif 'trade' in stream_name or data.get('e') == 'trade':
                    trade_quantity = float(data.get('q', 0))
                    is_buyer_maker = data.get('m', False)
                    latest_price = float(data.get('p', 0))  # Get real-time price from trade
                    
                    if not is_buyer_maker:
                        buy_volume_1s += trade_quantity
                    else:
                        sell_volume_1s += trade_quantity
                    
                    trade_count_1s += 1
                
                current_time = time.time()
                # Send every 1 second if we have ticker data (even without new trades)
                if latest_ticker and (current_time - last_send_time >= 1.0):
                    # Calculate order book metrics first to get current market price
                    # Calculate order book metrics (use empty if not available yet)
                    if latest_depth:
                        order_metrics = calculate_order_book_metrics(latest_depth['bids'], latest_depth['asks'])
                    else:
                        order_metrics = {
                            'best_bid_price': 0, 'best_ask_price': 0, 'bid_ask_spread': 0,
                            'spread_percent': 0, 'total_bid_volume': 0, 'total_ask_volume': 0,
                            'buy_sell_ratio': 0, 'top_5_bids': [], 'top_5_asks': []
                        }
                    
                    # Always use order book mid-price for most current market price
                    # This updates every second via depth stream, more accurate than infrequent trades
                    if order_metrics['best_bid_price'] > 0 and order_metrics['best_ask_price'] > 0:
                        current_price = (order_metrics['best_bid_price'] + order_metrics['best_ask_price']) / 2
                    elif latest_price > 0:
                        # Fallback to latest trade price if order book unavailable
                        current_price = latest_price
                    else:
                        # Final fallback to 24hr ticker
                        current_price = latest_ticker.get('price', 0)
                    
                    total_volume_1s = buy_volume_1s + sell_volume_1s
                    buy_sell_volume_ratio = buy_volume_1s / sell_volume_1s if sell_volume_1s > 0 else 0
                    
                    price_history.append({
                        'timestamp': current_time,
                        'price': current_price,
                        'buy_vol': buy_volume_1s,
                        'sell_vol': sell_volume_1s
                    })
                    
                    rolling_metrics = calculate_rolling_metrics(price_history)
                    # Construct Kafka message with current price
                    kafka_message = {
                        **latest_ticker,
                        'price': current_price,  # Override ticker price with real-time price
                        **order_metrics,
                        'high_1s': rolling_metrics['1s']['high'],
                        'low_1s': rolling_metrics['1s']['low'],
                        'price_change_1s': rolling_metrics['1s']['price_change'],
                        'price_change_percent_1s': rolling_metrics['1s']['price_change_percent'],
                        'volume_1s': total_volume_1s,
                        'buy_volume_1s': buy_volume_1s,
                        'sell_volume_1s': sell_volume_1s,
                        'buy_sell_volume_ratio_1s': buy_sell_volume_ratio,
                        'trade_count_1s': trade_count_1s,
                        'high_1min': rolling_metrics['1min']['high'],
                        'low_1min': rolling_metrics['1min']['low'],
                        'price_change_1min': rolling_metrics['1min']['price_change'],
                        'price_change_percent_1min': rolling_metrics['1min']['price_change_percent'],
                        'volume_1min': rolling_metrics['1min']['volume'],
                        'buy_volume_1min': rolling_metrics['1min']['buy_volume'],
                        'sell_volume_1min': rolling_metrics['1min']['sell_volume'],
                        'high_1h': rolling_metrics['1h']['high'],
                        'low_1h': rolling_metrics['1h']['low'],
                        'price_change_1h': rolling_metrics['1h']['price_change'],
                        'price_change_percent_1h': rolling_metrics['1h']['price_change_percent'],
                        'volume_1h': rolling_metrics['1h']['volume'],
                        'buy_volume_1h': rolling_metrics['1h']['buy_volume'],
                        'sell_volume_1h': rolling_metrics['1h']['sell_volume'],
                        'volume_24h_calculated': rolling_metrics['24h']['volume'],
                        'buy_volume_24h_calculated': rolling_metrics['24h']['buy_volume'],
                        'sell_volume_24h_calculated': rolling_metrics['24h']['sell_volume'],
                        'ingestion_time': datetime.now(timezone.utc).isoformat()
                    }
                    
                    buy_volume_1s = 0.0
                    sell_volume_1s = 0.0
                    trade_count_1s = 0
                    
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=kafka_message['symbol'],
                        value=json.dumps(kafka_message),
                        callback=delivery_callback
                    )
                    producer.poll(0)
                    
                    message_count += 1
                    last_send_time = current_time
                    
                    if message_count % 10 == 0:
                        logger.info(
                            f"Sent {message_count} | Price: ${kafka_message['price']:.2f} | "
                            f"Vol: {kafka_message['volume_1s']:.2f} | Buy/Sell: {kafka_message['buy_sell_volume_ratio_1s']:.2f}"
                        )
                    
                    await asyncio.sleep(0.05)
                
    except websockets.exceptions.ConnectionClosed:
        logger.warning("Connection closed. Reconnecting in 5s...")
        await asyncio.sleep(5)
        await stream_binance_to_kafka()
        
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        
    finally:
        producer.flush()
        logger.info("Shutdown complete")

# Entry point
def main():
    required_vars = ['CONFLUENT_KAFKA_BOOTSTRAP_SERVERS', 'CONFLUENT_KAFKA_API_KEY_GCS', 'CONFLUENT_KAFKA_API_KEY_SECRET_GCS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing env vars: {', '.join(missing_vars)}")
        return
    
    asyncio.run(stream_binance_to_kafka())


if __name__ == "__main__":
    main()
