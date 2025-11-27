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

# Setting up Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_KAFKA_API_KEY_GCS'),
    'sasl.password': os.getenv('CONFLUENT_KAFKA_API_KEY_SECRET_GCS'),
    'client.id': 'coinbase-btc-producer',
}

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'BTC-USD')

# Coinbase WebSocket URL (works from US without VPN)
COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

# Health check settings
STALE_DATA_THRESHOLD = 30  # seconds without price change = stale
HEALTH_CHECK_INTERVAL = 10  # check every 10 seconds


def delivery_callback(err, msg):
    if err:
        logger.error(f'Delivery failed: {err}')


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
        
        prices = [d['price'] for d in window_data if d['price'] > 0]  # Filter out zero prices
        
        if not prices:  # If all prices were zero
            metrics[window_name] = {
                'high': 0, 'low': 0, 'price_change': 0, 'price_change_percent': 0,
                'volume': 0, 'buy_volume': 0, 'sell_volume': 0
            }
            continue
        
        first_price = prices[0]
        last_price = prices[-1]
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


async def receive_websocket_data(websocket, state):
    """Task to receive and process WebSocket messages"""
    while True:
        try:
            raw_message = await websocket.recv()
            message = json.loads(raw_message)
            msg_type = message.get('type')
            
            # Log subscription confirmations
            if msg_type == 'subscriptions':
                logger.info(f"Subscription confirmed: {message.get('channels', [])}")
                continue
            
            # Handle ticker updates (24hr stats)
            if msg_type == 'ticker':
                latest_price = float(message.get('price', 0))
                state['latest_ticker'] = {
                    'symbol': message.get('product_id', PRODUCT_ID),
                    'price': latest_price,
                    'volume_24h': float(message.get('volume_24h', 0)),
                    'high_24h': float(message.get('high_24h', 0)),
                    'low_24h': float(message.get('low_24h', 0)),
                    'open_24h': float(message.get('open_24h', 0)),
                    'timestamp': int(time.time() * 1000)
                }
                state['latest_price'] = latest_price
                
                # Update 24hr stats
                if state['latest_ticker']['high_24h'] > state['high_24h']:
                    state['high_24h'] = state['latest_ticker']['high_24h']
                if state['latest_ticker']['low_24h'] < state['low_24h'] and state['latest_ticker']['low_24h'] > 0:
                    state['low_24h'] = state['latest_ticker']['low_24h']
            
            # Handle trade matches
            elif msg_type == 'match':
                trade_price = float(message.get('price', 0))
                trade_size = float(message.get('size', 0))
                is_buy = message.get('side') == 'buy'
                
                state['latest_price'] = trade_price
                state['last_trade_time'] = time.time()  # Update last trade time
                
                if is_buy:
                    state['buy_volume_1s'] += trade_size
                else:
                    state['sell_volume_1s'] += trade_size
                
                state['trade_count_1s'] += 1
        
        except Exception as e:
            logger.error(f"Error in WebSocket receive: {e}")
            break


async def send_to_kafka_periodically(producer, state):
    """Task to send data to Kafka every 1 second"""
    await asyncio.sleep(2)  # Wait for initial data
    
    while not state.get('should_reconnect', False):
        start_time = time.time()
        
        if state['latest_ticker'].get('price', 0) > 0:
            # Use latest trade price or ticker price
            if state['latest_price'] > 0:
                current_price = state['latest_price']
            else:
                current_price = state['latest_ticker'].get('price', 0)
            
            total_volume_1s = state['buy_volume_1s'] + state['sell_volume_1s']
            buy_sell_volume_ratio = state['buy_volume_1s'] / state['sell_volume_1s'] if state['sell_volume_1s'] > 0 else 0
            buy_sell_ratio = buy_sell_volume_ratio
            
            # Add to price history
            state['price_history'].append({
                'timestamp': start_time,
                'price': current_price,
                'buy_vol': state['buy_volume_1s'],
                'sell_vol': state['sell_volume_1s']
            })
            
            rolling_metrics = calculate_rolling_metrics(state['price_history'])
            
            # Calculate 24hr metrics
            price_change_24h = current_price - state['latest_ticker'].get('open_24h', current_price)
            price_change_percent_24h = (price_change_24h / state['latest_ticker'].get('open_24h', 1) * 100) if state['latest_ticker'].get('open_24h', 0) > 0 else 0
            
            # Construct Kafka message
            kafka_message = {
                'symbol': PRODUCT_ID,
                'price': current_price,
                'timestamp': int(start_time * 1000),
                'volume_24h': state['latest_ticker'].get('volume_24h', 0),
                'high_24h': state['latest_ticker'].get('high_24h', 0),
                'low_24h': state['latest_ticker'].get('low_24h', 0),
                'price_change_24h': price_change_24h,
                'price_change_percent_24h': price_change_percent_24h,
                'number_of_trades': state['trade_count_1s'],
                'high_1s': rolling_metrics['1s']['high'],
                'low_1s': rolling_metrics['1s']['low'],
                'price_change_1s': rolling_metrics['1s']['price_change'],
                'price_change_percent_1s': rolling_metrics['1s']['price_change_percent'],
                'volume_1s': total_volume_1s,
                'buy_volume_1s': state['buy_volume_1s'],
                'sell_volume_1s': state['sell_volume_1s'],
                'buy_sell_volume_ratio_1s': buy_sell_volume_ratio,
                'buy_sell_ratio': buy_sell_ratio,
                'trade_count_1s': state['trade_count_1s'],
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
            
            # Reset 1s counters
            state['buy_volume_1s'] = 0.0
            state['sell_volume_1s'] = 0.0
            state['trade_count_1s'] = 0
            
            # Send to Kafka
            producer.produce(
                topic=KAFKA_TOPIC,
                key=kafka_message['symbol'],
                value=json.dumps(kafka_message),
                callback=delivery_callback
            )
            producer.poll(0)
            
            state['message_count'] += 1
            
            if state['message_count'] % 10 == 0:
                logger.info(
                    f"Sent {state['message_count']} | Price: ${kafka_message['price']:.2f} | "
                    f"Vol: {kafka_message['volume_1s']:.4f} | Buy/Sell: {kafka_message['buy_sell_volume_ratio_1s']:.2f}"
                )
        
        # Sleep to maintain exact 1-second intervals
        elapsed = time.time() - start_time
        sleep_time = max(0, 1.0 - elapsed)
        await asyncio.sleep(sleep_time)


async def health_check(state, websocket):
    """Monitor data freshness and trigger reconnect if stale"""
    logger.info("🏥 Health check started")
    consecutive_stale = 0
    
    while not state.get('should_reconnect', False):
        await asyncio.sleep(HEALTH_CHECK_INTERVAL)
        
        current_time = time.time()
        last_trade_time = state.get('last_trade_time', current_time)
        time_since_trade = current_time - last_trade_time
        
        # Check if we're receiving trades
        if time_since_trade > STALE_DATA_THRESHOLD:
            consecutive_stale += 1
            logger.warning(
                f"⚠️ STALE DATA: No trades for {time_since_trade:.0f}s "
                f"(threshold: {STALE_DATA_THRESHOLD}s) - count: {consecutive_stale}"
            )
            
            # After 3 consecutive stale checks, trigger reconnect
            if consecutive_stale >= 3:
                logger.error("❌ Data stale for too long - triggering reconnect!")
                state['should_reconnect'] = True
                await websocket.close()
                break
        else:
            if consecutive_stale > 0:
                logger.info(f"✅ Data flowing again - last trade {time_since_trade:.1f}s ago")
            consecutive_stale = 0
            
            # Log health status periodically
            logger.info(
                f"🏥 Health OK | Price: ${state.get('latest_price', 0):.2f} | "
                f"Messages: {state.get('message_count', 0)} | "
                f"Last trade: {time_since_trade:.1f}s ago"
            )


async def stream_coinbase_to_kafka():
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"Starting Coinbase producer → Topic: {KAFKA_TOPIC}")
    
    # Shared state dictionary
    state = {
        'latest_ticker': {'symbol': PRODUCT_ID, 'price': 0, 'volume_24h': 0},
        'latest_price': 0.0,
        'buy_volume_1s': 0.0,
        'sell_volume_1s': 0.0,
        'trade_count_1s': 0,
        'price_history': deque(maxlen=86400),
        'message_count': 0,
        'high_24h': 0.0,
        'low_24h': float('inf'),
        'last_trade_time': time.time(),  # Track last trade for health check
        'should_reconnect': False  # Flag to trigger reconnect
    }
    
    try:
        async with websockets.connect(COINBASE_WS_URL) as websocket:
            # Subscribe to ticker and matches only (no order book to avoid connection issues)
            subscribe_message = {
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": [
                    "ticker",
                    "matches"
                ]
            }
            await websocket.send(json.dumps(subscribe_message))
            logger.info(f"Subscribed to Coinbase {PRODUCT_ID} channels: ticker, matches")
            
            # Run all tasks concurrently (receive, send, health check)
            await asyncio.gather(
                receive_websocket_data(websocket, state),
                send_to_kafka_periodically(producer, state),
                health_check(state, websocket)
            )
    
    except websockets.exceptions.ConnectionClosed as e:
        logger.error(f"Connection closed unexpectedly: {e}")
        raise
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    
    finally:
        producer.flush()
        logger.info("Shutdown complete")


# Entry point with retry logic
def main():
    required_vars = ['CONFLUENT_KAFKA_BOOTSTRAP_SERVERS', 'CONFLUENT_KAFKA_API_KEY_GCS', 'CONFLUENT_KAFKA_API_KEY_SECRET_GCS']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing env vars: {', '.join(missing_vars)}")
        return
    
    retry_count = 0
    
    while True:  # Infinite retry - always try to reconnect
        try:
            retry_count += 1
            logger.info(f"🚀 Starting Coinbase producer (attempt #{retry_count})")
            asyncio.run(stream_coinbase_to_kafka())
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"⚠️ Connection closed: {e}. Reconnecting in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"❌ Error: {e}. Reconnecting in 10s...")
            time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
            break
        
        logger.info("🔄 Reconnecting...")


if __name__ == "__main__":
    main()
