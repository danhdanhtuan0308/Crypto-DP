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
import pytz
import numpy as np
import websockets.exceptions

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Eastern Time Zone - ALL timestamps will be in EST
EASTERN = pytz.timezone('America/New_York')

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


# Microstructure Metrics Calculations
def calculate_bid_ask_spread(best_bid, best_ask):
    """Calculate Bid-Ask Spread"""
    if best_bid > 0 and best_ask > 0:
        return best_ask - best_bid
    return 0


def calculate_mid_price(best_bid, best_ask):
    """Calculate Mid-Price"""
    if best_bid > 0 and best_ask > 0:
        return (best_bid + best_ask) / 2
    return 0


def calculate_order_book_depth_2pct(order_book, mid_price):
    """
    Calculate Order Book Depth within 2% of mid-price
    Returns total volume of bids and asks within 2% range
    """
    if mid_price <= 0:
        return 0, 0, 0
    
    bid_depth = 0
    ask_depth = 0
    
    # 2% range
    bid_threshold = mid_price * 0.98  # mid-price - 2%
    ask_threshold = mid_price * 1.02  # mid-price + 2%
    
    # Sum bid volumes within range
    for price, volume in order_book.get('bids', []):
        if price >= bid_threshold:
            bid_depth += volume
    
    # Sum ask volumes within range
    for price, volume in order_book.get('asks', []):
        if price <= ask_threshold:
            ask_depth += volume
    
    total_depth = bid_depth + ask_depth
    return total_depth, bid_depth, ask_depth


def calculate_vwap(trades):
    """
    Calculate Volume Weighted Average Price (VWAP)
    trades: list of {'price': float, 'volume': float}
    """
    if not trades:
        return 0
    
    total_pv = sum(t['price'] * t['volume'] for t in trades if t.get('volume', 0) > 0)
    total_volume = sum(t['volume'] for t in trades if t.get('volume', 0) > 0)
    
    if total_volume > 0:
        return total_pv / total_volume
    return 0


def calculate_micro_price_deviation(trade_price, mid_price):
    """
    Calculate Micro-Price Deviation
    Deviation = Trade Price - Mid-Price
    """
    if mid_price > 0:
        return trade_price - mid_price
    return 0


def calculate_order_flow_imbalance(delta_bid_volume, delta_ask_volume):
    """
    Calculate Order Flow Imbalance (OFI)
    OFI ≈ ΔBid Volume - ΔAsk Volume
    """
    return delta_bid_volume - delta_ask_volume


def calculate_kyles_lambda(price_change, signed_order_volume):
    """
    Calculate Kyle's Lambda (market impact coefficient)
    λ = ΔPrice / Signed Order Volume
    """
    if signed_order_volume != 0:
        return price_change / signed_order_volume
    return 0


def calculate_liquidity_health(order_book_depth, bid_ask_spread, volatility):
    """
    Calculate Liquidity Health
    Health ∝ Depth / (Spread × Volatility)
    """
    denominator = bid_ask_spread * volatility if volatility > 0 else 1
    if denominator > 0 and bid_ask_spread > 0:
        return order_book_depth / denominator
    return 0


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
            
            # Handle Level 2 snapshot (initial order book state)
            elif msg_type == 'snapshot':
                state['order_book'] = {
                    'bids': [(float(p), float(v)) for p, v in message.get('bids', [])[:50]],  # Top 50
                    'asks': [(float(p), float(v)) for p, v in message.get('asks', [])[:50]]
                }
                logger.info(f"📚 Order book snapshot received: {len(state['order_book']['bids'])} bids, {len(state['order_book']['asks'])} asks")
            
            # Handle Level 2 updates (incremental order book changes)
            elif msg_type == 'l2update':
                changes = message.get('changes', [])
                for change in changes:
                    side, price_str, size_str = change
                    price = float(price_str)
                    size = float(size_str)
                    
                    book_side = 'bids' if side == 'buy' else 'asks'
                    
                    # Track volume deltas for OFI calculation
                    if side == 'buy':
                        state['delta_bid_volume_1s'] += size
                    else:
                        state['delta_ask_volume_1s'] += size
                    
                    # Update order book (remove if size = 0, else update)
                    if size == 0:
                        state['order_book'][book_side] = [(p, v) for p, v in state['order_book'][book_side] if p != price]
                    else:
                        # Update existing or add new
                        found = False
                        for i, (p, v) in enumerate(state['order_book'][book_side]):
                            if p == price:
                                state['order_book'][book_side][i] = (price, size)
                                found = True
                                break
                        if not found:
                            state['order_book'][book_side].append((price, size))
                    
                    # Keep order book sorted and limited
                    if side == 'buy':
                        state['order_book']['bids'].sort(reverse=True, key=lambda x: x[0])
                        state['order_book']['bids'] = state['order_book']['bids'][:50]
                    else:
                        state['order_book']['asks'].sort(key=lambda x: x[0])
                        state['order_book']['asks'] = state['order_book']['asks'][:50]
            
            # Handle ticker updates (24hr stats + best bid/ask)
            elif msg_type == 'ticker':
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
                
                # Extract best bid/ask from ticker (for spread calculation)
                best_bid_str = message.get('best_bid')
                best_ask_str = message.get('best_ask')
                if best_bid_str and best_ask_str:
                    state['best_bid_from_ticker'] = float(best_bid_str)
                    state['best_ask_from_ticker'] = float(best_ask_str)
                    state['ticker_spread_available'] = True
                
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
                
                # Store trade for VWAP calculation
                state['trades_1s'].append({
                    'price': trade_price,
                    'volume': trade_size,
                    'side': 'buy' if is_buy else 'sell',
                    'timestamp': time.time()
                })
                
                # Track previous price for Kyle's Lambda
                if state['previous_price'] > 0:
                    price_change = trade_price - state['previous_price']
                    signed_volume = trade_size if is_buy else -trade_size
                    state['kyles_lambda_data'].append({
                        'price_change': price_change,
                        'signed_volume': signed_volume
                    })
                state['previous_price'] = trade_price
                
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
            
            # ===============================
            # CALCULATE MICROSTRUCTURE METRICS
            # ===============================
            
            # Get best bid/ask from order book (Level 2) OR ticker as fallback
            if state['order_book']['bids'] and state['order_book']['asks']:
                # Use Level 2 order book if available (more accurate)
                best_bid = state['order_book']['bids'][0][0]
                best_ask = state['order_book']['asks'][0][0]
            elif state.get('ticker_spread_available', False):
                # Fallback to ticker best bid/ask (updated every ~1 second)
                best_bid = state.get('best_bid_from_ticker', 0)
                best_ask = state.get('best_ask_from_ticker', 0)
            else:
                # No bid/ask data available
                best_bid = 0
                best_ask = 0
            
            # 1. Bid-Ask Spread
            bid_ask_spread_1s = calculate_bid_ask_spread(best_bid, best_ask)
            
            # 2. Mid-Price
            mid_price_1s = calculate_mid_price(best_bid, best_ask)
            
            # 3. Order Book Depth (2%)
            depth_2pct_1s, bid_depth_2pct_1s, ask_depth_2pct_1s = calculate_order_book_depth_2pct(
                state['order_book'], mid_price_1s
            )
            
            # 4. VWAP (Volume Weighted Average Price)
            vwap_1s = calculate_vwap(state['trades_1s'])
            
            # 5. Micro-Price Deviation (average deviation of trades from mid-price)
            micro_price_deviations = [
                calculate_micro_price_deviation(t['price'], mid_price_1s)
                for t in state['trades_1s']
            ]
            avg_micro_price_deviation_1s = np.mean(micro_price_deviations) if micro_price_deviations else 0
            
            # 6. Cumulative Volume Delta (CVD)
            delta_volume_1s = state['buy_volume_1s'] - state['sell_volume_1s']
            state['cvd'] += delta_volume_1s
            cvd_1s = state['cvd']
            
            # 7. Order Flow Imbalance (OFI)
            ofi_1s = calculate_order_flow_imbalance(
                state['delta_bid_volume_1s'],
                state['delta_ask_volume_1s']
            )
            
            # 8. Kyle's Lambda (market impact coefficient)
            if state['kyles_lambda_data']:
                total_price_change = sum(d['price_change'] for d in state['kyles_lambda_data'])
                total_signed_volume = sum(d['signed_volume'] for d in state['kyles_lambda_data'])
                kyles_lambda_1s = calculate_kyles_lambda(total_price_change, total_signed_volume)
            else:
                kyles_lambda_1s = 0
            
            # 9. Volatility (from rolling metrics - already calculated)
            volatility_1s = rolling_metrics['1s']['price_change_percent']
            
            # 10. Liquidity Health
            liquidity_health_1s = calculate_liquidity_health(
                depth_2pct_1s,
                bid_ask_spread_1s,
                abs(volatility_1s) if volatility_1s != 0 else 1
            )
            
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
                
                # ===============================
                # NEW MICROSTRUCTURE METRICS (1s)
                # ===============================
                'bid_ask_spread_1s': bid_ask_spread_1s,
                'mid_price_1s': mid_price_1s,
                'best_bid_1s': best_bid,
                'best_ask_1s': best_ask,
                'depth_2pct_1s': depth_2pct_1s,
                'bid_depth_2pct_1s': bid_depth_2pct_1s,
                'ask_depth_2pct_1s': ask_depth_2pct_1s,
                'vwap_1s': vwap_1s,
                'micro_price_deviation_1s': avg_micro_price_deviation_1s,
                'cvd_1s': cvd_1s,
                'ofi_1s': ofi_1s,
                'kyles_lambda_1s': kyles_lambda_1s,
                'liquidity_health_1s': liquidity_health_1s,
                
                'ingestion_time': datetime.now(EASTERN).isoformat()  # EST timezone
            }
            
            # Reset 1s counters
            state['buy_volume_1s'] = 0.0
            state['sell_volume_1s'] = 0.0
            state['trade_count_1s'] = 0
            state['trades_1s'].clear()
            state['kyles_lambda_data'].clear()
            state['delta_bid_volume_1s'] = 0.0
            state['delta_ask_volume_1s'] = 0.0
            
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
                # Indicate data source
                source = "📚 L2" if state['order_book']['bids'] else "📊 Ticker"
                logger.info(
                    f"📤 Sent {state['message_count']} | {source} | "
                    f"Price: ${kafka_message['price']:.2f} | "
                    f"Spread: ${kafka_message['bid_ask_spread_1s']:.2f} | "
                    f"Vol: {kafka_message['volume_1s']:.4f} | "
                    f"CVD: {kafka_message['cvd_1s']:.2f} | "
                    f"VWAP: ${kafka_message['vwap_1s']:.2f}"
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
        'should_reconnect': False,  # Flag to trigger reconnect
        
        # Order book state
        'order_book': {'bids': [], 'asks': []},
        
        # Best bid/ask from ticker (fallback if Level 2 not available)
        'best_bid_from_ticker': 0.0,
        'best_ask_from_ticker': 0.0,
        'ticker_spread_available': False,
        
        # Microstructure metrics state
        'trades_1s': [],  # Store trades for VWAP calculation
        'cvd': 0.0,  # Cumulative Volume Delta (persistent across windows)
        'delta_bid_volume_1s': 0.0,  # For OFI calculation
        'delta_ask_volume_1s': 0.0,
        'previous_price': 0.0,  # For Kyle's Lambda
        'kyles_lambda_data': [],  # Store price changes and signed volumes
    }
    
    try:
        # Increase message size limit for Level 2 snapshots (can be ~1MB)
        async with websockets.connect(
            COINBASE_WS_URL,
            max_size=2 * 1024 * 1024,  # 2MB limit (default is 1MB)
            ping_interval=20,
            ping_timeout=20
        ) as websocket:
            # Subscribe to ticker, matches, and Level 2 order book
            # Use level2_batch for limited depth (top 50 levels)
            # Ticker includes best_bid/best_ask as fallback
            subscribe_message = {
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": [
                    "ticker",       # Includes best_bid, best_ask (updated ~1/sec)
                    "matches",      # Real-time trades
                    "level2_batch"  # Top 50 order book levels, batch updates
                ]
            }
            await websocket.send(json.dumps(subscribe_message))
            logger.info(f"📡 Subscribed to Coinbase {PRODUCT_ID} channels: ticker, matches, level2_batch")
            
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
