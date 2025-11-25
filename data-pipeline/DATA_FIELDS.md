# Kafka Message Data Fields

This document describes all fields sent to Kafka for BTC-USD dynamic pricing model.

## Message Structure

Each Kafka message contains the following fields (using Coinbase data):

### Basic Information
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `symbol` | string | Trading pair (e.g., "BTC-USD") | Feature: Asset identifier |
| `timestamp` | long | Event timestamp (ms) | Feature: Time-based patterns |
| `ingestion_time` | string | When data was ingested (ISO 8601) | Feature: Processing latency |

### Price Data
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `price` | float | Current market price | **Target variable** for prediction |
| **1-Second Window** |
| `high_1s` | float | Highest price in 1 second | Feature: Micro volatility |
| `low_1s` | float | Lowest price in 1 second | Feature: Micro volatility |
| `price_change_1s` | float | Price change in 1 second | Feature: Instant momentum |
| `price_change_percent_1s` | float | % change in 1 second | **Key Feature: Real-time trend** |
| **1-Minute Window** |
| `high_1min` | float | Highest price in 1 min | Feature: Short-term volatility |
| `low_1min` | float | Lowest price in 1 min | Feature: Short-term volatility |
| `price_change_1min` | float | Price change in 1 min | Feature: Immediate momentum |
| `price_change_percent_1min` | float | % change in 1 min | **Key Feature: Short-term trend** |
| **1-Hour Window** |
| `high_1h` | float | Highest price in 1 hour | Feature: Medium-term volatility |
| `low_1h` | float | Lowest price in 1 hour | Feature: Medium-term volatility |
| `price_change_1h` | float | Price change in 1 hour | Feature: Hourly momentum |
| `price_change_percent_1h` | float | % change in 1 hour | **Key Feature: Hourly trend** |
| **24-Hour Window** |
| `high_24h` | float | Highest price in 24h | Feature: Daily volatility |
| `low_24h` | float | Lowest price in 24h | Feature: Daily volatility |
| `price_change_24h` | float | Absolute price change | Feature: Daily momentum |
| `price_change_percent_24h` | float | Percentage change | Feature: Daily trend strength |

### Volume Data (Trading Activity)

#### 1-Second Window (Real-Time)
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| **`volume_1s`** | float | **Total LTC traded in 1 second** | **Key Feature: Immediate activity** |
| **`buy_volume_1s`** | float | **Buy volume in 1 second** | **Key Feature: Instant buy pressure** |
| **`sell_volume_1s`** | float | **Sell volume in 1 second** | **Key Feature: Instant sell pressure** |
| **`buy_sell_volume_ratio_1s`** | float | **Buy/Sell volume ratio (1s)** | **Key Feature: >1 = buying pressure** |
| **`trade_count_1s`** | int | **Number of trades in 1 second** | **Feature: Trading intensity** |

#### 1-Minute Window
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `volume_1min` | float | Total LTC traded in 1 min | Feature: Short-term activity |
| `buy_volume_1min` | float | Buy volume in 1 min | Feature: Short-term buy pressure |
| `sell_volume_1min` | float | Sell volume in 1 min | Feature: Short-term sell pressure |

#### 1-Hour Window
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `volume_1h` | float | Total LTC traded in 1 hour | Feature: Hourly activity |
| `buy_volume_1h` | float | Buy volume in 1 hour | Feature: Hourly buy pressure |
| `sell_volume_1h` | float | Sell volume in 1 hour | Feature: Hourly sell pressure |

#### 24-Hour Window
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `volume_24h` | float | Total BTC traded in 24h (from ticker) | Feature: Daily market activity |
| `volume_24h_calculated` | float | Calculated 24h volume | Feature: Verification metric |
| `buy_volume_24h_calculated` | float | Calculated 24h buy volume | Feature: Daily buy pressure |
| `sell_volume_24h_calculated` | float | Calculated 24h sell volume | Feature: Daily sell pressure |
| `number_of_trades` | int | Number of trades in 1 second | **Feature: Real-time trading intensity** |
| `buy_sell_ratio` | float | Buy/Sell volume ratio (1s) | **Key Feature: >1 = buying pressure** |

### Notes on Data Availability
- **Order book data NOT available**: Coinbase WebSocket connection issues prevented order book streaming
- **Focus on trade data**: Using real-time matches (trades) for volume and price analysis
- **No bid/ask spread**: Not collecting order book depth metrics

## Key Features for Dynamic Pricing Model

### Primary Indicators (Multi-Timeframe Analysis)

#### Immediate (1-Second)
1. **`price`** - What you're predicting
2. **`buy_sell_volume_ratio_1s`** - >1 = more buying RIGHT NOW (strongest real-time signal)
3. **`volume_1s`** - Immediate trading activity (spike = price movement coming)
4. **`trade_count_1s`** - High trade count = market is active/volatile

#### Short-Term (1-Minute)
5. **`price_change_percent_1min`** - Immediate price momentum
6. **`volume_1min`** - Short-term trading intensity
7. **`buy_volume_1min` vs `sell_volume_1min`** - Short-term pressure direction

#### Medium-Term (1-Hour)
8. **`price_change_percent_1h`** - Hourly trend strength
9. **`high_1h` - `low_1h`** - Hourly volatility range
10. **`volume_1h`** - Sustained activity level

#### Long-Term (24-Hour)
11. **`price_change_percent_24h`** - Daily trend
12. **`volume_24h`** - Overall market interest
13. **`buy_sell_ratio`** - Buy/Sell volume ratio from trades (>1 = more buying)

### Multi-Timeframe Strategy for ML

**For Predicting Next 1-Second Price:**
- **Weight 1s metrics highest** (70%): `buy_sell_volume_ratio_1s`, `volume_1s`
- **Weight 1min metrics** (20%): `price_change_percent_1min`
- **Weight 1h/24h** (10%): Trend confirmation

**For Predicting Next 1-Minute Price:**
- **Weight 1min metrics highest** (50%): `volume_1min`, `price_change_1min`
- **Weight 1s metrics** (30%): Real-time momentum
- **Weight 1h metrics** (20%): Trend direction

**Signal Strength (Trade-Based):**
- `buy_sell_volume_ratio_1s > 2.0` + `price_change_percent_1min > 0.5%` → **STRONG BUY**
- `buy_sell_volume_ratio_1s < 0.5` + `price_change_percent_1min < -0.5%` → **STRONG SELL**
- `volume_1s > 3x average_volume_1min` → **HIGH VOLATILITY ALERT**
- `trade_count_1s > 50` → **HIGH TRADING ACTIVITY** (institutional interest)

## Example Message (Coinbase)

```json
{
  "symbol": "BTC-USD",
  "price": 87809.34,
  "timestamp": 1764043253104,
  "volume_24h": 14130.78755984,
  "high_24h": 89225.6,
  "low_24h": 85213.17,
  "price_change_24h": 401.34,
  "price_change_percent_24h": 0.459,
  "number_of_trades": 3,
  "high_1s": 87809.34,
  "low_1s": 87809.34,
  "price_change_1s": 0.0,
  "price_change_percent_1s": 0.0,
  "volume_1s": 0.0434,
  "buy_volume_1s": 0.0234,
  "sell_volume_1s": 0.0200,
  "buy_sell_volume_ratio_1s": 1.17,
  "buy_sell_ratio": 1.17,
  "trade_count_1s": 3,
  "high_1min": 87815.50,
  "low_1min": 87800.00,
  "price_change_1min": 9.34,
  "price_change_percent_1min": 0.011,
  "volume_1min": 2.5643,
  "buy_volume_1min": 1.3421,
  "sell_volume_1min": 1.2222,
  "high_1h": 88100.00,
  "low_1h": 87200.00,
  "price_change_1h": 234.50,
  "price_change_percent_1h": 0.268,
  "volume_1h": 145.234,
  "buy_volume_1h": 78.123,
  "sell_volume_1h": 67.111,
  "volume_24h_calculated": 2345.67,
  "buy_volume_24h_calculated": 1234.56,
  "sell_volume_24h_calculated": 1111.11,
  "ingestion_time": "2025-11-25T04:00:53.104220+00:00"
}
```

## Data Update Frequency

- **Ticker data**: ~1 second (24h stats from Coinbase)
- **Trade stream**: Real-time (every trade as it happens via matches channel)
- **Kafka messages**: Exactly 1 per second (aggregates all trades in that second)
- **Precision**: Timestamp accuracy ±10ms using async timer

## Data Source

- **Exchange**: Coinbase Advanced Trade API
- **WebSocket URL**: wss://ws-feed.exchange.coinbase.com
- **Channels**: 
  - `ticker`: 24hr volume, high, low, open
  - `matches`: Real-time trades (price, size, side)
- **No Order Book**: Order book streaming disabled due to connection issues

## Notes for ML Model

- **Target Variable**: `price` (next 1-second or 1-minute price prediction)
- **Lagging Features**: Create rolling windows (5s, 30s, 1m, 5m averages)
- **Leading Indicators**: `buy_sell_ratio`, `volume_1s`, `trade_count_1s`
- **Sentiment**: When `buy_sell_ratio > 1.2`, strong buying pressure → price likely to increase
- **Trade Intensity**: When `trade_count_1s > 20`, high activity → potential breakout
- **Volume Confirmation**: `buy_volume_1s` increasing + `price_change_1s` positive → bullish confirmation

## Implementation Details

- **Producer**: `coinbase_kafka_producer.py`
- **Architecture**: Dual async tasks (WebSocket receiver + 1-second timer sender)
- **Timing**: Independent timer ensures exactly 1-second intervals
- **State Management**: Shared dictionary for real-time updates between tasks
- **Reliability**: Automatic retry logic (max 5 retries on connection loss)
