# Kafka Message Data Fields

This document describes all fields sent to Kafka for LTC/USDT dynamic pricing model.

## Message Structure

Each Kafka message contains the following fields:

### Basic Information
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `symbol` | string | Trading pair (e.g., "LTCUSDT") | Feature: Asset identifier |
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
| `volume_24h` | float | Total LTC traded in 24h (from ticker) | Feature: Daily market activity |
| `volume_24h_calculated` | float | Calculated 24h volume | Feature: Verification metric |
| `buy_volume_24h_calculated` | float | Calculated 24h buy volume | Feature: Daily buy pressure |
| `sell_volume_24h_calculated` | float | Calculated 24h sell volume | Feature: Daily sell pressure |
| `quote_volume_24h` | float | Total USDT traded in 24h | Feature: Liquidity |
| `number_of_trades` | int | Total trades in 24h | Feature: Market interest |

### Order Book Data (Supply & Demand)
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `best_bid_price` | float | Highest buy order price | Feature: Immediate demand |
| `best_ask_price` | float | Lowest sell order price | Feature: Immediate supply |
| `bid_ask_spread` | float | Difference between bid/ask | **Key Feature**: Market liquidity |
| `spread_percent` | float | Spread as % of price | Feature: Market efficiency |
| `total_bid_volume` | float | Total volume of top 10 buy orders | **Feature: Demand strength** |
| `total_ask_volume` | float | Total volume of top 10 sell orders | **Feature: Supply strength** |
| `buy_sell_ratio` | float | Bid volume / Ask volume | **Key Feature**: Market sentiment |
| `top_5_bids` | array | Top 5 buy orders [price, quantity] | Feature: Order book depth |
| `top_5_asks` | array | Top 5 sell orders [price, quantity] | Feature: Order book depth |

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
13. **`buy_sell_ratio`** (order book) - >1 means more buyers waiting (bullish)
14. **`bid_ask_spread`** - Market liquidity indicator

### Multi-Timeframe Strategy for ML

**For Predicting Next 1-Second Price:**
- **Weight 1s metrics highest** (70%): `buy_sell_volume_ratio_1s`, `volume_1s`
- **Weight 1min metrics** (20%): `price_change_percent_1min`
- **Weight 1h/24h** (10%): Trend confirmation

**For Predicting Next 1-Minute Price:**
- **Weight 1min metrics highest** (50%): `volume_1min`, `price_change_1min`
- **Weight 1s metrics** (30%): Real-time momentum
- **Weight 1h metrics** (20%): Trend direction

**Signal Strength:**
- `buy_sell_volume_ratio_1s > 2.0` + `price_change_percent_1min > 0.5%` → **STRONG BUY**
- `buy_sell_volume_ratio_1s < 0.5` + `price_change_percent_1min < -0.5%` → **STRONG SELL**
- `volume_1s > 3x average_volume_1min` → **HIGH VOLATILITY ALERT**

## Example Message

```json
{
  "symbol": "LTCUSDT",
  "price": 95.43,
  "timestamp": 1700000000000,
  "volume_24h": 1234567.89,
  "quote_volume_24h": 117800000.0,
  "high_24h": 96.50,
  "low_24h": 94.20,
  "price_change_24h": 1.23,
  "price_change_percent_24h": 1.31,
  "number_of_trades": 45678,
  "buy_volume_24h": 650000.0,
  "volume_1s": 12.5,
  "buy_volume_1s": 8.3,
  "sell_volume_1s": 4.2,
  "buy_sell_volume_ratio_1s": 1.98,
  "trade_count_1s": 15,
  "best_bid_price": 95.42,
  "best_ask_price": 95.44,
  "bid_ask_spread": 0.02,
  "spread_percent": 0.021,
  "total_bid_volume": 1500.5,
  "total_ask_volume": 1200.3,
  "buy_sell_ratio": 1.25,
  "top_5_bids": [[95.42, 100.5], [95.41, 200.0], ...],
  "top_5_asks": [[95.44, 150.3], [95.45, 180.0], ...],
  "ingestion_time": "2025-11-19T23:00:00.000Z"
}
```

## Data Update Frequency

- **Ticker data**: ~1 second (24h stats)
- **Order book depth**: 1 second (top 20 bids/asks)
- **Trade stream**: Real-time (every trade as it happens)
- **Kafka messages**: 1 per second (aggregates all trades in that second)

## Notes for ML Model

- **Target Variable**: `price` (next price prediction)
- **Lagging Features**: Create rolling windows (5s, 30s, 1m, 5m averages)
- **Leading Indicators**: `buy_sell_ratio`, `bid_ask_spread`, `total_bid_volume`
- **Sentiment**: When `buy_sell_ratio > 1.2`, strong buying pressure → price likely to increase
- **Liquidity Risk**: When `spread_percent > 0.1%`, market is less liquid → higher volatility
