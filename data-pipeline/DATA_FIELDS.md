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

### Microstructure Metrics (Level 2 Order Book) - NEW!

#### 1-Second Window
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| **`bid_ask_spread_1s`** | float | **Best Ask - Best Bid** | **Key Feature: Liquidity cost** |
| **`mid_price_1s`** | float | **(Best Bid + Best Ask) / 2** | **Feature: Fair value estimate** |
| **`best_bid_1s`** | float | **Highest bid price** | Feature: Top of book |
| **`best_ask_1s`** | float | **Lowest ask price** | Feature: Top of book |
| **`depth_2pct_1s`** | float | **Total volume within ±2% of mid-price** | **Key Feature: Available liquidity** |
| `bid_depth_2pct_1s` | float | Bid volume within 2% range | Feature: Buy-side liquidity |
| `ask_depth_2pct_1s` | float | Ask volume within 2% range | Feature: Sell-side liquidity |
| **`vwap_1s`** | float | **Volume-weighted average trade price** | **Feature: Execution quality** |
| **`micro_price_deviation_1s`** | float | **Avg(Trade Price - Mid-Price)** | **Key Feature: Aggressive order flow** |
| **`cvd_1s`** | float | **Cumulative (Buy Vol - Sell Vol)** | **Key Feature: Net accumulation/distribution** |
| **`ofi_1s`** | float | **Order Flow Imbalance: ΔBid Vol - ΔAsk Vol** | **🔥 Leading Indicator: Predicts price movement** |
| **`kyles_lambda_1s`** | float | **Market impact: ΔPrice / Signed Volume** | **Feature: Liquidity depth measure** |
| **`liquidity_health_1s`** | float | **Depth / (Spread × Volatility)** | **Feature: Composite market quality** |

#### 1-Minute Aggregated
| Field | Type | Description | Use for ML |
|-------|------|-------------|------------|
| `avg_bid_ask_spread_1m` | float | Average spread over 1 minute | Feature: Average liquidity cost |
| `avg_depth_2pct_1m` | float | Average total depth | Feature: Average liquidity |
| `avg_bid_depth_2pct_1m` | float | Average bid depth | Feature: Buy-side liquidity trend |
| `avg_ask_depth_2pct_1m` | float | Average ask depth | Feature: Sell-side liquidity trend |
| `avg_vwap_1m` | float | Average VWAP | Feature: Average execution price |
| `avg_micro_price_deviation_1m` | float | Average price deviation | Feature: Persistent order flow direction |
| `cvd_1m` | float | Latest CVD (cumulative) | **Key Feature: Net position** |
| `total_ofi_1m` | float | Sum of OFI over 1 minute | Feature: Total order flow pressure |
| `avg_ofi_1m` | float | Average OFI | Feature: Average order flow |
| `avg_kyles_lambda_1m` | float | Average market impact | Feature: Average liquidity depth |
| `avg_liquidity_health_1m` | float | Average liquidity health | Feature: Market quality trend |
| `avg_mid_price_1m` | float | Average mid-price | Feature: Fair value over window |
| `latest_best_bid_1m` | float | Best bid at end of minute | Feature: Latest top of book |
| `latest_best_ask_1m` | float | Best ask at end of minute | Feature: Latest top of book |
| `volatility_regime` | string | Classification: "low", "medium", "high" | **Feature: Market state** |

### Data Sources
- **Trade data**: Real-time matches (trades) from Coinbase WebSocket
- **Order book data**: Level 2 snapshots and updates from Coinbase WebSocket
- **Ticker data**: 24-hour statistics from Coinbase WebSocket
- **Order book depth**: Top 50 bid/ask levels maintained in memory

---

## GCS Parquet Files (btc_1min_agg Topic)

**What gets stored in GCS**: Only the **1-minute aggregated data** from `btc_1min_agg` topic.

### Stored Fields (40+ metrics)

#### Basic Information
| Field | Type | Description |
|-------|------|-------------|
| `symbol` | string | Trading pair (BTC-USD) |
| `window_start` | long | Window start timestamp (ms, UTC) |
| `window_end` | long | Window end timestamp (ms, UTC) |
| `window_start_est` | string | Human-readable EST time |
| `last_ingestion_time` | string | Last ingestion timestamp |
| `num_ticks` | int | Number of 1-second ticks in window |

#### OHLC (1-Minute)
| Field | Type | Description |
|-------|------|-------------|
| `open` | float | Opening price |
| `high` | float | Highest price |
| `low` | float | Lowest price |
| `close` | float | Closing price |
| `price_change_1m` | float | Close - Open |
| `price_change_percent_1m` | float | % change |

#### Volume (1-Minute)
| Field | Type | Description |
|-------|------|-------------|
| `total_volume_1m` | float | Total volume traded |
| `total_buy_volume_1m` | float | Total buy volume |
| `total_sell_volume_1m` | float | Total sell volume |
| `trade_count_1m` | int | Number of trades |
| `avg_buy_sell_ratio_1m` | float | Average buy/sell ratio |

#### 24-Hour Stats (Pass-through)
| Field | Type | Description |
|-------|------|-------------|
| `volume_24h` | float | 24-hour volume from ticker |
| `high_24h` | float | 24-hour high |
| `low_24h` | float | 24-hour low |

#### Market Microstructure (1-Minute Aggregated)
| Field | Type | Description |
|-------|------|-------------|
| `volatility_1m` | float | Standard deviation of 1s returns |
| `order_imbalance_ratio_1m` | float | (Buy - Sell) / (Buy + Sell) |
| `volatility_regime` | string | "low", "medium", or "high" |
| `avg_bid_ask_spread_1m` | float | Average spread |
| `avg_depth_2pct_1m` | float | Average order book depth |
| `avg_bid_depth_2pct_1m` | float | Average bid depth |
| `avg_ask_depth_2pct_1m` | float | Average ask depth |
| `avg_vwap_1m` | float | Average VWAP |
| `avg_micro_price_deviation_1m` | float | Average price deviation from mid |
| `cvd_1m` | float | Cumulative volume delta (latest) |
| `total_ofi_1m` | float | Total order flow imbalance |
| `avg_ofi_1m` | float | Average order flow imbalance |
| `avg_kyles_lambda_1m` | float | Average market impact coefficient |
| `avg_liquidity_health_1m` | float | Average liquidity health |
| `avg_mid_price_1m` | float | Average mid-price |
| `latest_best_bid_1m` | float | Best bid at window end |
| `latest_best_ask_1m` | float | Best ask at window end |

### File Organization
- **Path**: `gs://crypto-db-east1/year=YYYY/month=MM/day=DD/hour=HH/btc_1min_agg+0+XXXXXXXXXX.snappy.parquet`
- **Frequency**: 1 file per hour (60 rows)
- **Compression**: Snappy
- **Timezone**: All timestamps in EST/EDT

## Data Pipeline Flow

### 1️⃣ Coinbase WebSocket → Producer (70+ fields, 1 msg/sec)

**Raw Data Received:**
- **Ticker channel**: `price`, `volume_24h`, `high_24h`, `low_24h`, `open_24h`, `best_bid`, `best_ask`
- **Matches channel**: `price`, `size`, `side` (buy/sell)
- **Level2 channel**: Order book snapshot & updates (top 50 bids/asks)

**Producer Calculates (Real-time, 1-second windows):**

#### Price Metrics
- `price`, `high_1s`, `low_1s`, `price_change_1s`, `price_change_percent_1s`
- Rolling windows: `high_1min`, `low_1min`, `price_change_1min`, `price_change_percent_1min`
- Hourly: `high_1h`, `low_1h`, `price_change_1h`, `price_change_percent_1h`
- Daily: `price_change_24h`, `price_change_percent_24h`

#### Volume Metrics
- `volume_1s`, `buy_volume_1s`, `sell_volume_1s`, `buy_sell_volume_ratio_1s`, `trade_count_1s`
- Rolling: `volume_1min`, `buy_volume_1min`, `sell_volume_1min`
- Hourly: `volume_1h`, `buy_volume_1h`, `sell_volume_1h`

#### Microstructure Metrics (from Order Book + Trades)
- `bid_ask_spread_1s`, `mid_price_1s`, `best_bid_1s`, `best_ask_1s`
- `depth_2pct_1s`, `bid_depth_2pct_1s`, `ask_depth_2pct_1s`
- `vwap_1s`, `micro_price_deviation_1s`
- `cvd_1s` (cumulative), `ofi_1s`, `kyles_lambda_1s`, `liquidity_health_1s`

**Publishes to**: `BTC-USD` Kafka topic (1 message/second, 70+ fields)

---

### 2️⃣ Aggregator → 1-Minute Windows (37 fields, 1 msg/min)

**Consumes from**: `BTC-USD` topic (60 messages = 1 minute)

**Aggregation Logic:**

#### OHLC (calculated from 60 prices)
- `open` ← first price
- `high` ← max price
- `low` ← min price
- `close` ← last price
- `price_change_1m`, `price_change_percent_1m`

#### Volume (sum of 60 seconds)
- `total_volume_1m` ← sum(`volume_1s`)
- `total_buy_volume_1m` ← sum(`buy_volume_1s`)
- `total_sell_volume_1m` ← sum(`sell_volume_1s`)
- `trade_count_1m` ← sum(`trade_count_1s`)
- `avg_buy_sell_ratio_1m` ← average

#### Microstructure (averaged over 60 seconds)
- `avg_bid_ask_spread_1m` ← mean(`bid_ask_spread_1s`)
- `avg_depth_2pct_1m` ← mean(`depth_2pct_1s`)
- `avg_bid_depth_2pct_1m`, `avg_ask_depth_2pct_1m`
- `avg_vwap_1m` ← mean(`vwap_1s`)
- `avg_micro_price_deviation_1m` ← mean(`micro_price_deviation_1s`)
- `avg_ofi_1m` ← mean(`ofi_1s`)
- `total_ofi_1m` ← sum(`ofi_1s`)
- `avg_kyles_lambda_1m` ← mean(`kyles_lambda_1s`)
- `avg_liquidity_health_1m` ← mean(`liquidity_health_1s`)
- `avg_mid_price_1m` ← mean(`mid_price_1s`)
- `latest_best_bid_1m`, `latest_best_ask_1m` ← last values

#### Calculated Metrics
- `volatility_1m` ← std dev of price returns
- `order_imbalance_ratio_1m` ← (Buy - Sell) / (Buy + Sell)
- `volatility_regime` ← "low" (<0.1%), "medium" (<0.5%), "high" (>0.5%)
- `cvd_1m` ← latest cumulative value

#### Pass-through (from last tick)
- `volume_24h`, `high_24h`, `low_24h`

#### Metadata
- `window_start`, `window_end`, `window_start_est`
- `num_ticks` (should be 60)
- `last_ingestion_time`

**Publishes to**: `btc_1min_agg` Kafka topic (1 message/minute, 37 fields)

---

### 3️⃣ GCS Consumer → Parquet Files (37 fields stored)

**Consumes from**: `btc_1min_agg` topic

**Storage Format:**
- Path: `gs://crypto-db-east1/year=YYYY/month=MM/day=DD/hour=HH/`
- File: `btc_1min_agg+0+XXXXXXXXXX.snappy.parquet`
- Frequency: 1 file/hour (60 rows)
- Compression: Snappy

**All 37 Fields Stored in GCS:**
1. `symbol`
2. `window_start`
3. `window_end`
4. `window_start_est`
5. `open`
6. `high`
7. `low`
8. `close`
9. `price_change_1m`
10. `price_change_percent_1m`
11. `total_volume_1m`
12. `total_buy_volume_1m`
13. `total_sell_volume_1m`
14. `trade_count_1m`
15. `avg_buy_sell_ratio_1m`
16. `volume_24h`
17. `high_24h`
18. `low_24h`
19. `volatility_1m`
20. `order_imbalance_ratio_1m`
21. `volatility_regime`
22. `avg_bid_ask_spread_1m`
23. `avg_depth_2pct_1m`
24. `avg_bid_depth_2pct_1m`
25. `avg_ask_depth_2pct_1m`
26. `avg_vwap_1m`
27. `avg_micro_price_deviation_1m`
28. `cvd_1m`
29. `total_ofi_1m`
30. `avg_ofi_1m`
31. `avg_kyles_lambda_1m`
32. `avg_liquidity_health_1m`
33. `avg_mid_price_1m`
34. `latest_best_bid_1m`
35. `latest_best_ask_1m`
36. `last_ingestion_time`
37. `num_ticks`

### Example Row (Parquet)
```json
{
  "symbol": "BTC-USD",
  "window_start": 1733270400000,
  "window_end": 1733270460000,
  "window_start_est": "2025-12-03 19:00:00",
  "open": 97500.00,
  "high": 97525.00,
  "low": 97490.00,
  "close": 97520.00,
  "price_change_1m": 20.00,
  "price_change_percent_1m": 0.021,
  "total_volume_1m": 2.5643,
  "total_buy_volume_1m": 1.3421,
  "total_sell_volume_1m": 1.2222,
  "trade_count_1m": 45,
  "avg_buy_sell_ratio_1m": 1.098,
  "volume_24h": 12500.50,
  "high_24h": 98000.00,
  "low_24h": 96500.00,
  "volatility_1m": 0.035,
  "order_imbalance_ratio_1m": 0.047,
  "volatility_regime": "low",
  "avg_bid_ask_spread_1m": 2.50,
  "avg_depth_2pct_1m": 125.34,
  "avg_bid_depth_2pct_1m": 65.20,
  "avg_ask_depth_2pct_1m": 60.14,
  "avg_vwap_1m": 97512.50,
  "avg_micro_price_deviation_1m": 0.15,
  "cvd_1m": 15.67,
  "total_ofi_1m": 2.34,
  "avg_ofi_1m": 0.039,
  "avg_kyles_lambda_1m": 0.00015,
  "avg_liquidity_health_1m": 45.67,
  "avg_mid_price_1m": 97510.00,
  "latest_best_bid_1m": 97518.00,
  "latest_best_ask_1m": 97522.00,
  "last_ingestion_time": "2025-12-03T19:00:00-05:00",
  "num_ticks": 60
}
```

---

## Example Message

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
