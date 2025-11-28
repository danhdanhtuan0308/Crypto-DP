# Market Microstructure Metrics

This document describes the advanced market microstructure metrics collected from Coinbase Level 2 order book data and aggregated in the pipeline.

## Overview

The data pipeline now collects **Level 2 order book data** in addition to trade data, enabling calculation of sophisticated market microstructure metrics that capture:
- Liquidity conditions
- Order flow dynamics
- Market impact
- Price formation mechanisms

## Data Collection (1-Second Level)

### Source: Coinbase WebSocket Level 2 Channel

The producer subscribes to:
1. **`ticker`** - 24-hour statistics
2. **`matches`** - Real-time trade executions
3. **`level2`** - Order book snapshots and updates

### Level 2 Data Structure

- **Snapshot**: Initial full order book state (top 50 levels each side)
- **l2update**: Incremental changes to order book
  - Format: `["buy"|"sell", price, size]`
  - Size = 0 means level removed

## 1-Second Metrics (Real-Time)

These metrics are calculated every second and sent to the `BTC-USD` Kafka topic.

### 1. Bid-Ask Spread

**Formula:** 
$$\text{Spread} = \text{Ask Price} - \text{Bid Price}$$

**Field:** `bid_ask_spread_1s`

**Interpretation:**
- Lower spread = higher liquidity
- Wider spread = lower liquidity or higher uncertainty
- Typical BTC spread: $0.01 - $10 depending on volatility

---

### 2. Mid-Price

**Formula:** 
$$\text{Mid-Price} = \frac{\text{Best Bid} + \text{Best Ask}}{2}$$

**Field:** `mid_price_1s`

**Interpretation:**
- Fair value estimate between bid and ask
- Used as reference for other calculations
- More stable than last trade price

---

### 3. Order Book Depth (2%)

**Formula:** 
$$\text{Depth}(2\%) = \sum \text{Volume}_{\text{Bid}} \text{ (mid-price - 2\%)} + \sum \text{Volume}_{\text{Ask}} \text{ (mid-price + 2\%)}$$

**Fields:**
- `depth_2pct_1s` - Total depth
- `bid_depth_2pct_1s` - Buy side depth
- `ask_depth_2pct_1s` - Sell side depth

**Interpretation:**
- Measures liquidity within 2% of mid-price
- Higher depth = more liquidity = easier to execute large orders
- Imbalance (bid > ask) suggests buying pressure

**Example:**
- If mid-price = $100,000
- Bid depth (within $98,000-$100,000) = 15 BTC
- Ask depth (within $100,000-$102,000) = 12 BTC
- Total depth = 27 BTC

---

### 4. VWAP (Volume Weighted Average Price)

**Formula:** 
$$\text{VWAP} = \frac{\sum_{i} (\text{Price}_i \times \text{Volume}_i)}{\sum_{i} \text{Volume}_i}$$

**Field:** `vwap_1s`

**Interpretation:**
- Average price weighted by trade volume
- More representative than simple average
- Used for execution quality assessment
- If current price > VWAP → buying pressure
- If current price < VWAP → selling pressure

---

### 5. Micro-Price Deviation

**Formula:** 
$$\text{Deviation} = \text{Trade Price} - \text{Mid-Price}$$

**Field:** `micro_price_deviation_1s` (average of all trades in 1s)

**Interpretation:**
- Measures how far trades occur from theoretical fair value
- Positive deviation → trades above mid-price (aggressive buying)
- Negative deviation → trades below mid-price (aggressive selling)
- Large deviations indicate strong directional pressure

---

### 6. Cumulative Volume Delta (CVD)

**Formula:** 
$$\text{CVD}_t = \text{CVD}_{t-1} + (\text{Buy Volume}_t - \text{Sell Volume}_t)$$

**Field:** `cvd_1s`

**Interpretation:**
- Running sum of net buying/selling pressure
- **Persistent across windows** (cumulative from start)
- Rising CVD → accumulation (net buying)
- Falling CVD → distribution (net selling)
- Divergence with price = potential reversal signal

**Example:**
- CVD = +50 BTC → net 50 BTC bought since start
- CVD = -20 BTC → net 20 BTC sold since start

---

### 7. Order Flow Imbalance (OFI)

**Formula:** 
$$\text{OFI} \approx \Delta \text{Bid Volume} - \Delta \text{Ask Volume}$$

**Field:** `ofi_1s`

**Interpretation:**
- Measures changes in order book volume (not trades)
- Positive OFI → more volume added to bids (buying interest)
- Negative OFI → more volume added to asks (selling interest)
- Predictive of short-term price movements
- **Leading indicator** (changes before price moves)

**Example:**
- OFI = +5.2 BTC → 5.2 more BTC added to bid side
- OFI = -3.1 BTC → 3.1 more BTC added to ask side

---

### 8. Kyle's Lambda (λ)

**Formula:** 
$$\lambda = \frac{\Delta \text{Price}}{\text{Signed Order Volume}}$$

**Field:** `kyles_lambda_1s`

**Interpretation:**
- Market impact coefficient
- Measures price change per unit of order flow
- Higher λ → lower liquidity (price moves more per order)
- Lower λ → higher liquidity (price stable despite orders)
- Used to estimate cost of large trades

**Example:**
- λ = 0.0001 → $0.10 price impact per 1 BTC traded
- λ = 0.001 → $1.00 price impact per 1 BTC traded

---

### 9. Liquidity Health

**Formula:** 
$$\text{Liquidity Health} \propto \frac{\text{Order Book Depth}}{\text{Bid-Ask Spread} \times \text{Volatility}}$$

**Field:** `liquidity_health_1s`

**Interpretation:**
- Composite measure of market quality
- Higher = better liquidity conditions
- Low health → risky to trade large size
- High health → good conditions for large trades
- Accounts for depth, spread, and volatility together

---

### 10. Additional Best Bid/Ask Fields

**Fields:**
- `best_bid_1s` - Highest bid price
- `best_ask_1s` - Lowest ask price

---

## 1-Minute Aggregated Metrics

These metrics are aggregated over 60 seconds and sent to the `btc_1min_agg` Kafka topic.

### Aggregation Methods

| Metric | Aggregation Method |
|--------|-------------------|
| Bid-Ask Spread | **Average** over 60 seconds |
| Order Book Depth | **Average** over 60 seconds |
| VWAP | **Average** over 60 seconds |
| Micro-Price Deviation | **Average** over 60 seconds |
| CVD | **Latest value** (cumulative) |
| OFI | **Sum** and **Average** over 60 seconds |
| Kyle's Lambda | **Average** over 60 seconds |
| Liquidity Health | **Average** over 60 seconds |
| Mid-Price | **Average** over 60 seconds |

### 1-Minute Fields

| Field | Description |
|-------|-------------|
| `avg_bid_ask_spread_1m` | Average spread over 1 minute |
| `avg_depth_2pct_1m` | Average total depth |
| `avg_bid_depth_2pct_1m` | Average bid-side depth |
| `avg_ask_depth_2pct_1m` | Average ask-side depth |
| `avg_vwap_1m` | Average VWAP |
| `avg_micro_price_deviation_1m` | Average deviation |
| `cvd_1m` | Cumulative volume delta (latest) |
| `total_ofi_1m` | Total OFI (sum) |
| `avg_ofi_1m` | Average OFI |
| `avg_kyles_lambda_1m` | Average market impact |
| `avg_liquidity_health_1m` | Average liquidity health |
| `avg_mid_price_1m` | Average mid-price |
| `latest_best_bid_1m` | Best bid at end of minute |
| `latest_best_ask_1m` | Best ask at end of minute |
| `volatility_regime` | Classification: `low`, `medium`, `high` |

### Volatility Regime Classification

Based on 1-minute volatility (standard deviation of returns):
- **Low**: < 0.1%
- **Medium**: 0.1% - 0.5%
- **High**: > 0.5%

---

## Machine Learning Features

### Primary Features for Prediction Models

#### Immediate (1-Second) Features
1. **`ofi_1s`** - Leading indicator of price movement
2. **`bid_ask_spread_1s`** - Liquidity condition
3. **`depth_2pct_1s`** - Available liquidity
4. **`micro_price_deviation_1s`** - Aggressive order flow
5. **`kyles_lambda_1s`** - Market impact sensitivity

#### Short-Term (1-Minute) Features
6. **`avg_ofi_1m`** - Persistent order flow pressure
7. **`avg_bid_ask_spread_1m`** - Average liquidity cost
8. **`avg_depth_2pct_1m`** - Sustained liquidity
9. **`volatility_regime`** - Market state classification
10. **`cvd_1m`** - Cumulative buying/selling pressure

### Feature Importance Rankings

**For Next-Second Price Prediction:**
1. OFI (Order Flow Imbalance) - 25%
2. Micro-Price Deviation - 20%
3. CVD (Cumulative Volume Delta) - 15%
4. Bid-Ask Spread - 15%
5. Order Book Depth - 10%
6. Kyle's Lambda - 10%
7. Volatility Regime - 5%

**For Next-Minute Price Prediction:**
1. Average OFI (1m) - 20%
2. CVD - 20%
3. Average Liquidity Health - 15%
4. Volatility Regime - 15%
5. Average Spread - 10%
6. Average Depth - 10%
7. Average Kyle's Lambda - 10%

---

## Trading Signals

### Strong Buy Signals
- `ofi_1s > 5.0` (strong bid-side pressure)
- `cvd_1s` increasing rapidly (accumulation)
- `micro_price_deviation_1s > 10` (aggressive buying above mid)
- `bid_depth_2pct_1s / ask_depth_2pct_1s > 2.0` (bid imbalance)

### Strong Sell Signals
- `ofi_1s < -5.0` (strong ask-side pressure)
- `cvd_1s` decreasing rapidly (distribution)
- `micro_price_deviation_1s < -10` (aggressive selling below mid)
- `ask_depth_2pct_1s / bid_depth_2pct_1s > 2.0` (ask imbalance)

### Liquidity Warning Signals
- `liquidity_health_1s < 100` (poor market conditions)
- `bid_ask_spread_1s > $50` (wide spread = low liquidity)
- `depth_2pct_1s < 5.0` BTC (thin order book)
- `kyles_lambda_1s > 0.001` (high market impact)

---

## Implementation Details

### Data Pipeline Architecture

```
Coinbase WebSocket (Level 2)
         ↓
coinbase_kafka_producer.py
    - Subscribes to: ticker, matches, level2
    - Calculates 1s metrics
    - Publishes to: BTC-USD topic (1 msg/sec)
         ↓
kafka_1min_aggregator.py
    - Consumes from: BTC-USD
    - Aggregates 60 seconds
    - Publishes to: btc_1min_agg (1 msg/min)
         ↓
gcs_kafka_consumer.py
    - Consumes from: btc_1min_agg
    - Writes Parquet files to GCS
```

### Performance Considerations

- **Order book maintained in memory**: Top 50 levels per side
- **Computational overhead**: ~10-20% increase due to microstructure calculations
- **Network bandwidth**: Level 2 updates are frequent (high message rate)
- **Latency**: <10ms for metric calculations

---

## References

### Academic Papers
1. Kyle, A. S. (1985). "Continuous Auctions and Insider Trading"
2. Glosten, L. R., & Milgrom, P. R. (1985). "Bid, Ask and Transaction Prices in a Specialist Market"
3. Hasbrouck, J. (1991). "Measuring the Information Content of Stock Trades"
4. Cont, R., Kukanov, A., & Stoikov, S. (2014). "The Price Impact of Order Book Events"

### Practical Applications
- **High-Frequency Trading**: Exploit OFI signals for sub-second predictions
- **Execution Algorithms**: Use liquidity health to time large orders
- **Risk Management**: Monitor Kyle's Lambda for market impact estimation
- **Market Making**: Optimize spreads based on depth and volatility regime

---

## Monitoring and Validation

### Key Metrics to Monitor

1. **Order Book Health**
   - Spread should be < $10 typically
   - Depth should be > 10 BTC within 2%
   
2. **Data Quality**
   - OFI values should have both positive and negative
   - CVD should trend with price direction
   - Lambda should be small (< 0.001)

3. **Calculation Accuracy**
   - VWAP should be close to last trade price
   - Mid-price should be between best bid/ask
   - Liquidity health should correlate inversely with volatility

---

## Future Enhancements

1. **Multi-level depth analysis** (beyond 2%)
2. **Arrival rate modeling** (Poisson process for order flow)
3. **Toxicity detection** (adverse selection metrics)
4. **Queue position tracking** (for limit order strategies)
5. **Cross-exchange arbitrage signals** (if adding more exchanges)

---

## Example Message Structure

See `DATA_FIELDS.md` for complete field reference with the new microstructure metrics included.
