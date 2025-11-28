# 🚨 Microstructure Metrics Showing Zero - Solution

## The Problem

You're seeing these fields as `0` in the `btc_1min_agg` Kafka topic:
```json
{
  "avg_bid_ask_spread_1m": 0,
  "avg_depth_2pct_1m": 0,
  "total_ofi_1m": 0,
  "avg_liquidity_health_1m": 0,
  ...
}
```

## Root Cause

**The producer needs to be restarted with the new code!**

The old producer was running **without Level 2 order book support**. It was only subscribed to:
- `ticker` (24h stats)
- `matches` (trades)

The new code subscribes to:
- `ticker`
- `matches`  
- **`level2`** ← NEW! (order book)

Without Level 2 data, all microstructure metrics default to 0.

---

## Solution - Step by Step

### Step 1: Test Level 2 Connection (Optional but Recommended)

```bash
# Test if Coinbase Level 2 works
python test_level2_connection.py
```

**Expected output:**
```
✅ Order book snapshot received!
✅ L2 update received
✅ Trade match received
🎉 Your connection is working!
```

**If you see errors:**
- Network issues → Check internet connection
- Firewall → Check if WebSocket port 443 is open
- Coinbase blocking → Try VPN or different network

### Step 2: Stop Old Processes

```bash
# Kill any running producers/aggregators
pkill -f coinbase_kafka_producer.py
pkill -f kafka_1min_aggregator.py
```

### Step 3: Install Dependencies

```bash
# Make sure numpy is installed
pip install -r requirements.txt
```

### Step 4: Start New Producer

```bash
# Start producer with Level 2 support
python data-pipeline/coinbase_kafka_producer.py
```

**Look for these log messages:**
```
📚 Order book snapshot received: 50 bids, 50 asks  ← MUST SEE THIS!
Subscribed to Coinbase BTC-USD channels: ticker, matches, level2
📤 Sent 10 | Price: $87,809.34 | Spread: $2.50 | Depth: 27.34 BTC
```

**Red flags (bad):**
```
Spread: $0.00    ← Order book not working
Depth: 0.00 BTC  ← No order book data
```

### Step 5: Start Aggregator

```bash
# Start aggregator
python data-pipeline/kafka_1min_aggregator.py
```

**Look for:**
```
✅ Agg #42 | Spread: $2.35 | Depth: 26.89 | Vol: 0.0234% | λ: 0.000123
```

**NOT (bad):**
```
Spread: $0.00 | Depth: 0.00  ← Still getting zeros from old producer
```

### Step 6: Verify Kafka Topic

```bash
# Check the btc_1min_agg topic
confluent kafka topic consume btc_1min_agg --from-beginning | tail -1 | jq .
```

**You should now see:**
```json
{
  "avg_bid_ask_spread_1m": 2.35,      ← NOT ZERO!
  "avg_depth_2pct_1m": 26.89,         ← NOT ZERO!
  "total_ofi_1m": 12.34,              ← NOT ZERO!
  "avg_liquidity_health_1m": 1205.43  ← NOT ZERO!
}
```

---

## Troubleshooting

### Issue 1: Still seeing zeros after restart

**Check:**
```bash
# Make sure producer is running NEW code
ps aux | grep coinbase_kafka_producer.py

# Check producer logs
tail -f <log_file>  # wherever you're logging
```

**Look for:**
- "Order book snapshot received" ← MUST appear
- "Subscribed to ... level2" ← Must include level2 channel

**If NOT appearing:**
- Producer is still running old code
- Solution: Make sure you pulled latest code, then restart

### Issue 2: Order book snapshot never received

**Symptoms:**
```
Subscribed to Coinbase BTC-USD channels: ticker, matches, level2
# But no "Order book snapshot received" message
```

**Possible causes:**
1. **Slow network** - Wait 30 seconds
2. **Coinbase throttling** - Too many requests
3. **API issue** - Check https://status.coinbase.com

**Quick fix:**
```python
# In coinbase_kafka_producer.py, line ~320:
async def send_to_kafka_periodically(producer, state):
    await asyncio.sleep(5)  # Increase from 2 to 5 seconds
```

### Issue 3: Connection drops frequently

**Symptoms:**
```
Connection closed unexpectedly
❌ STALE DATA: No trades for 90s
```

**Cause:** Level 2 has high bandwidth (~100-200 KB/sec)

**Solutions:**
1. **Reduce order book depth:**
```python
# Line ~222 in producer:
'bids': [(float(p), float(v)) for p, v in message.get('bids', [])[:25]],  # 50→25
'asks': [(float(p), float(v)) for p, v in message.get('asks', [])[:25]]
```

2. **Better network:** Use wired connection or VPN

3. **Coinbase issue:** Wait and retry

### Issue 4: Some metrics are zero, others are not

**Example:**
```json
{
  "avg_bid_ask_spread_1m": 2.35,  ← Working
  "avg_depth_2pct_1m": 0,         ← Not working
  "total_ofi_1m": 0               ← Not working
}
```

**Cause:** Order book updates (`l2update`) not being processed

**Check:**
```bash
# Look for l2update messages in logs
grep "l2update" <log_file>
```

**If not found:**
- Only snapshot received, no updates
- Increase timeout or check network

---

## Quick Diagnosis Commands

```bash
# 1. Run troubleshooting script
bash troubleshoot_microstructure.sh

# 2. Test Level 2 connection
python test_level2_connection.py

# 3. Check if producer is running
ps aux | grep coinbase_kafka_producer

# 4. Check producer logs for order book
grep "Order book snapshot" <log_file>
grep "level2" <log_file>

# 5. Consume latest message and check metrics
confluent kafka topic consume btc_1min_agg --from-beginning | tail -1 | jq '.avg_bid_ask_spread_1m'
# Should output a number > 0 (e.g., 2.35)
```

---

## Expected Timeline

After restarting with new code:

- **0-5 seconds:** WebSocket connects, subscriptions sent
- **5-10 seconds:** Order book snapshot received 📚
- **10-60 seconds:** First 1-second message with metrics
- **60-120 seconds:** First 1-minute aggregated message
- **120+ seconds:** Steady stream of non-zero metrics

---

## Summary Checklist

- [ ] Stopped old producer/aggregator processes
- [ ] Pulled latest code with Level 2 support
- [ ] Installed numpy (`pip install -r requirements.txt`)
- [ ] Tested Level 2 connection (`python test_level2_connection.py`)
- [ ] Started new producer
- [ ] Confirmed "Order book snapshot received" in logs
- [ ] Confirmed spread/depth > 0 in producer logs
- [ ] Started aggregator
- [ ] Verified non-zero metrics in `btc_1min_agg` topic

---

## Still Not Working?

If metrics are still zero after following all steps:

1. **Share your logs:**
   ```bash
   # Last 50 lines of producer log
   tail -50 <producer_log_file>
   ```

2. **Check a sample message:**
   ```bash
   # Get latest 1-second message
   confluent kafka topic consume BTC-USD --from-beginning | tail -1 | jq '.'
   ```

3. **Verify code version:**
   ```bash
   # Check if level2 is in subscription
   grep "level2" data-pipeline/coinbase_kafka_producer.py
   # Should find: "channels": ["ticker", "matches", "level2"]
   ```

---

**TL;DR:** Restart the producer! The old one doesn't have Level 2 order book support, so all microstructure metrics are 0.
