#!/usr/bin/env python3
"""
Comprehensive test for Coinbase Level 2 order book
Tests multiple channel names and configurations
"""

import asyncio
import json
import websockets
import time

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

async def test_level2_channels():
    """Test different Level 2 channel configurations"""
    
    # Test configurations
    test_cases = [
        {
            "name": "level2 (standard)",
            "channels": ["level2"]
        },
        {
            "name": "level2_batch (batched updates)",
            "channels": ["level2_batch"]
        },
        {
            "name": "All channels together",
            "channels": ["ticker", "matches", "level2"]
        },
        {
            "name": "Just ticker + level2",
            "channels": ["ticker", "level2"]
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n{'='*70}")
        print(f"TEST {i}/{len(test_cases)}: {test['name']}")
        print(f"{'='*70}")
        
        try:
            async with websockets.connect(COINBASE_WS_URL) as websocket:
                subscribe_message = {
                    "type": "subscribe",
                    "product_ids": [PRODUCT_ID],
                    "channels": test['channels']
                }
                
                await websocket.send(json.dumps(subscribe_message))
                print(f"📤 Sent subscription for: {test['channels']}")
                print("-" * 70)
                
                # Wait for confirmation and initial messages
                snapshot_received = False
                l2update_received = False
                timeout_seconds = 10
                start_time = time.time()
                
                while (time.time() - start_time) < timeout_seconds:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                        data = json.loads(message)
                        msg_type = data.get('type')
                        
                        if msg_type == 'subscriptions':
                            confirmed_channels = data.get('channels', [])
                            print(f"✅ Subscription confirmed:")
                            for ch in confirmed_channels:
                                print(f"   • {ch['name']}: {ch.get('product_ids', [])}")
                            
                            # Check if level2 was confirmed
                            channel_names = [ch['name'] for ch in confirmed_channels]
                            if 'level2' in channel_names or 'level2_batch' in channel_names:
                                print(f"   🎉 Level 2 subscription ACCEPTED!")
                            else:
                                print(f"   ⚠️  Level 2 subscription was REJECTED or IGNORED")
                        
                        elif msg_type == 'snapshot':
                            snapshot_received = True
                            num_bids = len(data.get('bids', []))
                            num_asks = len(data.get('asks', []))
                            print(f"📚 Order book SNAPSHOT received!")
                            print(f"   Bids: {num_bids}, Asks: {num_asks}")
                            if num_bids > 0:
                                print(f"   Best 3 bids: {data['bids'][:3]}")
                            if num_asks > 0:
                                print(f"   Best 3 asks: {data['asks'][:3]}")
                            
                            # Calculate depth within 2%
                            if num_bids > 0 and num_asks > 0:
                                best_bid = float(data['bids'][0][0])
                                best_ask = float(data['asks'][0][0])
                                mid_price = (best_bid + best_ask) / 2
                                
                                bid_threshold = mid_price * 0.98
                                ask_threshold = mid_price * 1.02
                                
                                bid_depth = sum(float(v) for p, v in data['bids'] if float(p) >= bid_threshold)
                                ask_depth = sum(float(v) for p, v in data['asks'] if float(p) <= ask_threshold)
                                
                                print(f"   📊 Depth Analysis (±2%):")
                                print(f"      Mid-price: ${mid_price:.2f}")
                                print(f"      Bid depth: {bid_depth:.4f} BTC")
                                print(f"      Ask depth: {ask_depth:.4f} BTC")
                                print(f"      Total depth: {bid_depth + ask_depth:.4f} BTC")
                                
                                print(f"\n   ✅ ALL 3 METRICS CAN BE CALCULATED!")
                                return True  # Success!
                        
                        elif msg_type == 'l2update':
                            if not l2update_received:
                                l2update_received = True
                                changes = data.get('changes', [])
                                print(f"📊 L2 UPDATE received: {len(changes)} changes")
                                if changes:
                                    print(f"   Example: {changes[0]}")
                                print(f"   ✅ OFI (Order Flow Imbalance) CAN BE CALCULATED!")
                        
                        # If we got snapshot, we're good
                        if snapshot_received:
                            print(f"\n{'='*70}")
                            print(f"✅ SUCCESS! Level 2 works with: {test['channels']}")
                            print(f"{'='*70}\n")
                            return True
                    
                    except asyncio.TimeoutError:
                        continue
                
                # Timeout for this test
                if not snapshot_received:
                    print(f"\n⚠️  TIMEOUT - No snapshot received for: {test['channels']}")
                    print(f"   This configuration doesn't provide Level 2 data\n")
        
        except Exception as e:
            print(f"❌ ERROR with {test['channels']}: {e}\n")
        
        # Small delay between tests
        await asyncio.sleep(1)
    
    # None of the tests succeeded
    print(f"\n{'='*70}")
    print(f"❌ CONCLUSION: Level 2 is NOT accessible in your environment")
    print(f"{'='*70}")
    print(f"\nPossible reasons:")
    print(f"1. Geographic restrictions")
    print(f"2. Coinbase API rate limiting")
    print(f"3. Network/firewall blocking")
    print(f"4. Account-level restrictions (may need Coinbase Pro account)")
    print(f"\nSolution: Use ticker fallback (already implemented)")
    print(f"You'll get 6 out of 9 metrics, which is enough for ML models.")
    return False


if __name__ == "__main__":
    print("\n🔍 COMPREHENSIVE LEVEL 2 TEST")
    print("Testing multiple configurations to find what works...\n")
    
    result = asyncio.run(test_level2_channels())
    
    if result:
        print("\n✅ GOOD NEWS: Level 2 works!")
        print("   Update producer to use the working configuration")
        print("   All 9 metrics will be available")
    else:
        print("\n⚠️  Level 2 unavailable")
        print("   Ticker fallback is already implemented")
        print("   6 out of 9 metrics will work")
