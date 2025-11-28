#!/usr/bin/env python3
"""
Quick test to verify Coinbase Level 2 connection works
Run this before starting the full producer
"""

import asyncio
import json
import websockets

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

async def test_level2_connection():
    print("🔌 Testing Coinbase Level 2 WebSocket connection...")
    print(f"URL: {COINBASE_WS_URL}")
    print(f"Product: {PRODUCT_ID}")
    print("-" * 60)
    
    try:
        async with websockets.connect(COINBASE_WS_URL) as websocket:
            # Subscribe to level2
            subscribe_message = {
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": ["level2", "matches"]
            }
            
            await websocket.send(json.dumps(subscribe_message))
            print("✅ Subscription sent")
            print("-" * 60)
            
            # Wait for messages (up to 30 seconds)
            snapshot_received = False
            l2update_received = False
            match_received = False
            
            timeout = 30
            start_time = asyncio.get_event_loop().time()
            
            while (asyncio.get_event_loop().time() - start_time) < timeout:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    msg_type = data.get('type')
                    
                    if msg_type == 'subscriptions':
                        print(f"✅ Subscription confirmed: {data.get('channels', [])}")
                    
                    elif msg_type == 'snapshot':
                        snapshot_received = True
                        num_bids = len(data.get('bids', []))
                        num_asks = len(data.get('asks', []))
                        print(f"📚 Order book snapshot received!")
                        print(f"   Bids: {num_bids}, Asks: {num_asks}")
                        if num_bids > 0 and num_asks > 0:
                            best_bid = float(data['bids'][0][0])
                            best_ask = float(data['asks'][0][0])
                            spread = best_ask - best_bid
                            print(f"   Best Bid: ${best_bid:.2f}")
                            print(f"   Best Ask: ${best_ask:.2f}")
                            print(f"   Spread: ${spread:.2f}")
                    
                    elif msg_type == 'l2update':
                        if not l2update_received:
                            l2update_received = True
                            changes = data.get('changes', [])
                            print(f"📊 L2 update received: {len(changes)} changes")
                    
                    elif msg_type == 'match':
                        if not match_received:
                            match_received = True
                            price = float(data.get('price', 0))
                            size = float(data.get('size', 0))
                            side = data.get('side')
                            print(f"💱 Trade match received: {side} {size:.4f} @ ${price:.2f}")
                    
                    # Break if we got all message types
                    if snapshot_received and l2update_received and match_received:
                        print("-" * 60)
                        print("✅ ALL MESSAGE TYPES RECEIVED SUCCESSFULLY!")
                        print()
                        print("📋 Summary:")
                        print(f"   • Order book snapshot: ✅")
                        print(f"   • Order book updates: ✅")
                        print(f"   • Trade matches: ✅")
                        print()
                        print("🎉 Your connection is working! You can start the producer.")
                        return True
                
                except asyncio.TimeoutError:
                    print("⏳ Waiting for messages...")
                    continue
            
            # Timeout reached
            print("-" * 60)
            print("⚠️  TIMEOUT REACHED (30 seconds)")
            print()
            print("📋 Summary:")
            print(f"   • Order book snapshot: {'✅' if snapshot_received else '❌'}")
            print(f"   • Order book updates: {'✅' if l2update_received else '❌'}")
            print(f"   • Trade matches: {'✅' if match_received else '❌'}")
            print()
            
            if not snapshot_received:
                print("❌ ISSUE: Order book snapshot not received")
                print("   This means microstructure metrics will be 0")
                print("   Possible causes:")
                print("   • Coinbase API is slow to send snapshot")
                print("   • Network issues")
                print("   • Coinbase blocking Level 2 requests")
                return False
            else:
                print("✅ Snapshot received - metrics should work!")
                return True
    
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print()
        print("Possible issues:")
        print("• Network connectivity")
        print("• Coinbase API unavailable")
        print("• Firewall blocking WebSocket")
        return False

if __name__ == "__main__":
    print()
    result = asyncio.run(test_level2_connection())
    print()
    
    if result:
        print("✅ READY TO START PRODUCER")
        print()
        print("Run: python data-pipeline/coinbase_kafka_producer.py")
    else:
        print("❌ FIX ISSUES BEFORE STARTING PRODUCER")
        print()
        print("The producer will start but metrics will be 0")
    
    print()
