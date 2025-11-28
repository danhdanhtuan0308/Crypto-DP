#!/usr/bin/env python3
"""
Test what fields are available in Coinbase ticker channel
"""

import asyncio
import json
import websockets

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"

async def test_ticker_fields():
    print("🔌 Testing Coinbase Ticker channel fields...")
    print("-" * 60)
    
    try:
        async with websockets.connect(COINBASE_WS_URL) as websocket:
            subscribe_message = {
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": ["ticker"]
            }
            
            await websocket.send(json.dumps(subscribe_message))
            print("✅ Subscribed to ticker channel")
            print("-" * 60)
            
            # Get first ticker message
            for _ in range(10):
                message = await websocket.recv()
                data = json.loads(message)
                
                if data.get('type') == 'ticker':
                    print("📊 TICKER MESSAGE FIELDS:")
                    print(json.dumps(data, indent=2))
                    print("-" * 60)
                    
                    # Check for bid/ask
                    if 'best_bid' in data and 'best_ask' in data:
                        print("✅ Ticker HAS best_bid and best_ask!")
                        print(f"   Best Bid: ${data['best_bid']}")
                        print(f"   Best Ask: ${data['best_ask']}")
                        spread = float(data['best_ask']) - float(data['best_bid'])
                        print(f"   Spread: ${spread:.2f}")
                        print()
                        print("🎉 We can calculate spread without Level 2!")
                    else:
                        print("❌ Ticker does NOT have best_bid/best_ask")
                        print("   Available fields:", list(data.keys()))
                    break
    
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(test_ticker_fields())
