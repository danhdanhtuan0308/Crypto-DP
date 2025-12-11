#!/usr/bin/env python3
"""
Test Discord Alerts
Sends test alerts to verify Discord webhook is working
"""

import os
import sys
from dotenv import load_dotenv

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

from alerts import get_discord_alert, get_health_monitor

def main():
    print("Testing Discord Alerts...")
    
    webhook = os.getenv('DISCORD_WEBHOOK_URL')
    if webhook:
        print(f"Webhook URL: {webhook[:50]}...")
    else:
        print("⚠️  DISCORD_WEBHOOK_URL not found in environment")
    
    alert = get_discord_alert()
    monitor = get_health_monitor()
    
    if not alert.enabled:
        print("❌ Discord webhook not configured!")
        print("Set DISCORD_WEBHOOK_URL in .env file")
        return
    
    print("\n1. Testing producer started alert...")
    alert.producer_started("BTC-USD")
    
    print("2. Testing consumer started alert...")
    alert.consumer_started("btc_1min_agg", "gcs-parquet-writer")
    
    print("3. Testing aggregator started alert...")
    alert.aggregator_started("BTC-USD", "btc_1min_agg")
    
    print("4. Testing pipeline healthy alert...")
    status = {
        "Producer": "✅ Running (1234 msgs)",
        "Aggregator": "✅ Running (456 msgs)",
        "GCS Writer": "✅ Running (78 msgs)",
        "Errors": "✅ None"
    }
    alert.pipeline_healthy(status)
    
    print("5. Testing warning alert...")
    alert.consumer_lag_warning("BTC-USD", 150, 100)
    
    print("6. Testing error alert...")
    alert.producer_failed("BTC-USD", "WebSocket connection lost")
    
    print("\n✅ Test alerts sent! Check your Discord channel.")

if __name__ == "__main__":
    main()
