#!/bin/bash

# Troubleshooting Script for Microstructure Metrics
# This script helps diagnose why metrics are showing as 0

echo "================================================"
echo "🔍 Microstructure Metrics Troubleshooting"
echo "================================================"
echo ""

# Check if producer is running
echo "1️⃣ Checking if coinbase_kafka_producer.py is running..."
if ps aux | grep -q "[c]oinbase_kafka_producer.py"; then
    echo "✅ Producer is running"
    echo ""
    echo "⚠️  ACTION REQUIRED: You need to RESTART the producer!"
    echo "   The old producer doesn't have Level 2 order book support."
    echo ""
    echo "   Steps to restart:"
    echo "   1. Kill old producer: pkill -f coinbase_kafka_producer.py"
    echo "   2. Start new producer: python data-pipeline/coinbase_kafka_producer.py"
else
    echo "❌ Producer is NOT running"
    echo ""
    echo "   Start it with: python data-pipeline/coinbase_kafka_producer.py"
fi

echo ""
echo "------------------------------------------------"

# Check if aggregator is running
echo "2️⃣ Checking if kafka_1min_aggregator.py is running..."
if ps aux | grep -q "[k]afka_1min_aggregator.py"; then
    echo "✅ Aggregator is running"
else
    echo "❌ Aggregator is NOT running"
    echo ""
    echo "   Start it with: python data-pipeline/kafka_1min_aggregator.py"
fi

echo ""
echo "------------------------------------------------"

# Check if numpy is installed
echo "3️⃣ Checking dependencies..."
if python3 -c "import numpy" 2>/dev/null; then
    echo "✅ numpy is installed"
else
    echo "❌ numpy is NOT installed"
    echo ""
    echo "   Install it with: pip install -r requirements.txt"
fi

echo ""
echo "================================================"
echo "🔧 Quick Fix Commands"
echo "================================================"
echo ""
echo "# 1. Kill old processes"
echo "pkill -f coinbase_kafka_producer.py"
echo "pkill -f kafka_1min_aggregator.py"
echo ""
echo "# 2. Install dependencies"
echo "pip install -r requirements.txt"
echo ""
echo "# 3. Start new producer (with Level 2 support)"
echo "python data-pipeline/coinbase_kafka_producer.py &"
echo ""
echo "# 4. Start aggregator"
echo "python data-pipeline/kafka_1min_aggregator.py &"
echo ""
echo "# 5. Check logs"
echo "tail -f nohup.out  # or wherever your logs are"
echo ""
echo "================================================"
echo "📊 What to Look For in Logs"
echo "================================================"
echo ""
echo "✅ Producer logs should show:"
echo "   • '📚 Order book snapshot received: 50 bids, 50 asks'"
echo "   • '📤 Sent X | Price: \$87809.34 | Spread: \$2.50'"
echo "   • Spread > 0, Depth > 0, OFI != 0"
echo ""
echo "✅ Aggregator logs should show:"
echo "   • '✅ Agg #X | Spread: \$2.35 | Depth: 26.89'"
echo "   • NOT showing all zeros"
echo ""
echo "❌ If you see:"
echo "   • Spread: \$0.00, Depth: 0.00 → Order book not received"
echo "   • Connection errors → Check Coinbase API access"
echo ""
echo "================================================"
echo "🚨 Common Issues"
echo "================================================"
echo ""
echo "Issue 1: All metrics are 0"
echo "  → Producer is running OLD code without Level 2"
echo "  → Solution: Restart producer with new code"
echo ""
echo "Issue 2: Connection drops frequently"
echo "  → Level 2 has high bandwidth requirements"
echo "  → Solution: Check network, may need VPN"
echo ""
echo "Issue 3: Order book snapshot never received"
echo "  → Coinbase may be blocking Level 2 requests"
echo "  → Solution: Check Coinbase status page"
echo ""
echo "================================================"
echo ""
