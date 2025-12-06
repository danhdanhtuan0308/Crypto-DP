#!/bin/bash

# CI Test Runner Script
# Runs all tests for the Crypto Data Pipeline

set -e

echo "=================================="
echo "Crypto Data Pipeline - CI Testing"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Navigate to data-pipeline directory
cd "$(dirname "$0")"

echo "📦 Installing test dependencies..."
pip install -q -r requirements-test.txt

echo ""
echo "=================================="
echo "🧪 Running Unit Tests"
echo "=================================="

# Producer Tests
echo ""
echo "${YELLOW}Testing Producer...${NC}"
pytest tests/test_producer.py -v --tb=short || {
    echo "${RED}❌ Producer tests failed${NC}"
    exit 1
}
echo "${GREEN}✅ Producer tests passed${NC}"

# Consumer Tests
echo ""
echo "${YELLOW}Testing Consumer...${NC}"
pytest tests/test_consumer.py -v --tb=short || {
    echo "${RED}❌ Consumer tests failed${NC}"
    exit 1
}
echo "${GREEN}✅ Consumer tests passed${NC}"

# Consumer Extended Tests
echo ""
echo "${YELLOW}Testing Consumer Extended Cases...${NC}"
pytest tests/test_consumer_extended.py -v --tb=short || {
    echo "${RED}❌ Consumer extended tests failed${NC}"
    exit 1
}
echo "${GREEN}✅ Consumer extended tests passed${NC}"

# Aggregator Tests
echo ""
echo "${YELLOW}Testing Aggregator...${NC}"
pytest tests/test_aggregator.py -v --tb=short || {
    echo "${RED}❌ Aggregator tests failed${NC}"
    exit 1
}
echo "${GREEN}✅ Aggregator tests passed${NC}"

echo ""
echo "=================================="
echo "🔗 Running Integration Tests"
echo "=================================="
pytest tests/test_integration.py -v --tb=short || {
    echo "${RED}❌ Integration tests failed${NC}"
    exit 1
}
echo "${GREEN}✅ Integration tests passed${NC}"

echo ""
echo "=================================="
echo "📊 Generating Coverage Report"
echo "=================================="
pytest tests/ --cov=. --cov-report=term-missing --cov-report=html --cov-report=xml

echo ""
echo "=================================="
echo "${GREEN}✅ All Tests Passed!${NC}"
echo "=================================="
echo ""
echo "📈 Coverage report generated:"
echo "   - HTML: htmlcov/index.html"
echo "   - XML: coverage.xml"
echo ""
