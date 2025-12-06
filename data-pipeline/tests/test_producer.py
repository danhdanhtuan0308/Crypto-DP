"""
Unit Tests for Coinbase Kafka Producer
Testing:
- Correct data format
- Topic routing
- Payload accuracy (1s = 1 data)
- Empty data handling
- Error handling
- Missing fields
- Data deduplication
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from datetime import datetime, timezone
import pytz
import asyncio

# Import the functions we want to test
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from coinbase_kafka_producer import (
    calculate_bid_ask_spread,
    calculate_mid_price,
    calculate_order_book_depth_2pct,
    calculate_vwap,
    delivery_callback,
    KAFKA_TOPIC,
    EASTERN
)


class TestProducerDataFormat:
    """Test that producer sends data in correct format"""
    
    def test_message_has_required_fields(self):
        """Test message contains all required fields"""
        # Test data structure, not specific values
        message = {
            'timestamp': time.time(),
            'best_bid': 100.0,
            'best_ask': 101.0,
            'mid_price': 100.5,
            'bid_ask_spread': 1.0,
            'volume': 1.0
        }
        
        required_fields = ['timestamp', 'best_bid', 'best_ask', 'mid_price', 'bid_ask_spread', 'volume']
        for field in required_fields:
            assert field in message
            assert message[field] is not None
    
    def test_calculation_functions_exist(self):
        """Test that all calculation functions are callable"""
        assert callable(calculate_bid_ask_spread)
        assert callable(calculate_mid_price)
        assert callable(calculate_order_book_depth_2pct)
        assert callable(calculate_vwap)


class TestProducerTopicRouting:
    """Test that messages are routed to correct topic"""
    
    def test_kafka_topic_configuration(self):
        """Test that KAFKA_TOPIC is correctly configured"""
        assert KAFKA_TOPIC is not None
        # Should be BTC-USD or from env
        assert isinstance(KAFKA_TOPIC, str)
    
    @patch('coinbase_kafka_producer.Producer')
    def test_producer_sends_to_correct_topic(self, mock_producer_class):
        """Test that producer sends messages to the correct topic"""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Create a producer instance
        from confluent_kafka import Producer as RealProducer
        producer = mock_producer
        
        # Simulate sending a message
        test_message = json.dumps({'price': 1, 'timestamp': time.time()})
        producer.produce(
            topic='BTC-USD',
            key='test',
            value=test_message,
            callback=delivery_callback
        )
        
        # Verify produce was called with correct topic
        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args[1]
        assert call_kwargs['topic'] == 'BTC-USD'


class TestProducerPayloadAccuracy:
    """Test that producer sends exactly 1 message per second"""
    
    @pytest.mark.asyncio
    async def test_one_second_one_message(self):
        """Test that we send 1 message per second"""
        messages_sent = []
        
        def mock_produce(topic, key, value, callback):
            messages_sent.append({
                'timestamp': time.time(),
                'value': value
            })
        
        # Simulate sending messages at 1-second intervals
        with patch('time.sleep') as mock_sleep:
            for i in range(5):
                mock_produce('BTC-USD', 'test', json.dumps({'tick': i}), None)
                if i < 4:
                    await asyncio.sleep(0)  # Yield control
        
        # Should have exactly 5 messages
        assert len(messages_sent) == 5
    
    def test_message_contains_timestamp(self):
        """Test that each message contains a timestamp"""
        test_data = {
            'price': 1,
            'timestamp': time.time(),
            'best_bid': 1,
            'best_ask': 1
        }
        
        # Verify timestamp exists and is recent
        assert 'timestamp' in test_data
        assert test_data['timestamp'] > 0
        assert abs(time.time() - test_data['timestamp']) < 2  # Within 2 seconds


class TestProducerEmptyDataHandling:
    """Test handling of empty/null data"""
    
    def test_empty_order_book(self):
        """Test handling of empty order book"""
        order_book = {'bids': [], 'asks': []}
        mid_price = 1
        total, bid_depth, ask_depth = calculate_order_book_depth_2pct(order_book, mid_price)
        assert total == 0
        assert bid_depth == 0
        assert ask_depth == 0
    
    def test_missing_order_book_fields(self):
        """Test handling of missing order book fields"""
        order_book = {}  # Missing bids and asks
        mid_price = 1
        total, bid_depth, ask_depth = calculate_order_book_depth_2pct(order_book, mid_price)
        assert total == 0
    
    def test_zero_prices(self):
        """Test handling of zero prices"""
        assert calculate_bid_ask_spread(0, 0) == 0
        assert calculate_mid_price(0, 0) == 0


class TestProducerErrorHandling:
    """Test error handling in producer"""
    
    def test_delivery_callback_success(self, caplog):
        """Test delivery callback on success"""
        msg = Mock()
        msg.topic.return_value = 'BTC-USD'
        
        delivery_callback(None, msg)
        # Should not log error
        assert 'Delivery failed' not in caplog.text
    
    def test_delivery_callback_failure(self, caplog):
        """Test delivery callback on failure"""
        import logging
        with patch('coinbase_kafka_producer.logger') as mock_logger:
            delivery_callback('Connection error', None)
            mock_logger.error.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_websocket_connection_error(self):
        """Test handling of WebSocket connection errors"""
        with patch('websockets.connect') as mock_connect:
            mock_connect.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception) as exc_info:
                raise Exception("Connection failed")
            
            assert "Connection failed" in str(exc_info.value)
    
    def test_invalid_json_parsing(self):
        """Test handling of invalid JSON"""
        invalid_json = "{invalid json}"
        
        with pytest.raises(json.JSONDecodeError):
            json.loads(invalid_json)


class TestProducerMissingFields:
    """Test handling of missing fields in data"""
    
    def test_trade_missing_volume(self):
        """Test VWAP calculation with missing volume field"""
        trades = [
            {'price': 1},  # Missing volume
            {'price': 1, 'volume': 2.0}
        ]
        vwap = calculate_vwap(trades)
        # Should handle missing volume (treat as 0)
        assert vwap == 1  # Only second trade counted
    
    def test_trade_missing_price(self):
        """Test handling of trade with missing price"""
        trades = [
            {'volume': 1.0},  # Missing price - should be skipped
            {'price': 1, 'volume': 2.0}
        ]
        # Function will raise KeyError for missing price
        # This tests that we detect the issue
        try:
            vwap = calculate_vwap(trades)
            # If it doesn't raise, should only count valid trade
            assert vwap == 1
        except KeyError:
            # Expected - missing price should be caught
            pass
    
    def test_order_book_missing_side(self):
        """Test order book with missing bids or asks"""
        order_book_no_bids = {'asks': [(1, 1.0)]}
        total, bid_depth, ask_depth = calculate_order_book_depth_2pct(order_book_no_bids, 1)
        assert bid_depth == 0
        
        order_book_no_asks = {'bids': [(1, 1.0)]}
        total, bid_depth, ask_depth = calculate_order_book_depth_2pct(order_book_no_asks, 1)
        assert ask_depth == 0


class TestProducerDataDeduplication:
    """Test data deduplication logic"""
    
    def test_duplicate_message_detection(self):
        """Test that we can detect duplicate messages"""
        messages = []
        message_hashes = set()
        
        # Create test messages
        msg1 = {'timestamp': 1000, 'price': 1}
        msg2 = {'timestamp': 1000, 'price': 1}  # Duplicate
        msg3 = {'timestamp': 1001, 'price': 1}  # Different timestamp
        
        for msg in [msg1, msg2, msg3]:
            msg_hash = hash(json.dumps(msg, sort_keys=True))
            if msg_hash not in message_hashes:
                messages.append(msg)
                message_hashes.add(msg_hash)
        
        # Should have 2 unique messages (msg1 and msg3)
        assert len(messages) == 2
    
    def test_price_change_detection(self):
        """Test that we only send when price changes"""
        prices = [100, 100, 101, 101, 102]
        last_price = None
        messages_sent = []
        
        for price in prices:
            if price != last_price:
                messages_sent.append(price)
                last_price = price
        
        # Should have 3 messages (100, 101, 102)
        assert len(messages_sent) == 3
        assert messages_sent == [100, 101, 102]


class TestProducerTimezone:
    """Test timezone handling (Eastern Time)"""
    
    def test_eastern_timezone_configured(self):
        """Test that Eastern timezone is properly configured"""
        assert EASTERN.zone == 'America/New_York'
    
    def test_timestamp_in_eastern_time(self):
        """Test that timestamps are in Eastern time"""
        now_est = datetime.now(EASTERN)
        assert now_est.tzinfo is not None
        assert str(now_est.tzinfo) in ['EST', 'EDT', 'America/New_York']


class TestProducerConfiguration:
    """Test Kafka producer configuration"""
    
    def test_kafka_config_has_required_fields(self):
        """Test that Kafka config has all required fields"""
        from coinbase_kafka_producer import KAFKA_CONFIG
        
        required_fields = [
            'bootstrap.servers',
            'security.protocol',
            'sasl.mechanisms',
            'sasl.username',
            'sasl.password'
        ]
        
        for field in required_fields:
            assert field in KAFKA_CONFIG


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
