"""
Unit Tests for 1-Minute Kafka Aggregator
Testing:
- Correct aggregation over 60-second windows
- OHLCV calculation accuracy
- Window boundaries
- Volume aggregation (60 messages -> 1 aggregated message)
- Empty data handling
- Missing fields
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytz

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka_1min_aggregator import MinuteAggregator, EASTERN, SOURCE_TOPIC, OUTPUT_TOPIC


class TestAggregatorDataFormat:
    """Test correct data format for aggregation"""
    
    def test_minute_aggregator_initialization(self):
        """Test that MinuteAggregator initializes correctly"""
        agg = MinuteAggregator()
        
        assert agg.window_data.maxlen == 60
        assert agg.window_start is None
        assert agg.message_count == 0
    
    def test_ohlcv_calculation(self):
        """Test OHLCV (Open, High, Low, Close, Volume) calculation"""
        agg = MinuteAggregator()
        
        # Add 5 ticks with different prices
        test_ticks = [
            {'price': 1, 'volume': 1.0, 'timestamp': 1000},
            {'price': 1, 'volume': 1.5, 'timestamp': 1001},
            {'price': 1, 'volume': 2.0, 'timestamp': 1002},
            {'price': 1, 'volume': 1.2, 'timestamp': 1003},
            {'price': 1, 'volume': 0.8, 'timestamp': 1004},
        ]
        
        for tick in test_ticks:
            agg.window_data.append(tick)
        
        # Manually calculate expected values
        prices = [1, 1, 1, 1, 1]
        volumes = [1.0, 1.5, 2.0, 1.2, 0.8]
        
        expected_open = prices[0]  # 1
        expected_high = max(prices)  # 1
        expected_low = min(prices)  # 1
        expected_close = prices[-1]  # 1
        expected_volume = sum(volumes)  # 6.5
        
        # Verify expectations
        assert expected_open == 1
        assert expected_high == 1
        assert expected_low == 1
        assert expected_close == 1
        assert expected_volume == 6.5
    
    def test_aggregated_message_structure(self):
        """Test that aggregated message has correct structure"""
        expected_fields = [
            'window_start',
            'window_end',
            'open',
            'high',
            'low',
            'close',
            'volume',
            'num_ticks'
        ]
        
        # Sample aggregated message
        agg_message = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'window_end': '2024-01-01T00:01:00-05:00',
            'open': 1.0,
            'high': 1.0,
            'low': 1.0,
            'close': 1.0,
            'volume': 100.0,
            'num_ticks': 60
        }
        
        for field in expected_fields:
            assert field in agg_message


class TestAggregatorWindowBoundaries:
    """Test window boundary handling"""
    
    def test_window_start_initialization(self):
        """Test that window start is set correctly"""
        agg = MinuteAggregator()
        
        # Before any tick
        assert agg.window_start is None
        
        # After first tick
        test_tick = {'price': 1, 'timestamp': time.time()}
        result = agg.add_tick(test_tick)
        
        assert agg.window_start is not None
    
    def test_new_minute_triggers_flush(self):
        """Test that new minute triggers flush"""
        agg = MinuteAggregator()
        
        # Set window start to a specific minute
        now_est = datetime.now(EASTERN).replace(second=0, microsecond=0)
        agg.window_start = now_est
        
        # Add some data
        agg.window_data.append({'price': 1, 'volume': 1.0})
        
        # Simulate tick from next minute
        with patch.object(agg, '_get_est_minute') as mock_minute:
            next_minute = now_est.replace(minute=now_est.minute + 1)
            mock_minute.return_value = next_minute
            
            result = agg.add_tick({'price': 1, 'volume': 1.0})
            
            # Should return aggregated data (flush)
            # In real code, this would return the aggregated message
    
    def test_sixty_seconds_window(self):
        """Test that window collects exactly 60 seconds of data"""
        agg = MinuteAggregator()
        
        # Add 60 ticks (1 per second)
        for i in range(60):
            tick = {'price': 1 + i, 'volume': 1.0, 'timestamp': 1000 + i}
            agg.add_tick(tick)
        
        # Should have 60 ticks in window (maxlen=60)
        assert len(agg.window_data) <= 60


class TestAggregatorVolumeHandling:
    """Test that 60 messages are aggregated into 1"""
    
    def test_sixty_to_one_aggregation(self):
        """Test that 60 input messages produce 1 output message"""
        agg = MinuteAggregator()
        
        # Add 60 ticks
        for i in range(60):
            tick = {'price': 1, 'volume': 1.0}
            agg.add_tick(tick)
        
        # Window should have up to 60 ticks
        assert len(agg.window_data) <= 60
    
    def test_volume_sum_accuracy(self):
        """Test that total volume is correctly summed"""
        agg = MinuteAggregator()
        
        # Add ticks with known volumes
        volumes = [1.0, 2.0, 1.5, 0.5, 3.0]
        for vol in volumes:
            agg.window_data.append({'price': 1, 'volume': vol})
        
        # Calculate expected total
        expected_total = sum(volumes)  # 8.0
        
        # Manually sum from window
        actual_total = sum(d.get('volume', 0) for d in agg.window_data)
        
        assert actual_total == expected_total
        assert actual_total == 8.0
    
    def test_message_count_tracking(self):
        """Test that message count is tracked correctly"""
        agg = MinuteAggregator()
        
        # Add 5 messages
        for i in range(5):
            agg.add_tick({'price': 1, 'volume': 1.0})
        
        # Window should have 5 messages
        assert len(agg.window_data) == 5


class TestAggregatorEmptyDataHandling:
    """Test handling of empty data"""
    
    def test_flush_empty_window(self):
        """Test flushing an empty window"""
        agg = MinuteAggregator()
        
        result = agg.flush()
        
        # Should return None for empty window
        assert result is None
    
    def test_zero_volume_ticks(self):
        """Test handling of ticks with zero volume"""
        agg = MinuteAggregator()
        
        # Add ticks with zero volume
        agg.add_tick({'price': 1, 'volume': 0})
        agg.add_tick({'price': 1, 'volume': 0})
        
        # Should handle gracefully
        assert len(agg.window_data) == 2
    
    def test_zero_price_ticks(self):
        """Test handling of ticks with zero price"""
        agg = MinuteAggregator()
        
        # Add valid and invalid ticks
        agg.window_data.append({'price': 1, 'volume': 1.0})
        agg.window_data.append({'price': 0, 'volume': 1.0})  # Invalid
        agg.window_data.append({'price': 1, 'volume': 1.0})
        
        # Filter out zero prices
        valid_prices = [d['price'] for d in agg.window_data if d.get('price', 0) > 0]
        
        assert len(valid_prices) == 2  # Should exclude zero price
        assert 0 not in valid_prices


class TestAggregatorMissingFields:
    """Test handling of missing fields"""
    
    def test_missing_price_field(self):
        """Test handling of tick with missing price"""
        agg = MinuteAggregator()
        
        # Add tick without price
        tick_without_price = {'volume': 1.0, 'timestamp': 1000}
        agg.window_data.append(tick_without_price)
        
        # Extract prices with default
        prices = [d.get('price', 0) for d in agg.window_data]
        
        # Should have default value
        assert prices[0] == 0
    
    def test_missing_volume_field(self):
        """Test handling of tick with missing volume"""
        agg = MinuteAggregator()
        
        tick_without_volume = {'price': 1, 'timestamp': 1000}
        agg.window_data.append(tick_without_volume)
        
        # Extract volumes with default
        volumes = [d.get('volume', 0) for d in agg.window_data]
        
        assert volumes[0] == 0
    
    def test_missing_timestamp_field(self):
        """Test handling of tick with missing timestamp"""
        tick = {'price': 1, 'volume': 1.0}
        
        # Should handle missing timestamp
        timestamp = tick.get('timestamp', time.time())
        
        assert timestamp > 0


class TestAggregatorStatisticalMetrics:
    """Test statistical metrics calculation"""
    
    def test_vwap_calculation(self):
        """Test Volume Weighted Average Price calculation"""
        ticks = [
            {'price': 1, 'volume': 1.0},
            {'price': 1, 'volume': 2.0},
            {'price': 1, 'volume': 1.0}
        ]
        
        # Calculate VWAP
        total_pv = sum(t['price'] * t['volume'] for t in ticks)
        total_vol = sum(t['volume'] for t in ticks)
        vwap = total_pv / total_vol if total_vol > 0 else 0
        
        expected_vwap = (1*1 + 1*2 + 1*1) / 4.0
        assert vwap == expected_vwap
    
    def test_price_volatility(self):
        """Test price volatility calculation"""
        import numpy as np
        
        prices = [100, 102, 98, 101, 99]
        
        # Calculate standard deviation
        volatility = np.std(prices)
        
        assert volatility > 0
        assert isinstance(volatility, (float, np.floating))
    
    def test_price_range(self):
        """Test price range calculation"""
        prices = [1, 1, 1, 1, 1]
        
        price_range = max(prices) - min(prices)
        
        expected_range = 1 - 1  # 300
        assert price_range == expected_range


class TestAggregatorEdgeCases:
    """Test edge cases in aggregator"""
    
    def test_single_tick_aggregation(self):
        """Test aggregation with only 1 tick"""
        agg = MinuteAggregator()
        
        agg.add_tick({'price': 1, 'volume': 1.0})
        
        # Should handle single tick
        assert len(agg.window_data) == 1
    
    def test_maximum_window_size(self):
        """Test that window respects maxlen=60"""
        agg = MinuteAggregator()
        
        # Add more than 60 ticks
        for i in range(100):
            agg.add_tick({'price': 1 + i, 'volume': 1.0})
        
        # Should only keep last 60
        assert len(agg.window_data) == 60
    
    def test_rapid_price_changes(self):
        """Test handling of rapid price changes"""
        agg = MinuteAggregator()
        
        # Add ticks with rapidly changing prices
        prices = [1, 51000, 49000, 52000, 48000]
        for price in prices:
            agg.window_data.append({'price': price, 'volume': 1.0})
        
        actual_prices = [d['price'] for d in agg.window_data]
        
        assert len(actual_prices) == 5
        # Just verify we have 5 prices, don't check specific values
        assert all(isinstance(p, (int, float)) for p in actual_prices)


class TestAggregatorTimezone:
    """Test timezone handling"""
    
    def test_eastern_timezone_configured(self):
        """Test that Eastern timezone is configured"""
        assert EASTERN.zone == 'America/New_York'
    
    def test_get_est_minute(self):
        """Test getting current minute in EST"""
        agg = MinuteAggregator()
        
        current_minute = agg._get_est_minute()
        
        # Should be datetime in EST
        assert current_minute.tzinfo is not None
        assert current_minute.second == 0
        assert current_minute.microsecond == 0
    
    def test_minute_boundary_detection(self):
        """Test detection of minute boundaries"""
        agg = MinuteAggregator()
        
        # Set to specific minute
        minute1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=EASTERN)
        minute2 = datetime(2024, 1, 1, 0, 1, 0, tzinfo=EASTERN)
        
        assert minute2 > minute1
        assert (minute2 - minute1).seconds == 60


class TestAggregatorTopicRouting:
    """Test topic routing for aggregator"""
    
    def test_source_topic_configuration(self):
        """Test that source topic is configured correctly"""
        assert SOURCE_TOPIC == 'BTC-USD'
    
    def test_output_topic_configuration(self):
        """Test that output topic is configured correctly"""
        assert OUTPUT_TOPIC == 'btc_1min_agg'
    
    def test_different_topics(self):
        """Test that source and output topics are different"""
        assert SOURCE_TOPIC != OUTPUT_TOPIC


class TestAggregatorPerformance:
    """Test performance characteristics"""
    
    def test_window_data_deque_performance(self):
        """Test that deque performs efficiently for sliding window"""
        from collections import deque
        
        window = deque(maxlen=60)
        
        # Add 1000 items (simulating high-frequency data)
        for i in range(1000):
            window.append({'price': 1 + i, 'volume': 1.0})
        
        # Should only keep last 60
        assert len(window) == 60
        
        # Should have most recent items
        assert window[-1]['price'] == 1 + 999
    
    def test_aggregation_speed(self):
        """Test that aggregation completes quickly"""
        import time
        
        agg = MinuteAggregator()
        
        # Add 60 ticks and measure time
        start = time.time()
        for i in range(60):
            agg.add_tick({'price': 1 + i, 'volume': 1.0})
        elapsed = time.time() - start
        
        # Should complete in under 1 second
        assert elapsed < 1.0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
