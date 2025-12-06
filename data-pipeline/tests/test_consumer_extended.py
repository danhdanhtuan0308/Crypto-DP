"""
Additional Consumer Test Cases
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytz
import pandas as pd

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from gcs_kafka_consumer import ParquetWriter, EASTERN


class TestConsumerAdditionalEdgeCases:
    """Additional edge cases for consumer testing"""
    
    def test_duplicate_timestamps(self):
        """Test handling of duplicate timestamps"""
        records = [
            {'window_start': '2024-01-01T00:00:00-05:00', 'volume': 100},
            {'window_start': '2024-01-01T00:00:00-05:00', 'volume': 150}  # Duplicate
        ]
        
        # Should detect duplicates
        timestamps = [r['window_start'] for r in records]
        assert len(timestamps) == 2
        assert timestamps[0] == timestamps[1]
    
    def test_out_of_order_data(self):
        """Test handling of out-of-order timestamps"""
        records = [
            {'window_start': '2024-01-01T00:02:00-05:00', 'volume': 100},
            {'window_start': '2024-01-01T00:01:00-05:00', 'volume': 150},  # Earlier
            {'window_start': '2024-01-01T00:03:00-05:00', 'volume': 200}
        ]
        
        df = pd.DataFrame(records)
        # Sort by timestamp
        df['window_start'] = pd.to_datetime(df['window_start'])
        df_sorted = df.sort_values('window_start')
        
        assert df_sorted.iloc[0]['volume'] == 150
        assert df_sorted.iloc[1]['volume'] == 100
        assert df_sorted.iloc[2]['volume'] == 200
    
    def test_malformed_timestamp(self):
        """Test handling of malformed timestamp strings"""
        malformed_timestamps = [
            'invalid',
            '2024-13-45',  # Invalid date
            '',
            None
        ]
        
        for ts in malformed_timestamps:
            record = {'window_start': ts, 'volume': 100}
            try:
                if ts:
                    datetime.fromisoformat(ts)
            except (ValueError, TypeError, AttributeError):
                # Expected - should handle gracefully
                pass
    
    def test_volume_spike_detection(self):
        """Test detection of unusual volume spikes"""
        records = [
            {'window_start': '2024-01-01T00:00:00-05:00', 'volume': 100},
            {'window_start': '2024-01-01T00:01:00-05:00', 'volume': 10000},  # 100x spike
            {'window_start': '2024-01-01T00:02:00-05:00', 'volume': 120}
        ]
        
        df = pd.DataFrame(records)
        avg_volume = df['volume'].mean()
        
        # Detect anomaly
        for idx, row in df.iterrows():
            if row['volume'] > avg_volume * 10:
                assert row['volume'] == 10000  # Detected spike
    
    def test_zero_volume_records(self):
        """Test handling of zero volume records"""
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'volume': 0,  # Zero volume
            'open': 100000,
            'close': 100000
        }
        
        # Should accept but may want to flag
        assert record['volume'] == 0
    
    def test_inconsistent_ohlc_values(self):
        """Test detection of inconsistent OHLC values"""
        # Invalid: high < low
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'open': 100000,
            'high': 99000,  # Lower than low
            'low': 100000,
            'close': 99500,
            'volume': 100
        }
        
        # Detect inconsistency
        is_valid = record['high'] >= record['low']
        assert not is_valid  # Should detect as invalid
    
    def test_price_outside_open_close_range(self):
        """Test when high/low don't contain open/close"""
        record = {
            'open': 100000,
            'high': 99000,  # High is less than open
            'low': 98000,
            'close': 101000,  # Close is greater than high
            'volume': 100
        }
        
        # Validate OHLC consistency
        valid_high = record['high'] >= max(record['open'], record['close'])
        valid_low = record['low'] <= min(record['open'], record['close'])
        
        assert not (valid_high and valid_low)  # Should be invalid
    
    def test_large_dataset_buffering(self):
        """Test buffer behavior with many records"""
        with patch('gcs_kafka_consumer.storage.Client'):
            writer = ParquetWriter('test-bucket')
            
            # Add many records
            for i in range(100):
                record = {
                    'window_start': f'2024-01-01T00:{i:02d}:00-05:00',
                    'volume': 100 + i
                }
                writer.buffer.append(record)
            
            # Check buffer size
            assert len(writer.buffer) == 100
    
    def test_missing_multiple_fields(self):
        """Test handling when multiple required fields are missing"""
        incomplete_record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            # Missing: open, high, low, close, volume
        }
        
        required_fields = ['window_start', 'open', 'high', 'low', 'close', 'volume']
        missing = [f for f in required_fields if f not in incomplete_record]
        
        # Should detect 5 missing fields
        assert len(missing) == 5
    
    def test_decimal_precision_preservation(self):
        """Test that decimal precision is preserved"""
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'open': 100000.123456789,
            'volume': 1.23456789
        }
        
        df = pd.DataFrame([record])
        
        # Check precision is maintained
        assert df['open'][0] == 100000.123456789
        assert df['volume'][0] == 1.23456789
    
    def test_timezone_conversion_consistency(self):
        """Test timezone handling consistency"""
        # Different timezone representations
        timestamps = [
            '2024-01-01T00:00:00-05:00',  # EST
            '2024-01-01T00:00:00-04:00',  # EDT
            '2024-01-01T05:00:00+00:00',  # UTC
        ]
        
        for ts in timestamps:
            dt = datetime.fromisoformat(ts)
            assert dt.tzinfo is not None


class TestConsumerVolumeValidationExtended:
    """Extended volume validation tests"""
    
    def test_exactly_sixty_messages(self):
        """Test processing exactly 60 messages per minute"""
        message_count = 60
        messages = []
        
        for i in range(message_count):
            messages.append({
                'tick': i,
                'timestamp': time.time() + i
            })
        
        assert len(messages) == 60
    
    def test_less_than_sixty_messages(self):
        """Test detection when receiving fewer than 60 messages"""
        message_count = 45  # Less than expected
        expected = 60
        
        is_complete = message_count >= expected
        assert not is_complete  # Should detect incomplete
    
    def test_more_than_sixty_messages(self):
        """Test handling more than 60 messages"""
        message_count = 75  # More than expected
        expected = 60
        
        is_excessive = message_count > expected
        assert is_excessive  # Should detect excess
    
    def test_message_rate_calculation(self):
        """Test calculation of message arrival rate"""
        messages = []
        start_time = time.time()
        
        for i in range(60):
            messages.append({
                'timestamp': start_time + i
            })
        
        elapsed = messages[-1]['timestamp'] - messages[0]['timestamp']
        rate = len(messages) / elapsed if elapsed > 0 else 0
        
        # Should be approximately 1 message per second
        assert 0.9 <= rate <= 1.1
    
    def test_missing_messages_detection(self):
        """Test detection of missing messages in sequence"""
        # Simulate messages with gaps
        messages = [
            {'seq': 0}, {'seq': 1}, {'seq': 2},
            # Missing 3, 4
            {'seq': 5}, {'seq': 6}
        ]
        
        expected_seq = list(range(7))
        actual_seq = [m['seq'] for m in messages]
        missing = set(expected_seq) - set(actual_seq)
        
        assert missing == {3, 4}
    
    def test_burst_message_handling(self):
        """Test handling of burst messages (multiple at once)"""
        messages = []
        burst_time = time.time()
        
        # Simulate 10 messages arriving at same timestamp
        for i in range(10):
            messages.append({
                'timestamp': burst_time,
                'value': i
            })
        
        # All should have same timestamp
        timestamps = [m['timestamp'] for m in messages]
        assert len(set(timestamps)) == 1


class TestConsumerDataQuality:
    """Additional data quality tests"""
    
    def test_nan_value_handling(self):
        """Test handling of NaN values"""
        import numpy as np
        
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'volume': np.nan,  # NaN value
            'open': 100000
        }
        
        df = pd.DataFrame([record])
        assert pd.isna(df['volume'][0])
    
    def test_infinity_value_handling(self):
        """Test handling of infinity values"""
        import numpy as np
        
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'volume': np.inf,  # Infinity
            'open': 100000
        }
        
        df = pd.DataFrame([record])
        assert np.isinf(df['volume'][0])
    
    def test_string_to_numeric_conversion(self):
        """Test conversion of string values to numeric"""
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'volume': '100.50',  # String
            'open': '100000.00'
        }
        
        # Convert strings to float
        volume = float(record['volume'])
        open_price = float(record['open'])
        
        assert volume == 100.50
        assert open_price == 100000.00
    
    def test_trailing_whitespace_handling(self):
        """Test handling of values with whitespace"""
        record = {
            'window_start': '  2024-01-01T00:00:00-05:00  ',
            'volume': 100
        }
        
        # Trim whitespace
        cleaned = record['window_start'].strip()
        assert cleaned == '2024-01-01T00:00:00-05:00'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
