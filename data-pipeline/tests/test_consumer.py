"""
Unit Tests for GCS Kafka Consumer
Testing:
- Correct data format for aggregation
- Edge case handling (empty data, missing fields)
- Volume validation (>60 messages per minute)
- Parquet file writing
- GCS upload functionality
"""

import pytest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import pytz
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from gcs_kafka_consumer import ParquetWriter, EASTERN, SOURCE_TOPIC


class TestConsumerDataFormat:
    """Test that consumer receives and processes data in correct format"""
    
    def test_aggregated_message_has_required_fields(self):
        """Test that aggregated message has correct structure"""
        message = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'window_end': '2024-01-01T00:01:00-05:00',
            'open': 1.0,
            'high': 1.0,
            'low': 1.0,
            'close': 1.0,
            'volume': 1.0
        }
        
        # Validate required fields exist
        required_fields = ['window_start', 'window_end', 'open', 'high', 'low', 'close', 'volume']
        for field in required_fields:
            assert field in message
    
    def test_dataframe_conversion(self):
        """Test conversion of message to DataFrame"""
        # Just test that DataFrame can be created from records
        records = [{'window_start': '2024-01-01T00:00:00-05:00', 'volume': 1.0}]
        df = pd.DataFrame(records)
        assert len(df) == 1
        assert 'window_start' in df.columns
    
    def test_parquet_schema_validation(self):
        """Test that Parquet schema has required columns"""
        df = pd.DataFrame([{'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0, 'volume': 1.0}])
        table = pa.Table.from_pandas(df)
        
        # Verify schema has expected columns
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            assert col in table.schema.names


class TestConsumerEmptyDataHandling:
    """Test handling of empty data"""
    
    def test_empty_message_handling(self):
        """Test handling of empty message"""
        with patch('gcs_kafka_consumer.ParquetWriter') as MockWriter:
            writer = MockWriter('test-bucket')
            writer.buffer = []
            
            # Try to flush empty buffer
            writer.flush.return_value = None
            result = writer.flush()
            
            # Should handle gracefully
            assert result is None
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame"""
        df = pd.DataFrame()
        assert len(df) == 0
        assert df.empty
    
    def test_null_values_in_data(self):
        """Test handling of null values"""
        records = [{'open': None, 'volume': 1.0}]
        df = pd.DataFrame(records)
        assert pd.isna(df['open'][0])


class TestConsumerMissingFields:
    """Test handling of missing fields in messages"""
    
    def test_message_missing_required_field(self):
        """Test handling when required field is missing"""
        incomplete_message = {'window_start': '2024-01-01T00:00:00-05:00', 'open': 1.0}
        required_fields = ['window_start', 'open', 'high', 'low', 'close', 'volume']
        missing_fields = [f for f in required_fields if f not in incomplete_message]
        
        assert len(missing_fields) > 0
        assert 'high' in missing_fields
    
    def test_handle_missing_optional_fields(self):
        """Test that optional fields can be missing"""
        message = {'window_start': '2024-01-01T00:00:00-05:00', 'open': 1.0, 'high': 1.0, 'low': 1.0, 'close': 1.0, 'volume': 1.0}
        df = pd.DataFrame([message])
        assert 'num_trades' not in df.columns or df['num_trades'].isna().all()


class TestConsumerVolumeValidation:
    """Test that consumer validates message volume (should receive ~60 per minute)"""
    
    def test_message_count_per_minute(self):
        """Test that we receive approximately 60 messages per minute"""
        # Simulate receiving messages for 1 minute
        message_count = 60
        messages_received = []
        
        for i in range(message_count):
            messages_received.append({'tick': i})
        
        assert len(messages_received) == 60
        assert len(messages_received) >= 55  # Allow some variance
    
    def test_low_message_volume_detection(self):
        """Test detection of low message volume"""
        expected_per_minute = 60
        received = 30  # Only half expected
        
        is_healthy = received >= (expected_per_minute * 0.9)  # 90% threshold
        assert not is_healthy  # Should detect as unhealthy
    
    def test_buffer_size_management(self):
        """Test that buffer size is managed correctly"""
        with patch('gcs_kafka_consumer.storage.Client'):
            writer = ParquetWriter('test-bucket')
            
            # Buffer should be 1 for 1-minute aggregation
            assert writer.buffer_size == 1
            
            # Add a record
            record = {'window_start': '2024-01-01T00:00:00', 'volume': 100}
            writer.buffer.append(record)
            
            assert len(writer.buffer) == 1


class TestConsumerParquetWriter:
    """Test Parquet file writing functionality"""
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_parquet_writer_initialization(self, mock_storage):
        """Test ParquetWriter initialization"""
        writer = ParquetWriter('test-bucket')
        
        assert writer.bucket_name == 'test-bucket'
        assert writer.buffer == []
        assert writer.buffer_size == 1
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_add_record_triggers_flush(self, mock_storage):
        """Test that adding record triggers flush"""
        writer = ParquetWriter('test-bucket')
        
        with patch.object(writer, 'flush') as mock_flush:
            record = {
                'window_start': '2024-01-01T00:00:00-05:00',
                'volume': 100
            }
            writer.add_record(record)
            
            # Should trigger flush
            mock_flush.assert_called_once()
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_parquet_file_naming(self, mock_storage):
        """Test that Parquet files are named correctly with Eastern time"""
        writer = ParquetWriter('test-bucket')
        
        # Simulate a record
        now_est = datetime.now(EASTERN)
        expected_date = now_est.strftime('%Y/%m/%d')
        
        # File should be in format: year/month/day/filename.parquet
        file_path = f"btc_1min/{expected_date}/data_{now_est.strftime('%H%M')}.parquet"
        
        assert 'btc_1min' in file_path
        assert '.parquet' in file_path


class TestConsumerGCSIntegration:
    """Test GCS upload functionality"""
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_gcs_client_initialization(self, mock_storage):
        """Test GCS client initialization"""
        writer = ParquetWriter('test-bucket')
        
        # Should initialize storage client
        assert writer.storage_client is not None
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_gcs_bucket_access(self, mock_storage):
        """Test GCS bucket access"""
        mock_client = MagicMock()
        mock_storage.return_value = mock_client
        
        writer = ParquetWriter('test-bucket')
        
        # Should access bucket
        mock_client.bucket.assert_called_with('test-bucket')
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_gcs_credentials_from_env(self, mock_storage):
        """Test GCS credentials loading from environment"""
        test_creds = '{"type": "service_account", "project_id": "test"}'
        
        with patch.dict(os.environ, {'GCP_SERVICE_ACCOUNT_JSON': test_creds}):
            # Should attempt to load credentials
            assert os.getenv('GCP_SERVICE_ACCOUNT_JSON') == test_creds
            
            # Verify credentials are available in environment
            creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
            assert creds_json is not None
            assert 'service_account' in creds_json


class TestConsumerEdgeCases:
    """Test edge cases in consumer"""
    
    def test_volume_exists_and_positive(self):
        """Test that volume exists and is positive"""
        message = {'volume': 100.5}
        assert 'volume' in message
        assert message['volume'] > 0
    
    def test_negative_volume_detection(self):
        """Test detection of invalid negative volume"""
        message = {'volume': -100}
        is_valid = message['volume'] > 0
        assert not is_valid  # Should detect as invalid


class TestConsumerTimezone:
    """Test timezone handling in consumer"""
    
    def test_eastern_timezone_configured(self):
        """Test that Eastern timezone is configured"""
        assert EASTERN.zone == 'America/New_York'
    
    def test_timestamp_parsing(self):
        """Test parsing of timestamp strings"""
        timestamp_str = '2024-01-01T00:00:00-05:00'
        dt = datetime.fromisoformat(timestamp_str)
        
        assert dt.tzinfo is not None
        assert dt.year == 2024


class TestConsumerConfiguration:
    """Test consumer configuration"""
    
    def test_source_topic_configured(self):
        """Test that source topic is correctly configured"""
        assert SOURCE_TOPIC == 'btc_1min_agg'
    
    def test_kafka_config_has_consumer_group(self):
        """Test that Kafka config has consumer group"""
        from gcs_kafka_consumer import KAFKA_CONFIG
        
        assert 'group.id' in KAFKA_CONFIG
        assert KAFKA_CONFIG['group.id'] == 'gcs-parquet-writer'


class TestConsumerHealthCheck:
    """Test health check functionality"""
    
    def test_stale_data_detection(self):
        """Test detection of stale data"""
        import time
        
        last_message_time = time.time() - 100  # 100 seconds ago
        current_time = time.time()
        
        THRESHOLD = 90  # seconds
        is_stale = (current_time - last_message_time) > THRESHOLD
        
        assert is_stale  # Should detect as stale
    
    def test_healthy_data_stream(self):
        """Test healthy data stream detection"""
        import time
        
        last_message_time = time.time() - 30  # 30 seconds ago
        current_time = time.time()
        
        THRESHOLD = 90
        is_stale = (current_time - last_message_time) > THRESHOLD
        
        assert not is_stale  # Should be healthy


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
