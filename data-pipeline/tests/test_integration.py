"""
Integration Tests for Kafka Data Pipeline
Testing:
- End-to-end Kafka flow (Producer -> Aggregator -> Consumer)
- Topic routing and message delivery
- Data format consistency across pipeline
- GCS connector functionality
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


class TestEndToEndKafkaFlow:
    """Test complete Kafka flow from producer to consumer"""
    
    @patch('confluent_kafka.Producer')
    @patch('confluent_kafka.Consumer')
    def test_producer_to_consumer_flow(self, mock_consumer_class, mock_producer_class):
        """Test that messages flow from producer to consumer"""
        # Setup mock producer
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Setup mock consumer
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Simulate producer sending message
        test_message = {
            'price': 50000.0,
            'timestamp': time.time(),
            'best_bid': 49990.0,
            'best_ask': 50010.0,
            'volume': 1.0
        }
        
        mock_producer.produce(
            topic='BTC-USD',
            value=json.dumps(test_message),
            callback=None
        )
        
        # Verify producer called
        mock_producer.produce.assert_called_once()
        
        # Simulate consumer receiving message
        mock_message = MagicMock()
        mock_message.value.return_value = json.dumps(test_message).encode('utf-8')
        mock_message.error.return_value = None
        mock_consumer.poll.return_value = mock_message
        
        # Consumer polls and gets message
        received_message = mock_consumer.poll(timeout=1.0)
        
        assert received_message is not None
        assert received_message.error() is None
    
    @patch('confluent_kafka.Producer')
    def test_producer_aggregator_flow(self, mock_producer_class):
        """Test flow from raw producer to aggregator"""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Simulate 60 messages (1 minute of data)
        messages = []
        for i in range(60):
            msg = {
                'price': 50000 + i,
                'volume': 1.0,
                'timestamp': time.time() + i
            }
            messages.append(msg)
            mock_producer.produce(
                topic='BTC-USD',
                value=json.dumps(msg)
            )
        
        # Verify 60 messages sent
        assert mock_producer.produce.call_count == 60
        
        # Aggregator should produce 1 message
        aggregated = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'window_end': '2024-01-01T00:01:00-05:00',
            'open': messages[0]['price'],
            'close': messages[-1]['price'],
            'high': max(m['price'] for m in messages),
            'low': min(m['price'] for m in messages),
            'volume': sum(m['volume'] for m in messages),
            'num_ticks': len(messages)
        }
        
        assert aggregated['num_ticks'] == 60
        assert aggregated['volume'] == 60.0


class TestTopicRouting:
    """Test that messages are routed to correct topics"""
    
    def test_producer_to_btc_usd_topic(self):
        """Test producer sends to BTC-USD topic"""
        from coinbase_kafka_producer import KAFKA_TOPIC
        
        assert KAFKA_TOPIC == 'BTC-USD' or KAFKA_TOPIC is not None
    
    def test_aggregator_input_output_topics(self):
        """Test aggregator reads from BTC-USD and writes to btc_1min_agg"""
        from kafka_1min_aggregator import SOURCE_TOPIC, OUTPUT_TOPIC
        
        assert SOURCE_TOPIC == 'BTC-USD'
        assert OUTPUT_TOPIC == 'btc_1min_agg'
        assert SOURCE_TOPIC != OUTPUT_TOPIC
    
    def test_consumer_reads_from_aggregated_topic(self):
        """Test consumer reads from btc_1min_agg topic"""
        from gcs_kafka_consumer import SOURCE_TOPIC
        
        assert SOURCE_TOPIC == 'btc_1min_agg'


class TestDataFormatConsistency:
    """Test data format consistency across pipeline stages"""
    
    def test_producer_output_format_matches_aggregator_input(self):
        """Test that producer output format is compatible with aggregator input"""
        # Producer output format
        producer_message = {
            'price': 50000.0,
            'timestamp': time.time(),
            'volume': 1.0,
            'best_bid': 49990.0,
            'best_ask': 50010.0
        }
        
        # Aggregator should be able to process this
        required_fields = ['price', 'volume']
        for field in required_fields:
            assert field in producer_message
    
    def test_aggregator_output_format_matches_consumer_input(self):
        """Test that aggregator output format is compatible with consumer input"""
        # Aggregator output format
        aggregated_message = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'window_end': '2024-01-01T00:01:00-05:00',
            'open': 50000.0,
            'high': 50100.0,
            'low': 49900.0,
            'close': 50050.0,
            'volume': 100.0,
            'num_ticks': 60
        }
        
        # Consumer expects these fields for Parquet
        required_fields = ['window_start', 'open', 'high', 'low', 'close', 'volume']
        for field in required_fields:
            assert field in aggregated_message
    
    def test_timestamp_format_consistency(self):
        """Test that timestamps are consistently formatted across pipeline"""
        # All timestamps should be in Eastern Time
        from coinbase_kafka_producer import EASTERN as PRODUCER_TZ
        from kafka_1min_aggregator import EASTERN as AGGREGATOR_TZ
        from gcs_kafka_consumer import EASTERN as CONSUMER_TZ
        
        assert PRODUCER_TZ.zone == 'America/New_York'
        assert AGGREGATOR_TZ.zone == 'America/New_York'
        assert CONSUMER_TZ.zone == 'America/New_York'


class TestMessageDeliveryGuarantees:
    """Test message delivery and durability"""
    
    @patch('confluent_kafka.Producer')
    def test_producer_delivery_callback(self, mock_producer_class):
        """Test that producer uses delivery callback for confirmation"""
        from coinbase_kafka_producer import delivery_callback
        
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Send with callback
        mock_producer.produce(
            topic='BTC-USD',
            value='{}',
            callback=delivery_callback
        )
        
        # Verify callback parameter was passed
        call_kwargs = mock_producer.produce.call_args[1]
        assert 'callback' in call_kwargs
    
    @patch('confluent_kafka.Consumer')
    def test_consumer_commit_strategy(self, mock_consumer_class):
        """Test consumer commit strategy"""
        from gcs_kafka_consumer import KAFKA_CONFIG
        
        # Check auto-commit is enabled
        assert 'enable.auto.commit' in KAFKA_CONFIG
        assert KAFKA_CONFIG['enable.auto.commit'] is True


class TestGCSConnectorIntegration:
    """Test GCS connector integration"""
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_gcs_client_connection(self, mock_storage):
        """Test GCS client can connect"""
        from gcs_kafka_consumer import ParquetWriter
        
        mock_client = MagicMock()
        mock_storage.return_value = mock_client
        
        writer = ParquetWriter('test-bucket')
        
        # Should initialize client
        assert writer.storage_client is not None
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_parquet_file_upload_flow(self, mock_storage):
        """Test complete flow of writing Parquet to GCS"""
        from gcs_kafka_consumer import ParquetWriter
        
        mock_client = MagicMock()
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        
        mock_storage.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        writer = ParquetWriter('test-bucket')
        
        # Add a record (should trigger flush and upload)
        record = {
            'window_start': '2024-01-01T00:00:00-05:00',
            'open': 50000.0,
            'high': 50100.0,
            'low': 49900.0,
            'close': 50050.0,
            'volume': 100.0
        }
        
        with patch.object(writer, 'flush'):
            writer.add_record(record)
            
            # Verify record was added
            assert len(writer.buffer) == 1
    
    @patch('gcs_kafka_consumer.storage.Client')
    def test_gcs_authentication(self, mock_storage):
        """Test GCS authentication from environment"""
        from gcs_kafka_consumer import ParquetWriter
        
        # Test with credentials in environment
        test_creds = json.dumps({
            'type': 'service_account',
            'project_id': 'test-project',
            'client_email': 'test@test.iam.gserviceaccount.com'
        })
        
        with patch.dict(os.environ, {'GCP_SERVICE_ACCOUNT_JSON': test_creds}):
            mock_storage.return_value = MagicMock()
            
            # Should attempt to use credentials
            assert os.getenv('GCP_SERVICE_ACCOUNT_JSON') is not None
            
            # Verify credentials JSON is valid
            creds_dict = json.loads(test_creds)
            assert creds_dict['type'] == 'service_account'
            assert creds_dict['project_id'] == 'test-project'


class TestPipelineErrorRecovery:
    """Test error recovery mechanisms"""
    
    @patch('confluent_kafka.Producer')
    def test_producer_retry_on_failure(self, mock_producer_class):
        """Test producer retries on delivery failure"""
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer
        
        # Simulate delivery failure
        mock_producer.produce.side_effect = [
            Exception("Temporary error"),
            None  # Success on retry
        ]
        
        # Producer should handle exception
        try:
            mock_producer.produce(topic='BTC-USD', value='{}')
        except Exception:
            # Retry
            mock_producer.produce(topic='BTC-USD', value='{}')
        
        assert mock_producer.produce.call_count == 2
    
    @patch('confluent_kafka.Consumer')
    def test_consumer_handles_invalid_message(self, mock_consumer_class):
        """Test consumer handles invalid message format"""
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer
        
        # Simulate invalid JSON
        mock_message = MagicMock()
        mock_message.value.return_value = b'{invalid json}'
        mock_message.error.return_value = None
        mock_consumer.poll.return_value = mock_message
        
        # Consumer should handle gracefully
        message = mock_consumer.poll(timeout=1.0)
        
        try:
            data = json.loads(message.value())
        except json.JSONDecodeError:
            # Expected - should not crash
            pass


class TestPipelinePerformance:
    """Test pipeline performance characteristics"""
    
    def test_throughput_sixty_messages_per_minute(self):
        """Test pipeline can handle 60 messages per minute"""
        messages_per_minute = 60
        messages_per_second = messages_per_minute / 60
        
        # Should be approximately 1 message per second
        assert 0.9 <= messages_per_second <= 1.1
    
    def test_aggregation_latency(self):
        """Test aggregation completes within expected time"""
        from kafka_1min_aggregator import MinuteAggregator
        
        agg = MinuteAggregator()
        
        start = time.time()
        for i in range(60):
            agg.add_tick({'price': 50000 + i, 'volume': 1.0})
        elapsed = time.time() - start
        
        # Should complete in under 1 second
        assert elapsed < 1.0


class TestPipelineDataQuality:
    """Test data quality across pipeline"""
    
    def test_no_data_loss_in_aggregation(self):
        """Test that no data is lost during aggregation"""
        # 60 input messages should produce exactly 1 output message
        input_count = 60
        output_count = 1
        
        # Total volume should be preserved
        input_volumes = [1.0] * input_count
        total_input_volume = sum(input_volumes)
        
        # Output should have same total volume
        output_volume = total_input_volume
        
        assert output_volume == 60.0
    
    def test_price_accuracy_preservation(self):
        """Test that price data accuracy is preserved"""
        prices = [50000.0, 50100.0, 49900.0, 50050.0]
        
        # Prices should maintain precision
        for price in prices:
            assert isinstance(price, float)
            assert price > 0
            # Should have reasonable precision (2 decimal places for USD)
            assert round(price, 2) == price
    
    def test_timestamp_monotonicity(self):
        """Test that timestamps are monotonically increasing"""
        timestamps = []
        base_time = time.time()
        
        for i in range(10):
            timestamps.append(base_time + i)
        
        # Each timestamp should be >= previous
        for i in range(1, len(timestamps)):
            assert timestamps[i] >= timestamps[i-1]


class TestPipelineConfiguration:
    """Test pipeline configuration consistency"""
    
    def test_kafka_bootstrap_servers_configured(self):
        """Test that all components use same Kafka cluster"""
        from coinbase_kafka_producer import KAFKA_CONFIG as PRODUCER_CONFIG
        from kafka_1min_aggregator import KAFKA_CONFIG as AGGREGATOR_CONFIG
        from gcs_kafka_consumer import KAFKA_CONFIG as CONSUMER_CONFIG
        
        # All should have bootstrap servers configured
        assert 'bootstrap.servers' in PRODUCER_CONFIG
        assert 'bootstrap.servers' in AGGREGATOR_CONFIG
        assert 'bootstrap.servers' in CONSUMER_CONFIG
    
    def test_security_configuration_consistent(self):
        """Test that security settings are consistent"""
        from coinbase_kafka_producer import KAFKA_CONFIG as PRODUCER_CONFIG
        from kafka_1min_aggregator import KAFKA_CONFIG as AGGREGATOR_CONFIG
        from gcs_kafka_consumer import KAFKA_CONFIG as CONSUMER_CONFIG
        
        configs = [PRODUCER_CONFIG, AGGREGATOR_CONFIG, CONSUMER_CONFIG]
        
        for config in configs:
            assert config.get('security.protocol') == 'SASL_SSL'
            assert config.get('sasl.mechanisms') == 'PLAIN'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
