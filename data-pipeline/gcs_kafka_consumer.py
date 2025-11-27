"""
GCS Parquet Writer
Consumes from btc_1min_agg topic and writes Parquet files to GCS
"""

import json
import os
from datetime import datetime, timezone
from confluent_kafka import Consumer
import pandas as pd
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('CONFLUENT_KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('CONFLUENT_KAFKA_API_KEY_GCS'),
    'sasl.password': os.getenv('CONFLUENT_KAFKA_API_KEY_SECRET_GCS'),
    'group.id': 'gcs-parquet-writer',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
}

SOURCE_TOPIC = 'btc_1min_agg'
GCS_BUCKET = os.getenv('GCS_BUCKET', 'crypto-db-east1')
GCS_CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH')

# Set GCS credentials
if GCS_CREDENTIALS_PATH:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GCS_CREDENTIALS_PATH


class ParquetWriter:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        
        # Load credentials from environment variable or file
        # Check both possible env var names
        creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        if creds_json:
            import json
            from google.oauth2 import service_account
            creds_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            self.storage_client = storage.Client(credentials=credentials, project=creds_dict.get('project_id', 'crypto-dp'))
            logger.info(f"🔑 Using service account: {creds_dict.get('client_email', 'unknown')}")
        else:
            logger.warning("⚠️ No GCP credentials found, using default")
            self.storage_client = storage.Client()
        
        self.bucket = self.storage_client.bucket(bucket_name)
        self.buffer = []
        self.file_count = 0
        
        # 1 file per minute (1 row each)
        self.buffer_size = 1
        self.flush_interval = 60  # 1 minute
        logger.info("📝 Writing new file every 1 minute")
        
        self.last_flush = time.time()
    
    def add_record(self, record):
        """Add a record to the buffer"""
        self.buffer.append(record)
        logger.info(f"📥 Received 1min agg record, buffer size: {len(self.buffer)}")
        
        # Flush immediately when we have 1 record (1-minute data)
        self.flush()
    
    def flush(self):
        """Write buffer to GCS as Parquet file"""
        if not self.buffer:
            return
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.buffer)
            
            # Generate file path
            timestamp = datetime.now(timezone.utc)
            year = timestamp.strftime('%Y')
            month = timestamp.strftime('%m')
            day = timestamp.strftime('%d')
            hour = timestamp.strftime('%H')
            
            # Path format: year=2025/month=11/day=25/hour=19/btc_1min_agg+0+0000000001.parquet
            file_name = f"btc_1min_agg+0+{self.file_count:010d}.snappy.parquet"
            blob_path = f"year={year}/month={month}/day={day}/hour={hour}/{file_name}"
            
            # Convert to Parquet in memory
            table = pa.Table.from_pandas(df)
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(
                table, 
                parquet_buffer,
                compression='snappy',
                use_dictionary=True,
                version='2.6'
            )
            
            # Upload to GCS
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(
                parquet_buffer.getvalue().to_pybytes(),
                content_type='application/octet-stream'
            )
            
            logger.info(
                f"✅ Uploaded: gs://{self.bucket_name}/{blob_path} "
                f"({len(self.buffer)} rows, {blob.size / 1024:.2f} KB)"
            )
            
            # Reset buffer
            self.buffer.clear()
            self.file_count += 1
            self.last_flush = time.time()
            
        except Exception as e:
            logger.error(f"❌ Failed to write Parquet: {e}")
            raise


def main():
    logger.info("=" * 70)
    logger.info("🚀 GCS Parquet Writer Started")
    logger.info("=" * 70)
    logger.info(f"Source topic: {SOURCE_TOPIC}")
    logger.info(f"Target bucket: gs://{GCS_BUCKET}")
    
    consumer = Consumer(KAFKA_CONFIG)
    writer = ParquetWriter(GCS_BUCKET)
    
    consumer.subscribe([SOURCE_TOPIC])
    logger.info(f"Subscribed to {SOURCE_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                # Check if we should flush due to timeout
                current_time = time.time()
                if writer.buffer and (current_time - writer.last_flush) >= writer.flush_interval:
                    logger.info("⏰ Flush interval reached, writing file...")
                    writer.flush()
                continue
            
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Parse message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                writer.add_record(data)
                logger.debug(f"Added record to buffer (buffer size: {len(writer.buffer)})")
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                continue
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        # Flush remaining buffer
        if writer.buffer:
            logger.info("Flushing remaining buffer...")
            writer.flush()
    
    finally:
        consumer.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
