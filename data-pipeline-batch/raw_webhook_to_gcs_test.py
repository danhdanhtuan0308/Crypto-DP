"""
TEST VERSION: Write function that creates separate files per minute
Used by batch_etl_dag_1min_test.py

Production uses hour-based files (btc_1h_14.parquet)
Test uses minute-based files (btc_1min_14_16.parquet, btc_1min_14_17.parquet, etc.)
"""

import json
import os
import io
from datetime import datetime
from google.cloud import storage
import logging
import pytz
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)
EASTERN = pytz.timezone('America/New_York')


def write_parquet_to_gcs_test(minute_rows: list, bucket_name: str):
    """
    TEST VERSION: Write 1-2 rows of 1-minute data as separate Parquet files
    Each minute gets its own file: btc_1min_{hour}_{minute}.parquet
    """
    if not minute_rows or len(minute_rows) == 0:
        logger.warning("No data to write")
        return False
    
    try:
        # Initialize GCS client with credentials from environment
        creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
        if creds_json:
            from google.oauth2 import service_account
            creds_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            client = storage.Client(credentials=credentials, project=creds_dict.get('project_id'))
        else:
            client = storage.Client()
        
        bucket = client.bucket(bucket_name)
        
        # Use first row's timestamp for file path
        first_row = minute_rows[0]
        window_start_est = datetime.strptime(first_row['window_start_est'], '%Y-%m-%d %H:%M:%S')
        window_start_est = EASTERN.localize(window_start_est)
        
        # Path: {year}/{month}/{day}/btc_1min_{hour}_{minute}.parquet
        year = window_start_est.strftime('%Y')
        month = window_start_est.strftime('%m')
        day = window_start_est.strftime('%d')
        hour = window_start_est.strftime('%H')
        minute = window_start_est.strftime('%M')
        
        blob_path = f"{year}/{month}/{day}/btc_1min_{hour}_{minute}.parquet"
        
        # Convert list of dicts to PyArrow table
        table = pa.Table.from_pylist(minute_rows)
        
        # Write to buffer
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Upload to GCS
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        
        logger.info(
            f"✅ TEST PARQUET: gs://{bucket_name}/{blob_path} | "
            f"{len(minute_rows)} row(s) | "
            f"Time: {hour}:{minute} EST"
        )
        return True
        
    except Exception as e:
        logger.error(f"Failed to write Parquet: {e}")
        return False
