"""
Airflow DAG: Batch Layer - Write 60 rows (1-min data) to GCS every hour

Triggered: Every hour at minute 0 (0 * * * *)
Action: Signals the batch collector to flush its hourly buffer to GCS
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import os


default_args = {
    'owner': 'crypto-dp',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}


def trigger_batch_write(**context):
    """
    Trigger the batch collector to write buffered data to GCS
    The collector service exposes an HTTP endpoint for this
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Get the batch collector service URL from environment
    collector_url = os.getenv('BATCH_COLLECTOR_URL', 'http://localhost:5000')
    
    try:
        # Send trigger to batch collector
        response = requests.post(f"{collector_url}/flush", timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Batch write successful: {result}")
            return result
        else:
            raise Exception(f"Batch write failed: {response.status_code} - {response.text}")
    
    except Exception as e:
        logger.error(f"❌ Failed to trigger batch write: {e}")
        raise


def validate_gcs_data(**context):
    """Validate that data was written to GCS"""
    from google.cloud import storage
    import pytz
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    
    # Get current hour in EST
    now_est = datetime.now(EASTERN)
    current_hour = now_est.replace(minute=0, second=0, microsecond=0)
    
    year = current_hour.strftime('%Y')
    month = current_hour.strftime('%m')
    day = current_hour.strftime('%d')
    hour = current_hour.strftime('%H')
    
    bucket_name = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')
    
    # Initialize GCS client
    creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
    if creds_json:
        from google.oauth2 import service_account
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        storage_client = storage.Client(credentials=credentials, project=creds_dict.get('project_id'))
    else:
        storage_client = storage.Client()
    
    bucket = storage_client.bucket(bucket_name)
    blob_path = f"{year}/{month}/{day}/btc_1h_{hour}.parquet"
    blob = bucket.blob(blob_path)
    
    if not blob.exists():
        raise Exception(f"❌ File not found: gs://{bucket_name}/{blob_path}")
    
    # Check file size
    blob.reload()
    if blob.size == 0:
        raise Exception(f"❌ Empty file: gs://{bucket_name}/{blob_path}")
    
    logger.info(f"✅ Validated: gs://{bucket_name}/{blob_path} ({blob.size/1024:.2f} KB)")
    return True


# Production DAG - runs every hour
with DAG(
    'batch_etl_1hour_prod',
    default_args=default_args,
    description='Batch Layer: Write 60 rows to GCS every hour',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'etl', 'production', '1hour'],
) as dag:
    
    trigger_write = PythonOperator(
        task_id='trigger_batch_write',
        python_callable=trigger_batch_write,
        provide_context=True,
    )
    
    validate_data = PythonOperator(
        task_id='validate_gcs_data',
        python_callable=validate_gcs_data,
        provide_context=True,
    )
    
    # Task dependencies
    trigger_write >> validate_data
