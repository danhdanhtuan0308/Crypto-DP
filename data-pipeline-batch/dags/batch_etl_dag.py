"""
Airflow DAG for Batch Layer Validation (Lambda Architecture)

This DAG validates the batch layer data collection:
- Production: Triggers every 1 hour (0 * * * *)
- Validates raw data from Coinbase WebSocket collector
- Checks data completeness and file sizes
- Provides fault tolerance and monitoring

Raw data is collected continuously by raw_webhook_to_gcs.py service.
Airflow only validates and monitors the data collection.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta


# DAG Configuration
default_args = {
    'owner': 'crypto-dp',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),
}


def monitor_data_collection(**context):
    """Monitor the raw data collection from Coinbase WebSocket"""
    from google.cloud import storage
    from datetime import datetime, timedelta
    import pytz
    import os
    import json
    
    EASTERN = pytz.timezone('America/New_York')
    now_est = datetime.now(EASTERN)
    previous_hour = now_est - timedelta(hours=1)
    
    year = previous_hour.strftime('%Y')
    month = previous_hour.strftime('%m')
    day = previous_hour.strftime('%d')
    hour = previous_hour.strftime('%H')
    
    # Initialize GCS client
    bucket_name = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')
    
    creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if creds_json:
        from google.oauth2 import service_account
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        storage_client = storage.Client(credentials=credentials, project=creds_dict.get('project_id', 'crypto-dp'))
    else:
        storage_client = storage.Client()
    
    bucket = storage_client.bucket(bucket_name)
    prefix = f"{year}/{month}/{day}/"
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    total_size = sum(blob.size for blob in blobs)
    
    if not blobs:
        logger.warning(f"No data collected for {year}-{month}-{day} {hour}:00 EST")
        return f"No data found"
    
    print(f"✓ Data collection healthy: {len(blobs)} files, {total_size/1024/1024:.2f} MB for {year}-{month}-{day} {hour}:00 EST")
    return len(blobs)


def check_raw_data_availability(**context):
    """Check if raw data is available in the current hour"""
    from google.cloud import storage
    from datetime import datetime, timedelta
    import pytz
    import os
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    now_est = datetime.now(EASTERN)
    current_hour = now_est.replace(minute=0, second=0, microsecond=0)
    
    year = current_hour.strftime('%Y')
    month = current_hour.strftime('%m')
    day = current_hour.strftime('%d')
    hour = current_hour.strftime('%H')
    
    # Initialize GCS client - batch-btc-1h-east1 bucket
    bucket_name = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')
    
    creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if creds_json:
        from google.oauth2 import service_account
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        storage_client = storage.Client(credentials=credentials, project=creds_dict.get('project_id', 'crypto-dp'))
    else:
        storage_client = storage.Client()
    
    bucket = storage_client.bucket(bucket_name)
    prefix = f"{year}/{month}/{day}/"
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        logger.warning(f"No raw data found for {year}-{month}-{day} {hour}:00 EST - May be first run")
        return 0
    
    print(f"✓ Found {len(blobs)} raw data files for {year}-{month}-{day} {hour}:00 EST")
    return len(blobs)


def validate_batch_output(**context):
    """Validate that batch data was written successfully"""
    from google.cloud import storage
    from datetime import datetime, timedelta
    import pytz
    import os
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    now_est = datetime.now(EASTERN)
    current_hour = now_est.replace(minute=0, second=0, microsecond=0)
    
    year = current_hour.strftime('%Y')
    month = current_hour.strftime('%m')
    day = current_hour.strftime('%d')
    hour = current_hour.strftime('%H')
    
    # Initialize GCS client - batch-btc-1h-east1 bucket
    bucket_name = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')
    
    creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if creds_json:
        from google.oauth2 import service_account
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(creds_dict)
        storage_client = storage.Client(credentials=credentials, project=creds_dict.get('project_id', 'crypto-dp'))
    else:
        storage_client = storage.Client()
    
    bucket = storage_client.bucket(bucket_name)
    prefix = f"{year}/{month}/{day}/"
    
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        logger.warning(f"No batch data found in gs://{bucket_name}/{prefix} - May be first run")
        return 0
    
    # Check file sizes
    total_size = 0
    for blob in blobs:
        if blob.size == 0:
            logger.error(f"Empty file detected: {blob.name}")
        total_size += blob.size
    
    print(f"✓ Validated {len(blobs)} batch files ({total_size/1024/1024:.2f} MB) in gs://{bucket_name}/{prefix}")
    return True


# Production DAG - 1 hour schedule
with DAG(
    'batch_etl_1hour_prod',
    default_args=default_args,
    description='Batch ETL every 1 hour (PRODUCTION)',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'etl', 'production', '1hour'],
) as dag_1hour:
    
    check_data_prod = PythonOperator(
        task_id='check_raw_data',
        python_callable=check_raw_data_availability,
        provide_context=True,
    )
    
    monitor_collection_prod = PythonOperator(
        task_id='monitor_data_collection',
        python_callable=monitor_data_collection,
        provide_context=True,
    )
    
    validate_output_prod = PythonOperator(
        task_id='validate_batch_output',
        python_callable=validate_batch_output,
        provide_context=True,
    )
    
    # Task dependencies
    check_data_prod >> monitor_collection_prod >> validate_output_prod
