"""
Airflow DAG: Batch Layer ETL - TEST VERSION (1 minute collection)

Triggered: Every minute (* * * * *)
Process: 
1. Collect 1 minute of Coinbase data (1-min aggregation)
2. Write 1 row to GCS as Parquet

This is for TESTING ONLY - validate the pipeline works before using the hourly production DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import sys

# Add parent directory to import the ETL code
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'crypto-dp',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=3),  # 1 min collection + 2 min buffer
}


def collect_and_aggregate_data(**context):
    """
    Collect 1 minute of Coinbase data - TEST VERSION
    """
    import json
    import asyncio
    import websockets
    import pytz
    import logging
    from datetime import datetime, timedelta
    import time
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    
    # Import the MinuteAggregator from raw_webhook_to_gcs
    from raw_webhook_to_gcs import MinuteAggregator, COINBASE_WS_URL, PRODUCT_ID
    
    async def collect_for_duration(duration_minutes=1):
        """
        Collect data for 1 minute (TEST VERSION)
        """
        aggregator = MinuteAggregator()
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        tick_count = 0
        
        logger.info(f"🧪 TEST: Starting data collection for {duration_minutes} minute...")
        
        async with websockets.connect(COINBASE_WS_URL, ping_interval=20, ping_timeout=20) as ws:
            # Subscribe to ticker
            subscribe_msg = {
                "type": "subscribe",
                "product_ids": [PRODUCT_ID],
                "channels": ["ticker"]
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info(f"✅ Connected to Coinbase WebSocket")
            
            while time.time() < end_time:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(msg)
                    
                    if data.get('type') != 'ticker':
                        continue
                    
                    # Parse tick (same format as data-pipeline)
                    tick = {
                        'price': float(data.get('price', 0)),
                        'volume_1s': float(data.get('last_size', 0)),
                        'buy_volume_1s': 0,
                        'sell_volume_1s': 0,
                        'trade_count_1s': 1,
                        'volume_24h': float(data.get('volume_24h', 0)),
                        'high_24h': float(data.get('high_24h', 0)),
                        'low_24h': float(data.get('low_24h', 0)),
                        'symbol': PRODUCT_ID,
                        'ingestion_time': datetime.now(EASTERN).isoformat(),
                        'bid_ask_spread_1s': 0,
                        'depth_2pct_1s': 0,
                        'bid_depth_2pct_1s': 0,
                        'ask_depth_2pct_1s': 0,
                        'vwap_1s': 0,
                        'micro_price_deviation_1s': 0,
                        'cvd_1s': 0,
                        'ofi_1s': 0,
                        'kyles_lambda_1s': 0,
                        'liquidity_health_1s': 0,
                        'mid_price_1s': 0,
                        'best_bid_1s': 0,
                        'best_ask_1s': 0,
                        'buy_sell_ratio': 0,
                    }
                    
                    tick_count += 1
                    
                    # ETL: Add tick to aggregator
                    aggregator.add_tick(tick)
                    
                    # Log progress every 10 ticks
                    if tick_count % 10 == 0:
                        current_minute_buffer = len(aggregator.window_data)
                        logger.info(f"📊 Progress: {tick_count} ticks in current minute buffer")
                    
                except asyncio.TimeoutError:
                    await ws.ping()
        
        # Collection ended - force flush the current minute
        minute_agg = aggregator._aggregate_minute()
        if minute_agg:
            aggregator.hourly_buffer.append(minute_agg)
        
        final_row_count = len(aggregator.hourly_buffer)
        logger.info(f"✅ Collection complete: {tick_count} ticks → {final_row_count} minute-row")
        
        if final_row_count == 0:
            logger.error("❌ No data aggregated!")
            raise Exception("No data collected")
        
        return aggregator.hourly_buffer
    
    # Run the collection
    minute_data = asyncio.run(collect_for_duration(1))
    
    # Store in XCom for next task
    context['task_instance'].xcom_push(key='minute_data', value=minute_data)
    
    return len(minute_data)


def write_to_gcs(**context):
    """Write the aggregated data to GCS as Parquet - TEST VERSION (separate file per minute)"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Get data from previous task
    ti = context['task_instance']
    minute_data = ti.xcom_pull(task_ids='collect_and_aggregate', key='minute_data')
    
    if not minute_data or len(minute_data) == 0:
        raise Exception("No data collected!")
    
    logger.info(f"🧪 TEST: Writing {len(minute_data)} row(s) to GCS (separate file per minute)...")
    
    # Import TEST write function (writes separate files per minute)
    from raw_webhook_to_gcs_test import write_parquet_to_gcs_test
    from raw_webhook_to_gcs import GCS_BUCKET
    
    success = write_parquet_to_gcs_test(minute_data, GCS_BUCKET)
    
    if not success:
        raise Exception("Failed to write to GCS!")
    
    logger.info(f"✅ Successfully wrote {len(minute_data)} row(s) to separate file")
    return len(minute_data)


def validate_gcs_data(**context):
    """Validate that data was written to GCS - TEST VERSION (checks minute-based file)"""
    from google.cloud import storage
    import pytz
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    
    # Get data from collect task to find exact timestamp
    ti = context['task_instance']
    minute_data = ti.xcom_pull(task_ids='collect_and_aggregate', key='minute_data')
    
    if not minute_data or len(minute_data) == 0:
        raise Exception("No data to validate!")
    
    # Parse timestamp from first row
    first_row = minute_data[0]
    window_start_est = datetime.strptime(first_row['window_start_est'], '%Y-%m-%d %H:%M:%S')
    window_start_est = EASTERN.localize(window_start_est)
    
    year = window_start_est.strftime('%Y')
    month = window_start_est.strftime('%m')
    day = window_start_est.strftime('%d')
    hour = window_start_est.strftime('%H')
    minute = window_start_est.strftime('%M')
    
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
    blob_path = f"{year}/{month}/{day}/btc_1min_{hour}_{minute}.parquet"
    blob = bucket.blob(blob_path)
    
    if not blob.exists():
        raise Exception(f"❌ File not found: gs://{bucket_name}/{blob_path}")
    
    # Check file size
    blob.reload()
    if blob.size == 0:
        raise Exception(f"❌ Empty file: gs://{bucket_name}/{blob_path}")
    
    logger.info(f"✅ Validated: gs://{bucket_name}/{blob_path} ({blob.size/1024:.2f} KB)")
    return True


# TEST DAG - runs every minute for quick validation
with DAG(
    'batch_etl_1min_test',
    default_args=default_args,
    description='TEST: Collect 1 min data, aggregate, write to GCS (runs every minute)',
    schedule_interval='* * * * *',  # Every minute
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'etl', 'test', '1minute'],
) as dag:
    
    collect_task = PythonOperator(
        task_id='collect_and_aggregate',
        python_callable=collect_and_aggregate_data,
        provide_context=True,
    )
    
    write_task = PythonOperator(
        task_id='write_to_gcs',
        python_callable=write_to_gcs,
        provide_context=True,
    )
    
    validate_task = PythonOperator(
        task_id='validate_gcs_data',
        python_callable=validate_gcs_data,
        provide_context=True,
    )
    
    # Task dependencies
    collect_task >> write_task >> validate_task
