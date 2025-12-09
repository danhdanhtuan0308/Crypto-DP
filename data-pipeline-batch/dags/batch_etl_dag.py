"""
Airflow DAG: Batch Layer ETL - Run entire collection process every 5 minutes

Triggered: Every 5 minutes (*/5 * * * *)
Process: 
1. Collect 5 minutes of Coinbase data (1-min aggregation)
2. Write 5 rows to GCS as Parquet

This DAG RUNS the ETL, not just triggers it.
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
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=15),  # 5 min collection + buffer
}


def collect_and_aggregate_data(**context):
    """
    Collect 5 minutes of Coinbase data and aggregate into 1-min rows
    This runs the ENTIRE ETL process for the past 5 minutes
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
    
    async def collect_for_duration(duration_minutes=5):
        """
        Collect data for duration_minutes and aggregate into 1-min rows
        Uses MinuteAggregator - same ETL logic as kafka_1min_aggregator.py
        """
        aggregator = MinuteAggregator()
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        tick_count = 0
        minute_count = 0
        
        logger.info(f"🚀 Starting data collection for {duration_minutes} minutes...")
        
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
                    
                    # ETL: Add tick to aggregator (triggers 1-min aggregation when minute completes)
                    hourly_result = aggregator.add_tick(tick)
                    
                    # Check if hour completed (returns 60 rows)
                    if hourly_result:
                        logger.info(f"⚠️ Hour completed early! Got {len(hourly_result)} rows")
                        return hourly_result
                    
                    # Log progress every 100 ticks
                    if tick_count % 100 == 0:
                        current_minute_buffer = len(aggregator.window_data)
                        hourly_buffer = len(aggregator.hourly_buffer)
                        logger.info(
                            f"📊 Progress: {tick_count} ticks | "
                            f"Current minute: {current_minute_buffer} ticks | "
                            f"Completed minutes: {hourly_buffer}/60"
                        )
                    
                except asyncio.TimeoutError:
                    await ws.ping()
                    
                    # Check if minute should flush due to timeout
                    current_minute_est = aggregator._get_est_minute()
                    if aggregator.window_start and current_minute_est > aggregator.window_start:
                        minute_agg = aggregator._aggregate_minute()
                        if minute_agg:
                            aggregator.hourly_buffer.append(minute_agg)
                            aggregator.window_start = current_minute_est
                            aggregator.window_data = []
                            minute_count += 1
                            logger.info(f"⏱️ Timeout flush: minute {minute_count}/60")
        
        # Collection ended - return whatever we have
        final_row_count = len(aggregator.hourly_buffer)
        logger.info(f"✅ Collection complete: {tick_count} ticks → {final_row_count} minute-rows")
        
        if final_row_count == 0:
            logger.error("❌ No data aggregated!")
            raise Exception("No data collected")
        
        return aggregator.hourly_buffer
    
    # Run the collection
    hourly_data = asyncio.run(collect_for_duration(5))
    
    # Store in XCom for next task
    context['task_instance'].xcom_push(key='hourly_data', value=hourly_data)
    
    return len(hourly_data)


def write_to_gcs(**context):
    """Write the aggregated data to GCS as Parquet"""
    import logging
    logger = logging.getLogger(__name__)
    
    # Get data from previous task
    ti = context['task_instance']
    hourly_data = ti.xcom_pull(task_ids='collect_and_aggregate', key='hourly_data')
    
    if not hourly_data or len(hourly_data) == 0:
        raise Exception("No data collected!")
    
    logger.info(f"Writing {len(hourly_data)} rows to GCS...")
    
    # Import write function
    from raw_webhook_to_gcs import write_parquet_to_gcs, GCS_BUCKET
    
    success = write_parquet_to_gcs(hourly_data, GCS_BUCKET)
    
    if not success:
        raise Exception("Failed to write to GCS!")
    
    logger.info(f"✅ Successfully wrote {len(hourly_data)} rows to GCS")
    return len(hourly_data)


def validate_gcs_data(**context):
    """Validate that data was written to GCS"""
    from google.cloud import storage
    import pytz
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    EASTERN = pytz.timezone('America/New_York')
    
    # Get data from collect task to determine the actual hour written
    ti = context['task_instance']
    hourly_data = ti.xcom_pull(task_ids='collect_and_aggregate', key='hourly_data')
    
    if not hourly_data or len(hourly_data) == 0:
        raise Exception("No data to validate!")
    
    # Parse timestamp from first row (the hour that was actually written)
    first_row = hourly_data[0]
    window_start_est = datetime.strptime(first_row['window_start_est'], '%Y-%m-%d %H:%M:%S')
    window_start_est = EASTERN.localize(window_start_est)
    
    year = window_start_est.strftime('%Y')
    month = window_start_est.strftime('%m')
    day = window_start_est.strftime('%d')
    hour = window_start_est.strftime('%H')
    
    bucket_name = os.getenv('GCS_BUCKET', 'batch-btc-1h-east1')
    
    logger.info(f"🔍 Validating files for hour {hour}:00 EST (written data timestamp)")
    
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
    
    # List all files matching the hour pattern (with timestamp)
    prefix = f"{year}/{month}/{day}/btc_1h_{hour}_"
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        raise Exception(f"❌ No files found with prefix: gs://{bucket_name}/{prefix}")
    
    # Get the most recent file (sorted by name, timestamp is in filename)
    latest_blob = sorted(blobs, key=lambda b: b.name)[-1]
    
    # Check file size
    if latest_blob.size == 0:
        raise Exception(f"❌ Empty file: gs://{bucket_name}/{latest_blob.name}")
    
    logger.info(f"✅ Validated: gs://{bucket_name}/{latest_blob.name} ({latest_blob.size/1024:.2f} KB)")
    return True


# Production DAG - runs entire ETL every hour
with DAG(
    'batch_etl_5min_prod',
    default_args=default_args,
    description='Batch ETL: Collect 5 min data, aggregate, write to GCS',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['batch', 'etl', 'production', '5minute'],
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
