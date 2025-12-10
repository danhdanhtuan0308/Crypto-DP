"""
BTC Dashboard Data Loader
Handles loading data from Google Cloud Storage
"""

import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pytz
import json
import os
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Eastern Time Zone
EASTERN = pytz.timezone('America/New_York')

# GCS Configuration
BUCKET_NAME = 'crypto-db-east1'
PREFIX = 'RealTime/'
GCP_PROJECT_ID = 'crypto-dp'


class DataLoader:
    """Handles loading parquet files from Google Cloud Storage"""
    
    def __init__(self):
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize GCS client with credentials"""
        try:
            # Try environment variable first
            creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
            
            if creds_json:
                creds_dict = json.loads(creds_json)
                credentials = service_account.Credentials.from_service_account_info(creds_dict)
                self.client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
            else:
                # Fall back to default credentials
                self.client = storage.Client(project=GCP_PROJECT_ID)
        except Exception as e:
            print(f"Error initializing GCS client: {str(e)}")
            self.client = None
    
    def load_new_data_from_gcs(self, since_time=None, retry_count=0):
        """
        Load new parquet files from GCS (incremental loading)
        
        Args:
            since_time: datetime - only load files created after this time
            retry_count: int - number of retries attempted
        
        Returns:
            tuple: (DataFrame, error_message)
        """
        max_retries = 3
        
        if not self.client:
            return None, "GCS client not initialized. Check credentials."
        
        try:
            bucket = self.client.bucket(BUCKET_NAME)
            now_est = datetime.now(EASTERN)
            blobs = []
            
            # INCREMENTAL LOAD: Only load NEW files (1-2 per minute)
            if since_time is not None:
                if since_time.tzinfo is None:
                    since_time = pytz.UTC.localize(since_time)
                
                # Only check current hour + previous hour folders
                current_hour_prefix = f"RealTime/year={now_est.year}/month={now_est.month:02d}/day={now_est.day:02d}/hour={now_est.hour:02d}/"
                prev_hour_est = now_est - timedelta(hours=1)
                prev_hour_prefix = f"RealTime/year={prev_hour_est.year}/month={prev_hour_est.month:02d}/day={prev_hour_est.day:02d}/hour={prev_hour_est.hour:02d}/"
                
                # List only current and previous hour
                current_hour_blobs = list(bucket.list_blobs(prefix=current_hour_prefix, max_results=100))
                prev_hour_blobs = list(bucket.list_blobs(prefix=prev_hour_prefix, max_results=100))
                all_blobs = current_hour_blobs + prev_hour_blobs
                
                # Filter to only files created AFTER since_time (with small buffer)
                since_time_with_buffer = since_time - timedelta(seconds=60)
                blobs = [b for b in all_blobs if b.time_created.replace(tzinfo=pytz.UTC) > since_time_with_buffer]
                
                info_msg = f"Incremental: {len(blobs)} new files"
            
            # FULL LOAD: Load rolling 24 hours
            else:
                info_msg = "Full load: Rolling 24 hours"
                
                # Load ONLY the specific hours we need (current hour + last 24 hours)
                for i in range(25):
                    hour_time = now_est - timedelta(hours=i)
                    hour_prefix = f"RealTime/year={hour_time.year}/month={hour_time.month:02d}/day={hour_time.day:02d}/hour={hour_time.hour:02d}/"
                    hour_blobs = list(bucket.list_blobs(prefix=hour_prefix, max_results=100))
                    blobs.extend(hour_blobs)
                
                # Safety check: should be around 1440-1500 files for 24 hours
                if len(blobs) > 2000:
                    blobs = sorted(blobs, key=lambda x: x.time_created, reverse=True)[:1500]
            
            # Handle different cases
            if not blobs:
                if since_time is not None:
                    return pd.DataFrame(), None
                else:
                    return None, f"No data found in gs://{BUCKET_NAME}/{PREFIX}"
            
            # Sort by creation time and limit
            blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
            max_load = min(len(blobs_sorted), 100 if since_time is not None else 1500)
            latest_blobs = blobs_sorted[:max_load]
            
            # Read parquet files in parallel
            dfs = []
            
            def load_blob(blob):
                try:
                    blob_data = blob.download_as_bytes()
                    return pd.read_parquet(io.BytesIO(blob_data))
                except:
                    return None
            
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(load_blob, blob) for blob in latest_blobs]
                for future in as_completed(futures):
                    df = future.result()
                    if df is not None:
                        dfs.append(df)
            
            if not dfs:
                return None, "No valid parquet files found"
            
            # Combine and process dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            del dfs
            
            # Convert timestamps from UTC milliseconds to EST
            combined_df['window_start'] = pd.to_datetime(combined_df['window_start'], unit='ms', utc=True).dt.tz_convert(EASTERN)
            combined_df['window_end'] = pd.to_datetime(combined_df['window_end'], unit='ms', utc=True).dt.tz_convert(EASTERN)
            
            # Sort and remove duplicates
            combined_df = combined_df.sort_values('window_start', ascending=True)
            combined_df = combined_df.drop_duplicates(subset=['window_start'], keep='last')
            
            # Apply 24h cutoff
            cutoff = datetime.now(EASTERN) - timedelta(hours=24, minutes=30)
            combined_df = combined_df[combined_df['window_start'] >= cutoff].copy()
            
            return combined_df, None
            
        except Exception as e:
            error_msg = f"Error loading data: {str(e)}"
            
            # Retry logic
            if retry_count < max_retries:
                wait_time = 2 ** retry_count
                time.sleep(wait_time)
                return self.load_new_data_from_gcs(since_time=since_time, retry_count=retry_count + 1)
            else:
                return None, f"{error_msg} - Max retries reached"
    
    def check_for_new_files(self, since_time):
        """
        Check if new files are available in GCS without downloading
        
        Args:
            since_time: datetime - check for files created after this time
        
        Returns:
            bool: True if new files exist, False otherwise
        """
        if not self.client or since_time is None:
            return False
        
        try:
            if since_time.tzinfo is None:
                since_time = pytz.UTC.localize(since_time)
            
            bucket = self.client.bucket(BUCKET_NAME)
            now_est = datetime.now(EASTERN)
            
            # Check only current hour folder for new files
            current_hour_prefix = f"RealTime/year={now_est.year}/month={now_est.month:02d}/day={now_est.day:02d}/hour={now_est.hour:02d}/"
            
            # List only recent files (max 10 to keep it fast)
            blobs = list(bucket.list_blobs(prefix=current_hour_prefix, max_results=10))
            
            # Check if any blob was created after since_time
            since_time_with_buffer = since_time - timedelta(seconds=30)
            for blob in blobs:
                if blob.time_created.replace(tzinfo=pytz.UTC) > since_time_with_buffer:
                    return True
            
            return False
        except Exception as e:
            print(f"Error checking for new files: {str(e)}")
            return False
