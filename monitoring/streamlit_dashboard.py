"""
BTC 1-Minute Live Dashboard
Monitors BTC price and metrics with 1-minute updates from GCS
"""

import streamlit as st
import pandas as pd
from google.cloud import storage
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta, timezone
import pytz

# Page configuration
st.set_page_config(
    page_title="BTC Live Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("₿ Bitcoin Live Dashboard - 1-Minute Updates")
st.markdown("Real-time cryptocurrency data aggregated every minute")

# Initialize session state for timeline persistence
if 'selected_timeline' not in st.session_state:
    st.session_state.selected_timeline = "1 Day"

# Sidebar configuration
st.sidebar.header("Settings")
refresh_interval = 60  # Fixed 60 seconds refresh
st.sidebar.info(f"⏱️ Auto-refresh: Every {refresh_interval} seconds")

# Clear cache button
if st.sidebar.button("🗑️ Clear Cache"):
    st.cache_data.clear()
    st.rerun()

# Timeline selector (like TradingView)
st.sidebar.subheader("📊 Chart Timeline")
timeline_options = ["5 Minutes", "15 Minutes", "30 Minutes", "1 Hour", "1 Day"]
current_index = timeline_options.index(st.session_state.selected_timeline) if st.session_state.selected_timeline in timeline_options else 3

timeline_option = st.sidebar.radio(
    "Select timeframe:",
    timeline_options,
    index=current_index,
    key="timeline_radio"
)

# Save selection to session state
st.session_state.selected_timeline = timeline_option

# Map timeline to max files to load
timeline_map = {
    "5 Minutes": 5,       # 5 minutes
    "15 Minutes": 15,     # 15 minutes
    "30 Minutes": 30,     # 30 minutes
    "1 Hour": 60,         # 60 minutes
    "1 Day": 1440         # 24 * 60 minutes
}
MAX_FILES = timeline_map[timeline_option]

# Force cache invalidation by including current time bucket (changes every 10 seconds)
current_time_bucket = datetime.now().strftime("%Y-%m-%d %H:%M:%S")[:-1]  # Changes every 10 seconds

# GCS Configuration
BUCKET_NAME = 'crypto-db-east1'
PREFIX = 'year='  # Changed from 'btc_1min_agg/' to match your folder structure
GCP_PROJECT_ID = 'crypto-dp'  # Add your GCP project ID

# Eastern Time Zone
EASTERN = pytz.timezone('America/New_York')

@st.cache_data(ttl=10)  # Cache for 10 seconds for more frequent updates
def load_data_from_gcs(time_bucket, max_files, timeline):
    """Load latest parquet files from GCS
    
    Args:
        time_bucket: Current time bucket to force cache refresh every 10 seconds
        max_files: Number of files to load (changes cache when timeline changes)
        timeline: Timeline option (changes cache when timeframe changes)
    """
    try:
        # Initialize client with credentials from environment or Streamlit secrets
        import os
        import json
        
        # Try environment variable first (Railway, local)
        creds_json = os.getenv('GCP_SERVICE_ACCOUNT_JSON')
        
        # Debug: Check if env var exists
        if creds_json:
            st.sidebar.success(f"✅ Found GCP_SERVICE_ACCOUNT_JSON ({len(creds_json)} chars)")
            # Using environment variable (Railway/local)
            from google.oauth2 import service_account
            creds_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
            st.sidebar.info(f"🔑 Using service account: {creds_dict.get('client_email', 'unknown')}")
        elif hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            # Streamlit Cloud - use secrets
            st.sidebar.info("🔑 Using Streamlit secrets")
            from google.oauth2 import service_account
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
        else:
            # Fall back to default credentials (local with GOOGLE_APPLICATION_CREDENTIALS)
            st.sidebar.error("❌ GCP_SERVICE_ACCOUNT_JSON not found!")
            st.sidebar.error("⚠️ Falling back to default credentials (will likely fail)")
            st.error("**CONFIGURATION ERROR**: Set GCP_SERVICE_ACCOUNT_JSON in Railway environment variables!")
            client = storage.Client(project=GCP_PROJECT_ID)
        
        bucket = client.bucket(BUCKET_NAME)
        
        # Search for TODAY's data first (more specific) - use UTC for GCS path
        now_utc = datetime.now(timezone.utc)
        today_prefix = f"year={now_utc.year}/month={now_utc.month:02d}/day={now_utc.day:02d}/"
        
        # Also try current hour specifically
        current_hour_prefix = f"{today_prefix}hour={now_utc.hour:02d}/"
        
        st.sidebar.text(f"🕐 UTC Now: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")
        st.sidebar.warning(f"🔍 Searching: {today_prefix}...")
        
        # First try current hour, then fallback to whole day
        blobs = list(bucket.list_blobs(prefix=current_hour_prefix, max_results=100))
        if len(blobs) < max_files:
            # Get more from today
            all_today_blobs = list(bucket.list_blobs(prefix=today_prefix, max_results=200))
            blobs = all_today_blobs
        
        if not blobs:
            st.error(f"❌ No data found for today: gs://{BUCKET_NAME}/{today_prefix}")
            st.info("🔎 Trying to find ANY recent data...")
            # Fallback: search all files
            blobs = list(bucket.list_blobs(prefix=PREFIX, max_results=50))
            
        if not blobs:
            st.error(f"❌ No data found in gs://{BUCKET_NAME}/{PREFIX}")
            st.code(f"Expected path: gs://{BUCKET_NAME}/year=2025/month=11/day=27/...")
            return None
        
        # Sort by creation time and take only most recent
        blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
        latest_blobs = blobs_sorted[:max_files]  # Use parameter instead of global
        
        st.sidebar.success(f"✅ Found {len(blobs)} files, loading {len(latest_blobs)}...")
        
        # Read parquet files in parallel using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import io
        
        dfs = []
        progress_text = st.sidebar.empty()
        progress_bar = st.progress(0)
        
        def load_blob(blob):
            """Load a single blob and return dataframe"""
            try:
                # Download blob content to memory first (faster)
                blob_data = blob.download_as_bytes()
                df = pd.read_parquet(io.BytesIO(blob_data))
                return df, blob.name
            except Exception as e:
                return None, f"Error: {str(e)[:50]}"
        
        # Load files in parallel (up to 3 at a time)
        loaded = 0
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_blob = {executor.submit(load_blob, blob): blob for blob in latest_blobs}
            
            for future in as_completed(future_to_blob):
                df, info = future.result()
                loaded += 1
                
                if df is not None:
                    dfs.append(df)
                    progress_text.text(f"✅ Loaded {loaded}/{len(latest_blobs)} files")
                else:
                    progress_text.text(f"⚠️ Skipped 1 file - {loaded}/{len(latest_blobs)}")
                
                progress_bar.progress(loaded / len(latest_blobs))
        
        progress_bar.empty()
        progress_text.empty()
        
        if not dfs:
            st.error("No valid parquet files found")
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert timestamps
        combined_df['window_start'] = pd.to_datetime(combined_df['window_start'], unit='ms')
        combined_df['window_end'] = pd.to_datetime(combined_df['window_end'], unit='ms')
        
        # Sort by timestamp
        combined_df = combined_df.sort_values('window_start', ascending=True)
        
        # Remove duplicates
        combined_df = combined_df.drop_duplicates(subset=['window_start'], keep='last')
        
        return combined_df
        
    except Exception as e:
        st.error(f"Error loading data from GCS: {e}")
        return None

# Load data - pass time_bucket, MAX_FILES and timeline_option to force refresh
with st.spinner("Loading latest data from GCS..."):
    df_full = load_data_from_gcs(current_time_bucket, MAX_FILES, timeline_option)

if df_full is not None and not df_full.empty:
    # Filter data by selected timeframe
    now = datetime.now(EASTERN)
    
    # Calculate cutoff time based on timeline selection
    if timeline_option == "5 Minutes":
        cutoff_time = now - timedelta(minutes=5)
    elif timeline_option == "15 Minutes":
        cutoff_time = now - timedelta(minutes=15)
    elif timeline_option == "30 Minutes":
        cutoff_time = now - timedelta(minutes=30)
    elif timeline_option == "1 Hour":
        cutoff_time = now - timedelta(hours=1)
    else:  # 1 Day
        cutoff_time = now - timedelta(hours=24)
    
    # Debug: Show cutoff time (in Eastern)
    st.sidebar.text(f"DEBUG: Now ET: {now.strftime('%H:%M:%S')}")
    st.sidebar.text(f"DEBUG: Cutoff ET: {cutoff_time.strftime('%H:%M:%S')}")
    
    # Convert window_start from UTC to Eastern time for comparison
    df_full['window_start_et'] = pd.to_datetime(df_full['window_start']).dt.tz_localize('UTC').dt.tz_convert(EASTERN).dt.tz_localize(None)
    cutoff_time_naive = cutoff_time.replace(tzinfo=None)
    df = df_full[df_full['window_start_et'] >= cutoff_time_naive].copy()
    
    if df.empty:
        st.warning(f"No data available for {timeline_option} (loaded {len(df_full)} records)")
        df = df_full.copy()
    
    # Use Eastern time for all displays
    df['window_start'] = df['window_start_et']
    
    st.sidebar.info(f"Records loaded: {len(df)} / {len(df_full)}")
    
    # Display last update time - convert UTC to Eastern
    last_update_utc = df['window_start'].max()
    last_update_eastern = pd.Timestamp(last_update_utc).tz_localize('UTC').tz_convert(EASTERN)
    now_eastern = datetime.now(EASTERN)
    data_age_seconds = (now_eastern - last_update_eastern).total_seconds()
    
    st.sidebar.success(f"Last Data: {last_update_eastern.strftime('%Y-%m-%d %H:%M:%S')} ET")
    st.sidebar.info(f"⏱️ Timeframe: {timeline_option}")
    
    # Show data freshness
    if data_age_seconds < 90:
        st.sidebar.success(f"✅ Fresh data ({int(data_age_seconds)}s old)")
    elif data_age_seconds < 180:
        st.sidebar.warning(f"⚠️ Data is {int(data_age_seconds)}s old")
    else:
        st.sidebar.error(f"❌ Data is {int(data_age_seconds)}s old")
    
    st.sidebar.info(f"Records: {len(df)}")
    
    # Latest metrics
    latest = df.iloc[-1]
    
    # Key Metrics Row (6 columns)
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric(
            label="Current Price",
            value=f"${latest['close']:,.2f}",
            delta=f"{latest['price_change_percent_1m']:.2f}%"
        )
    
    with col2:
        st.metric(
            label="Volatility (1m)",
            value=f"{latest['volatility_1m']:.4f}",
        )
    
    with col3:
        st.metric(
            label="Buy Volume (1m)",
            value=f"{latest['total_buy_volume_1m']:.4f} BTC",
        )
    
    with col4:
        st.metric(
            label="Sell Volume (1m)",
            value=f"{latest['total_sell_volume_1m']:.4f} BTC",
        )
    
    with col5:
        st.metric(
            label="Total Volume (1m)",
            value=f"{latest['total_volume_1m']:.4f} BTC",
        )
    
    with col6:
        st.metric(
            label="Trade Count (1m)",
            value=f"{latest.get('trade_count_1m', 0):.0f}",
        )
    
    # Second row of metrics
    col7, col8, col9 = st.columns(3)
    
    with col7:
        st.metric(
            label="Order Imbalance",
            value=f"{latest['order_imbalance_ratio_1m']:.4f}",
            delta="Buy 📈" if latest['order_imbalance_ratio_1m'] > 0 else "Sell 📉"
        )
    
    # Chart 1: Price Chart (OHLC Candlestick)
    st.subheader("📈 BTC Price Movement (OHLC)")
    
    fig_price = go.Figure(data=[go.Candlestick(
        x=df['window_start'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='BTC Price'
    )])
    
    fig_price.update_layout(
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=400,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_price, width='stretch')
    
    # Chart 2: Volume Analysis
    st.subheader("📊 Volume Analysis")
    
    fig_volume = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Total Volume", "Buy vs Sell Volume"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # Total Volume
    fig_volume.add_trace(
        go.Bar(x=df['window_start'], y=df['total_volume_1m'], name='Total Volume', marker_color='blue'),
        row=1, col=1
    )
    
    # Buy vs Sell Volume
    fig_volume.add_trace(
        go.Bar(x=df['window_start'], y=df['total_buy_volume_1m'], name='Buy Volume', marker_color='green'),
        row=2, col=1
    )
    fig_volume.add_trace(
        go.Bar(x=df['window_start'], y=df['total_sell_volume_1m'], name='Sell Volume', marker_color='red'),
        row=2, col=1
    )
    
    fig_volume.update_layout(height=600, showlegend=True, hovermode='x unified')
    fig_volume.update_xaxes(title_text="Time", row=2, col=1)
    fig_volume.update_yaxes(title_text="Volume (BTC)", row=1, col=1)
    fig_volume.update_yaxes(title_text="Volume (BTC)", row=2, col=1)
    
    st.plotly_chart(fig_volume, width='stretch')
    
    # Chart 3: Advanced Metrics
    st.subheader("🔬 Advanced Metrics")
    
    fig_metrics = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Volatility", "Order Imbalance Ratio"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # Volatility
    fig_metrics.add_trace(
        go.Scatter(
            x=df['window_start'], 
            y=df['volatility_1m'], 
            mode='lines+markers',
            name='Volatility',
            line=dict(color='orange', width=2),
            fill='tozeroy'
        ),
        row=1, col=1
    )
    
    # Order Imbalance Ratio
    colors = ['green' if val > 0 else 'red' for val in df['order_imbalance_ratio_1m']]
    fig_metrics.add_trace(
        go.Bar(
            x=df['window_start'], 
            y=df['order_imbalance_ratio_1m'], 
            name='Order Imbalance',
            marker_color=colors
        ),
        row=2, col=1
    )
    
    # Add horizontal line at 0 for Order Imbalance
    fig_metrics.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    
    fig_metrics.update_layout(height=600, showlegend=True, hovermode='x unified')
    fig_metrics.update_xaxes(title_text="Time", row=2, col=1)
    fig_metrics.update_yaxes(title_text="Volatility", row=1, col=1)
    fig_metrics.update_yaxes(title_text="Ratio", row=2, col=1)
    
    st.plotly_chart(fig_metrics, width='stretch')
    
    # Data Table
    st.subheader("📋 Recent Data")
    
    # Display latest 20 records
    display_df = df[['window_start', 'open', 'high', 'low', 'close', 'total_volume_1m', 
                     'volatility_1m', 'order_imbalance_ratio_1m', 'price_change_percent_1m']].tail(20)
    
    display_df = display_df.rename(columns={
        'window_start': 'Time',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'total_volume_1m': 'Volume',
        'volatility_1m': 'Volatility',
        'order_imbalance_ratio_1m': 'Order Imbalance',
        'price_change_percent_1m': 'Change %'
    })
    
    st.dataframe(display_df.iloc[::-1], width='stretch')
    
    # Statistics
    st.subheader("📊 Statistics (Last 24h)")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**Price Statistics**")
        st.write(f"High: ${df['high'].max():,.2f}")
        st.write(f"Low: ${df['low'].min():,.2f}")
        st.write(f"Average: ${df['close'].mean():,.2f}")
    
    with col2:
        st.write("**Volume Statistics**")
        st.write(f"Total: {df['total_volume_1m'].sum():,.2f} BTC")
        st.write(f"Average: {df['total_volume_1m'].mean():,.2f} BTC")
        st.write(f"Max: {df['total_volume_1m'].max():,.2f} BTC")
    
    with col3:
        st.write("**Volatility Statistics**")
        st.write(f"Average: {df['volatility_1m'].mean():.4f}")
        st.write(f"Max: {df['volatility_1m'].max():.4f}")
        st.write(f"Current: {latest['volatility_1m']:.4f}")
    
    # Auto-refresh with countdown
    st.sidebar.markdown("---")
    countdown_placeholder = st.sidebar.empty()
    
    # Countdown timer
    for remaining in range(refresh_interval, 0, -1):
        countdown_placeholder.write(f"🔄 Refreshing in {remaining} seconds...")
        time.sleep(1)
    
    countdown_placeholder.write("🔄 Refreshing now...")
    st.rerun()

else:
    st.error("No data available. Please check your GCS bucket and ensure data is being written.")
    st.info(f"Looking for data in: gs://{BUCKET_NAME}/{PREFIX}**/")
    st.info("Expected structure: gs://crypto-db-east1/year=2025/month=11/day=27/hour=04/*.parquet")
    
    # Retry button
    if st.button("Retry"):
        st.rerun()
