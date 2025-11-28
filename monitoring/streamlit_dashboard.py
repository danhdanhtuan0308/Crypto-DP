"""
BTC 1-Minute Live Dashboard
Monitors BTC price and metrics with 1-minute updates from GCS
ALL TIMES ARE IN EASTERN TIME (EST/EDT)
"""

import streamlit as st
import pandas as pd
from google.cloud import storage
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta
import pytz

# Page configuration
st.set_page_config(
    page_title="BTC Live Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Eastern Time Zone - ALL times are in EST
EASTERN = pytz.timezone('America/New_York')

# Title
st.title("₿ Bitcoin Live Dashboard - 1-Minute Updates")
st.markdown("Real-time cryptocurrency data aggregated every minute")

# Display current time in EST
now_est = datetime.now(EASTERN)
st.markdown(f"**🕐 Current Time (EST):** {now_est.strftime('%Y-%m-%d %H:%M:%S')}")

# Initialize session state for timeline persistence and last refresh time
if 'selected_timeline' not in st.session_state:
    st.session_state.selected_timeline = "1 Day"
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = time.time()

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

# GCS Configuration
BUCKET_NAME = 'crypto-db-east1'
PREFIX = 'year='  # Changed from 'btc_1min_agg/' to match your folder structure
GCP_PROJECT_ID = 'crypto-dp'  # Add your GCP project ID

@st.cache_data(ttl=None)  # No automatic TTL - we control refresh manually via session state
def load_data_from_gcs():
    """Load latest parquet files from GCS
    
    Returns:
        DataFrame with all loaded data (will be filtered by caller based on timeline)
    
    Note: Loads all data from today+yesterday (~1400 files, 24 hours).
    Cache controlled manually via session state to avoid unwanted refreshes.
    Only refreshes when user triggers or 60-second countdown completes.
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
        
        # Calculate date range based on timeline - ALL IN EASTERN TIME
        now_est = datetime.now(EASTERN)
        
        # For longer timeframes, we need to load from previous day(s) too
        blobs = []
        
        # Load last 24 hours of data for "1 Day" timeline support
        st.sidebar.text(f"🕐 EST Now: {now_est.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load from today and yesterday
        today_prefix = f"year={now_est.year}/month={now_est.month:02d}/day={now_est.day:02d}/"
        yesterday_est = now_est - timedelta(days=1)
        yesterday_prefix = f"year={yesterday_est.year}/month={yesterday_est.month:02d}/day={yesterday_est.day:02d}/"
        
        st.sidebar.warning(f"🔍 Loading: {today_prefix} + yesterday...")
        
        # Load both days
        today_blobs = list(bucket.list_blobs(prefix=today_prefix, max_results=1500))
        yesterday_blobs = list(bucket.list_blobs(prefix=yesterday_prefix, max_results=1500))
        blobs = today_blobs + yesterday_blobs
        
        st.sidebar.info(f"📅 Today: {len(today_blobs)}, Yesterday: {len(yesterday_blobs)} files")
        
        if not blobs:
            # Fallback: try loading from entire today if no data in last 3 hours
            today_prefix = f"year={now_est.year}/month={now_est.month:02d}/day={now_est.day:02d}/"
            st.warning(f"⚠️ No data in last 3 hours, loading full day: {today_prefix}")
            blobs = list(bucket.list_blobs(prefix=today_prefix, max_results=1500))
            
        if not blobs:
            st.error(f"❌ No data found in gs://{BUCKET_NAME}/{PREFIX}")
            st.code(f"Expected path: gs://{BUCKET_NAME}/year=2025/month=11/day=27/...")
            return None
        
        # Sort by creation time - take most recent up to 1500 (covers 24+ hours)
        blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
        # Limit to reasonable max to avoid loading too much
        max_load = min(len(blobs_sorted), 1500)
        latest_blobs = blobs_sorted[:max_load]
        
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
        
        # Convert timestamps from UTC milliseconds to EST datetime
        # The aggregator stores window_start as UTC Unix timestamp in milliseconds
        # We need to convert to EST for proper comparison with current time
        combined_df['window_start'] = pd.to_datetime(combined_df['window_start'], unit='ms', utc=True)
        combined_df['window_end'] = pd.to_datetime(combined_df['window_end'], unit='ms', utc=True)
        
        # Convert from UTC to EST
        combined_df['window_start'] = combined_df['window_start'].dt.tz_convert(EASTERN)
        combined_df['window_end'] = combined_df['window_end'].dt.tz_convert(EASTERN)
        
        # Sort by timestamp
        combined_df = combined_df.sort_values('window_start', ascending=True)
        
        # Remove duplicates
        combined_df = combined_df.drop_duplicates(subset=['window_start'], keep='last')
        
        return combined_df
        
    except Exception as e:
        st.error(f"Error loading data from GCS: {e}")
        return None

# Load data - cached with TTL, not dependent on any parameter
# This means switching timelines uses cached data, only reloads when TTL expires
load_start = time.time()
with st.spinner("Loading latest data from GCS..."):
    df_full = load_data_from_gcs()
load_time = time.time() - load_start

if df_full is not None and not df_full.empty:
    # Show if data was cached (fast) or freshly loaded (slow)
    if load_time < 0.1:
        st.sidebar.success(f"⚡ Using cached data ({load_time*1000:.0f}ms)")
    else:
        st.sidebar.info(f"📥 Loaded fresh data ({load_time:.1f}s)")
    # Filter data by selected timeframe - ALL IN EASTERN TIME
    now_est = datetime.now(EASTERN)  # Keep timezone aware
    
    # Calculate cutoff time based on timeline selection (EST)
    if timeline_option == "5 Minutes":
        cutoff_time = now_est - timedelta(minutes=5)
    elif timeline_option == "15 Minutes":
        cutoff_time = now_est - timedelta(minutes=15)
    elif timeline_option == "30 Minutes":
        cutoff_time = now_est - timedelta(minutes=30)
    elif timeline_option == "1 Hour":
        cutoff_time = now_est - timedelta(hours=1)
    else:  # 1 Day
        cutoff_time = now_est - timedelta(hours=24)
    
    # The data is already timezone-aware (EST) from load_data_from_gcs
    # Filter data - window_start is already EST timezone-aware
    df = df_full[df_full['window_start'] >= cutoff_time].copy()
    
    # Debug: Show data range vs cutoff
    if not df_full.empty:
        data_min = df_full['window_start'].min()
        data_max = df_full['window_start'].max()
        st.sidebar.text(f"📊 Data range: {data_min.strftime('%H:%M:%S')} - {data_max.strftime('%H:%M:%S')}")
        st.sidebar.text(f"⏰ Cutoff: {cutoff_time.strftime('%H:%M:%S')}")
    
    if df.empty:
        st.warning(f"No data available for {timeline_option} (loaded {len(df_full)} records)")
        # Show more debug info
        if not df_full.empty:
            st.info(f"Data timestamps range: {data_min.strftime('%Y-%m-%d %H:%M:%S')} to {data_max.strftime('%Y-%m-%d %H:%M:%S')} EST")
            st.info(f"Looking for data after: {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} EST")
        df = df_full.copy()
    
    st.sidebar.info(f"Records loaded: {len(df)} / {len(df_full)}")
    
    # Display last update time - in EST
    last_update_est = df['window_start'].max()
    # Convert now_est to pandas Timestamp for proper comparison
    now_est_ts = pd.Timestamp(now_est)
    data_age_seconds = (now_est_ts - last_update_est).total_seconds()
    
    # Format for display (remove timezone info for cleaner display)
    last_update_display = last_update_est.strftime('%Y-%m-%d %H:%M:%S')
    st.sidebar.success(f"Last Data: {last_update_display} EST")
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
    
    # Divider before metrics
    st.markdown("---")
    
    # Metrics in a container to prevent duplicates
    metrics_container = st.container()
    
    with metrics_container:
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
        xaxis_title="Time (EST)",
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
    fig_volume.update_xaxes(title_text="Time (EST)", row=2, col=1)
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
    
    # Auto-refresh logic using session state
    st.sidebar.markdown("---")
    
    # Check if it's time to refresh
    current_time = time.time()
    time_since_refresh = current_time - st.session_state.last_refresh_time
    
    if time_since_refresh >= refresh_interval:
        # Time to refresh - clear cache and reload
        st.session_state.last_refresh_time = current_time
        st.sidebar.info("🔄 Refreshing data now...")
        st.cache_data.clear()  # Clear cache to force reload
        time.sleep(0.1)
        st.rerun()
    else:
        # Show countdown
        remaining = int(refresh_interval - time_since_refresh)
        st.sidebar.info(f"🔄 Next refresh in {remaining} seconds")
        st.sidebar.caption("💡 Switch timelines instantly - uses cached data!")
        
        # Rerun after 1 second to update countdown
        time.sleep(1)
        st.rerun()

else:
    st.error("No data available. Please check your GCS bucket and ensure data is being written.")
    st.info(f"Looking for data in: gs://{BUCKET_NAME}/{PREFIX}**/")
    st.info("Expected structure: gs://crypto-db-east1/year=2025/month=11/day=27/hour=04/*.parquet")
    
    # Retry button
    if st.button("Retry"):
        st.rerun()
