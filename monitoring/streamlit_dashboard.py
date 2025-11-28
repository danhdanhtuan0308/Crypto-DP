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

# Custom CSS to prevent text shadows and duplicates
st.markdown("""
    <style>
    /* Prevent text shadows and duplicates */
    .stMarkdown, .stText, .stCaption {
        text-shadow: none !important;
    }
    /* Ensure no duplicate rendering */
    [data-testid="stMarkdownContainer"] {
        opacity: 1 !important;
    }
    </style>
""", unsafe_allow_html=True)

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
if 'cached_dataframe' not in st.session_state:
    st.session_state.cached_dataframe = None
if 'last_loaded_time' not in st.session_state:
    st.session_state.last_loaded_time = None

# Sidebar configuration
st.sidebar.header("Settings")
refresh_interval = 60  # Fixed 60 seconds refresh
st.sidebar.info(f"⏱️ Auto-refresh: Every {refresh_interval} seconds")

# Clear cache button
if st.sidebar.button("🗑️ Clear Cache"):
    st.cache_data.clear()
    st.session_state.cached_dataframe = None
    st.session_state.last_loaded_time = None
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

def load_new_data_from_gcs(since_time=None):
    """Load new parquet files from GCS (incremental loading)
    
    Args:
        since_time: datetime - only load files created after this time (for incremental updates)
    
    Returns:
        DataFrame with newly loaded data
    
    Note: If since_time is None, loads full 24 hours. Otherwise, only loads files
    created after since_time (incremental - typically just 1-2 new files per minute).
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
        
        # If incremental load (since_time provided), filter to only new files
        if since_time is not None:
            # Convert since_time to timezone-aware if needed
            if since_time.tzinfo is None:
                since_time = pytz.UTC.localize(since_time)
            
            initial_count = len(blobs)
            blobs = [b for b in blobs if b.time_created.replace(tzinfo=pytz.UTC) > since_time]
            st.sidebar.success(f"🔄 Incremental: {len(blobs)} new files (from {initial_count} total)")
        
        # Handle different cases
        if not blobs:
            if since_time is not None:
                # Incremental load found no new files - return empty DataFrame
                st.sidebar.info("ℹ️ No new files to load")
                return pd.DataFrame()
            else:
                # First load found no files - error
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

# Smart incremental loading logic
load_start = time.time()

# Check if we have cached data
if st.session_state.cached_dataframe is not None:
    # We have cached data - just load NEW files since last load
    st.sidebar.info("⚡ Using cached data + loading new files...")
    with st.spinner("Loading new data..."):
        new_df = load_new_data_from_gcs(since_time=st.session_state.last_loaded_time)
    
    if new_df is not None and not new_df.empty:
        # Merge new data with cached data
        df_full = pd.concat([st.session_state.cached_dataframe, new_df], ignore_index=True)
        
        # Remove duplicates and sort
        df_full = df_full.sort_values('window_start', ascending=True)
        df_full = df_full.drop_duplicates(subset=['window_start'], keep='last')
        
        # Keep only last 24 hours (sliding window)
        cutoff_24h = datetime.now(EASTERN) - timedelta(hours=25)  # 25 to have buffer
        df_full = df_full[df_full['window_start'] >= cutoff_24h].copy()
        
        # Update cache
        st.session_state.cached_dataframe = df_full
        st.session_state.last_loaded_time = datetime.now(pytz.UTC)
        
        load_time = time.time() - load_start
        st.sidebar.success(f"✅ Added {len(new_df)} new records ({load_time:.1f}s)")
    else:
        # No new data, use existing cache
        df_full = st.session_state.cached_dataframe
        load_time = time.time() - load_start
        st.sidebar.info(f"⚡ No new data, using cache ({load_time*1000:.0f}ms)")
else:
    # First load - get all data
    st.sidebar.warning("📥 First load: Loading full 24 hours...")
    with st.spinner("Loading data from GCS..."):
        df_full = load_new_data_from_gcs(since_time=None)
    
    if df_full is not None and not df_full.empty:
        # Cache the data
        st.session_state.cached_dataframe = df_full
        st.session_state.last_loaded_time = datetime.now(pytz.UTC)
        load_time = time.time() - load_start
        st.sidebar.success(f"📥 Loaded {len(df_full)} records ({load_time:.1f}s)")
    else:
        load_time = time.time() - load_start

if df_full is not None and not df_full.empty:
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
                help=f"Regime: {latest.get('volatility_regime', 'N/A').upper()}"
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
        
        # Second row of microstructure metrics
        st.markdown("### 📊 Market Microstructure Metrics")
        col7, col8, col9, col10, col11 = st.columns(5)
        
        with col7:
            st.metric(
                label="Bid-Ask Spread",
                value=f"${latest.get('avg_bid_ask_spread_1m', 0):.4f}",
                help="Average spread between best bid and ask"
            )
        
        with col8:
            st.metric(
                label="CVD (1m)",
                value=f"{latest.get('cvd_1m', 0):.4f}",
                help="Cumulative Volume Delta (buy vol - sell vol)"
            )
        
        with col9:
            st.metric(
                label="OFI (Total)",
                value=f"{latest.get('total_ofi_1m', 0):.2f}",
                help="Order Flow Imbalance (bid depth changes - ask depth changes)"
            )
        
        with col10:
            st.metric(
                label="Kyle's Lambda",
                value=f"{latest.get('avg_kyles_lambda_1m', 0):.2f}",
                help="Price impact per unit volume (market depth)"
            )
        
        with col11:
            st.metric(
                label="Liquidity Health",
                value=f"{latest.get('avg_liquidity_health_1m', 0):.2f}",
                help="Total depth at top of book (bids + asks)"
            )
        
        # Third row - additional microstructure metrics
        col12, col13, col14, col15 = st.columns(4)
        
        with col12:
            st.metric(
                label="VWAP (1m)",
                value=f"${latest.get('avg_vwap_1m', 0):,.2f}",
                help="Volume-Weighted Average Price"
            )
        
        with col13:
            st.metric(
                label="Mid-Price",
                value=f"${latest.get('avg_mid_price_1m', 0):,.2f}",
                help="Average of best bid and ask"
            )
        
        with col14:
            st.metric(
                label="Micro-Price Dev",
                value=f"{latest.get('avg_micro_price_deviation_1m', 0):.4f}",
                help="Deviation from volume-weighted mid-price"
            )
        
        with col15:
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
    
    # Chart 4: Trade Count Analysis
    st.subheader("📊 Trade Count Analysis")
    
    # Check if trade_count_1m column exists
    if 'trade_count_1m' in df.columns:
        fig_trades = go.Figure()
        
        # Add bar chart for trade counts
        fig_trades.add_trace(
            go.Bar(
                x=df['window_start'],
                y=df['trade_count_1m'],
                name='Trade Count',
                marker_color='purple',
                hovertemplate='<b>Time</b>: %{x}<br><b>Trades</b>: %{y}<extra></extra>'
            )
        )
        
        # Add a line for moving average (if enough data points)
        if len(df) >= 5:
            df['trade_count_ma'] = df['trade_count_1m'].rolling(window=5, min_periods=1).mean()
            fig_trades.add_trace(
                go.Scatter(
                    x=df['window_start'],
                    y=df['trade_count_ma'],
                    name='5-Min Moving Avg',
                    line=dict(color='orange', width=2, dash='dash')
                )
            )
        
        fig_trades.update_layout(
            xaxis_title="Time (EST)",
            yaxis_title="Number of Trades",
            height=400,
            hovermode='x unified',
            showlegend=True
        )
        
        st.plotly_chart(fig_trades, width='stretch')
    else:
        st.info("Trade count data not available in the current dataset")
    
    # Chart 5: Microstructure Metrics - Spread & Depth
    st.subheader("📏 Bid-Ask Spread & Order Book Depth")
    
    fig_spread_depth = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Bid-Ask Spread ($)", "Order Book Depth (2% levels)"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # Bid-Ask Spread
    if 'avg_bid_ask_spread_1m' in df.columns:
        fig_spread_depth.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_bid_ask_spread_1m'],
                mode='lines+markers',
                name='Bid-Ask Spread',
                line=dict(color='blue', width=2),
                fill='tozeroy',
                fillcolor='rgba(0, 100, 255, 0.2)'
            ),
            row=1, col=1
        )
    
    # Order Book Depth (Bid vs Ask)
    if 'avg_bid_depth_2pct_1m' in df.columns and 'avg_ask_depth_2pct_1m' in df.columns:
        fig_spread_depth.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_bid_depth_2pct_1m'],
                mode='lines',
                name='Bid Depth',
                line=dict(color='green', width=2),
                stackgroup='depth'
            ),
            row=2, col=1
        )
        fig_spread_depth.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_ask_depth_2pct_1m'],
                mode='lines',
                name='Ask Depth',
                line=dict(color='red', width=2),
                stackgroup='depth'
            ),
            row=2, col=1
        )
    
    fig_spread_depth.update_layout(height=600, showlegend=True, hovermode='x unified')
    fig_spread_depth.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig_spread_depth.update_yaxes(title_text="Spread ($)", row=1, col=1)
    fig_spread_depth.update_yaxes(title_text="Depth (BTC)", row=2, col=1)
    
    st.plotly_chart(fig_spread_depth, width='stretch')
    
    # Chart 6: Price Metrics - VWAP, Mid-Price, Close
    st.subheader("💰 Price Comparison: VWAP vs Mid-Price vs Close")
    
    fig_prices = go.Figure()
    
    if 'avg_vwap_1m' in df.columns:
        fig_prices.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_vwap_1m'],
                mode='lines',
                name='VWAP',
                line=dict(color='blue', width=2, dash='dot')
            )
        )
    
    if 'avg_mid_price_1m' in df.columns:
        fig_prices.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_mid_price_1m'],
                mode='lines',
                name='Mid-Price',
                line=dict(color='green', width=2, dash='dash')
            )
        )
    
    fig_prices.add_trace(
        go.Scatter(
            x=df['window_start'],
            y=df['close'],
            mode='lines',
            name='Close Price',
            line=dict(color='orange', width=2)
        )
    )
    
    fig_prices.update_layout(
        xaxis_title="Time (EST)",
        yaxis_title="Price ($)",
        height=400,
        hovermode='x unified',
        showlegend=True
    )
    
    st.plotly_chart(fig_prices, width='stretch')
    
    # Chart 7: CVD & OFI (Order Flow)
    st.subheader("🌊 Order Flow: CVD & OFI")
    
    fig_flow = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Cumulative Volume Delta (CVD)", "Order Flow Imbalance (OFI)"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # CVD
    if 'cvd_1m' in df.columns:
        colors_cvd = ['green' if val > 0 else 'red' for val in df['cvd_1m']]
        fig_flow.add_trace(
            go.Bar(
                x=df['window_start'],
                y=df['cvd_1m'],
                name='CVD',
                marker_color=colors_cvd
            ),
            row=1, col=1
        )
        fig_flow.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)
    
    # OFI
    if 'total_ofi_1m' in df.columns:
        colors_ofi = ['green' if val > 0 else 'red' for val in df['total_ofi_1m']]
        fig_flow.add_trace(
            go.Bar(
                x=df['window_start'],
                y=df['total_ofi_1m'],
                name='OFI',
                marker_color=colors_ofi
            ),
            row=2, col=1
        )
        fig_flow.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    
    fig_flow.update_layout(height=600, showlegend=True, hovermode='x unified')
    fig_flow.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig_flow.update_yaxes(title_text="CVD (BTC)", row=1, col=1)
    fig_flow.update_yaxes(title_text="OFI", row=2, col=1)
    
    st.plotly_chart(fig_flow, width='stretch')
    
    # Chart 8: Market Quality - Kyle's Lambda & Liquidity Health
    st.subheader("🏥 Market Quality: Price Impact & Liquidity")
    
    fig_quality = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Kyle's Lambda (Price Impact)", "Liquidity Health Score"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # Kyle's Lambda
    if 'avg_kyles_lambda_1m' in df.columns:
        fig_quality.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_kyles_lambda_1m'],
                mode='lines+markers',
                name="Kyle's Lambda",
                line=dict(color='purple', width=2),
                fill='tozeroy',
                fillcolor='rgba(128, 0, 128, 0.2)'
            ),
            row=1, col=1
        )
    
    # Liquidity Health
    if 'avg_liquidity_health_1m' in df.columns:
        fig_quality.add_trace(
            go.Scatter(
                x=df['window_start'],
                y=df['avg_liquidity_health_1m'],
                mode='lines+markers',
                name='Liquidity Health',
                line=dict(color='teal', width=2),
                fill='tozeroy',
                fillcolor='rgba(0, 128, 128, 0.2)'
            ),
            row=2, col=1
        )
    
    fig_quality.update_layout(height=600, showlegend=True, hovermode='x unified')
    fig_quality.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig_quality.update_yaxes(title_text="Lambda ($/BTC)", row=1, col=1)
    fig_quality.update_yaxes(title_text="Health (BTC)", row=2, col=1)
    
    st.plotly_chart(fig_quality, width='stretch')
    
    # Chart 9: Micro-Price Deviation
    st.subheader("🎯 Micro-Price Deviation")
    
    fig_micro = go.Figure()
    
    if 'avg_micro_price_deviation_1m' in df.columns:
        colors_micro = ['green' if val > 0 else 'red' for val in df['avg_micro_price_deviation_1m']]
        fig_micro.add_trace(
            go.Bar(
                x=df['window_start'],
                y=df['avg_micro_price_deviation_1m'],
                name='Micro-Price Deviation',
                marker_color=colors_micro
            )
        )
        fig_micro.add_hline(y=0, line_dash="dash", line_color="gray")
    
    fig_micro.update_layout(
        xaxis_title="Time (EST)",
        yaxis_title="Deviation ($)",
        height=400,
        hovermode='x unified',
        showlegend=True
    )
    
    st.plotly_chart(fig_micro, width='stretch')
    
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
    
    # Statistics section with clear separator
    st.markdown("---")
    st.subheader("📊 Statistics Summary")
    
    # Create tabs for different statistic categories
    tab1, tab2, tab3, tab4 = st.tabs(["💰 Price & Volume", "📏 Spreads & Depth", "🌊 Order Flow", "🏥 Market Quality"])
    
    with tab1:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Price Statistics**")
            st.write(f"High: ${df['high'].max():,.2f}")
            st.write(f"Low: ${df['low'].min():,.2f}")
            st.write(f"Average: ${df['close'].mean():,.2f}")
            st.write(f"Range: ${df['high'].max() - df['low'].min():,.2f}")
        
        with col2:
            st.write("**Volume Statistics**")
            st.write(f"Total: {df['total_volume_1m'].sum():,.2f} BTC")
            st.write(f"Average: {df['total_volume_1m'].mean():,.2f} BTC")
            st.write(f"Max: {df['total_volume_1m'].max():,.2f} BTC")
            st.write(f"Min: {df['total_volume_1m'].min():,.2f} BTC")
        
        with col3:
            st.write("**Volatility Statistics**")
            st.write(f"Average: {df['volatility_1m'].mean():.4f}")
            st.write(f"Max: {df['volatility_1m'].max():.4f}")
            st.write(f"Min: {df['volatility_1m'].min():.4f}")
            st.write(f"Current: {latest['volatility_1m']:.4f}")
    
    with tab2:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Bid-Ask Spread**")
            if 'avg_bid_ask_spread_1m' in df.columns:
                st.write(f"Average: ${df['avg_bid_ask_spread_1m'].mean():.4f}")
                st.write(f"Min: ${df['avg_bid_ask_spread_1m'].min():.4f}")
                st.write(f"Max: ${df['avg_bid_ask_spread_1m'].max():.4f}")
                st.write(f"Current: ${latest.get('avg_bid_ask_spread_1m', 0):.4f}")
            else:
                st.write("N/A")
        
        with col2:
            st.write("**Bid Depth (2%)**")
            if 'avg_bid_depth_2pct_1m' in df.columns:
                st.write(f"Average: {df['avg_bid_depth_2pct_1m'].mean():.4f} BTC")
                st.write(f"Min: {df['avg_bid_depth_2pct_1m'].min():.4f} BTC")
                st.write(f"Max: {df['avg_bid_depth_2pct_1m'].max():.4f} BTC")
                st.write(f"Current: {latest.get('avg_bid_depth_2pct_1m', 0):.4f} BTC")
            else:
                st.write("N/A")
        
        with col3:
            st.write("**Ask Depth (2%)**")
            if 'avg_ask_depth_2pct_1m' in df.columns:
                st.write(f"Average: {df['avg_ask_depth_2pct_1m'].mean():.4f} BTC")
                st.write(f"Min: {df['avg_ask_depth_2pct_1m'].min():.4f} BTC")
                st.write(f"Max: {df['avg_ask_depth_2pct_1m'].max():.4f} BTC")
                st.write(f"Current: {latest.get('avg_ask_depth_2pct_1m', 0):.4f} BTC")
            else:
                st.write("N/A")
    
    with tab3:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**CVD (Cumulative Volume Delta)**")
            if 'cvd_1m' in df.columns:
                st.write(f"Average: {df['cvd_1m'].mean():.4f}")
                st.write(f"Total Range: {df['cvd_1m'].max() - df['cvd_1m'].min():.4f}")
                st.write(f"Current: {latest.get('cvd_1m', 0):.4f}")
                cvd_direction = "📈 Buying Pressure" if latest.get('cvd_1m', 0) > 0 else "📉 Selling Pressure"
                st.write(cvd_direction)
            else:
                st.write("N/A")
        
        with col2:
            st.write("**OFI (Order Flow Imbalance)**")
            if 'total_ofi_1m' in df.columns:
                st.write(f"Average: {df['total_ofi_1m'].mean():.2f}")
                st.write(f"Sum: {df['total_ofi_1m'].sum():.2f}")
                st.write(f"Current: {latest.get('total_ofi_1m', 0):.2f}")
                ofi_direction = "📈 Bid Pressure" if latest.get('total_ofi_1m', 0) > 0 else "📉 Ask Pressure"
                st.write(ofi_direction)
            else:
                st.write("N/A")
        
        with col3:
            st.write("**Micro-Price Deviation**")
            if 'avg_micro_price_deviation_1m' in df.columns:
                st.write(f"Average: ${df['avg_micro_price_deviation_1m'].mean():.4f}")
                st.write(f"Std Dev: ${df['avg_micro_price_deviation_1m'].std():.4f}")
                st.write(f"Current: ${latest.get('avg_micro_price_deviation_1m', 0):.4f}")
            else:
                st.write("N/A")
    
    with tab4:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write("**Kyle's Lambda (Price Impact)**")
            if 'avg_kyles_lambda_1m' in df.columns:
                st.write(f"Average: {df['avg_kyles_lambda_1m'].mean():.2f}")
                st.write(f"Std Dev: {df['avg_kyles_lambda_1m'].std():.2f}")
                st.write(f"Current: {latest.get('avg_kyles_lambda_1m', 0):.2f}")
                impact_level = "Low Impact" if abs(latest.get('avg_kyles_lambda_1m', 0)) < 1000 else "High Impact"
                st.write(f"Status: {impact_level}")
            else:
                st.write("N/A")
        
        with col2:
            st.write("**Liquidity Health**")
            if 'avg_liquidity_health_1m' in df.columns:
                st.write(f"Average: {df['avg_liquidity_health_1m'].mean():.2f} BTC")
                st.write(f"Min: {df['avg_liquidity_health_1m'].min():.2f} BTC")
                st.write(f"Max: {df['avg_liquidity_health_1m'].max():.2f} BTC")
                st.write(f"Current: {latest.get('avg_liquidity_health_1m', 0):.2f} BTC")
            else:
                st.write("N/A")
        
        with col3:
            st.write("**Volatility Regime**")
            if 'volatility_regime' in df.columns:
                regime_counts = df['volatility_regime'].value_counts()
                for regime, count in regime_counts.items():
                    pct = (count / len(df)) * 100
                    st.write(f"{regime.upper()}: {pct:.1f}%")
                st.write(f"Current: {latest.get('volatility_regime', 'N/A').upper()}")
            else:
                st.write("N/A")
    
    # Auto-refresh logic using session state
    st.sidebar.markdown("---")
    
    # Check if it's time to refresh
    current_time = time.time()
    time_since_refresh = current_time - st.session_state.last_refresh_time
    
    if time_since_refresh >= refresh_interval:
        # Time to refresh - DON'T clear cache, just reload page to fetch new data
        st.session_state.last_refresh_time = current_time
        st.sidebar.info("🔄 Fetching new data...")
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
