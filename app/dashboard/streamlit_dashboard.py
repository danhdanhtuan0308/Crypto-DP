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
from pathlib import Path
import urllib.parse

# Local developer convenience: load env vars from .env if present
try:
    from dotenv import load_dotenv  # type: ignore

    _here = Path(__file__).resolve().parent
    load_dotenv(_here / ".env", override=False)
    load_dotenv(_here.parents[1] / ".env", override=False)  # repo root
except Exception:
    pass

from ai_assistant.chat import answer_with_deepseek

# Page configuration
st.set_page_config(
    page_title="BTC Live Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Eastern Time Zone - ALL times are in EST
EASTERN = pytz.timezone('America/New_York')

# Title
st.title("Bitcoin Live Dashboard")
st.caption("Real-time BTC data aggregated every minute")

# Plotly rendering config (avoids deprecated st.plotly_chart kwargs)
PLOTLY_CONFIG = {"responsive": True}

# Initialize session state for timeline persistence and last refresh time
if 'selected_timeline' not in st.session_state:
    st.session_state.selected_timeline = "1 Day"
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = time.time()
if 'cached_dataframe' not in st.session_state:
    st.session_state.cached_dataframe = None
if 'last_loaded_time' not in st.session_state:
    st.session_state.last_loaded_time = None
if 'last_full_reload' not in st.session_state:
    st.session_state.last_full_reload = time.time()

# Sidebar configuration
st.sidebar.header("Settings")
refresh_interval = 60  # Fixed 60 seconds refresh
st.sidebar.info(f"Auto-refresh: {refresh_interval}s")

# AI state
if 'ai_open' not in st.session_state:
        st.session_state.ai_open = False
if 'ai_messages' not in st.session_state:
        st.session_state.ai_messages = []



def _render_ai_fab() -> bool:
    """Render a floating AI button that toggles the panel without navigating."""
    st.markdown(
        """
<style>
/* FAB button styling - target the Streamlit button directly */
div[style*=\"position: fixed\"][style*=\"bottom: 30px\"] button {
    width: auto !important;
    height: 60px !important;
    border-radius: 30px !important;
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
    color: white !important;
    font-size: 16px !important;
    padding: 0 20px !important;
    cursor: pointer !important;
    box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
    border: none !important;
    min-width: auto !important;
    min-height: 60px !important;
    white-space: nowrap !important;
}
div[style*=\"position: fixed\"][style*=\"bottom: 30px\"] button:hover {
    transform: scale(1.05) !important;
    box-shadow: 0 6px 20px rgba(0,0,0,0.4) !important;
}
/* AI Panel overlay */
/* Only target the SPECIFIC empty container, not parent blocks */
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) {
    position: fixed !important;
    right: 30px !important;
    bottom: 110px !important;
    width: 400px !important;
    max-height: 550px !important;
    background: white !important;
    border-radius: 16px !important;
    border: 1px solid #e0e0e0 !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.12) !important;
    z-index: 99998 !important;
    overflow-y: auto !important;
    padding: 0px !important;
    padding-top: 45px !important; /* Space for close button */
}
/* Remove all default Streamlit padding/margins inside panel */
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) > div {
    padding: 0 !important;
    margin: 0 !important;
}
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) .element-container {
    padding: 10px !important;
    margin: 0 !important;
}
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) .stMarkdown {
    padding: 0 10px !important;
}
/* Ensure form is visible */
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) form {
    padding: 10px !important;
    margin: 0 !important;
    background: white !important;
}
/* Position close button in FIXED top-right corner of panel */
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) [data-testid="stButton"]:first-of-type {
    position: fixed !important;
    top: calc(100vh - 550px - 110px + 10px) !important; /* Match panel top + 10px */
    right: 40px !important; /* Panel right (30px) + 10px padding */
    width: 30px !important;
    height: 30px !important;
    z-index: 99999 !important;
    margin: 0 !important;
}
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) [data-testid="stButton"]:first-of-type button {
    width: 30px !important;
    height: 30px !important;
    min-width: 30px !important;
    min-height: 30px !important;
    padding: 0 !important;
    border-radius: 50% !important;
    background: rgba(0,0,0,0.1) !important;
    color: #333 !important;
    font-size: 18px !important;
    line-height: 1 !important;
    border: none !important;
}
div[data-testid="stVerticalBlock"] > div > div:has(#ai-panel-marker) [data-testid="stButton"]:first-of-type button:hover {
    background: rgba(0,0,0,0.2) !important;
}
/* Hide the marker itself */
#ai-panel-marker {
    display: none;
}
.ai-panel-header h3 {
    margin: 0;
    font-size: 18px;
    color: white;
}
.ai-panel-header p {
    margin: 4px 0 0 0;
    font-size: 12px;
    opacity: 0.9;
}
.ai-close-btn {
    background: transparent;
    border: none;
    color: white;
    font-size: 20px;
    cursor: pointer;
    padding: 4px 8px;
}
.ai-chat-messages {
    flex: 1;
    overflow-y: auto;
    padding: 16px;
    background: #f8f9fa;
}
.ai-chat-input {
    padding: 16px;
    background: white;
    border-top: 1px solid #e0e0e0;
}
</style>
""",
        unsafe_allow_html=True,
    )

    # Render button in fixed position container
    st.markdown('<div style="position: fixed; right: 30px; bottom: 30px; z-index: 99999;">', unsafe_allow_html=True)
    clicked = st.button("💬 Ask Danziel-AI", key="ai_fab_button")
    st.markdown('</div>', unsafe_allow_html=True)
    return clicked

# Clear cache button
if st.sidebar.button("Clear Cache"):
    st.cache_data.clear()
    st.session_state.cached_dataframe = None
    st.session_state.last_loaded_time = None
    st.rerun()

# Timeline selector
st.sidebar.subheader("Chart Timeline")
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

# Render floating AI button (always visible at top level)
if _render_ai_fab():
    st.session_state.ai_open = not st.session_state.ai_open
    st.rerun()

# GCS Configuration
BUCKET_NAME = 'crypto-db-east1'
PREFIX = 'RealTime/'  # Kafka pipeline writes to RealTime/ folder
GCP_PROJECT_ID = 'crypto-dp'  # Add your GCP project ID

def load_new_data_from_gcs(since_time=None, retry_count=0):
    """Load new parquet files from GCS (incremental loading)
    
    Args:
        since_time: datetime - only load files created after this time (for incremental updates)
        retry_count: int - number of retries attempted (for exponential backoff)
    
    Returns:
        DataFrame with newly loaded data
    
    Note: If since_time is None, loads full 24 hours. Otherwise, only loads files
    created after since_time (incremental - typically just 1-2 new files per minute).
    """
    max_retries = 3
    
    try:
        # Initialize client with credentials from environment or Streamlit secrets
        import os
        import json
        from pathlib import Path
        
        # Try environment variable first (Railway, local)
        creds_json = (
            os.getenv('GCP_SERVICE_ACCOUNT_JSON')
            or os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
        )

        # Try a credentials file path (local/dev)
        creds_path = os.getenv('GCS_CREDENTIALS_PATH') or os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        # Setup credentials
        if creds_json:
            from google.oauth2 import service_account
            creds_dict = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
        elif creds_path and Path(creds_path).exists():
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(creds_path)
            client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
        else:
            # Streamlit secrets are optional; accessing st.secrets can throw if no secrets.toml exists
            secrets_dict = {}
            try:
                secrets_dict = dict(getattr(st, "secrets", {}) or {})
            except Exception:
                secrets_dict = {}

            if 'gcp_service_account' in secrets_dict:
                from google.oauth2 import service_account

                credentials = service_account.Credentials.from_service_account_info(
                    secrets_dict["gcp_service_account"]
                )
                client = storage.Client(credentials=credentials, project=GCP_PROJECT_ID)
            else:
                st.error(
                    "GCP credentials not found. Set `GCP_SERVICE_ACCOUNT_JSON` (recommended) "
                    "or `GCS_CREDENTIALS_PATH` / `GOOGLE_APPLICATION_CREDENTIALS`."
                )
                client = storage.Client(project=GCP_PROJECT_ID)
        
        bucket = client.bucket(BUCKET_NAME)
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
            
            # List only current and previous hour (max ~120 files, typically just 1-2 new)
            current_hour_blobs = list(bucket.list_blobs(prefix=current_hour_prefix, max_results=100))
            prev_hour_blobs = list(bucket.list_blobs(prefix=prev_hour_prefix, max_results=100))
            all_blobs = current_hour_blobs + prev_hour_blobs
            
            # Filter to only files created AFTER since_time (with small buffer)
            since_time_with_buffer = since_time - timedelta(seconds=60)
            blobs = [b for b in all_blobs if b.time_created.replace(tzinfo=pytz.UTC) > since_time_with_buffer]
            
            st.sidebar.info(f"Incremental: {len(blobs)} new files")
        
        # FULL LOAD: Load rolling 24 hours
        else:
            st.sidebar.info("Full load: Rolling 24 hours")
            
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
                return pd.DataFrame()
            else:
                st.error(f"No data found in gs://{BUCKET_NAME}/{PREFIX}")
                return None
        
        # Sort by creation time and limit
        blobs_sorted = sorted(blobs, key=lambda x: x.time_created, reverse=True)
        max_load = min(len(blobs_sorted), 100 if since_time is not None else 1500)
        latest_blobs = blobs_sorted[:max_load]
        
        # Read parquet files in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import io
        
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
            st.error("No valid parquet files found")
            return None
        
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
        
        return combined_df
        
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        
        # Retry logic
        if retry_count < max_retries:
            wait_time = 2 ** retry_count
            time.sleep(wait_time)
            return load_new_data_from_gcs(since_time=since_time, retry_count=retry_count + 1)
        else:
            st.error("Max retries reached. Check GCS connection.")
            return None


def refresh_cached_data_from_gcs_for_ai() -> None:
    """Ensure the in-memory dataframe includes the newest 1-minute files from GCS."""
    if st.session_state.cached_dataframe is None:
        return

    new_df = load_new_data_from_gcs(since_time=st.session_state.last_loaded_time)
    if new_df is None or new_df.empty:
        return

    df_full_local = pd.concat([st.session_state.cached_dataframe, new_df], ignore_index=True)
    df_full_local = df_full_local.sort_values('window_start', ascending=True)
    df_full_local = df_full_local.drop_duplicates(subset=['window_start'], keep='last')

    cutoff_24h = datetime.now(EASTERN) - timedelta(hours=24, minutes=30)
    df_full_local = df_full_local[df_full_local['window_start'] >= cutoff_24h].copy()

    del st.session_state.cached_dataframe
    st.session_state.cached_dataframe = df_full_local
    st.session_state.last_loaded_time = datetime.now(pytz.UTC)

# Smart incremental loading logic with periodic full reload
load_start = time.time()

# Force full reload every 2 hours to prevent memory leaks and stale data
time_since_full_reload = time.time() - st.session_state.last_full_reload
FULL_RELOAD_INTERVAL = 2 * 3600  # 2 hours in seconds

if time_since_full_reload > FULL_RELOAD_INTERVAL:
    st.session_state.cached_dataframe = None
    st.session_state.last_loaded_time = None
    st.session_state.last_full_reload = time.time()

# Check if we have cached data
if st.session_state.cached_dataframe is not None:
    # Check cache size
    cache_size_mb = st.session_state.cached_dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
    
    if cache_size_mb > 100:
        st.session_state.cached_dataframe = None
        st.session_state.last_loaded_time = None
        st.session_state.last_full_reload = time.time()
    else:
        new_df = load_new_data_from_gcs(since_time=st.session_state.last_loaded_time)
        
        if new_df is not None and not new_df.empty:
            df_full = pd.concat([st.session_state.cached_dataframe, new_df], ignore_index=True)
            df_full = df_full.sort_values('window_start', ascending=True)
            df_full = df_full.drop_duplicates(subset=['window_start'], keep='last')
            
            # Keep only last 24 hours
            cutoff_24h = datetime.now(EASTERN) - timedelta(hours=24, minutes=30)
            df_full = df_full[df_full['window_start'] >= cutoff_24h].copy()
            
            del st.session_state.cached_dataframe
            st.session_state.cached_dataframe = df_full
            st.session_state.last_loaded_time = datetime.now(pytz.UTC)
        else:
            df_full = st.session_state.cached_dataframe

# First load or forced reload
if st.session_state.cached_dataframe is None:
    df_full = load_new_data_from_gcs(since_time=None)
    
    if df_full is not None and not df_full.empty:
        cutoff_24h = datetime.now(EASTERN) - timedelta(hours=24, minutes=30)
        df_full = df_full[df_full['window_start'] >= cutoff_24h].copy()
        
        st.session_state.cached_dataframe = df_full
        st.session_state.last_loaded_time = datetime.now(pytz.UTC)
    else:
        df_full = None

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
    
    if df.empty:
        df = df_full.copy()
    
    # Display status
    last_update_est = df['window_start'].max()
    data_age_seconds = (pd.Timestamp(now_est) - last_update_est).total_seconds()
    
    st.sidebar.text(f"Last Update: {last_update_est.strftime('%H:%M:%S')}")
    st.sidebar.text(f"Data Age: {int(data_age_seconds)}s")
    st.sidebar.text(f"Records: {len(df)}")
    
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
    
    st.plotly_chart(fig_price, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_volume, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_metrics, config=PLOTLY_CONFIG)
    
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
        
        st.plotly_chart(fig_trades, config=PLOTLY_CONFIG)
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
    
    st.plotly_chart(fig_spread_depth, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_prices, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_flow, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_quality, config=PLOTLY_CONFIG)
    
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
    
    st.plotly_chart(fig_micro, config=PLOTLY_CONFIG)
    
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
    
    st.dataframe(display_df.iloc[::-1])
    
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
    
    # Auto-refresh
    st.sidebar.markdown("---")
    
    if st.session_state.cached_dataframe is not None:
        mem_mb = st.session_state.cached_dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
        st.sidebar.text(f"Memory: {mem_mb:.1f}MB")
    
    # Auto-refresh (completely disabled while AI panel is open)
    if st.session_state.ai_open:
        st.sidebar.info("Auto-refresh paused while AI is open")
    else:
        current_time = time.time()
        time_since_refresh = current_time - st.session_state.last_refresh_time

        if time_since_refresh >= refresh_interval:
            st.session_state.last_refresh_time = current_time
            time.sleep(0.1)
            st.rerun()
        else:
            remaining = int(refresh_interval - time_since_refresh)
            st.sidebar.text(f"Refresh in: {remaining}s")
            time.sleep(1)
            st.rerun()

else:
    st.error("No data available. Check GCS bucket.")
    if st.button("Retry"):
        st.rerun()

# Render AI panel AFTER all dashboard content
if st.session_state.ai_open:
    # Use empty() to create a placeholder that we can style with CSS
    ai_container = st.empty()
    
    with ai_container.container():
        st.markdown('<div id="ai-panel-marker"></div>', unsafe_allow_html=True)
        
        # Close button (absolute positioned in top-right)
        if st.button("✕", key="ai_close_button"):
            st.session_state.ai_open = False
            st.rerun()
        
        # Header
        st.markdown("### 💬 Danziel-AI")
        st.caption("Ask questions about BTC market data")
        st.markdown("---")
        
        # Messages
        if len(st.session_state.ai_messages) == 0:
            st.info("Hi! I can help you analyze the current BTC market data. Ask me anything about prices, volumes, volatility, or order flow!")
        else:
            # Show chat history in a scrollable area
            for i, msg in enumerate(st.session_state.ai_messages):
                role = msg.get("role", "assistant")
                content = msg.get("content", "")
                if role == "user":
                    st.markdown(f"**You:** {content}")
                else:
                    st.markdown(f"**AI:** {content}")
                    st.markdown("")  # Add spacing between messages
        
        # Input
        with st.form("ai_chat_form", clear_on_submit=True):
            user_q = st.text_input("Type your question...", value="", label_visibility="collapsed")
            submitted = st.form_submit_button("Send", use_container_width=True)

        if submitted and user_q.strip():
            # Add user message immediately
            st.session_state.ai_messages.append({"role": "user", "content": user_q.strip()})
            
            # Add a "thinking" placeholder
            st.session_state.ai_messages.append({"role": "assistant", "content": "🤔 Thinking..."})
            st.rerun()

        # Check if last message is "thinking" - if so, get the actual response
        if (len(st.session_state.ai_messages) > 0 and 
            st.session_state.ai_messages[-1].get("content") == "🤔 Thinking..."):
            
            # Get the user's question (second to last message)
            user_question = st.session_state.ai_messages[-2].get("content", "")
            
            # Show spinner while getting response
            with st.spinner("AI is responding..."):
                refresh_cached_data_from_gcs_for_ai()
                df_for_ai = st.session_state.cached_dataframe
                
                if df_for_ai is not None:
                    now_est = datetime.now(EASTERN)
                    answer = answer_with_deepseek(
                        question=user_question,
                        df_full=df_for_ai,
                        timeline_label=st.session_state.selected_timeline,
                        now_est=now_est,
                    )
                    # Replace "thinking" message with actual response
                    st.session_state.ai_messages[-1] = {"role": "assistant", "content": answer}
                else:
                    st.session_state.ai_messages[-1] = {"role": "assistant", "content": "Sorry, no data available to answer your question."}
            
            st.rerun()
