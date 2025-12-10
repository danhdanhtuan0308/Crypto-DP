"""
Dashboard State Management
Handles all state logic for the BTC Live Dashboard
"""

import reflex as rx
import pandas as pd
from datetime import datetime, timedelta
import pytz
import time
import asyncio
from typing import Optional
import plotly.graph_objects as go

from .data_loader import DataLoader, EASTERN


class DashboardState(rx.State):
    """State management for BTC Dashboard"""
    
    # Data storage
    cached_dataframe: Optional[pd.DataFrame] = None
    filtered_dataframe: Optional[pd.DataFrame] = None
    last_loaded_time: Optional[datetime] = None
    last_full_reload: float = 0.0
    
    # UI State
    selected_timeline: str = "1 Day"
    is_loading: bool = False
    error_message: str = ""
    status_message: str = ""
    
    # Metrics (latest values)
    current_price: float = 0.0
    price_change_percent: float = 0.0
    volatility: float = 0.0
    volatility_regime: str = ""
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    total_volume: float = 0.0
    trade_count: float = 0.0
    bid_ask_spread: float = 0.0
    cvd: float = 0.0
    ofi: float = 0.0
    kyles_lambda: float = 0.0
    liquidity_health: float = 0.0
    vwap: float = 0.0
    mid_price: float = 0.0
    micro_price_dev: float = 0.0
    order_imbalance: float = 0.0
    
    # Status info
    last_update_time: str = ""
    data_age_seconds: int = 0
    record_count: int = 0
    memory_usage_mb: float = 0.0
    
    # Auto-refresh state
    is_checking_for_new_data: bool = False
    
    # Chart data (stored as Plotly Figure objects for rx.plotly)
    price_chart_data: go.Figure = go.Figure()
    volume_chart_data: go.Figure = go.Figure()
    metrics_chart_data: go.Figure = go.Figure()
    trade_count_chart_data: go.Figure = go.Figure()
    spread_depth_chart_data: go.Figure = go.Figure()
    price_comparison_chart_data: go.Figure = go.Figure()
    order_flow_chart_data: go.Figure = go.Figure()
    market_quality_chart_data: go.Figure = go.Figure()
    micro_price_chart_data: go.Figure = go.Figure()
    
    async def on_load(self):
        """Called when the state is loaded - automatically load data like Streamlit"""
        if self.last_full_reload == 0.0:
            self.last_full_reload = time.time()
        # Automatically trigger data loading on every page load
        await self.load_data()
        # Start background polling for new files
        return DashboardState.check_for_new_data_background
    
    async def check_for_new_data_background(self):
        """Background task that checks for new files every 10 seconds"""
        while True:
            await asyncio.sleep(10)  # Check every 10 seconds
            
            if self.is_loading or self.is_checking_for_new_data:
                continue
            
            self.is_checking_for_new_data = True
            
            try:
                if self.last_loaded_time is not None:
                    data_loader = DataLoader()
                    has_new_files = data_loader.check_for_new_files(self.last_loaded_time)
                    
                    if has_new_files:
                        # New files detected - load them immediately
                        await self.load_data()
            except Exception as e:
                print(f"Error checking for new data: {str(e)}")
            finally:
                self.is_checking_for_new_data = False
    
    async def load_data(self):
        """Load data from GCS (background task)"""
        self.is_loading = True
        self.error_message = ""
        
        try:
            # Initialize data loader
            data_loader = DataLoader()
            
            # Force full reload every 2 hours
            time_since_full_reload = time.time() - self.last_full_reload
            FULL_RELOAD_INTERVAL = 2 * 3600  # 2 hours
            
            if time_since_full_reload > FULL_RELOAD_INTERVAL:
                self.cached_dataframe = None
                self.last_loaded_time = None
                self.last_full_reload = time.time()
            
            # Check if we have cached data
            if self.cached_dataframe is not None:
                # Check cache size
                cache_size_mb = self.cached_dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
                
                if cache_size_mb > 100:
                    self.cached_dataframe = None
                    self.last_loaded_time = None
                    self.last_full_reload = time.time()
                else:
                    # Incremental load - only get new minutes
                    new_df, error = data_loader.load_new_data_from_gcs(since_time=self.last_loaded_time)
                    
                    if error:
                        self.error_message = error
                    elif new_df is not None and not new_df.empty:
                        # Add new data to existing cache
                        df_full = pd.concat([self.cached_dataframe, new_df], ignore_index=True)
                        df_full = df_full.sort_values('window_start', ascending=True)
                        df_full = df_full.drop_duplicates(subset=['window_start'], keep='last')
                        
                        # ROLLING WINDOW: Keep exactly the last 1440 minutes (24 hours)
                        # When new minutes come in, oldest minutes are automatically dropped
                        if len(df_full) > 1440:
                            # Keep only the most recent 1440 minutes
                            df_full = df_full.tail(1440).copy()
                        else:
                            # Also enforce time-based cutoff (24 hours)
                            cutoff_24h = datetime.now(EASTERN) - timedelta(hours=24)
                            df_full = df_full[df_full['window_start'] >= cutoff_24h].copy()
                        
                        self.cached_dataframe = df_full
                        self.last_loaded_time = datetime.now(pytz.UTC)
                        self.status_message = f"Rolling window: {len(df_full)} minutes"
            
            # First load or forced reload
            if self.cached_dataframe is None:
                df_full, error = data_loader.load_new_data_from_gcs(since_time=None)
                
                if error:
                    self.error_message = error
                elif df_full is not None and not df_full.empty:
                    # Initial load: Get last 24 hours of data
                    cutoff_24h = datetime.now(EASTERN) - timedelta(hours=24)
                    df_full = df_full[df_full['window_start'] >= cutoff_24h].copy()
                    
                    # Limit to exactly 1440 minutes if we have more
                    if len(df_full) > 1440:
                        df_full = df_full.tail(1440).copy()
                    
                    self.cached_dataframe = df_full
                    self.last_loaded_time = datetime.now(pytz.UTC)
                    self.status_message = f"Initial load: {len(df_full)} minutes"
            
            # Filter data and update metrics
            await self.filter_data()
            
        except Exception as e:
            self.error_message = f"Error loading data: {str(e)}"
        finally:
            self.is_loading = False
    
    async def filter_data(self):
        """Filter data based on selected timeline and update metrics"""
        if self.cached_dataframe is None or self.cached_dataframe.empty:
            return
        
        now_est = datetime.now(EASTERN)
        
        # Calculate cutoff time based on timeline selection
        if self.selected_timeline == "5 Minutes":
            cutoff_time = now_est - timedelta(minutes=5)
        elif self.selected_timeline == "15 Minutes":
            cutoff_time = now_est - timedelta(minutes=15)
        elif self.selected_timeline == "30 Minutes":
            cutoff_time = now_est - timedelta(minutes=30)
        elif self.selected_timeline == "1 Hour":
            cutoff_time = now_est - timedelta(hours=1)
        else:  # 1 Day
            cutoff_time = now_est - timedelta(hours=24)
        
        # Filter data
        df = self.cached_dataframe[self.cached_dataframe['window_start'] >= cutoff_time].copy()
        
        if df.empty:
            df = self.cached_dataframe.copy()
        
        self.filtered_dataframe = df
        
        # Update status info
        last_update_est = df['window_start'].max()
        data_age_seconds = (pd.Timestamp(now_est) - last_update_est).total_seconds()
        
        self.last_update_time = last_update_est.strftime('%H:%M:%S')
        self.data_age_seconds = int(data_age_seconds)
        self.record_count = len(df)
        
        if self.cached_dataframe is not None:
            self.memory_usage_mb = self.cached_dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
        
        # Update metrics from latest record
        if not df.empty:
            latest = df.iloc[-1]
            
            self.current_price = float(latest['close'])
            self.price_change_percent = float(latest['price_change_percent_1m'])
            self.volatility = float(latest['volatility_1m'])
            self.volatility_regime = str(latest.get('volatility_regime', 'N/A')).upper()
            self.buy_volume = float(latest['total_buy_volume_1m'])
            self.sell_volume = float(latest.get('total_sell_volume_1m', 0))
            self.total_volume = float(latest.get('total_volume_1m', 0))
            self.trade_count = float(latest.get('trade_count_1m', 0))
            self.bid_ask_spread = float(latest.get('avg_bid_ask_spread_1m', 0))
            self.cvd = float(latest.get('cvd_1m', 0))
            self.ofi = float(latest.get('total_ofi_1m', 0))
            self.kyles_lambda = float(latest.get('avg_kyles_lambda_1m', 0))
            self.liquidity_health = float(latest.get('avg_liquidity_health_1m', 0))
            self.vwap = float(latest.get('avg_vwap_1m', 0))
            self.mid_price = float(latest.get('avg_mid_price_1m', 0))
            self.micro_price_dev = float(latest.get('avg_micro_price_deviation_1m', 0))
            self.order_imbalance = float(latest['order_imbalance_ratio_1m'])
        
        # Generate all charts
        self.update_charts()
    
    def update_charts(self):
        """Update all chart data"""
        if self.filtered_dataframe is None or self.filtered_dataframe.empty:
            return
        
        from . import charts
        
        try:
            self.price_chart_data = charts.create_price_chart(self.filtered_dataframe)
            self.volume_chart_data = charts.create_volume_chart(self.filtered_dataframe)
            self.metrics_chart_data = charts.create_metrics_chart(self.filtered_dataframe)
            self.trade_count_chart_data = charts.create_trade_count_chart(self.filtered_dataframe)
            self.spread_depth_chart_data = charts.create_spread_depth_chart(self.filtered_dataframe)
            self.price_comparison_chart_data = charts.create_price_comparison_chart(self.filtered_dataframe)
            self.order_flow_chart_data = charts.create_order_flow_chart(self.filtered_dataframe)
            self.market_quality_chart_data = charts.create_market_quality_chart(self.filtered_dataframe)
            self.micro_price_chart_data = charts.create_micro_price_chart(self.filtered_dataframe)
        except Exception as e:
            print(f"Error generating charts: {str(e)}")
    
    async def change_timeline(self, timeline: str):
        """Change the selected timeline"""
        self.selected_timeline = timeline
        await self.filter_data()
    
    async def clear_cache(self):
        """Clear cached data and force full reload"""
        self.cached_dataframe = None
        self.filtered_dataframe = None
        self.last_loaded_time = None
        self.last_full_reload = time.time()
        self.status_message = "Cache cleared"
        await self.load_data()
    def check_auto_refresh(self):
        """Check if it's time to auto-refresh (called every second)"""
        current_time = time.time()
        time_since_refresh = current_time - self.last_refresh_time
