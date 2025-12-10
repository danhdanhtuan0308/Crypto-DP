"""
Chart generation utilities using Plotly
"""

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd


def create_price_chart(df: pd.DataFrame) -> go.Figure:
    """Create OHLC candlestick chart"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = go.Figure(data=[go.Candlestick(
        x=df['window_start'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='BTC Price'
    )])
    
    fig.update_layout(
        xaxis_title="Time (EST)",
        yaxis_title="Price (USD)",
        height=500,
        xaxis_rangeslider_visible=False,
        hovermode='x unified',
        template='plotly_dark',
        autosize=True,
        margin=dict(l=50, r=50, t=50, b=50)
    )
    
    return fig


def create_volume_chart(df: pd.DataFrame) -> go.Figure:
    """Create volume analysis chart with total and buy/sell volumes"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Total Volume", "Buy vs Sell Volume"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    # Total Volume
    fig.add_trace(
        go.Bar(x=df['window_start'], y=df['total_volume_1m'], name='Total Volume', marker_color='blue'),
        row=1, col=1
    )
    
    # Buy vs Sell Volume
    fig.add_trace(
        go.Bar(x=df['window_start'], y=df['total_buy_volume_1m'], name='Buy Volume', marker_color='green'),
        row=2, col=1
    )
    fig.add_trace(
        go.Bar(x=df['window_start'], y=df['total_sell_volume_1m'], name='Sell Volume', marker_color='red'),
        row=2, col=1
    )
    
    fig.update_layout(height=700, showlegend=True, hovermode='x unified', template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    fig.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig.update_yaxes(title_text="Volume (BTC)", row=1, col=1)
    fig.update_yaxes(title_text="Volume (BTC)", row=2, col=1)
    
    return fig


def create_metrics_chart(df: pd.DataFrame) -> go.Figure:
    """Create advanced metrics chart with volatility and order imbalance"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Volatility", "Order Imbalance Ratio"),
        vertical_spacing=0.15,
        row_heights=[0.5, 0.5]
    )
    
    fig.add_trace(go.Scatter(x=df['window_start'], y=df['volatility_1m'], mode='lines+markers',
                            name='Volatility', line=dict(color='orange', width=2), fill='tozeroy'), row=1, col=1)
    
    colors = ['green' if val > 0 else 'red' for val in df['order_imbalance_ratio_1m']]
    fig.add_trace(go.Bar(x=df['window_start'], y=df['order_imbalance_ratio_1m'], 
                        name='Order Imbalance', marker_color=colors), row=2, col=1)
    
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    fig.update_layout(height=700, showlegend=True, hovermode='x unified', template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Volatility", row=1, col=1)
    fig.update_yaxes(title_text="Ratio", row=2, col=1)
    
    return fig


def create_trade_count_chart(df: pd.DataFrame) -> go.Figure:
    """Create trade count analysis chart"""
    if df is None or df.empty or 'trade_count_1m' not in df.columns:
        return go.Figure()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df['window_start'], y=df['trade_count_1m'], name='Trade Count',
                        marker_color='purple', hovertemplate='<b>Time</b>: %{x}<br><b>Trades</b>: %{y}<extra></extra>'))
    
    if len(df) >= 5:
        df_copy = df.copy()
        df_copy['trade_count_ma'] = df_copy['trade_count_1m'].rolling(window=5, min_periods=1).mean()
        fig.add_trace(go.Scatter(x=df_copy['window_start'], y=df_copy['trade_count_ma'],
                                name='5-Min Moving Avg', line=dict(color='orange', width=2, dash='dash')))
    
    fig.update_layout(xaxis_title="Time (EST)", yaxis_title="Number of Trades", height=500,
                     hovermode='x unified', showlegend=True, template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    
    return fig


def create_spread_depth_chart(df: pd.DataFrame) -> go.Figure:
    """Create bid-ask spread and order book depth chart"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = make_subplots(rows=2, cols=1, subplot_titles=("Bid-Ask Spread ($)", "Order Book Depth (2% levels)"),
                       vertical_spacing=0.15, row_heights=[0.5, 0.5])
    
    if 'avg_bid_ask_spread_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_bid_ask_spread_1m'], mode='lines+markers',
                                name='Bid-Ask Spread', line=dict(color='blue', width=2), fill='tozeroy',
                                fillcolor='rgba(0, 100, 255, 0.2)'), row=1, col=1)
    
    if 'avg_bid_depth_2pct_1m' in df.columns and 'avg_ask_depth_2pct_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_bid_depth_2pct_1m'], mode='lines',
                                name='Bid Depth', line=dict(color='green', width=2), stackgroup='depth'), row=2, col=1)
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_ask_depth_2pct_1m'], mode='lines',
                                name='Ask Depth', line=dict(color='red', width=2), stackgroup='depth'), row=2, col=1)
    
    fig.update_layout(height=700, showlegend=True, hovermode='x unified', template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    fig.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig.update_yaxes(title_text="Spread ($)", row=1, col=1)
    fig.update_yaxes(title_text="Depth (BTC)", row=2, col=1)
    
    return fig


def create_price_comparison_chart(df: pd.DataFrame) -> go.Figure:
    """Create price comparison chart: VWAP vs Mid-Price vs Close"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = go.Figure()
    
    if 'avg_vwap_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_vwap_1m'], mode='lines',
                                name='VWAP', line=dict(color='blue', width=2, dash='dot')))
    
    if 'avg_mid_price_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_mid_price_1m'], mode='lines',
                                name='Mid-Price', line=dict(color='green', width=2, dash='dash')))
    
    fig.add_trace(go.Scatter(x=df['window_start'], y=df['close'], mode='lines',
                            name='Close Price', line=dict(color='orange', width=2)))
    
    fig.update_layout(xaxis_title="Time (EST)", yaxis_title="Price ($)", height=500,
                     hovermode='x unified', showlegend=True, template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    
    return fig


def create_order_flow_chart(df: pd.DataFrame) -> go.Figure:
    """Create CVD & OFI (Order Flow) chart"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = make_subplots(rows=2, cols=1, subplot_titles=("Cumulative Volume Delta (CVD)", "Order Flow Imbalance (OFI)"),
                       vertical_spacing=0.15, row_heights=[0.5, 0.5])
    
    if 'cvd_1m' in df.columns:
        colors_cvd = ['green' if val > 0 else 'red' for val in df['cvd_1m']]
        fig.add_trace(go.Bar(x=df['window_start'], y=df['cvd_1m'], name='CVD', marker_color=colors_cvd), row=1, col=1)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)
    
    if 'total_ofi_1m' in df.columns:
        colors_ofi = ['green' if val > 0 else 'red' for val in df['total_ofi_1m']]
        fig.add_trace(go.Bar(x=df['window_start'], y=df['total_ofi_1m'], name='OFI', marker_color=colors_ofi), row=2, col=1)
        fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)
    
    fig.update_layout(height=700, showlegend=True, hovermode='x unified', template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    fig.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig.update_yaxes(title_text="CVD (BTC)", row=1, col=1)
    fig.update_yaxes(title_text="OFI", row=2, col=1)
    
    return fig


def create_market_quality_chart(df: pd.DataFrame) -> go.Figure:
    """Create Kyle's Lambda & Liquidity Health chart"""
    if df is None or df.empty:
        return go.Figure()
    
    fig = make_subplots(rows=2, cols=1, subplot_titles=("Kyle's Lambda (Price Impact)", "Liquidity Health Score"),
                       vertical_spacing=0.15, row_heights=[0.5, 0.5])
    
    if 'avg_kyles_lambda_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_kyles_lambda_1m'], mode='lines+markers',
                                name="Kyle's Lambda", line=dict(color='purple', width=2), fill='tozeroy',
                                fillcolor='rgba(128, 0, 128, 0.2)'), row=1, col=1)
    
    if 'avg_liquidity_health_1m' in df.columns:
        fig.add_trace(go.Scatter(x=df['window_start'], y=df['avg_liquidity_health_1m'], mode='lines+markers',
                                name='Liquidity Health', line=dict(color='teal', width=2), fill='tozeroy',
                                fillcolor='rgba(0, 128, 128, 0.2)'), row=2, col=1)
    
    fig.update_layout(height=700, showlegend=True, hovermode='x unified', template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    fig.update_xaxes(title_text="Time (EST)", row=2, col=1)
    fig.update_yaxes(title_text="Lambda ($/BTC)", row=1, col=1)
    fig.update_yaxes(title_text="Health (BTC)", row=2, col=1)
    
    return fig


def create_micro_price_chart(df: pd.DataFrame) -> go.Figure:
    """Create micro-price deviation chart"""
    if df is None or df.empty or 'avg_micro_price_deviation_1m' not in df.columns:
        return go.Figure()
    
    fig = go.Figure()
    
    colors_micro = ['green' if val > 0 else 'red' for val in df['avg_micro_price_deviation_1m']]
    fig.add_trace(go.Bar(x=df['window_start'], y=df['avg_micro_price_deviation_1m'],
                        name='Micro-Price Deviation', marker_color=colors_micro))
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    
    fig.update_layout(xaxis_title="Time (EST)", yaxis_title="Deviation ($)", height=500,
                     hovermode='x unified', showlegend=True, template='plotly_dark',
        autosize=True, margin=dict(l=50, r=50, t=50, b=50))
    
    return fig
