"""
API endpoints for serving chart data
"""

import reflex as rx
from .state import DashboardState
from . import charts


@rx.route("/api/charts")
def get_charts():
    """API endpoint to return all chart data as JSON"""
    state = DashboardState()
    
    if state.filtered_dataframe is None or state.filtered_dataframe.empty:
        return {}
    
    df = state.filtered_dataframe
    
    result = {}
    
    try:
        result['price_chart'] = charts.create_price_chart(df)
        result['volume_chart'] = charts.create_volume_chart(df)
        result['metrics_chart'] = charts.create_metrics_chart(df)
        
        trade_count = charts.create_trade_count_chart(df)
        if trade_count:
            result['trade_count_chart'] = trade_count
        
        result['spread_depth_chart'] = charts.create_spread_depth_chart(df)
        result['price_comparison_chart'] = charts.create_price_comparison_chart(df)
        result['order_flow_chart'] = charts.create_order_flow_chart(df)
        result['market_quality_chart'] = charts.create_market_quality_chart(df)
        
        micro_price = charts.create_micro_price_chart(df)
        if micro_price:
            result['micro_price_chart'] = micro_price
    except Exception as e:
        print(f"Error generating charts: {str(e)}")
    
    return result
