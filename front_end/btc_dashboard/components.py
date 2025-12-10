"""
UI Components for BTC Dashboard
"""

import reflex as rx
from .state import DashboardState


def sidebar() -> rx.Component:
    """Create sidebar with controls and status"""
    return rx.box(
        rx.vstack(
            rx.heading("Settings", size="6"),
            
            # Status info
            rx.divider(),
            rx.text("Status", weight="bold", size="4"),
            rx.cond(
                DashboardState.is_loading,
                rx.text("Loading...", color="blue"),
                rx.text("Ready", color="green")
            ),
            rx.cond(
                DashboardState.error_message != "",
                rx.text(DashboardState.error_message, color="red", size="2"),
                rx.fragment()
            ),
            
            # Last update info
            rx.divider(),
            rx.text(f"Last Update: {DashboardState.last_update_time}", size="2"),
            
            # Timeline selector
            rx.divider(),
            rx.text("Chart Timeline", weight="bold", size="4"),
            rx.radio_group(
                ["5 Minutes", "15 Minutes", "30 Minutes", "1 Hour", "1 Day"],
                value=DashboardState.selected_timeline,
                on_change=DashboardState.change_timeline,
                direction="column",
                size="3",
                spacing="3",
            ),
            
            # Action buttons
            rx.divider(),
            rx.button(
                "🔄 Load Data",
                on_click=DashboardState.load_data,
                size="3",
                width="100%",
                color_scheme="blue",
                variant="solid",
            ),
            rx.button(
                "🗑️ Clear Cache",
                on_click=DashboardState.clear_cache,
                size="3",
                width="100%",
                color_scheme="red",
                variant="soft",
            ),
            
            spacing="4",
            padding="4",
        ),
        width="250px",
        height="100vh",
        bg="var(--gray-2)",
        border_right="1px solid var(--gray-6)",
        position="fixed",
        overflow_y="auto",
    )


def metric_card(label: str, value: rx.Var, delta: rx.Var = None, help_text: str = "") -> rx.Component:
    """Create a metric card"""
    return rx.card(
        rx.vstack(
            rx.text(label, size="2", color="gray"),
            rx.text(value, size="5", weight="bold"),
            rx.cond(
                delta is not None,
                rx.text(delta, size="2", color="green"),
                rx.fragment()
            ),
            rx.cond(
                help_text != "",
                rx.text(help_text, size="1", color="gray"),
                rx.fragment()
            ),
            spacing="1",
            align="start",
        ),
        width="100%",
    )


def metrics_section() -> rx.Component:
    """Create metrics section with all key metrics"""
    return rx.cond(
        DashboardState.is_loading,
        # Show loading spinner for metrics
        rx.vstack(
            rx.heading("Key Metrics", size="6"),
            rx.hstack(
                rx.spinner(size="3"),
                rx.text("Loading metrics...", color="blue", size="4"),
                spacing="3",
            ),
            spacing="4",
            width="100%",
        ),
        # Show actual metrics when loaded
        rx.vstack(
            # Key Metrics Row
            rx.heading("Key Metrics", size="6"),
        rx.grid(
            metric_card(
                "Current Price",
                rx.text(f"${DashboardState.current_price:,.2f}"),
                rx.text(f"{DashboardState.price_change_percent:.2f}%"),
            ),
            metric_card(
                "Volatility (1m)",
                rx.text(f"{DashboardState.volatility:.4f}"),
                help_text=f"Regime: {DashboardState.volatility_regime}",
            ),
            metric_card(
                "Buy Volume (1m)",
                rx.text(f"{DashboardState.buy_volume:.4f} BTC"),
            ),
            metric_card(
                "Sell Volume (1m)",
                rx.text(f"{DashboardState.sell_volume:.4f} BTC"),
            ),
            metric_card(
                "Total Volume (1m)",
                rx.text(f"{DashboardState.total_volume:.4f} BTC"),
            ),
            metric_card(
                "Trade Count (1m)",
                rx.text(f"{DashboardState.trade_count:.0f}"),
            ),
            columns="6",
            spacing="4",
            width="100%",
        ),
        
        # Microstructure Metrics
        rx.heading("Market Microstructure Metrics", size="5", margin_top="4"),
        rx.grid(
            metric_card(
                "Bid-Ask Spread",
                rx.text(f"${DashboardState.bid_ask_spread:.4f}"),
                help_text="Average spread between best bid and ask",
            ),
            metric_card(
                "CVD (1m)",
                rx.text(f"{DashboardState.cvd:.4f}"),
                help_text="Cumulative Volume Delta",
            ),
            metric_card(
                "OFI (Total)",
                rx.text(f"{DashboardState.ofi:.2f}"),
                help_text="Order Flow Imbalance",
            ),
            metric_card(
                "Kyle's Lambda",
                rx.text(f"{DashboardState.kyles_lambda:.2f}"),
                help_text="Price impact per unit volume",
            ),
            metric_card(
                "Liquidity Health",
                rx.text(f"{DashboardState.liquidity_health:.2f}"),
                help_text="Total depth at top of book",
            ),
            columns="5",
            spacing="4",
            width="100%",
        ),
        
        # Additional Metrics
        rx.grid(
            metric_card(
                "VWAP (1m)",
                rx.text(f"${DashboardState.vwap:,.2f}"),
                help_text="Volume-Weighted Average Price",
            ),
            metric_card(
                "Mid-Price",
                rx.text(f"${DashboardState.mid_price:,.2f}"),
                help_text="Average of best bid and ask",
            ),
            metric_card(
                "Micro-Price Dev",
                rx.text(f"{DashboardState.micro_price_dev:.4f}"),
                help_text="Deviation from volume-weighted mid-price",
            ),
            metric_card(
                "Order Imbalance",
                rx.text(f"{DashboardState.order_imbalance:.4f}"),
                delta=rx.cond(
                    DashboardState.order_imbalance > 0,
                    rx.text("Buy 📈"),
                    rx.text("Sell 📉")
                ),
            ),
            columns="4",
            spacing="4",
            width="100%",
        ),
        
        spacing="4",
        width="100%",
        )
    )


def charts_section() -> rx.Component:
    """Create charts section with all Plotly charts"""
    return rx.cond(
        DashboardState.is_loading,
        # Loading state
        rx.vstack(
            rx.heading("Charts", size="6", margin_top="6"),
            rx.spinner(size="3"),
            rx.text("Loading data from GCS...", color="blue", size="4"),
            spacing="4",
            width="100%",
            align="center",
            padding="8",
        ),
        rx.cond(
            DashboardState.record_count == 0,
            # No data state
            rx.vstack(
                rx.heading("Charts", size="6", margin_top="6"),
                rx.text("No data available. Data will load automatically...", color="gray", size="4"),
                spacing="4",
                width="100%",
            ),
            # Data loaded - show charts
            rx.vstack(
            rx.heading("Charts", size="6", margin_top="6"),
            
            # Price Chart
            rx.heading("📈 BTC Price Movement (OHLC)", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.price_chart_data), width="100%"),
            
            # Volume Chart
            rx.heading("📊 Volume Analysis", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.volume_chart_data), width="100%"),
            
            # Metrics Chart
            rx.heading("🔬 Advanced Metrics", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.metrics_chart_data), width="100%"),
            
            # Trade Count Chart
            rx.heading("📊 Trade Count Analysis", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.trade_count_chart_data), width="100%"),
            
            # Spread & Depth Chart
            rx.heading("📏 Bid-Ask Spread & Order Book Depth", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.spread_depth_chart_data), width="100%"),
            
            # Price Comparison Chart
            rx.heading("💰 Price Comparison: VWAP vs Mid-Price vs Close", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.price_comparison_chart_data), width="100%"),
            
            # Order Flow Chart
            rx.heading("🌊 Order Flow: CVD & OFI", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.order_flow_chart_data), width="100%"),
            
            # Market Quality Chart
            rx.heading("🏥 Market Quality: Price Impact & Liquidity", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.market_quality_chart_data), width="100%"),
            
            # Micro Price Chart
            rx.heading("🎯 Micro-Price Deviation", size="5", margin_top="4"),
            rx.box(rx.plotly(data=DashboardState.micro_price_chart_data), width="100%"),
            
            spacing="4",
            width="100%",
            )
        )
    )
