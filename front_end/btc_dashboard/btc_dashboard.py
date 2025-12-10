"""
Main BTC Live Dashboard Application
"""

import reflex as rx
from .state import DashboardState
from .components import sidebar, metrics_section, charts_section


def index() -> rx.Component:
    """Main dashboard page"""
    return rx.fragment(
        # Auto-refresh every 10 seconds - simple and reliable
        rx.script("""
            console.log('Auto-refresh setup: reloading every 10 seconds');
            setInterval(function() {
                console.log('[' + new Date().toLocaleTimeString() + '] Auto-reloading for new data');
                window.location.reload();
            }, 10000);
        """),
        
        rx.box(
            # Sidebar
            sidebar(),
            
            # Main content
            rx.box(
                rx.vstack(
                    # Header
                    rx.heading("Bitcoin Live Dashboard", size="8"),
                    rx.text("Real-time BTC data aggregated every minute", size="3", color="gray"),
                    
                    rx.divider(),
                    
                    # Metrics
                    metrics_section(),
                    
                    # Charts
                    charts_section(),
                    
                    spacing="4",
                    padding="6",
                    width="100%",
                ),
                margin_left="250px",
                width="calc(100% - 250px)",
                min_height="100vh",
            ),
        )
    )


# Create the app
app = rx.App()

# Add the index page with on_load event
app.add_page(index, route="/", title="BTC Live Dashboard", on_load=DashboardState.on_load)
