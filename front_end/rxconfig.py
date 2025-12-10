import reflex as rx

config = rx.Config(
    app_name="btc_dashboard",
    api_url="http://localhost:8001",
    disable_plugins=["reflex.plugins.sitemap.SitemapPlugin"],
)
