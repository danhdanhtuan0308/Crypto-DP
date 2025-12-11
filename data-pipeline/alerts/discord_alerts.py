"""
Discord Webhook Alerting System
Monitors Kafka pipeline health and sends alerts to Discord
"""

import os
import requests
import json
import logging
from datetime import datetime
import pytz
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

EASTERN = pytz.timezone('America/New_York')


class DiscordAlert:
    """Discord webhook alerting for pipeline monitoring"""
    
    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        self.enabled = bool(self.webhook_url)
        
        if not self.enabled:
            logger.warning("Discord webhook not configured - alerts disabled")
        else:
            logger.info("Discord alerts enabled")
    
    def _send_webhook(self, embed: Dict[str, Any]) -> bool:
        """Send a Discord webhook message with embed"""
        if not self.enabled:
            return False
        
        try:
            payload = {"embeds": [embed]}
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=5
            )
            
            if response.status_code == 204:
                return True
            else:
                logger.error(f"Discord webhook failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send Discord alert: {e}")
            return False
    
    def _create_embed(self, title: str, description: str, color: int, fields: list = None) -> Dict[str, Any]:
        """Create Discord embed structure"""
        timestamp = datetime.now(EASTERN).isoformat()
        
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": timestamp,
            "footer": {"text": "Crypto Data Pipeline Monitor"}
        }
        
        if fields:
            embed["fields"] = fields
        
        return embed
    
    # ========== Producer Alerts ==========
    
    def producer_started(self, product_id: str):
        """Alert when producer starts"""
        embed = self._create_embed(
            title="🚀 Producer Started",
            description=f"Kafka producer started for {product_id}",
            color=0x00FF00,  # Green
            fields=[
                {"name": "Product", "value": product_id, "inline": True},
                {"name": "Status", "value": "Running", "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def producer_failed(self, product_id: str, error: str):
        """Alert when producer fails"""
        embed = self._create_embed(
            title="❌ Producer Failed",
            description=f"Kafka producer failed for {product_id}",
            color=0xFF0000,  # Red
            fields=[
                {"name": "Product", "value": product_id, "inline": True},
                {"name": "Error", "value": error[:1000], "inline": False}
            ]
        )
        return self._send_webhook(embed)
    
    def producer_reconnecting(self, product_id: str, attempt: int):
        """Alert when producer is reconnecting"""
        embed = self._create_embed(
            title="🔄 Producer Reconnecting",
            description=f"WebSocket disconnected, reconnecting...",
            color=0xFFA500,  # Orange
            fields=[
                {"name": "Product", "value": product_id, "inline": True},
                {"name": "Attempt", "value": str(attempt), "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def producer_stale_data(self, product_id: str, seconds_stale: int):
        """Alert when producer has stale data (no price updates)"""
        embed = self._create_embed(
            title="⚠️ Stale Data Detected",
            description=f"No price updates for {seconds_stale} seconds",
            color=0xFFFF00,  # Yellow
            fields=[
                {"name": "Product", "value": product_id, "inline": True},
                {"name": "Time Since Update", "value": f"{seconds_stale}s", "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    # ========== Consumer Alerts ==========
    
    def consumer_started(self, topic: str, group_id: str):
        """Alert when consumer starts"""
        embed = self._create_embed(
            title="🚀 Consumer Started",
            description=f"Kafka consumer started",
            color=0x00FF00,  # Green
            fields=[
                {"name": "Topic", "value": topic, "inline": True},
                {"name": "Group ID", "value": group_id, "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def consumer_failed(self, topic: str, error: str):
        """Alert when consumer fails"""
        embed = self._create_embed(
            title="❌ Consumer Failed",
            description=f"Kafka consumer failed",
            color=0xFF0000,  # Red
            fields=[
                {"name": "Topic", "value": topic, "inline": True},
                {"name": "Error", "value": error[:1000], "inline": False}
            ]
        )
        return self._send_webhook(embed)
    
    def consumer_lag_warning(self, topic: str, lag: int, threshold: int):
        """Alert when consumer lag is high"""
        embed = self._create_embed(
            title="⚠️ Consumer Lag Warning",
            description=f"Consumer lag is high",
            color=0xFFA500,  # Orange
            fields=[
                {"name": "Topic", "value": topic, "inline": True},
                {"name": "Lag", "value": f"{lag} messages", "inline": True},
                {"name": "Threshold", "value": f"{threshold} messages", "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def consumer_no_data(self, topic: str, seconds: int):
        """Alert when consumer receives no data"""
        embed = self._create_embed(
            title="⚠️ No Data Received",
            description=f"Consumer has not received data for {seconds} seconds",
            color=0xFFFF00,  # Yellow
            fields=[
                {"name": "Topic", "value": topic, "inline": True},
                {"name": "Time Since Last Message", "value": f"{seconds}s", "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    # ========== Aggregator Alerts ==========
    
    def aggregator_started(self, source_topic: str, output_topic: str):
        """Alert when aggregator starts"""
        embed = self._create_embed(
            title="🚀 Aggregator Started",
            description=f"1-minute aggregator started",
            color=0x00FF00,  # Green
            fields=[
                {"name": "Source", "value": source_topic, "inline": True},
                {"name": "Output", "value": output_topic, "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def aggregator_failed(self, error: str):
        """Alert when aggregator fails"""
        embed = self._create_embed(
            title="❌ Aggregator Failed",
            description=f"1-minute aggregator failed",
            color=0xFF0000,  # Red
            fields=[
                {"name": "Error", "value": error[:1000], "inline": False}
            ]
        )
        return self._send_webhook(embed)
    
    # ========== GCS Writer Alerts ==========
    
    def gcs_write_success(self, file_path: str, record_count: int):
        """Alert when data is successfully written to GCS"""
        embed = self._create_embed(
            title="✅ GCS Write Success",
            description=f"Data written to GCS",
            color=0x00FF00,  # Green
            fields=[
                {"name": "File", "value": file_path, "inline": False},
                {"name": "Records", "value": str(record_count), "inline": True}
            ]
        )
        return self._send_webhook(embed)
    
    def gcs_write_failed(self, file_path: str, error: str):
        """Alert when GCS write fails"""
        embed = self._create_embed(
            title="❌ GCS Write Failed",
            description=f"Failed to write data to GCS",
            color=0xFF0000,  # Red
            fields=[
                {"name": "File", "value": file_path, "inline": False},
                {"name": "Error", "value": error[:1000], "inline": False}
            ]
        )
        return self._send_webhook(embed)
    
    # ========== Pipeline Health ==========
    
    def pipeline_healthy(self, components: Dict[str, str]):
        """Alert that pipeline is healthy"""
        fields = [{"name": k, "value": v, "inline": True} for k, v in components.items()]
        
        embed = self._create_embed(
            title="💚 Pipeline Healthy",
            description="All pipeline components running normally",
            color=0x00FF00,  # Green
            fields=fields
        )
        return self._send_webhook(embed)
    
    def pipeline_degraded(self, issues: Dict[str, str]):
        """Alert that pipeline is degraded"""
        fields = [{"name": k, "value": v, "inline": False} for k, v in issues.items()]
        
        embed = self._create_embed(
            title="⚠️ Pipeline Degraded",
            description="Some pipeline components have issues",
            color=0xFFA500,  # Orange
            fields=fields
        )
        return self._send_webhook(embed)
    
    # ========== General Alerts ==========
    
    def custom_alert(self, title: str, message: str, severity: str = "info"):
        """Send a custom alert"""
        color_map = {
            "info": 0x0099FF,    # Blue
            "success": 0x00FF00, # Green
            "warning": 0xFFA500, # Orange
            "error": 0xFF0000    # Red
        }
        
        embed = self._create_embed(
            title=title,
            description=message,
            color=color_map.get(severity, 0x0099FF)
        )
        return self._send_webhook(embed)


# Singleton instance
_alert_instance = None

def get_discord_alert() -> DiscordAlert:
    """Get or create Discord alert instance"""
    global _alert_instance
    if _alert_instance is None:
        _alert_instance = DiscordAlert()
    return _alert_instance
