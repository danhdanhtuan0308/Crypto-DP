"""
Pipeline Health Monitor
Monitors Kafka pipeline components and sends Discord alerts
"""

import time
import logging
from typing import Dict, Optional
from datetime import datetime
import pytz
from discord_alerts import get_discord_alert

logger = logging.getLogger(__name__)
EASTERN = pytz.timezone('America/New_York')


class PipelineHealthMonitor:
    """Monitor pipeline health and send alerts"""
    
    def __init__(self):
        self.alert = get_discord_alert()
        self.last_message_time = {}
        self.message_counts = {}
        self.error_counts = {}
        self.alert_cooldown = {}  # Prevent alert spam
        self.cooldown_period = 300  # 5 minutes between same alerts
        
    def _can_send_alert(self, alert_key: str) -> bool:
        """Check if we can send an alert (not in cooldown)"""
        now = time.time()
        if alert_key in self.alert_cooldown:
            if now - self.alert_cooldown[alert_key] < self.cooldown_period:
                return False
        self.alert_cooldown[alert_key] = now
        return True
    
    # ========== Producer Monitoring ==========
    
    def track_producer_message(self, component: str = "producer"):
        """Track producer message"""
        self.last_message_time[component] = time.time()
        self.message_counts[component] = self.message_counts.get(component, 0) + 1
    
    def check_producer_health(self, component: str = "producer", stale_threshold: int = 30) -> bool:
        """Check if producer is healthy (receiving data)"""
        if component not in self.last_message_time:
            return True  # Not started yet
        
        time_since_last = time.time() - self.last_message_time[component]
        
        if time_since_last > stale_threshold:
            alert_key = f"{component}_stale"
            if self._can_send_alert(alert_key):
                self.alert.producer_stale_data("BTC-USD", int(time_since_last))
                logger.warning(f"Producer stale: {time_since_last}s since last message")
            return False
        
        return True
    
    def report_producer_error(self, error: str):
        """Report producer error"""
        self.error_counts["producer"] = self.error_counts.get("producer", 0) + 1
        alert_key = "producer_error"
        if self._can_send_alert(alert_key):
            self.alert.producer_failed("BTC-USD", error)
    
    # ========== Consumer Monitoring ==========
    
    def track_consumer_message(self, topic: str):
        """Track consumer message"""
        key = f"consumer_{topic}"
        self.last_message_time[key] = time.time()
        self.message_counts[key] = self.message_counts.get(key, 0) + 1
    
    def check_consumer_health(self, topic: str, stale_threshold: int = 90) -> bool:
        """Check if consumer is healthy (receiving data)"""
        key = f"consumer_{topic}"
        
        if key not in self.last_message_time:
            return True  # Not started yet
        
        time_since_last = time.time() - self.last_message_time[key]
        
        if time_since_last > stale_threshold:
            alert_key = f"{key}_no_data"
            if self._can_send_alert(alert_key):
                self.alert.consumer_no_data(topic, int(time_since_last))
                logger.warning(f"Consumer no data: {time_since_last}s since last message on {topic}")
            return False
        
        return True
    
    def report_consumer_error(self, topic: str, error: str):
        """Report consumer error"""
        key = f"consumer_{topic}"
        self.error_counts[key] = self.error_counts.get(key, 0) + 1
        alert_key = f"{key}_error"
        if self._can_send_alert(alert_key):
            self.alert.consumer_failed(topic, error)
    
    def check_consumer_lag(self, topic: str, lag: int, threshold: int = 100):
        """Check consumer lag and alert if high"""
        if lag > threshold:
            alert_key = f"{topic}_lag_{lag//100}"  # Group by 100s to reduce spam
            if self._can_send_alert(alert_key):
                self.alert.consumer_lag_warning(topic, lag, threshold)
                logger.warning(f"High consumer lag on {topic}: {lag} messages")
    
    # ========== GCS Monitoring ==========
    
    def report_gcs_write_success(self, file_path: str, record_count: int, silent: bool = True):
        """Report successful GCS write"""
        # Only send alert for hourly writes or on request
        if not silent:
            self.alert.gcs_write_success(file_path, record_count)
        logger.info(f"GCS write success: {file_path} ({record_count} records)")
    
    def report_gcs_write_failure(self, file_path: str, error: str):
        """Report GCS write failure"""
        alert_key = "gcs_write_error"
        if self._can_send_alert(alert_key):
            self.alert.gcs_write_failed(file_path, error)
        logger.error(f"GCS write failed: {file_path} - {error}")
    
    # ========== Overall Health Check ==========
    
    def get_pipeline_status(self) -> Dict[str, str]:
        """Get status of all pipeline components"""
        status = {}
        now = time.time()
        
        # Check producer
        if "producer" in self.last_message_time:
            time_since = now - self.last_message_time["producer"]
            if time_since < 30:
                status["Producer"] = f"✅ Running ({self.message_counts.get('producer', 0)} msgs)"
            else:
                status["Producer"] = f"⚠️ Stale ({int(time_since)}s)"
        else:
            status["Producer"] = "⚪ Not Started"
        
        # Check aggregator
        if "consumer_BTC-USD" in self.last_message_time:
            time_since = now - self.last_message_time["consumer_BTC-USD"]
            if time_since < 90:
                status["Aggregator"] = f"✅ Running ({self.message_counts.get('consumer_BTC-USD', 0)} msgs)"
            else:
                status["Aggregator"] = f"⚠️ Stale ({int(time_since)}s)"
        else:
            status["Aggregator"] = "⚪ Not Started"
        
        # Check GCS consumer
        if "consumer_btc_1min_agg" in self.last_message_time:
            time_since = now - self.last_message_time["consumer_btc_1min_agg"]
            if time_since < 90:
                status["GCS Writer"] = f"✅ Running ({self.message_counts.get('consumer_btc_1min_agg', 0)} msgs)"
            else:
                status["GCS Writer"] = f"⚠️ Stale ({int(time_since)}s)"
        else:
            status["GCS Writer"] = "⚪ Not Started"
        
        # Check errors
        total_errors = sum(self.error_counts.values())
        if total_errors > 0:
            status["Errors"] = f"⚠️ {total_errors} total"
        else:
            status["Errors"] = "✅ None"
        
        return status
    
    def send_health_report(self):
        """Send periodic health report"""
        status = self.get_pipeline_status()
        
        # Check if any component has issues
        has_issues = any("⚠️" in v or "❌" in v for v in status.values())
        
        if has_issues:
            issues = {k: v for k, v in status.items() if "⚠️" in v or "❌" in v}
            self.alert.pipeline_degraded(issues)
        else:
            alert_key = "health_report"
            # Only send healthy report once per hour
            if self._can_send_alert(alert_key):
                self.alert.pipeline_healthy(status)


# Singleton instance
_monitor_instance = None

def get_health_monitor() -> PipelineHealthMonitor:
    """Get or create health monitor instance"""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = PipelineHealthMonitor()
    return _monitor_instance
