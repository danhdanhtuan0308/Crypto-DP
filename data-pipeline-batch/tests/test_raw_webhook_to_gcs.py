import io
import os
from datetime import datetime, timedelta

import pyarrow.parquet as pq
import pytest

from raw_webhook_to_gcs import MinuteAggregator, write_parquet_to_gcs, EASTERN


def test_minute_aggregator_basic(monkeypatch):
    """Aggregate two minutes of ticks and verify calculations and positive prices."""
    agg = MinuteAggregator()

    base_minute = datetime(2025, 12, 8, 12, 0, tzinfo=EASTERN)
    minute_sequence = iter([
        base_minute,
        base_minute,
        base_minute + timedelta(minutes=1),
    ])

    def fake_get_est_minute():
        try:
            return next(minute_sequence)
        except StopIteration:
            return base_minute + timedelta(minutes=1)

    def fake_get_est_hour():
        return base_minute.replace(minute=0)

    monkeypatch.setattr(agg, "_get_est_minute", fake_get_est_minute)
    monkeypatch.setattr(agg, "_get_est_hour", fake_get_est_hour)

    tick1 = {"price": 100.0, "volume_1s": 1.0, "trade_count_1s": 1, "volume_24h": 10, "high_24h": 110, "low_24h": 90}
    tick2 = {"price": 110.0, "volume_1s": 2.0, "trade_count_1s": 1, "volume_24h": 10, "high_24h": 110, "low_24h": 90}
    tick3 = {"price": 105.0, "volume_1s": 3.0, "trade_count_1s": 1, "volume_24h": 10, "high_24h": 110, "low_24h": 90}

    agg.add_tick(tick1)
    agg.add_tick(tick2)
    agg.add_tick(tick3)

    assert len(agg.hourly_buffer) == 1
    row = agg.hourly_buffer[0]

    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 100.0
    assert row["close"] == 110.0
    assert row["total_volume_1m"] == 3.0
    assert row["trade_count_1m"] == 2
    assert row["price_change_1m"] == 10.0
    assert row["price_change_percent_1m"] == pytest.approx(10.0)
    assert row["volatility_regime"] in {"low", "medium", "high"}
    assert row["open"] > 0 and row["close"] > 0


class _FakeBlob:
    def __init__(self, name, bucket_store):
        self.name = name
        self.bucket_store = bucket_store
        self.size = 0

    def upload_from_file(self, file_obj, content_type=None):
        data = file_obj.read()
        self.bucket_store[self.name] = data
        self.size = len(data)


class _FakeBucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name):
        return _FakeBlob(name, self.store)


class _FakeClient:
    def __init__(self, store):
        self.store = store

    def bucket(self, name):
        return _FakeBucket(self.store)


@pytest.fixture
def fake_storage(monkeypatch):
    store = {}

    def fake_client(*args, **kwargs):
        return _FakeClient(store)

    monkeypatch.setenv("GCP_SERVICE_ACCOUNT_JSON", '{"project_id": "test", "client_email": "test@test.iam.gserviceaccount.com", "token_uri": "https://oauth2.googleapis.com/token", "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7W8jxkKEEqL3l\\n-----END PRIVATE KEY-----\\n"}')
    monkeypatch.setattr("raw_webhook_to_gcs.storage.Client", fake_client)
    return store
