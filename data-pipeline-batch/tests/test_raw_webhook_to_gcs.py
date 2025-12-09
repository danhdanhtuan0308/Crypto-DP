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


def test_write_parquet_to_gcs_writes_expected_schema(fake_storage):
    """Ensure write_parquet_to_gcs writes parquet with expected columns and positive prices."""
    sample_row = {
        "symbol": "BTC-USD",
        "window_start": 1,
        "window_end": 60001,
        "window_start_est": "2025-12-08 12:00:00",
        "open": 100.0,
        "high": 110.0,
        "low": 99.5,
        "close": 105.0,
        "total_volume_1m": 6.0,
        "total_buy_volume_1m": 3.0,
        "total_sell_volume_1m": 3.0,
        "volume_24h": 10.0,
        "high_24h": 110.0,
        "low_24h": 90.0,
        "volatility_1m": 0.5,
        "order_imbalance_ratio_1m": 0.0,
        "trade_count_1m": 3,
        "avg_buy_sell_ratio_1m": 0.0,
        "price_change_1m": 5.0,
        "price_change_percent_1m": 5.0,
        "num_ticks": 3,
        "last_ingestion_time": "2025-12-08T12:00:59",
        "avg_bid_ask_spread_1m": 0.0,
        "avg_depth_2pct_1m": 0.0,
        "avg_bid_depth_2pct_1m": 0.0,
        "avg_ask_depth_2pct_1m": 0.0,
        "avg_vwap_1m": 0.0,
        "avg_micro_price_deviation_1m": 0.0,
        "cvd_1m": 0.0,
        "total_ofi_1m": 0.0,
        "avg_ofi_1m": 0.0,
        "avg_kyles_lambda_1m": 0.0,
        "avg_liquidity_health_1m": 0.0,
        "avg_mid_price_1m": 0.0,
        "latest_best_bid_1m": 0.0,
        "latest_best_ask_1m": 0.0,
        "volatility_regime": "medium",
    }

    ok = write_parquet_to_gcs([sample_row], "batch-btc-1h-east1")
    assert ok is True

    assert len(fake_storage) == 1
    path, data = next(iter(fake_storage.items()))
    assert "btc_1h_" in path and path.endswith(".parquet")
    assert len(data) > 0

    table = pq.read_table(io.BytesIO(data))
    expected_columns = set(sample_row.keys())
    assert set(table.column_names) == expected_columns
    assert table.num_rows == 1
    price_col = table.column("close").to_pylist()[0]
    assert price_col > 0
