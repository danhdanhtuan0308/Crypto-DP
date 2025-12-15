from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass(frozen=True)
class ContextBuildResult:
    context: Dict[str, Any]
    rows_used: int


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        # Preserve timezone in ISO-8601 if present
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if pd.isna(value):
        return None
    # numpy scalars
    try:
        import numpy as np  # type: ignore

        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value)
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except Exception:
        pass
    return value


def build_market_context(
    df_full: pd.DataFrame,
    timeline_label: str,
    now_est: datetime,
    *,
    rows_max: int = 180,
    columns: Optional[List[str]] = None,
) -> ContextBuildResult:
    """Build a compact JSON-serializable context for LLM Q&A."""

    if df_full is None or df_full.empty:
        return ContextBuildResult(
            context={
                "meta": {
                    "timeline": timeline_label,
                    "now_est": now_est.isoformat(),
                    "rows": 0,
                    "note": "No data available.",
                },
                "latest": None,
                "rows": [],
            },
            rows_used=0,
        )

    df = df_full.copy()

    default_cols = [
        "window_start",
        "window_end",
        "open",
        "high",
        "low",
        "close",
        "total_volume_1m",
        "total_buy_volume_1m",
        "total_sell_volume_1m",
        "trade_count_1m",
        "volatility_1m",
        "price_change_percent_1m",
        "order_imbalance_ratio_1m",
        "avg_bid_ask_spread_1m",
        "cvd_1m",
        "total_ofi_1m",
        "avg_kyles_lambda_1m",
        "avg_liquidity_health_1m",
    ]

    use_cols = columns or [c for c in default_cols if c in df.columns]
    df = df[use_cols]

    # Window used for analysis (e.g., 1440 rows = ~24h). We will not necessarily send all rows.
    df_window = df.tail(max(1, rows_max)).copy()
    rows_window = int(len(df_window))

    latest_row = df_window.iloc[-1].to_dict()

    def row_to_json(record: Dict[str, Any]) -> Dict[str, Any]:
        return {k: _json_safe_value(v) for k, v in record.items()}

    # Always send only the most recent rows (keeps prompts small/stable)
    rows_sent_max = 180
    df_recent = df_window.tail(min(rows_sent_max, rows_window)).copy()
    rows = [row_to_json(r) for r in df_recent.to_dict(orient="records")]

    # 24h (or window) summary and hourly aggregates for longer-horizon context
    summary: Dict[str, Any] = {}
    try:
        ws_col = "window_start" if "window_start" in df_window.columns else None
        we_col = "window_end" if "window_end" in df_window.columns else None

        open_col = "open" if "open" in df_window.columns else None
        high_col = "high" if "high" in df_window.columns else None
        low_col = "low" if "low" in df_window.columns else None
        close_col = "close" if "close" in df_window.columns else None

        first = df_window.iloc[0]
        last = df_window.iloc[-1]

        start_ts = first.get(ws_col) if ws_col else None
        end_ts = last.get(we_col) if we_col else (last.get(ws_col) if ws_col else None)

        o = float(first.get(open_col)) if open_col and pd.notna(first.get(open_col)) else None
        c = float(last.get(close_col)) if close_col and pd.notna(last.get(close_col)) else None

        summary = {
            "window_rows": rows_window,
            "window_start": _json_safe_value(start_ts),
            "window_end": _json_safe_value(end_ts),
        }

        if open_col and close_col:
            summary["open"] = _json_safe_value(first.get(open_col))
            summary["close"] = _json_safe_value(last.get(close_col))
            if o and c and o != 0:
                summary["return_pct"] = float((c / o - 1.0) * 100.0)

        if high_col:
            summary["high"] = _json_safe_value(df_window[high_col].max())
        if low_col:
            summary["low"] = _json_safe_value(df_window[low_col].min())

        for col, agg in [
            ("total_volume_1m", "sum"),
            ("total_buy_volume_1m", "sum"),
            ("total_sell_volume_1m", "sum"),
            ("trade_count_1m", "sum"),
            ("volatility_1m", "mean"),
            ("avg_bid_ask_spread_1m", "mean"),
            ("cvd_1m", "mean"),
            ("total_ofi_1m", "mean"),
        ]:
            if col in df_window.columns:
                series = df_window[col]
                val = getattr(series, agg)()  # type: ignore[attr-defined]
                summary[col + "_" + agg] = _json_safe_value(val)
    except Exception:
        summary = {"window_rows": rows_window}

    hourly: List[Dict[str, Any]] = []
    try:
        if "window_start" in df_window.columns:
            tmp = df_window.copy()
            tmp["window_start"] = pd.to_datetime(tmp["window_start"], errors="coerce")
            tmp = tmp.dropna(subset=["window_start"]).set_index("window_start")

            # Prefer 1-hour buckets for a 24h window (~24 rows)
            def _first(s: pd.Series) -> Any:
                return s.iloc[0] if len(s) else None

            def _last(s: pd.Series) -> Any:
                return s.iloc[-1] if len(s) else None

            agg_map: Dict[str, Any] = {}
            if "open" in tmp.columns:
                agg_map["open"] = _first
            if "high" in tmp.columns:
                agg_map["high"] = "max"
            if "low" in tmp.columns:
                agg_map["low"] = "min"
            if "close" in tmp.columns:
                agg_map["close"] = _last
            for col in ["total_volume_1m", "total_buy_volume_1m", "total_sell_volume_1m", "trade_count_1m"]:
                if col in tmp.columns:
                    agg_map[col] = "sum"
            for col in ["volatility_1m", "avg_bid_ask_spread_1m", "order_imbalance_ratio_1m", "cvd_1m", "total_ofi_1m"]:
                if col in tmp.columns:
                    agg_map[col] = "mean"

            if agg_map:
                res = tmp.resample("1h").agg(agg_map).dropna(how="all")
                # Keep last 30 buckets max
                res = res.tail(30)
                for idx, row in res.reset_index().iterrows():
                    d = row.to_dict()
                    hourly.append({k: _json_safe_value(v) for k, v in d.items()})
    except Exception:
        hourly = []

    context = {
        "meta": {
            "timeline": timeline_label,
            "now_est": now_est.isoformat(),
            "rows_window": rows_window,
            "rows_sent": int(len(rows)),
        },
        "latest": row_to_json(latest_row),
        "summary": summary,
        "hourly": hourly,
        "rows": rows,
    }

    return ContextBuildResult(context=context, rows_used=rows_window)
