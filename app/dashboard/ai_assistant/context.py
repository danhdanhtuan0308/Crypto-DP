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

    df_tail = df.tail(max(1, rows_max)).copy()
    rows_used = int(len(df_tail))

    latest_row = df_tail.iloc[-1].to_dict()

    def row_to_json(record: Dict[str, Any]) -> Dict[str, Any]:
        return {k: _json_safe_value(v) for k, v in record.items()}

    rows = [row_to_json(r) for r in df_tail.to_dict(orient="records")]

    context = {
        "meta": {
            "timeline": timeline_label,
            "now_est": now_est.isoformat(),
            "rows": rows_used,
        },
        "latest": row_to_json(latest_row),
        "rows": rows,
    }

    return ContextBuildResult(context=context, rows_used=rows_used)
