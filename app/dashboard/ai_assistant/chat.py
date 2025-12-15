from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .context import build_market_context
from .grok_client import GrokError, chat_completions, create_config
from .prompt_loader import load_yaml


def answer_with_ai(
    *,
    question: str,
    df_full: pd.DataFrame,
    timeline_label: str,
    now_est,
    package_dir: Optional[Path] = None,
) -> str:
    package_dir = package_dir or Path(__file__).resolve().parent

    prompts = load_yaml(package_dir / "prompts.yaml")
    ai_cfg = load_yaml(package_dir / "ai_config.yaml")

    rows_max = int(ai_cfg.get("data_rows_max", 180))
    ctx = build_market_context(
        df_full,
        timeline_label=timeline_label,
        now_est=now_est,
        rows_max=rows_max,
    )

    system_prompt = str(prompts.get("system", ""))
    user_prefix = str(prompts.get("user_prefix", "User question:\n"))
    context_prefix = str(prompts.get("context_prefix", "Data context (JSON):\n"))

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"{user_prefix}{question}\n\n"
                f"{context_prefix}{json.dumps(ctx.context, ensure_ascii=False)}"
            ),
        },
    ]

    cfg = create_config(
        base_url=str(ai_cfg.get("base_url", "https://api.x.ai/v1")),
        model=str(ai_cfg.get("model", "grok-4-1-fast-reasoning")),
        api_key_env=str(ai_cfg.get("api_key_env", "GROK_API_KEY")),
        timeout_seconds=float(ai_cfg.get("request_timeout_seconds", 30)),
    )

    try:
        return chat_completions(
            cfg,
            messages=messages,
            temperature=float(ai_cfg.get("temperature", 0.2)),
            max_tokens=int(ai_cfg.get("max_tokens", 500)),
        )
    except GrokError as e:
        return f"AI error: {e}"
