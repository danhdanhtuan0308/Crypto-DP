from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx


@dataclass(frozen=True)
class DeepSeekConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: float = 30.0


class DeepSeekError(RuntimeError):
    pass


def _read_api_key(env_name: str) -> str:
    api_key = os.getenv(env_name)
    if not api_key:
        raise DeepSeekError(
            f"Missing API key. Set environment variable `{env_name}`."
        )
    return api_key


def create_config(
    *,
    base_url: str,
    model: str,
    api_key_env: str,
    timeout_seconds: float,
) -> DeepSeekConfig:
    return DeepSeekConfig(
        base_url=base_url.rstrip("/"),
        api_key=_read_api_key(api_key_env),
        model=model,
        timeout_seconds=float(timeout_seconds),
    )


def chat_completions(
    config: DeepSeekConfig,
    *,
    messages: List[Dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 300,
) -> str:
    url = f"{config.base_url}/chat/completions"

    payload: Dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "stream": False,
    }

    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
    }

    try:
        with httpx.Client(timeout=config.timeout_seconds) as client:
            resp = client.post(url, json=payload, headers=headers)
    except httpx.RequestError as e:
        raise DeepSeekError(f"Network error calling DeepSeek: {e}") from e

    if resp.status_code >= 400:
        raise DeepSeekError(
            f"DeepSeek HTTP {resp.status_code}: {resp.text[:4000]}"
        )

    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        raise DeepSeekError(f"Unexpected DeepSeek response: {data}") from e
