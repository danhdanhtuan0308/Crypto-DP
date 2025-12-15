from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List

import httpx


@dataclass(frozen=True)
class GrokConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: float = 30.0


class GrokError(RuntimeError):
    pass


def _read_api_key(env_name: str) -> str:
    api_key = os.getenv(env_name)
    if not api_key:
        raise GrokError(f"Missing API key. Set environment variable `{env_name}`.")
    return api_key


def create_config(
    *,
    base_url: str,
    model: str,
    api_key_env: str,
    timeout_seconds: float,
) -> GrokConfig:
    return GrokConfig(
        base_url=base_url.rstrip("/"),
        api_key=_read_api_key(api_key_env),
        model=model,
        timeout_seconds=float(timeout_seconds),
    )


def chat_completions(
    config: GrokConfig,
    *,
    messages: List[Dict[str, str]],
    temperature: float = 0.2,
    max_tokens: int = 300,
) -> str:
    # xAI exposes an OpenAI-compatible chat completions endpoint
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
        raise GrokError(f"Network error calling Grok: {e}") from e

    if resp.status_code >= 400:
        raise GrokError(f"Grok HTTP {resp.status_code}: {resp.text[:4000]}")

    try:
        data = resp.json()
    except Exception as e:
        raise GrokError(f"Non-JSON Grok response: {resp.text[:4000]}") from e

    try:
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        raise GrokError(f"Unexpected Grok response: {data}") from e
