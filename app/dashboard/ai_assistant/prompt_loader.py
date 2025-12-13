from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        content = f.read()

    # Simple ${ENV:-default} expansion
    def expand_env(text: str) -> str:
        import re

        pattern = re.compile(r"\$\{([A-Z0-9_]+)(:-([^}]*))?\}")

        def repl(m: re.Match[str]) -> str:
            name = m.group(1)
            default = m.group(3) if m.group(3) is not None else ""
            return os.getenv(name, default)

        return pattern.sub(repl, text)

    content = expand_env(content)
    return yaml.safe_load(content) or {}
