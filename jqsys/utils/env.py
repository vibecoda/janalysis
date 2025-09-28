from __future__ import annotations

import os
from pathlib import Path
from typing import Dict


def load_env_file_if_present(path: str | Path = ".env") -> Dict[str, str]:
    """Load simple KEY=VALUE pairs from a .env file if present.

    This avoids adding a dependency on python-dotenv while supporting
    the common case where the refresh token is stored in `.env`.

    Returns a dict of loaded key-values (also updates os.environ for the process).
    Lines starting with '#' are ignored. Quoted values are unquoted.
    """
    env_path = Path(path)
    loaded: Dict[str, str] = {}
    if not env_path.exists():
        return loaded

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)
        loaded[key] = value
    return loaded

