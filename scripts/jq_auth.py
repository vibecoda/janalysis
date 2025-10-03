#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os

from jqsys.auth import get_id_token, load_refresh_token
from jqsys.utils.env import load_env_file_if_present


def main() -> int:
    parser = argparse.ArgumentParser(description="Exchange J-Quants refresh token for idToken")
    parser.add_argument(
        "--refresh-token",
        dest="refresh_token",
        default=None,
        help="Refresh token (defaults to $JQ_REFRESH_TOKEN or .env)",
    )
    args = parser.parse_args()

    load_env_file_if_present()  # populate env if .env exists
    refresh = args.refresh_token or os.getenv("JQ_REFRESH_TOKEN") or load_refresh_token()
    id_token = get_id_token(refresh)
    print(json.dumps({"ok": True, "idToken_prefix": id_token[:16] + "...", "len": len(id_token)}))
    return 0


if __name__ == "__main__":
    main()
