from __future__ import annotations

import os

import requests

from jqsys.core.utils.env import load_env_file_if_present

API_URL = "https://api.jquants.com"


class AuthError(RuntimeError):
    pass


def load_refresh_token(env_key: str = "JQ_REFRESH_TOKEN", dotenv: bool = True) -> str:
    """Return the J-Quants refresh token from environment or .env.

    Raises AuthError if missing.
    """
    if dotenv:
        load_env_file_if_present()
    token = os.getenv(env_key)
    if not token:
        raise AuthError(f"Missing refresh token. Set {env_key} in environment or .env")
    return token


def get_id_token(refresh_token: str | None = None, api_url: str = API_URL) -> str:
    """Exchange a refresh token for an idToken suitable for API calls.

    According to the J-Quants quick start, POST to
    /v1/token/auth_refresh?refreshtoken=<refresh_token>
    and extract `idToken` from the JSON response.
    """
    token = refresh_token or load_refresh_token()
    url = f"{api_url}/v1/token/auth_refresh"
    # Quick-start uses query param; keep consistent for simplicity.
    res = requests.post(f"{url}?refreshtoken={token}")
    if res.status_code != 200:
        try:
            detail = res.json()
        except Exception:
            detail = res.text
        raise AuthError(f"Auth refresh failed: {res.status_code} {detail}")
    data = res.json()
    id_token = data.get("idToken")
    if not id_token:
        raise AuthError("Auth refresh succeeded but idToken missing in response")
    return id_token


def build_auth_headers(id_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {id_token}"}
