from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .auth import API_URL, build_auth_headers, get_id_token, load_refresh_token


def _session_with_retries(total: int = 3, backoff: float = 0.5) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


@dataclass
class JQuantsClient:
    id_token: str
    api_url: str = API_URL
    timeout: float = 30.0

    def __post_init__(self) -> None:
        self.session = _session_with_retries()
        self.headers = build_auth_headers(self.id_token)

    @classmethod
    def from_env(cls) -> JQuantsClient:
        refresh = load_refresh_token()
        id_tok = get_id_token(refresh)
        return cls(id_token=id_tok)

    def get(self, path: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = f"{self.api_url}{path}"
        res = self.session.get(url, params=params or {}, headers=self.headers, timeout=self.timeout)
        return res

    def get_paginated(
        self, path: str, data_key: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Fetch all pages accumulating items under `data_key`.

        J-Quants uses `pagination_key` in response; pass it back in params to continue.
        """
        params = dict(params or {})
        res = self.get(path, params=params.copy())
        res.raise_for_status()
        payload = res.json()
        if data_key not in payload:
            return []
        data: list[dict[str, Any]] = list(payload[data_key])
        while "pagination_key" in payload:
            params["pagination_key"] = payload["pagination_key"]
            res = self.get(path, params=params.copy())
            res.raise_for_status()
            payload = res.json()
            data += payload.get(data_key, [])
        return data
