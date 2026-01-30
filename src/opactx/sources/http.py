from __future__ import annotations

from typing import Any

import httpx


class HttpSource:
    def __init__(
        self,
        project_dir: object,
        *,
        url: str,
        headers: dict[str, str] | None = None,
        timeout_s: float | None = None,
        **_: Any,
    ):
        self.url = url
        self.headers = headers or {}
        self.timeout = timeout_s

    def fetch(self) -> Any:
        response = httpx.get(self.url, headers=self.headers, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
