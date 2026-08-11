from __future__ import annotations

import logging
import time

import httpx

log = logging.getLogger("http")


class HttpError(Exception):
    """Неретраибельная ошибка HTTP (4xx кроме 429) или исчерпание ретраев."""


class BaseClient:
    """Синхронный HTTP-клиент с ретраями на 429/5xx и явными таймаутами."""

    def __init__(self, base_url: str, token: str, *, max_retries: int = 5):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self._client = httpx.Client(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={"Authorization": f"OAuth {token}"},
        )

    def get(self, path: str, params: dict | None = None) -> dict:
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        backoff = 1.0
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._client.get(url, params=params)
            except httpx.TransportError as exc:
                log.warning("GET %s transport error (%s/%s): %s",
                            url, attempt, self.max_retries, exc)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning("GET %s -> %s (%s/%s), retry in %.1fs",
                            url, resp.status_code, attempt, self.max_retries, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise HttpError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")

        raise HttpError(f"GET {url} failed after {self.max_retries} retries")

    def close(self) -> None:
        self._client.close()
