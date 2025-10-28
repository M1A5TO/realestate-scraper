from __future__ import annotations

import random
import time
import urllib.parse

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter


class RateLimiter:
    """Prosty limiter RPS. Blokujący, bez asyncio."""
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.01)
        self._t_last = 0.0
    def wait(self) -> None:
        now = time.monotonic()
        wait_for = self.min_interval - (now - self._t_last)
        if wait_for > 0:
            time.sleep(wait_for)
        self._t_last = time.monotonic()

class HttpClient:
    """Sync httpx + retry + limiter + proxy + nagłówki UA."""
    def __init__(
        self,
        user_agent: str,
        timeout_s: int = 20,
        rps: float = 0.3,
        proxies: dict[str, str] | None = None,
        extra_headers: dict[str, str] | None = None,
    ):
        self._limiter = RateLimiter(rps)
        headers = {
            "User-Agent": user_agent,
            "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
        }
        if extra_headers:
            headers.update(extra_headers)
        self._client = httpx.Client(
            timeout=httpx.Timeout(timeout_s),
            headers=headers,
            #proxies=proxies or None,
            follow_redirects=True,
            http2=False,
        )

    def close(self) -> None:
        self._client.close()

    @retry(
    retry=retry_if_exception_type(
        (
            httpx.ReadTimeout,
            httpx.ConnectTimeout,
            httpx.RemoteProtocolError,
            httpx.HTTPStatusError,
        )
    ),
    wait=wait_exponential_jitter(initial=1, max=20),
    stop=stop_after_attempt(5),
)
    def get(self, url: str, *,  accept: str | None = None) -> httpx.Response:
        self._limiter.wait()
        headers = {}
        if accept:
            headers["Accept"] = accept
        resp = self._client.get(url, headers=headers)
        # Honor Retry-After on 429/503 if obecne (prosty backoff lokalny)
        if resp.status_code in (429, 503):
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except ValueError:
                    time.sleep(3 + random.random() * 2)
        resp.raise_for_status()
        return resp

    @retry(
        retry=retry_if_exception_type(
            (
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                httpx.RemoteProtocolError,
                httpx.HTTPStatusError,
            )
        ),
        wait=wait_exponential_jitter(initial=1, max=20),
        stop=stop_after_attempt(5),
    )
    def head(self, url: str) -> httpx.Response:
        self._limiter.wait()
        resp = self._client.head(url)
        if resp.status_code in (429, 503):
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except ValueError:
                    time.sleep(3 + random.random() * 2)
        resp.raise_for_status()
        return resp

def build_proxies(http_proxy: str | None, https_proxy: str | None) -> dict[str, str] | None:
    proxies = {}
    if http_proxy:
        proxies["http://"] = http_proxy
    if https_proxy:
        proxies["https://"] = https_proxy
    return proxies or None

def join_url(base: str, href: str) -> str:
    return urllib.parse.urljoin(base, href)
