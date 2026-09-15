"""A small, well-behaved HTTP client for public open-data APIs.

Retries with exponential backoff and jitter on transient failures (connection
errors, timeouts, 429 and 5xx), respects ``Retry-After`` where servers send it,
never retries on 4xx that will not change (400, 404), and returns the raw body
bytes alongside a fetch record so bronze can store exactly what came off the
wire together with its provenance.
"""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from gridcarbon.config import Settings, get_settings
from gridcarbon.logging import get_logger

log = get_logger(__name__)

RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class FetchError(RuntimeError):
    """Raised after all retry attempts are exhausted or on a non-retryable failure."""

    def __init__(self, url: str, message: str, status_code: int | None = None) -> None:
        super().__init__(f"{message} ({url})")
        self.url = url
        self.status_code = status_code


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int
    body: bytes
    fetched_at: datetime
    elapsed_ms: int
    attempts: int
    content_type: str | None

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


def _sleep_for(
    attempt: int, settings: Settings, retry_after: str | None, status: int | None = None
) -> float:
    if retry_after:
        try:
            return min(float(retry_after), 120.0)
        except ValueError:
            pass
    if status == 429:
        # Rate limits need patience, not a fast exponential: wait longer on each attempt.
        return float(min(settings.http_rate_limit_backoff_seconds * attempt, 120.0))
    base = settings.http_min_backoff_seconds * (2 ** (attempt - 1))
    capped = min(base, settings.http_max_backoff_seconds)
    return float(capped * (0.5 + random.random() / 2))  # full jitter, never below half the cap


class HttpClient:
    """Thin wrapper over ``httpx.Client`` with retry semantics the sources rely on."""

    def __init__(
        self,
        settings: Settings | None = None,
        transport: httpx.BaseTransport | None = None,
        sleeper: Any = time.sleep,
    ) -> None:
        self.settings = settings or get_settings()
        self._sleep = sleeper
        self._last_request_at: dict[str, float] = {}
        self._client = httpx.Client(
            timeout=self.settings.http_timeout_seconds,
            headers={"User-Agent": self.settings.http_user_agent, "Accept": "application/json"},
            transport=transport,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self, url: str) -> None:
        """Space requests to the same host by ``http_min_interval_seconds``."""
        host = httpx.URL(url).host
        interval = self.settings.http_min_interval_seconds
        last = self._last_request_at.get(host)
        now = time.monotonic()
        if last is not None and interval > 0 and now - last < interval:
            self._sleep(interval - (now - last))
        self._last_request_at[host] = time.monotonic()

    def get(self, url: str, params: dict[str, Any] | None = None) -> FetchResult:
        attempts = self.settings.http_max_attempts
        started = time.perf_counter()
        last_error: str = "unknown"
        for attempt in range(1, attempts + 1):
            self._throttle(url)
            try:
                response = self._client.get(url, params=params)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("http.transport_error", url=url, attempt=attempt, error=last_error)
                if attempt == attempts:
                    raise FetchError(
                        url, f"gave up after {attempts} attempts: {last_error}"
                    ) from exc
                self._sleep(_sleep_for(attempt, self.settings, None))
                continue

            if response.status_code in RETRYABLE_STATUS:
                last_error = f"HTTP {response.status_code}"
                log.warning(
                    "http.retryable_status",
                    url=str(response.url),
                    attempt=attempt,
                    status=response.status_code,
                )
                if attempt == attempts:
                    raise FetchError(
                        url,
                        f"gave up after {attempts} attempts: {last_error}",
                        response.status_code,
                    )
                self._sleep(
                    _sleep_for(
                        attempt,
                        self.settings,
                        response.headers.get("Retry-After"),
                        response.status_code,
                    )
                )
                continue

            if response.status_code >= 400:
                raise FetchError(
                    str(response.url),
                    f"HTTP {response.status_code}: {response.text[:200]}",
                    response.status_code,
                )

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            log.info(
                "http.fetched",
                url=str(response.url),
                status=response.status_code,
                bytes=len(response.content),
                attempts=attempt,
                elapsed_ms=elapsed_ms,
            )
            return FetchResult(
                url=str(response.url),
                status_code=response.status_code,
                body=response.content,
                fetched_at=datetime.now(tz=UTC),
                elapsed_ms=elapsed_ms,
                attempts=attempt,
                content_type=response.headers.get("Content-Type"),
            )
        raise FetchError(url, f"unreachable: {last_error}")  # pragma: no cover
