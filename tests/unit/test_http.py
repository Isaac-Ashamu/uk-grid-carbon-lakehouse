import httpx
import pytest
import respx

from gridcarbon.config import Settings
from gridcarbon.http import FetchError, HttpClient

URL = "https://example.test/data"


def _client(settings: Settings, sleeps: list[float]) -> HttpClient:
    return HttpClient(settings, sleeper=sleeps.append)


@respx.mock
def test_retries_transient_then_succeeds(settings: Settings) -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"ok": 1}),
        ]
    )
    sleeps: list[float] = []
    with _client(settings, sleeps) as c:
        result = c.get(URL)
    assert result.status_code == 200
    assert result.attempts == 3
    assert route.call_count == 3
    assert len(sleeps) == 2
    assert len(result.sha256) == 64


@respx.mock
def test_does_not_retry_client_errors(settings: Settings) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(404, text="nope"))
    with _client(settings, []) as c, pytest.raises(FetchError) as exc:
        c.get(URL)
    assert exc.value.status_code == 404
    assert route.call_count == 1


@respx.mock
def test_gives_up_after_max_attempts(settings: Settings) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(500))
    with _client(settings, []) as c, pytest.raises(FetchError, match="gave up"):
        c.get(URL)
    assert route.call_count == settings.http_max_attempts


@respx.mock
def test_retries_transport_errors(settings: Settings) -> None:
    route = respx.get(URL).mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200, content=b"x")]
    )
    with _client(settings, []) as c:
        result = c.get(URL)
    assert result.body == b"x"
    assert route.call_count == 2


@respx.mock
def test_rate_limit_backoff_grows_with_attempts(settings: Settings) -> None:
    settings = settings.model_copy(
        update={"http_rate_limit_backoff_seconds": 3.0, "http_min_interval_seconds": 0.0}
    )
    respx.get(URL).mock(
        side_effect=[httpx.Response(429), httpx.Response(429), httpx.Response(200, json={})]
    )
    sleeps: list[float] = []
    with _client(settings, sleeps) as c:
        c.get(URL)
    assert sleeps == [3.0, 6.0]


@respx.mock
def test_requests_to_same_host_are_throttled(settings: Settings) -> None:
    settings = settings.model_copy(update={"http_min_interval_seconds": 0.5})
    respx.get(URL).mock(return_value=httpx.Response(200, json={}))
    sleeps: list[float] = []
    with _client(settings, sleeps) as c:
        c.get(URL)
        c.get(URL)
    assert len(sleeps) == 1
    assert 0 < sleeps[0] <= 0.5
