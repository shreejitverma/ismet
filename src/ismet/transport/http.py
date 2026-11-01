"""HTTP transport on httpx with retry, rate limiting, circuit breaking, hooks.

Every provider REST call goes through :class:`HttpTransport.request`, which:

1. checks the circuit breaker,
2. waits for the rate-limit bucket for ``rate_key``,
3. sends the request with the configured auth,
4. maps the status code to the ismet error hierarchy,
5. retries per the :class:`RetryPolicy` for retryable failures,
6. invokes hooks with latency and outcome.

Retries depend on whether a repeat is safe. ``GET``, ``HEAD``, and ``OPTIONS``
are retried under the policy. Any other method is sent exactly once unless the
caller passes ``idempotent=True``, which it may do only when the venue contract
makes a repeat harmless.

For any method other than those reads, a failure that may have reached the server
raises :class:`~ismet.errors.AmbiguousOutcome`, whether or not it was declared
idempotent: a read timeout, a broken connection, a 5xx, a redirect, or a success
status with an unreadable body. Only a failure that cannot have reached the server
(connect failure, pool timeout) raises a plain
:class:`~ismet.errors.TransportError`.

Response bodies are decoded with ``parse_float=Decimal`` so vendor floats never
touch a Python ``float``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Generator, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import httpx
from pydantic import SecretStr

from ismet.errors import (
    AmbiguousOutcome,
    AuthError,
    IsmetError,
    RateLimited,
    TransportError,
    VenueError,
)
from ismet.transport.backoff import DEFAULT_RETRY, NO_RETRY, RetryPolicy, retry
from ismet.transport.circuit import CircuitBreaker
from ismet.transport.clock import SYSTEM_CLOCK, Clock
from ismet.transport.ratelimit import RateLimiter

logger = logging.getLogger("ismet.transport")

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)


def loads_decimal(data: bytes | str) -> Any:
    """``json.loads`` with floats parsed as ``Decimal``."""
    return json.loads(data, parse_float=Decimal)


class HeaderAuth(httpx.Auth):
    """Send a secret in a request header, for example ``X-API-Key``."""

    def __init__(self, header: str, secret: SecretStr | str, prefix: str = "") -> None:
        self._header = header
        self._secret = secret if isinstance(secret, SecretStr) else SecretStr(secret)
        self._prefix = prefix

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers[self._header] = self._prefix + self._secret.get_secret_value()
        yield request


class BearerAuth(HeaderAuth):
    """``Authorization: Bearer <token>``."""

    def __init__(self, token: SecretStr | str) -> None:
        super().__init__("Authorization", token, prefix="Bearer ")


class QueryAuth(httpx.Auth):
    """Send a secret as a query parameter, for vendors that require it."""

    def __init__(self, param: str, secret: SecretStr | str) -> None:
        self._param = param
        self._secret = secret if isinstance(secret, SecretStr) else SecretStr(secret)

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.url = request.url.copy_add_param(
            self._param, self._secret.get_secret_value()
        )
        yield request


@dataclass(frozen=True)
class Response:
    """Decoded HTTP response."""

    status: int
    headers: Mapping[str, str]
    body: bytes
    latency: float

    def json(self) -> Any:
        """Decode the body as JSON; a non-JSON body is a :class:`TransportError`."""
        try:
            return loads_decimal(self.body)
        except ValueError as exc:
            raise TransportError(
                f"HTTP {self.status}: response body is not JSON: {self.text()[:200]!r}",
                retryable=False,
                status=self.status,
            ) from exc

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class RequestInfo:
    """Hook payload describing one attempt.

    ``url`` is the request path as given by the caller, never the final URL,
    so credentials injected by an auth flow do not reach logging hooks.
    """

    method: str
    url: str
    attempt: int
    latency: float
    status: int | None = None
    error: BaseException | None = None
    extra: dict[str, Any] = field(default_factory=dict)


Hook = Callable[[RequestInfo], None]


def _retry_after(headers: Mapping[str, str]) -> float | None:
    value = headers.get("retry-after") or headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def map_response_error(
    status: int, body: bytes, headers: Mapping[str, str]
) -> IsmetError:
    """Map a non-2xx response to the ismet error hierarchy."""
    text = body.decode("utf-8", errors="replace")[:500]
    if status < 400:
        location = headers.get("location") or headers.get("Location")
        return TransportError(
            f"HTTP {status}: unexpected non-success response"
            + (f" redirecting to {location!r}" if location else ""),
            retryable=False,
            status=status,
        )
    if status in (401, 403):
        return AuthError(f"HTTP {status}: {text}", status=status)
    if status == 429:
        return RateLimited(f"HTTP 429: {text}", retry_after=_retry_after(headers))
    if status >= 500:
        return TransportError(f"HTTP {status}: {text}", retryable=True, status=status)
    payload: Any = None
    try:
        payload = loads_decimal(body)
    except ValueError:
        payload = None
    code = None
    if isinstance(payload, dict):
        for key in ("code", "error_code", "errorCode", "error"):
            value = payload.get(key)
            if isinstance(value, str | int):
                code = str(value)
                break
    return VenueError(
        f"HTTP {status}: {text}", code=code, status=status, payload=payload
    )


class HttpTransport:
    """Resilient async HTTP client for one base URL."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        auth: httpx.Auth | None = None,
        headers: Mapping[str, str] | None = None,
        retry_policy: RetryPolicy = DEFAULT_RETRY,
        rate_limiter: RateLimiter | None = None,
        breaker: CircuitBreaker | None = None,
        hooks: list[Hook] | None = None,
        clock: Clock = SYSTEM_CLOCK,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retry_policy = retry_policy
        self.rate_limiter = rate_limiter
        self.breaker = breaker
        self.hooks: list[Hook] = list(hooks or [])
        self._clock = clock
        self._auth = auth
        self._headers = dict(headers or {})
        self._httpx_transport = transport
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> HttpTransport:
        await self.open()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def open(self) -> None:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers=self._headers,
                auth=self._auth,
                transport=self._httpx_transport,
                follow_redirects=False,
            )

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None

    @property
    def is_open(self) -> bool:
        return self._client is not None and not self._client.is_closed

    def _emit(self, info: RequestInfo) -> None:
        for hook in self.hooks:
            try:
                hook(info)
            except Exception:
                logger.exception("http hook failed")

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Any = None,
        headers: Mapping[str, str] | None = None,
        rate_key: str | None = None,
        retry_policy: RetryPolicy | None = None,
        idempotent: bool | None = None,
    ) -> Response:
        """Send one logical request and return the response.

        ``idempotent`` defaults from the method. When false, the request is sent
        once regardless of ``retry_policy``, and a possibly-delivered failure
        raises :class:`~ismet.errors.AmbiguousOutcome`.
        """
        await self.open()
        is_read = method.upper() in IDEMPOTENT_METHODS
        repeatable = is_read if idempotent is None else idempotent
        policy = (retry_policy or self.retry_policy) if repeatable else NO_RETRY
        attempt = 0

        async def once() -> Response:
            nonlocal attempt
            attempt += 1
            if self.breaker is None:
                return await self._send(
                    method,
                    path,
                    params,
                    json_body,
                    headers,
                    rate_key,
                    attempt,
                    is_read,
                    repeatable,
                )
            self.breaker.check()
            try:
                return await self._send(
                    method,
                    path,
                    params,
                    json_body,
                    headers,
                    rate_key,
                    attempt,
                    is_read,
                    repeatable,
                )
            finally:
                self.breaker.release_probe()

        return await retry(once, policy)

    async def _send(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None,
        json_body: Any,
        headers: Mapping[str, str] | None,
        rate_key: str | None,
        attempt: int,
        is_read: bool,
        repeatable: bool,
    ) -> Response:
        if self.rate_limiter is not None:
            await self.rate_limiter.acquire(rate_key)
        assert self._client is not None
        started = self._clock.monotonic()
        try:
            raw = await self._client.request(
                method, path, params=params, json=json_body, headers=headers
            )
        except httpx.HTTPError as exc:
            latency = self._clock.monotonic() - started
            err = self._network_error(exc, method, path, is_read, repeatable)
            self._fail(method, path, attempt, latency, err)
            raise err from exc
        latency = self._clock.monotonic() - started
        if not 200 <= raw.status_code < 300:
            mapped = map_response_error(raw.status_code, raw.content, raw.headers)
            if not is_read and (raw.status_code >= 500 or raw.status_code < 400):
                mapped = AmbiguousOutcome(
                    f"{method} {path} returned HTTP {raw.status_code}; the venue "
                    "may have processed it",
                    status=raw.status_code,
                    retryable=repeatable and raw.status_code >= 500,
                )
            self._fail(method, path, attempt, latency, mapped, raw.status_code)
            raise mapped
        if self.breaker is not None:
            self.breaker.record_success()
        self._emit(RequestInfo(method, path, attempt, latency, raw.status_code))
        return Response(raw.status_code, dict(raw.headers), raw.content, latency)

    @staticmethod
    def _network_error(
        exc: httpx.HTTPError, method: str, path: str, is_read: bool, repeatable: bool
    ) -> TransportError:
        kind = (
            "timeout"
            if isinstance(exc, httpx.TimeoutException)
            else "transport failure"
        )
        if is_read or isinstance(exc, _NOT_SENT_ERRORS):
            return TransportError(f"{kind}: {exc}", retryable=repeatable)
        return AmbiguousOutcome(
            f"{method} {path} failed after it may have been sent ({kind}: {exc}); "
            "query the venue before acting",
            retryable=repeatable,
        )

    def _fail(
        self,
        method: str,
        path: str,
        attempt: int,
        latency: float,
        exc: IsmetError,
        status: int | None = None,
    ) -> None:
        if self.breaker is not None and not isinstance(exc, AuthError | VenueError):
            self.breaker.record_failure()
        self._emit(RequestInfo(method, path, attempt, latency, status, exc))

    async def get_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        rate_key: str | None = None,
    ) -> Any:
        response = await self.request(
            "GET", path, params=params, headers=headers, rate_key=rate_key
        )
        return response.json()

    async def post_json(
        self,
        path: str,
        body: Any,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        rate_key: str | None = None,
        idempotent: bool | None = None,
    ) -> Any:
        response = await self.request(
            "POST",
            path,
            params=params,
            json_body=body,
            headers=headers,
            rate_key=rate_key,
            idempotent=idempotent,
        )
        try:
            return response.json()
        except TransportError as exc:
            # The server accepted the request and its answer cannot be read.
            raise AmbiguousOutcome(
                f"POST {path} succeeded with an unreadable response: {exc}",
                status=response.status,
            ) from exc


__all__ = [
    "IDEMPOTENT_METHODS",
    "BearerAuth",
    "HeaderAuth",
    "Hook",
    "HttpTransport",
    "QueryAuth",
    "RequestInfo",
    "Response",
    "loads_decimal",
    "map_response_error",
]
