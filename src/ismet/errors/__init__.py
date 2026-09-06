"""Exception hierarchy for ismet.

Every error raised by ismet derives from :class:`IsmetError`, so callers can
catch one type at the boundary. Transport-level failures carry a ``retryable``
flag that the retry policy consults; venue rejects preserve the raw venue code.
"""

from __future__ import annotations

from typing import Any


class IsmetError(Exception):
    """Base class for all ismet exceptions."""


class ConfigError(IsmetError):
    """Configuration is missing, malformed, or contradictory."""


class ValidationError(IsmetError):
    """A caller-supplied value failed ismet's own validation.

    Distinct from :class:`pydantic.ValidationError`, which signals a malformed
    model payload; this one signals a request that cannot be sent as given
    (unknown venue, ambiguous provider, size below lot size, and so on).
    """


class NotSupported(IsmetError):
    """A provider does not implement the requested capability."""

    def __init__(self, provider: str, capability: str) -> None:
        self.provider = provider
        self.capability = capability
        super().__init__(
            f"provider {provider!r} does not support capability {capability!r}"
        )


class TransportError(IsmetError):
    """Network-level failure: connection, timeout, 5xx, protocol error."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = True,
        status: int | None = None,
    ) -> None:
        self.retryable = retryable
        self.status = status
        super().__init__(message)


class CircuitOpen(TransportError):
    """The circuit breaker is open; the call was not attempted."""

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(
            f"circuit {name!r} is open; retry after {retry_after:.2f}s",
            retryable=False,
        )


class AuthError(IsmetError):
    """Credentials are missing, invalid, expired, or lack permission."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        self.status = status
        super().__init__(message)


class RateLimited(IsmetError):
    """The venue or provider throttled the request."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(message)


class VenueError(IsmetError):
    """The venue or provider rejected the request for a business reason.

    ``code`` is the raw vendor code, preserved verbatim; ``payload`` is the
    decoded response body when one was available.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        status: int | None = None,
        payload: Any = None,
    ) -> None:
        self.code = code
        self.status = status
        self.payload = payload
        super().__init__(message)


__all__ = [
    "AuthError",
    "CircuitOpen",
    "ConfigError",
    "IsmetError",
    "NotSupported",
    "RateLimited",
    "TransportError",
    "ValidationError",
    "VenueError",
]
