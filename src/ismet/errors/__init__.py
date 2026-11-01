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


class AmbiguousOutcome(TransportError):
    """A state-changing request may or may not have reached the venue.

    Raised when a non-idempotent request fails after it may have been sent: a
    read timeout, a dropped connection, or a 5xx. The only correct response is to
    query the venue for what happened. Resending can create a duplicate order, so
    this error is not retried, unless the caller declared the request idempotent.
    """

    def __init__(
        self, message: str, *, status: int | None = None, retryable: bool = False
    ) -> None:
        super().__init__(message, retryable=retryable, status=status)


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


class SequenceGap(IsmetError):
    """A stream skipped sequence numbers and the caller asked to treat it as fatal."""

    def __init__(self, key: object, expected: int, received: int) -> None:
        self.key = key
        self.expected = expected
        self.received = received
        super().__init__(f"sequence gap on {key}: expected {expected}, got {received}")


class CalendarOutOfRange(IsmetError):
    """A calendar query fell outside the dates the calendar's data covers.

    Raised instead of assuming a date outside the sourced data is a normal day.
    """


class RiskRejected(IsmetError):
    """A pre-trade risk check refused an order. It was not sent."""

    def __init__(self, check: str, reason: str) -> None:
        self.check = check
        self.reason = reason
        super().__init__(f"risk check {check!r} rejected the order: {reason}")


class KillSwitchEngaged(RiskRejected):
    """The kill switch is engaged, so no new order can be sent."""

    def __init__(self, reason: str) -> None:
        super().__init__("kill_switch", reason)


class OrderStateError(IsmetError):
    """An order event contradicts the order's recorded state or quantities."""


class JournalError(IsmetError):
    """The durable order journal failed or failed verification.

    Execution stops on this error: an order that cannot be journalled is not sent.
    """


class DuplicateOrderId(VenueError):
    """The venue already holds an order with this client order id.

    An order with this id exists at the venue, so its fate must be established by
    query. This is not a rejection of the order the caller is tracking.
    """


__all__ = [
    "AmbiguousOutcome",
    "AuthError",
    "CalendarOutOfRange",
    "CircuitOpen",
    "ConfigError",
    "DuplicateOrderId",
    "IsmetError",
    "JournalError",
    "KillSwitchEngaged",
    "NotSupported",
    "OrderStateError",
    "RateLimited",
    "RiskRejected",
    "SequenceGap",
    "TransportError",
    "ValidationError",
    "VenueError",
]
