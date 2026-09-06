from __future__ import annotations

import pytest

from ismet.errors import (
    CircuitOpen,
    IsmetError,
    NotSupported,
    RateLimited,
    TransportError,
    VenueError,
)


def test_hierarchy_and_attributes() -> None:
    assert issubclass(CircuitOpen, TransportError)
    err = NotSupported("mock", "trading")
    assert isinstance(err, IsmetError)
    assert "mock" in str(err) and "trading" in str(err)
    assert TransportError("x").retryable is True
    assert TransportError("x", retryable=False, status=503).status == 503
    assert CircuitOpen("api", 1.5).retryable is False
    assert RateLimited("slow", retry_after=2).retry_after == 2
    v = VenueError("bad", code="E1", status=400, payload={"a": 1})
    assert (v.code, v.status, v.payload) == ("E1", 400, {"a": 1})
    with pytest.raises(IsmetError):
        raise v
