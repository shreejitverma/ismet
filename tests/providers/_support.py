"""Shared helpers: fixture access and bounded stream draining."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar

from ismet.testing.replay import (
    RecordedSession,
    ReplayTransport,
    load_exchanges,
    load_session,
)
from ismet.transport.backoff import ExponentialBackoff

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
FAST_BACKOFF = ExponentialBackoff(base=0.01, maximum=0.01, jitter=False)
RECEIVED = datetime(2030, 1, 1, tzinfo=timezone.utc)

T = TypeVar("T")


def manifest(provider: str, key: str) -> datetime:
    data = json.loads((FIXTURES / provider / "manifest.json").read_text("utf-8"))
    return datetime.fromisoformat(data[key].replace("Z", "+00:00"))


def replay(provider: str, cassette: str) -> ReplayTransport:
    return ReplayTransport(
        load_exchanges(FIXTURES / provider / f"rest_{cassette}.json")
    )


def body(provider: str, cassette: str, url_part: str) -> Any:
    """The decoded body of the first exchange whose URL contains ``url_part``."""
    for exchange in load_exchanges(FIXTURES / provider / f"rest_{cassette}.json"):
        if url_part in exchange.url:
            return json.loads(exchange.body, parse_float=Decimal)
    raise AssertionError(f"no exchange matching {url_part!r} in {cassette}")


def session(provider: str, name: str) -> RecordedSession:
    return load_session(FIXTURES / provider / f"ws_{name}.json")


def frames(provider: str, name: str) -> list[dict[str, Any]]:
    return [
        json.loads(raw, parse_float=Decimal) for raw in session(provider, name).frames
    ]


async def take(stream: AsyncIterator[T], count: int, timeout: float = 10.0) -> list[T]:
    """The next ``count`` items of ``stream``, which is then closed."""
    received: list[T] = []

    async def drain() -> None:
        async for item in stream:
            received.append(item)
            if len(received) >= count:
                return

    try:
        await asyncio.wait_for(drain(), timeout)
    finally:
        await stream.aclose()  # type: ignore[attr-defined]
    return received


def ms(moment: datetime) -> int:
    return (moment - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(
        milliseconds=1
    )
