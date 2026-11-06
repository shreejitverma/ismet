from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from ismet.fix.message import FixMessage
from ismet.fix.session import (
    APPL_VER_FIX50SP2,
    ApplicationRejectError,
    FixSession,
    FixSessionConfig,
)
from ismet.fix.store import FixStore, MemoryFixStore
from ismet.transport.backoff import ExponentialBackoff

from .acceptor import FixAcceptor

FAST_BACKOFF = ExponentialBackoff(base=0.01, maximum=0.05, jitter=False)


def session_config(acceptor: FixAcceptor, **overrides: Any) -> FixSessionConfig:
    options: dict[str, Any] = {
        "host": "127.0.0.1",
        "port": acceptor.port,
        "begin_string": acceptor.begin_string,
        "sender_comp_id": "CLIENT",
        "target_comp_id": "VENUE",
        "heartbeat_interval": 30,
        "logon_timeout": 3.0,
        "logout_timeout": 1.0,
        "reconnect_backoff": FAST_BACKOFF,
        "max_reconnect_attempts": 200,
    }
    if acceptor.begin_string == "FIXT.1.1":
        options["default_appl_ver_id"] = APPL_VER_FIX50SP2
    options.update(overrides)
    return FixSessionConfig(**options)


class RecordingListener:
    """Records everything a session reports, for assertions."""

    def __init__(self) -> None:
        self.messages: list[FixMessage] = []
        self.rejects: list[FixMessage] = []
        self.discarded: list[tuple[FixMessage, str]] = []
        self.resets = 0
        self.possible_losses: list[tuple[int, int | None, str]] = []
        self.unread_before_reset: list[int] = []
        self.ready = 0
        self.downs: list[str] = []
        self.failures: list[Exception] = []
        self.reject_with: ApplicationRejectError | None = None
        self.crash_with: Exception | None = None

    async def on_application_message(self, message: FixMessage) -> None:
        if self.crash_with is not None:
            error, self.crash_with = self.crash_with, None
            raise error
        if self.reject_with is not None:
            error2, self.reject_with = self.reject_with, None
            raise error2
        self.messages.append(message)

    async def on_session_reject(self, message: FixMessage) -> None:
        self.rejects.append(message)

    async def on_discarded_message(self, message: FixMessage, reason: str) -> None:
        self.discarded.append((message, reason))

    async def on_possible_message_loss(
        self, first_seq: int, last_seq: int | None, reason: str
    ) -> None:
        self.possible_losses.append((first_seq, last_seq, reason))

    async def on_reset_discarding_unread(self, first_seq: int) -> None:
        self.unread_before_reset.append(first_seq)

    def on_sequence_reset(self) -> None:
        self.resets += 1

    def on_session_ready(self) -> None:
        self.ready += 1

    def on_session_down(self, reason: str) -> None:
        self.downs.append(reason)

    def on_session_failed(self, error: Exception) -> None:
        self.failures.append(error)

    def seqs(self) -> list[int]:
        return [int(m.require(34)) for m in self.messages]


async def eventually(
    predicate: Callable[[], bool], timeout: float = 30.0, message: str = ""
) -> None:
    """Poll until true. Used only where nothing can be awaited directly."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(message or "condition not met in time")
        await asyncio.sleep(0.005)


@pytest.fixture(params=["FIX.4.2", "FIX.4.4", "FIXT.1.1"])
def begin_string(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.fixture
async def acceptor() -> AsyncIterator[FixAcceptor]:
    instance = FixAcceptor()
    await instance.start()
    yield instance
    await instance.stop()


@pytest.fixture
async def versioned_acceptor(begin_string: str) -> AsyncIterator[FixAcceptor]:
    instance = FixAcceptor(begin_string)
    await instance.start()
    yield instance
    await instance.stop()


SessionFactory = Callable[..., Awaitable[tuple[FixSession, RecordingListener]]]


@pytest.fixture
async def make_session(
    acceptor: FixAcceptor,
) -> AsyncIterator[SessionFactory]:
    sessions: list[FixSession] = []

    async def build(
        *,
        store: FixStore | None = None,
        ready: bool = True,
        **overrides: Any,
    ) -> tuple[FixSession, RecordingListener]:
        listener = RecordingListener()
        session = FixSession(
            session_config(acceptor, **overrides),
            store if store is not None else MemoryFixStore(),
            listener,
        )
        sessions.append(session)
        await session.start()
        if ready:
            await session.wait_ready(5.0)
        return session, listener

    yield build
    for session in sessions:
        await session.stop()
