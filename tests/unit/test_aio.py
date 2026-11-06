"""ismet._aio.wait_for keeps the Python 3.12 cancellation rule on every version.

The oracle is CPython gh-86296: before 3.12, ``asyncio.wait_for`` hands a cancelled
caller the result when the operation finishes in the same loop turn as the cancel.
That hung the suite for days on 3.10, with a cancelled stream waiting for a message.
"""

from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

from ismet import _aio
from ismet.transport.ws import WebSocketTransport

SRC = Path(__file__).resolve().parents[2] / "src" / "ismet"


async def test_a_cancel_landing_as_the_operation_finishes_still_raises() -> None:
    event = asyncio.Event()
    waiting = asyncio.ensure_future(_aio.wait_for(event.wait(), 5))
    await asyncio.sleep(0)
    event.set()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting


async def test_a_transport_waiting_to_connect_keeps_its_cancel() -> None:
    transport = WebSocketTransport("ws://127.0.0.1:9")
    waiting = asyncio.ensure_future(transport.wait_connected(5))
    await asyncio.sleep(0)
    transport._connected.set()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting


async def test_a_cancelled_caller_cancels_the_operation() -> None:
    started, ended = asyncio.Event(), []

    async def operation() -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        finally:
            ended.append(True)

    waiting = asyncio.ensure_future(_aio.wait_for(operation(), 5))
    await started.wait()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert ended == [True]


async def test_the_result_and_the_error_come_through() -> None:
    async def answer() -> int:
        await asyncio.sleep(0)
        return 42

    async def fail() -> None:
        await asyncio.sleep(0)
        raise ValueError("venue said no")

    assert await _aio.wait_for(answer(), 1) == 42
    assert await _aio.wait_for(answer(), None) == 42
    with pytest.raises(ValueError, match="venue said no"):
        await _aio.wait_for(fail(), 1)


async def test_a_timeout_cancels_the_operation_and_raises() -> None:
    ended = []

    async def slow() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            ended.append(True)

    with pytest.raises(asyncio.TimeoutError):
        await _aio.wait_for(slow(), 0.01)
    assert ended == [True]


async def test_a_zero_timeout_never_runs_the_operation() -> None:
    ran = []

    async def operation() -> None:
        ran.append(True)

    with pytest.raises(asyncio.TimeoutError):
        await _aio.wait_for(operation(), 0)
    assert ran == []


async def test_an_operation_that_ignores_its_cancel_on_timeout_keeps_its_result() -> (
    None
):
    async def stubborn() -> str:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            return "finished anyway"
        return "slept"

    assert await _aio.wait_for(stubborn(), 0.01) == "finished anyway"


def direct_wait_for_uses(source: str) -> list[int]:
    """Lines that reach ``asyncio.wait_for`` without the helper, however it is named."""
    tree = ast.parse(source)
    modules = {"asyncio"}
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(
                a.asname or a.name for a in node.names if a.name == "asyncio"
            )
        elif isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "asyncio"
        ):
            found.extend(node.lineno for a in node.names if a.name == "wait_for")
    found.extend(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr == "wait_for"
        and isinstance(node.value, ast.Name)
        and node.value.id in modules
    )
    return sorted(found)


@pytest.mark.parametrize(
    ("source", "lines"),
    [
        ("import asyncio\nasyncio.wait_for(x, 1)\n", [2]),
        ("import asyncio as aio\naio.wait_for(x, 1)\n", [2]),
        ("from asyncio import wait_for as w\nw(x, 1)\n", [1]),
        ("from asyncio.tasks import wait_for\n", [1]),
        ("import asyncio\nasyncio.wait_for (x, 1)\n", [2]),
        ("# asyncio.wait_for(x, 1)\nfrom ismet import _aio\n_aio.wait_for(x, 1)\n", []),
    ],
    ids=["call", "module-alias", "from-import", "submodule", "spaced", "helper-only"],
)
def test_the_checker_finds_every_spelling(source: str, lines: list[int]) -> None:
    assert direct_wait_for_uses(source) == lines


def test_the_source_never_calls_asyncio_wait_for_directly() -> None:
    callers = {
        str(path.relative_to(SRC)): uses
        for path in SRC.rglob("*.py")
        if path.name != "_aio.py"
        and (uses := direct_wait_for_uses(path.read_text(encoding="utf-8")))
    }
    assert callers == {}
