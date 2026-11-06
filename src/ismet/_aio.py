"""Asyncio helpers that behave the same on every supported Python.

``asyncio.wait_for`` before Python 3.12 drops the caller's cancellation when the
awaited operation finishes in the same loop turn as the cancel (CPython gh-86296):
the cancelled caller is handed the result and carries on. A cancelled stream then
waits forever for a message, and a cancelled caller of an order path keeps going.
:func:`wait_for` here keeps the 3.12 rule on every version: a cancel always raises.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


async def wait_for(aw: Awaitable[T], timeout: float | None) -> T:
    """Await ``aw`` for at most ``timeout`` seconds, as ``asyncio.wait_for``.

    On timeout the operation is cancelled and awaited, then ``asyncio.TimeoutError``
    is raised. If the caller is cancelled, the operation is cancelled and awaited and
    ``CancelledError`` is raised, even when the operation finished in the same turn.
    """
    if sys.version_info >= (3, 12):
        return await asyncio.wait_for(aw, timeout)
    task = asyncio.ensure_future(aw)
    # Only a cancel that wait_for itself issued is a timeout. An operation that
    # ends cancelled for any other reason is CancelledError, as asyncio.wait_for.
    timed_out = False
    if timeout is not None and timeout <= 0:
        # As asyncio.wait_for: an operation not already done never gets to run.
        if not task.done():
            timed_out = True
            await _cancel_and_wait(task)
        return _outcome(task, timed_out)
    try:
        await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        await _cancel_and_wait(task)
        if not task.cancelled():
            task.exception()  # retrieved, so an error it ended with is not logged
        raise
    if not task.done():
        timed_out = True
        await _cancel_and_wait(task)
    return _outcome(task, timed_out)


def _outcome(task: asyncio.Future[T], timed_out: bool) -> T:
    """The result of a finished ``task``, or a timeout if this wait cancelled it.

    An operation that ignored its cancel and finished anyway keeps its result or its
    error, as ``asyncio.wait_for`` does. One that cancelled itself is not a timeout.
    """
    if task.cancelled():
        if timed_out:
            raise asyncio.TimeoutError
        raise asyncio.CancelledError
    return task.result()


async def _cancel_and_wait(task: asyncio.Future[T]) -> None:
    """Cancel ``task`` and wait until it has finished."""
    task.cancel()
    await asyncio.wait({task})


__all__ = ["wait_for"]
