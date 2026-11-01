"""Conformance for order entry: the contract the order manager's safety rests on.

The market data suite asks "is this data right". This one asks "will this adapter
keep the order manager honest". It places real orders, so it refuses a provider whose
``environment`` is ``LIVE``. Point it at a sandbox or paper account, or at recorded
fixtures.

``make_passive_order(client_order_id)`` must return an order that rests without
executing, such as a limit far from the market, so the suite can observe it working.
``make_marketable_order(client_order_id)``, if given, must return an order that fills
at once, which lets the suite check how fills are reported.
``make_unelected_stop_limit(client_order_id)``, if given, must return a stop limit
order whose stop the market has not reached, so the suite can check that it rests
unfilled under its own terms rather than going out as a plain limit.
``attempts(client_order_id)``, if given, reports how many requests actually reached the
venue or its test double, which is the only way to catch an adapter that resends
internally.
``arm_ambiguous_outcome()``, if given, makes the next ``place_order`` lose its answer
after the venue or its test double has accepted the order, so the suite can check
that the adapter reports :class:`~ismet.errors.AmbiguousOutcome` rather than a refusal,
and that a cancel of an order whose ack was never seen still takes effect.
``arm_status_failure()``, if given, makes the next ``order_status`` fail at the
transport, so the suite can check that the failure raises rather than looking like an
order the venue does not have.

A check whose hook was not given is reported as skipped, so a run that proves less
says so.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ismet.capabilities import AccountCapability, Capability, TradingCapability
from ismet.errors import (
    AmbiguousOutcome,
    ConfigError,
    DuplicateOrderId,
    IsmetError,
    VenueError,
)
from ismet.models.orders import (
    Environment,
    Fill,
    Filled,
    OrderRequest,
    OrderStatus,
)
from ismet.providers import Provider
from ismet.testing.conformance import Check, ConformanceReport

_CAP = Capability.TRADING


async def run_trading_conformance(
    provider: Provider,
    make_passive_order: Callable[[str], OrderRequest],
    *,
    id_prefix: str = "CONF",
    timeout: float = 15.0,
    make_marketable_order: Callable[[str], OrderRequest] | None = None,
    make_unelected_stop_limit: Callable[[str], OrderRequest] | None = None,
    attempts: Callable[[str], int] | None = None,
    unknown_status_error: type[Exception] | None = None,
    arm_ambiguous_outcome: Callable[[], None] | None = None,
    arm_status_failure: Callable[[], None] | None = None,
) -> ConformanceReport:
    """Exercise the order-entry contract and return a report.

    ``unknown_status_error`` declares a documented exception to the rule that a status
    query for an order the venue does not have returns ``None``. It is for a provider
    whose ``None`` would falsely say the venue has no such order, because the only
    record it can consult is local. Such a provider must then raise it: a ``None``
    from it is a failure.

    Queries are made both with and without a symbol, because the order manager's fill
    sweep asks for every symbol at once, and an adapter answering that with nothing
    would hide a fill on a closed order.
    """
    if provider.environment is Environment.LIVE:
        raise ConfigError(
            f"provider {provider.name!r} is LIVE; trading conformance sends orders and "
            "runs only against simulated or paper environments"
        )
    capabilities = provider.capabilities()
    report = ConformanceReport(provider.name, capabilities)
    add = report.checks.append
    if Capability.TRADING not in capabilities:
        add(Check(_CAP, "implements_trading", False, "TradingCapability is missing"))
        return report
    venue = cast(TradingCapability, provider)

    async def step(name: str, call: Coroutine[Any, Any, Any]) -> tuple[bool, Any]:
        try:
            result = await asyncio.wait_for(call, timeout)
        except Exception as exc:
            add(Check(_CAP, name, False, f"{type(exc).__name__}: {exc}"))
            return False, None
        return True, result

    def skip(name: str, hook: str) -> None:
        add(Check(_CAP, name, True, f"skipped: no {hook} given", skipped=True))

    rules = venue.trading_rules()
    add(Check(_CAP, "declares_order_types", bool(rules.order_types)))
    add(Check(_CAP, "declares_time_in_force", bool(rules.time_in_force)))

    request = make_passive_order(f"{id_prefix}-1")
    cid = request.client_order_id
    symbol = request.symbol

    ok, ack = await step("place_order", venue.place_order(request))
    if not ok:
        return report
    add(
        Check(
            _CAP,
            "ack_echoes_client_order_id",
            ack.client_order_id == cid,
            f"{ack.client_order_id!r} for {cid!r}",
        )
    )
    add(Check(_CAP, "ack_has_venue_order_id", bool(ack.venue_order_id)))

    ok, snapshot = await step("order_status", venue.order_status(cid, symbol))
    if ok:
        add(
            Check(
                _CAP,
                "status_finds_order_by_client_id",
                snapshot is not None and snapshot.client_order_id == cid,
            )
        )
        if snapshot is not None:
            add(
                Check(
                    _CAP,
                    "status_reports_terms",
                    snapshot.symbol == symbol
                    and snapshot.side is request.side
                    and snapshot.quantity == request.quantity
                    and snapshot.limit_price == request.limit_price,
                    f"{snapshot.side.value} {snapshot.quantity} {snapshot.symbol} at "
                    f"{snapshot.limit_price}",
                )
            )
            add(
                Check(
                    _CAP,
                    "passive_order_is_working",
                    snapshot.status is OrderStatus.ACCEPTED,
                    snapshot.status.value,
                )
            )

    try:
        missing = await asyncio.wait_for(
            venue.order_status(f"{id_prefix}-never-sent", symbol), timeout
        )
    except Exception as exc:
        declared = unknown_status_error is not None and isinstance(
            exc, unknown_status_error
        )
        add(Check(_CAP, "status_of_unknown_order", declared, type(exc).__name__))
    else:
        if unknown_status_error is not None:
            # The declared exception says this provider's None cannot be trusted.
            add(
                Check(
                    _CAP,
                    "status_of_unknown_order",
                    False,
                    f"returned {missing!r}; expected {unknown_status_error.__name__}",
                )
            )
        else:
            add(Check(_CAP, "status_none_when_not_found", missing is None))

    if arm_status_failure is None:
        skip("failed_status_query_raises", "arm_status_failure")
    else:
        arm_status_failure()
        try:
            answer = await asyncio.wait_for(venue.order_status(cid, symbol), timeout)
        except Exception as exc:
            add(Check(_CAP, "failed_status_query_raises", True, type(exc).__name__))
        else:
            add(
                Check(
                    _CAP,
                    "failed_status_query_raises",
                    False,
                    "no order" if answer is None else f"status {answer.status.value}",
                )
            )

    ok, listed = await step("open_orders", venue.open_orders(symbol))
    if ok:
        add(
            Check(
                _CAP,
                "open_orders_lists_order",
                any(item.client_order_id == cid for item in listed),
            )
        )
    ok, listed = await step("open_orders_without_symbol", venue.open_orders())
    if ok:
        add(
            Check(
                _CAP,
                "open_orders_lists_order_without_symbol",
                any(item.client_order_id == cid for item in listed),
            )
        )

    if attempts is None:
        skip("place_sends_exactly_once", "attempts")
    else:
        count = attempts(cid)
        add(Check(_CAP, "place_sends_exactly_once", count == 1, f"{count} requests"))
    await _check_duplicate_refused(venue, request, report, timeout)
    await _check_stream_reports(venue, make_passive_order, id_prefix, report, timeout)
    if make_marketable_order is None:
        skip("stream_reports_fill", "make_marketable_order")
    else:
        await _check_fill_reporting(
            venue, make_marketable_order(f"{id_prefix}-3"), report, timeout
        )
    if arm_ambiguous_outcome is None:
        skip("lost_answer_is_ambiguous_outcome", "arm_ambiguous_outcome")
    else:
        await _check_ambiguous_outcome(
            venue,
            make_passive_order(f"{id_prefix}-4"),
            arm_ambiguous_outcome,
            report,
            timeout,
        )
    if make_unelected_stop_limit is None:
        skip("stop_limit_rests_unelected", "make_unelected_stop_limit")
    else:
        await _check_stop_limit(
            venue, make_unelected_stop_limit(f"{id_prefix}-5"), report, timeout
        )

    ok, _ = await step("cancel_order", venue.cancel_order(cid, symbol))
    if ok:
        ok, after = await step("status_after_cancel", venue.order_status(cid, symbol))
        if ok:
            add(
                Check(
                    _CAP,
                    "cancel_takes_effect",
                    after is not None and after.status is OrderStatus.CANCELED,
                )
            )
        # A cancel can cross the order's close at the venue. The order manager reads
        # anything but VenueError from a cancel as "unknown whether applied" and marks
        # the order UNKNOWN, so a closed order must refuse with VenueError.
        try:
            await asyncio.wait_for(venue.cancel_order(cid, symbol), timeout)
        except VenueError:
            add(Check(_CAP, "cancel_of_closed_is_venue_error", True))
        except Exception as exc:
            add(
                Check(
                    _CAP, "cancel_of_closed_is_venue_error", False, type(exc).__name__
                )
            )
        else:
            add(Check(_CAP, "cancel_of_closed_is_venue_error", False, "no error"))

    try:
        await venue.cancel_order(f"{id_prefix}-never-sent", symbol)
    except VenueError:
        add(Check(_CAP, "cancel_unknown_is_venue_error", True))
    except IsmetError as exc:
        add(Check(_CAP, "cancel_unknown_is_venue_error", False, type(exc).__name__))
    else:
        add(Check(_CAP, "cancel_unknown_is_venue_error", False, "no error raised"))

    if isinstance(provider, AccountCapability):
        ok, positions = await step("positions", provider.positions())
        add(Check(Capability.ACCOUNT, "positions_callable", ok))
        ok, balances = await step("balances", provider.balances())
        add(Check(Capability.ACCOUNT, "balances_callable", ok))
        del positions, balances
    return report


async def _check_duplicate_refused(
    venue: TradingCapability,
    request: OrderRequest,
    report: ConformanceReport,
    timeout: float,
) -> None:
    """A repeated client order id must not create a second order."""
    add = report.checks.append
    try:
        await venue.place_order(request)
    except DuplicateOrderId as exc:
        add(Check(_CAP, "duplicate_client_id_refused", True, str(exc.code)))
    except IsmetError as exc:
        add(
            Check(
                _CAP,
                "duplicate_client_id_refused",
                False,
                f"expected DuplicateOrderId, got {type(exc).__name__}",
            )
        )
    else:
        add(Check(_CAP, "duplicate_client_id_refused", False, "second order accepted"))
    # The exception alone does not show that nothing was created.
    try:
        listed = await asyncio.wait_for(venue.open_orders(request.symbol), timeout)
    except Exception as exc:
        add(Check(_CAP, "duplicate_leaves_one_order", False, repr(exc)))
        return
    rows = sum(1 for item in listed if item.client_order_id == request.client_order_id)
    add(Check(_CAP, "duplicate_leaves_one_order", rows == 1, f"{rows} open rows"))


async def _check_ambiguous_outcome(
    venue: TradingCapability,
    request: OrderRequest,
    arm: Callable[[], None],
    report: ConformanceReport,
    timeout: float,
) -> None:
    """A lost answer must surface as ``AmbiguousOutcome``, never as a refusal.

    The order manager records a refusal as never delivered and closes the order. An
    adapter that reports a lost answer that way leaves a possibly live order unwatched.
    The order is then cancelled without an ack ever seen, as a kill sweep would.
    """
    add = report.checks.append
    cid, symbol = request.client_order_id, request.symbol
    arm()
    try:
        await venue.place_order(request)
    except AmbiguousOutcome:
        add(Check(_CAP, "lost_answer_is_ambiguous_outcome", True))
    except Exception as exc:
        add(
            Check(
                _CAP,
                "lost_answer_is_ambiguous_outcome",
                False,
                f"expected AmbiguousOutcome, got {type(exc).__name__}: {exc}",
            )
        )
    else:
        add(
            Check(
                _CAP,
                "lost_answer_is_ambiguous_outcome",
                False,
                "the armed fault did not fire",
            )
        )
    try:
        await asyncio.wait_for(venue.cancel_order(cid, symbol), timeout)
        after = await asyncio.wait_for(venue.order_status(cid, symbol), timeout)
    except Exception as exc:
        add(
            Check(
                _CAP,
                "cancel_without_ack_takes_effect",
                False,
                f"{type(exc).__name__}: {exc}",
            )
        )
        return
    add(
        Check(
            _CAP,
            "cancel_without_ack_takes_effect",
            after is not None and after.status is OrderStatus.CANCELED,
            "no order" if after is None else after.status.value,
        )
    )


async def _check_stop_limit(
    venue: TradingCapability,
    request: OrderRequest,
    report: ConformanceReport,
    timeout: float,
) -> None:
    """A stop limit whose stop is not reached must rest, unfilled, under its terms.

    An adapter that drops the stop sends a limit order that may execute at once.
    """
    add = report.checks.append
    cid, symbol = request.client_order_id, request.symbol
    try:
        try:
            await asyncio.wait_for(venue.place_order(request), timeout)
            snapshot = await asyncio.wait_for(venue.order_status(cid, symbol), timeout)
        except Exception as exc:
            add(
                Check(
                    _CAP,
                    "stop_limit_rests_unelected",
                    False,
                    f"{type(exc).__name__}: {exc}",
                )
            )
            return
        if snapshot is None:
            add(Check(_CAP, "stop_limit_rests_unelected", False, "no order"))
            return
        add(
            Check(
                _CAP,
                "stop_limit_rests_unelected",
                snapshot.status is OrderStatus.ACCEPTED
                and snapshot.filled_quantity == 0
                and snapshot.limit_price == request.limit_price,
                f"{snapshot.status.value}, {snapshot.filled_quantity} filled, limit "
                f"{snapshot.limit_price}",
            )
        )
    finally:
        with contextlib.suppress(IsmetError):
            await venue.cancel_order(cid, symbol)


async def _check_stream_reports(
    venue: TradingCapability,
    make_passive_order: Callable[[str], OrderRequest],
    id_prefix: str,
    report: ConformanceReport,
    timeout: float,
) -> None:
    """A new order's ack must arrive on the execution stream."""
    request = make_passive_order(f"{id_prefix}-2")
    stream = venue.stream_executions()
    received = asyncio.create_task(_first_event_for(stream, request.client_order_id))
    await asyncio.sleep(0)
    try:
        await venue.place_order(request)
        await asyncio.wait_for(received, timeout)
    except Exception as exc:
        report.checks.append(
            Check(
                _CAP, "stream_reports_new_order", False, f"{type(exc).__name__}: {exc}"
            )
        )
    else:
        report.checks.append(Check(_CAP, "stream_reports_new_order", True))
    finally:
        received.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await received
        with contextlib.suppress(IsmetError):
            await venue.cancel_order(request.client_order_id, request.symbol)


async def _check_fill_reporting(
    venue: TradingCapability,
    request: OrderRequest,
    report: ConformanceReport,
    timeout: float,
) -> None:
    """A fill must reach the stream and the query, under one ``fill_id`` and terms.

    Different ids on the two channels would make reconciliation count it twice, and
    different terms under one id would halt the order manager as a corrected fill.
    """
    stream = venue.stream_executions()
    started = datetime.now(timezone.utc) - timedelta(minutes=5)
    collected = asyncio.create_task(_first_fill_for(stream, request.client_order_id))
    await asyncio.sleep(0)
    add = report.checks.append
    try:
        await venue.place_order(request)
        streamed = await asyncio.wait_for(collected, timeout)
    except Exception as exc:
        add(Check(_CAP, "stream_reports_fill", False, f"{type(exc).__name__}: {exc}"))
        return
    finally:
        collected.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await collected
    add(Check(_CAP, "stream_reports_fill", True, streamed.fill_id))
    try:
        fills = await asyncio.wait_for(
            venue.fills_since(started, request.symbol), timeout
        )
    except Exception as exc:
        add(Check(_CAP, "fills_since_reports_fill", False, repr(exc)))
        return
    ours = [f for f in fills if f.client_order_id == request.client_order_id]
    ids = {f.fill_id for f in ours}
    add(Check(_CAP, "fills_since_reports_fill", bool(ids), f"{len(ids)} fills"))
    if ids:
        add(
            Check(
                _CAP,
                "fill_id_matches_across_channels",
                streamed.fill_id in ids,
                f"stream {streamed.fill_id}, query {sorted(ids)}",
            )
        )
    for queried in ours:
        if queried.fill_id == streamed.fill_id:
            add(
                Check(
                    _CAP,
                    "fill_terms_match_across_channels",
                    _traded(queried) == _traded(streamed),
                    f"stream {_traded(streamed)}, query {_traded(queried)}",
                )
            )
            break
    try:
        everything = await asyncio.wait_for(venue.fills_since(started), timeout)
    except Exception as exc:
        add(Check(_CAP, "fills_since_reports_fill_without_symbol", False, repr(exc)))
        return
    add(
        Check(
            _CAP,
            "fills_since_reports_fill_without_symbol",
            any(f.fill_id == streamed.fill_id for f in everything),
        )
    )
    # ``since`` is inclusive, by the fill's own time: the order manager's sweep window
    # starts at the last fill it knows, so an exclusive or shifted comparison loses
    # fills, and one ignored returns the whole history every pass.
    if not ours:
        return  # already failed above, and there is no fill time to ask by
    known = next((f for f in ours if f.fill_id == streamed.fill_id), ours[0])
    later = known.ts + timedelta(seconds=1)
    for name, since, wanted in (
        ("fills_since_includes_fill_at_since", known.ts, True),
        ("fills_since_excludes_fill_before_since", later, False),
    ):
        try:
            answer = await asyncio.wait_for(
                venue.fills_since(since, request.symbol), timeout
            )
        except Exception as exc:
            add(Check(_CAP, name, False, repr(exc)))
            continue
        found = any(f.fill_id == known.fill_id for f in answer)
        add(Check(_CAP, name, found == wanted, f"since {since.isoformat()}"))


def _traded(fill: Fill) -> tuple[str, str, str, str]:
    return (str(fill.symbol), fill.side.value, str(fill.price), str(fill.quantity))


async def _first_fill_for(stream: Any, client_order_id: str) -> Fill:
    try:
        async for event in stream:
            if isinstance(event, Filled) and event.client_order_id == client_order_id:
                return event.fill
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()
    raise ConfigError("the execution stream ended before reporting the fill")


async def _first_event_for(stream: Any, client_order_id: str) -> None:
    try:
        async for event in stream:
            if event.client_order_id == client_order_id:
                return
    finally:
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            await aclose()
    raise ConfigError("the execution stream ended before reporting the order")


__all__ = ["run_trading_conformance"]
