"""Conformance for order entry: the contract the order manager's safety rests on.

The market data suite asks "is this data right". This one asks "will this adapter
keep the order manager honest". It places real orders, so it refuses a provider whose
``environment`` is ``LIVE``. Point it at a sandbox or paper account, or at recorded
fixtures.

``make_passive_order(client_order_id)`` must return an order that rests without
executing, such as a limit far from the market, so the suite can observe it working.
``make_marketable_order(client_order_id)``, if given, must return an order that fills
at once, which lets the suite check how fills are reported.
``attempts(client_order_id)``, if given, reports how many requests actually reached the
venue or its test double, which is the only way to catch an adapter that resends
internally.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ismet.capabilities import AccountCapability, Capability, TradingCapability
from ismet.errors import ConfigError, IsmetError, VenueError
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
    attempts: Callable[[str], int] | None = None,
) -> ConformanceReport:
    """Exercise the order-entry contract and return a report."""
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
                    and snapshot.quantity == request.quantity,
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

    ok, missing = await step(
        "status_of_unknown_order",
        venue.order_status(f"{id_prefix}-never-sent", symbol),
    )
    if ok:
        add(Check(_CAP, "status_none_when_not_found", missing is None))

    ok, listed = await step("open_orders", venue.open_orders(symbol))
    if ok:
        add(
            Check(
                _CAP,
                "open_orders_lists_order",
                any(item.client_order_id == cid for item in listed),
            )
        )

    if attempts is not None:
        count = attempts(cid)
        add(Check(_CAP, "place_sends_exactly_once", count == 1, f"{count} requests"))
    await _check_duplicate_refused(venue, request, report)
    await _check_stream_reports(venue, make_passive_order, id_prefix, report, timeout)
    if make_marketable_order is not None:
        await _check_fill_reporting(
            venue, make_marketable_order(f"{id_prefix}-3"), report, timeout
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
    venue: TradingCapability, request: OrderRequest, report: ConformanceReport
) -> None:
    """A repeated client order id must not create a second order."""
    try:
        await venue.place_order(request)
    except VenueError as exc:
        report.checks.append(
            Check(_CAP, "duplicate_client_id_refused", True, str(exc.code))
        )
    except IsmetError as exc:
        report.checks.append(
            Check(
                _CAP,
                "duplicate_client_id_refused",
                False,
                f"expected VenueError, got {type(exc).__name__}",
            )
        )
    else:
        report.checks.append(
            Check(_CAP, "duplicate_client_id_refused", False, "second order accepted")
        )


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
    """A fill must reach the stream and the query, under one ``fill_id``.

    Different ids on the two channels would make reconciliation count it twice.
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
    ids = {f.fill_id for f in fills if f.client_order_id == request.client_order_id}
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


__all__ = ["run_trading_conformance"]
