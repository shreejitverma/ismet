"""The trading suite passes the simulated venue and catches broken adapters."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ismet.errors import (
    AmbiguousOutcome,
    ConfigError,
    DuplicateOrderId,
    TransportError,
    VenueError,
)
from ismet.models.orders import (
    Environment,
    OrderAck,
    OrderRequest,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from ismet.providers.mock import MockProvider
from ismet.testing.conformance import assert_conformant
from ismet.testing.simvenue import Fault, SimVenue, sim_instrument
from ismet.testing.trading_conformance import run_trading_conformance

from .conftest import ACME, order

UTC = timezone.utc


def passive(client_order_id: str) -> OrderRequest:
    return order(client_order_id, price="9.00")


def venue(cls: type[SimVenue] = SimVenue) -> SimVenue:
    sim = cls(instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    return sim


def marketable(client_order_id: str) -> OrderRequest:
    return order(client_order_id, price="10.01", quantity="1")


def unelected_stop_limit(client_order_id: str) -> OrderRequest:
    """A buy stop at 10.50 with its limit at 10.60, both above the 10.01 ask."""
    return order(
        client_order_id,
        price="10.60",
        order_type=OrderType.STOP_LIMIT,
        stop_price="10.50",
        time_in_force=TimeInForce.GTC,
    )


def arm_query_failure(sim: SimVenue) -> None:
    sim.fail_queries = 1


async def run_full(sim: SimVenue, **kw: object):  # type: ignore[no-untyped-def]
    return await run_trading_conformance(
        sim,
        passive,
        make_marketable_order=marketable,
        make_unelected_stop_limit=unelected_stop_limit,
        attempts=lambda cid: sim.attempts[cid],
        arm_ambiguous_outcome=lambda: sim.faults.append(Fault.LOSE_RESPONSE),
        arm_status_failure=lambda: arm_query_failure(sim),
        **kw,  # type: ignore[arg-type]
    )


HOOKED_CHECKS = {
    "place_sends_exactly_once",
    "stream_reports_fill",
    "lost_answer_is_ambiguous_outcome",
    "cancel_without_ack_takes_effect",
    "stop_limit_rests_unelected",
    "failed_status_query_raises",
}


async def test_simulated_venue_conforms() -> None:
    report = await run_full(venue())
    assert_conformant(report)
    assert report.skipped == []
    names = {check.name for check in report.checks}
    assert (
        HOOKED_CHECKS
        | {
            "duplicate_client_id_refused",
            "duplicate_leaves_one_order",
            "status_none_when_not_found",
            "fill_id_matches_across_channels",
            "fill_terms_match_across_channels",
            "fills_since_includes_fill_at_since",
            "fills_since_excludes_fill_before_since",
            "open_orders_lists_order_without_symbol",
            "fills_since_reports_fill_without_symbol",
            "cancel_of_closed_is_venue_error",
        }
        <= names
    )
    assert "[skip]" not in report.summary()


async def test_checks_without_a_hook_are_reported_as_skipped() -> None:
    """A run that proves less must say so, rather than listing a pass it never ran."""
    report = await run_trading_conformance(venue(), passive)
    assert report.passed
    skipped = {check.name for check in report.skipped}
    assert skipped == {
        "place_sends_exactly_once",
        "stream_reports_fill",
        "lost_answer_is_ambiguous_outcome",
        "stop_limit_rests_unelected",
        "failed_status_query_raises",
    }
    assert "cancel_without_ack_takes_effect" not in {c.name for c in report.checks}
    lines = report.summary().splitlines()
    assert lines[0] == "sim: PASS"
    skip_line = "  [skip] trading.stream_reports_fill: skipped"
    assert any(line.startswith(skip_line) for line in lines)


class StatusNoneDespiteDeclaredError(SimVenue):
    """Declares that a missing order raises, then answers None for one."""


async def test_a_declared_exception_must_be_raised_not_replaced_by_none() -> None:
    report = await run_full(
        venue(StatusNoneDespiteDeclaredError), unknown_status_error=VenueError
    )
    failed = {check.name: check.detail for check in report.failures}
    assert "VenueError" in failed["status_of_unknown_order"]
    assert "status_none_when_not_found" not in {c.name for c in report.checks}


class RetriesPlaceTwice(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        ack = await super().place_order(request)
        self.attempts[request.client_order_id] += 1
        return ack


class ForgetsFills(SimVenue):
    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        return []


class FillIdsDifferByChannel(SimVenue):
    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        fills = await super().fills_since(since, symbol)
        return [f.model_copy(update={"fill_id": f"Q{f.fill_id}"}) for f in fills]


class FillsNotOnStream(SimVenue):
    def _publish(self, event) -> None:  # type: ignore[no-untyped-def]
        from ismet.models.orders import Filled

        if not isinstance(event, Filled):
            super()._publish(event)


class FillTermsDifferByChannel(SimVenue):
    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        fills = await super().fills_since(since, symbol)
        return [f.model_copy(update={"quantity": f.quantity * 2}) for f in fills]


class AnswersOnlyWithASymbol(SimVenue):
    async def open_orders(self, symbol=None):  # type: ignore[no-untyped-def]
        return [] if symbol is None else await super().open_orders(symbol)

    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        return [] if symbol is None else await super().fills_since(since, symbol)


class HidesALostAnswer(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        try:
            return await super().place_order(request)
        except AmbiguousOutcome as exc:
            raise TransportError(str(exc)) from exc


class SwallowsQueryFailure(SimVenue):
    """A failed query looks like an order the venue does not have."""

    async def order_status(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        try:
            return await super().order_status(client_order_id, symbol)
        except TransportError:
            return None


class CancelNeedsAck(SimVenue):
    """Can only cancel an order whose acceptance it saw."""

    def __init__(self, **kw: object) -> None:
        super().__init__(**kw)  # type: ignore[arg-type]
        self.acked: set[str] = set()

    async def place_order(self, request: OrderRequest) -> OrderAck:
        ack = await super().place_order(request)
        self.acked.add(request.client_order_id)
        return ack

    async def cancel_order(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        if client_order_id not in self.acked:
            raise VenueError("no venue order id known", code="UNKNOWN_ORDER")
        await super().cancel_order(client_order_id, symbol)


class IgnoresSince(SimVenue):
    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        return await super().fills_since(datetime.min.replace(tzinfo=UTC), symbol)


class ExclusiveSince(SimVenue):
    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        fills = await super().fills_since(since, symbol)
        return [f for f in fills if f.ts > since]


class ShiftedSince(SimVenue):
    """Compares against a clock one hour off, as a naive timestamp would."""

    async def fills_since(self, since, symbol=None):  # type: ignore[no-untyped-def]
        return await super().fills_since(since - timedelta(hours=1), symbol)


class SendsStopLimitAsLimit(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        if request.order_type is OrderType.STOP_LIMIT:
            request = request.model_copy(
                update={"order_type": OrderType.LIMIT, "stop_price": None}
            )
        return await super().place_order(request)


class StatusDropsLimitPrice(SimVenue):
    def _snapshot(self, order_):  # type: ignore[no-untyped-def]
        return super()._snapshot(order_).model_copy(update={"limit_price": None})


class DuplicateCreatesSecondOrder(SimVenue):
    """Refuses the duplicate as it should, and still books a second order."""

    async def place_order(self, request: OrderRequest) -> OrderAck:
        try:
            return await super().place_order(request)
        except DuplicateOrderId:
            twin = request.model_copy(
                update={"client_order_id": f"{request.client_order_id}#2"}
            )
            self.add_external_order(twin)
            # A second venue order, reported under the same client order id.
            self._orders[twin.client_order_id].request = request
            raise


class RepeatCancelSilent(SimVenue):
    async def cancel_order(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        found = self._orders.get(client_order_id)
        if found is not None and found.status.is_terminal:
            return
        await super().cancel_order(client_order_id, symbol)


class RepeatCancelTransportError(SimVenue):
    async def cancel_order(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        found = self._orders.get(client_order_id)
        if found is not None and found.status.is_terminal:
            raise TransportError("HTTP 500")
        await super().cancel_order(client_order_id, symbol)


@pytest.mark.parametrize(
    ("broken", "failing_check", "timeout"),
    [
        (RetriesPlaceTwice, "place_sends_exactly_once", 2.0),
        (ForgetsFills, "fills_since_reports_fill", 2.0),
        (FillIdsDifferByChannel, "fill_id_matches_across_channels", 2.0),
        (FillTermsDifferByChannel, "fill_terms_match_across_channels", 2.0),
        (AnswersOnlyWithASymbol, "open_orders_lists_order_without_symbol", 2.0),
        (AnswersOnlyWithASymbol, "fills_since_reports_fill_without_symbol", 2.0),
        (HidesALostAnswer, "lost_answer_is_ambiguous_outcome", 2.0),
        (FillsNotOnStream, "stream_reports_fill", 0.05),
        (SwallowsQueryFailure, "failed_status_query_raises", 2.0),
        (CancelNeedsAck, "cancel_without_ack_takes_effect", 2.0),
        (IgnoresSince, "fills_since_excludes_fill_before_since", 2.0),
        (ExclusiveSince, "fills_since_includes_fill_at_since", 2.0),
        (ShiftedSince, "fills_since_excludes_fill_before_since", 2.0),
        (SendsStopLimitAsLimit, "stop_limit_rests_unelected", 2.0),
        (StatusDropsLimitPrice, "status_reports_terms", 2.0),
        (DuplicateCreatesSecondOrder, "duplicate_leaves_one_order", 2.0),
        (RepeatCancelSilent, "cancel_of_closed_is_venue_error", 2.0),
        (RepeatCancelTransportError, "cancel_of_closed_is_venue_error", 2.0),
    ],
)
async def test_suite_catches_adapters_that_would_break_the_safety_claims(
    broken: type[SimVenue], failing_check: str, timeout: float
) -> None:
    report = await run_full(venue(broken), timeout=timeout)
    assert failing_check in {check.name for check in report.failures}


async def test_refuses_to_run_against_a_live_provider() -> None:
    class Live(SimVenue):
        environment = Environment.LIVE

    live = venue(Live)
    with pytest.raises(ConfigError, match="LIVE"):
        await run_trading_conformance(live, passive)
    assert not live.attempts


async def test_provider_without_trading_is_reported() -> None:
    provider = MockProvider()
    provider.environment = Environment.SIMULATED
    report = await run_trading_conformance(provider, passive)
    assert not report.passed


class AcceptsDuplicates(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        try:
            return await super().place_order(request)
        except VenueError:
            return OrderAck(
                client_order_id=request.client_order_id,
                venue_order_id="DUP",
                ts=self.clock.now(),
            )


class WrongIdInAck(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        ack = await super().place_order(request)
        return ack.model_copy(update={"client_order_id": "other"})


class StatusRaisesForMissing(SimVenue):
    async def order_status(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        found = await super().order_status(client_order_id, symbol)
        if found is None:
            raise VenueError("not found")
        return found


class StatusGuessesMissing(SimVenue):
    async def order_status(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        return None


class CancelSwallowsUnknown(SimVenue):
    async def cancel_order(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        if client_order_id in self._orders:
            await super().cancel_order(client_order_id, symbol)


class CancelDoesNothing(SimVenue):
    async def cancel_order(self, client_order_id, symbol):  # type: ignore[no-untyped-def]
        if client_order_id not in self._orders:
            raise VenueError("unknown")


class NoStreamReports(SimVenue):
    def _publish(self, event) -> None:  # type: ignore[no-untyped-def]
        return


class HidesOpenOrders(SimVenue):
    async def open_orders(self, symbol=None):  # type: ignore[no-untyped-def]
        return []


class PlaceFails(SimVenue):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        raise VenueError("account disabled")


class ReportsWrongStatus(SimVenue):
    def _snapshot(self, order_):  # type: ignore[no-untyped-def]
        return (
            super()
            ._snapshot(order_)
            .model_copy(update={"status": OrderStatus.PARTIALLY_FILLED})
        )


@pytest.mark.parametrize(
    ("broken", "failing_check"),
    [
        (AcceptsDuplicates, "duplicate_client_id_refused"),
        (WrongIdInAck, "ack_echoes_client_order_id"),
        (StatusRaisesForMissing, "status_of_unknown_order"),
        (StatusGuessesMissing, "status_finds_order_by_client_id"),
        (CancelSwallowsUnknown, "cancel_unknown_is_venue_error"),
        (CancelDoesNothing, "cancel_takes_effect"),
        (HidesOpenOrders, "open_orders_lists_order"),
        (PlaceFails, "place_order"),
        (ReportsWrongStatus, "passive_order_is_working"),
    ],
)
async def test_suite_catches_each_kind_of_broken_adapter(
    broken: type[SimVenue], failing_check: str
) -> None:
    report = await run_trading_conformance(venue(broken), passive)
    assert failing_check in {check.name for check in report.failures}


async def test_suite_catches_an_adapter_with_a_silent_stream() -> None:
    report = await run_trading_conformance(
        venue(NoStreamReports), passive, timeout=0.05
    )
    assert "stream_reports_new_order" in {check.name for check in report.failures}
