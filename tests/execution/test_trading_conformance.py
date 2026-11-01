"""The trading suite passes the simulated venue and catches broken adapters."""

from __future__ import annotations

import pytest

from ismet.errors import AmbiguousOutcome, ConfigError, TransportError, VenueError
from ismet.models.orders import (
    Environment,
    OrderAck,
    OrderRequest,
    OrderStatus,
)
from ismet.providers.mock import MockProvider
from ismet.testing.conformance import assert_conformant
from ismet.testing.simvenue import Fault, SimVenue, sim_instrument
from ismet.testing.trading_conformance import run_trading_conformance

from .conftest import ACME, order


def passive(client_order_id: str) -> OrderRequest:
    return order(client_order_id, price="9.00")


def venue(cls: type[SimVenue] = SimVenue) -> SimVenue:
    sim = cls(instruments=[sim_instrument("ACME")])
    sim.set_quote(ACME, "9.99", "10.01")
    return sim


def marketable(client_order_id: str) -> OrderRequest:
    return order(client_order_id, price="10.01", quantity="1")


async def run_full(sim: SimVenue, **kw: object):  # type: ignore[no-untyped-def]
    return await run_trading_conformance(
        sim,
        passive,
        make_marketable_order=marketable,
        attempts=lambda cid: sim.attempts[cid],
        arm_ambiguous_outcome=lambda: sim.faults.append(Fault.LOSE_RESPONSE),
        **kw,  # type: ignore[arg-type]
    )


async def test_simulated_venue_conforms() -> None:
    report = await run_full(venue())
    assert_conformant(report)
    names = {check.name for check in report.checks}
    assert {
        "place_sends_exactly_once",
        "duplicate_client_id_refused",
        "status_none_when_not_found",
        "fill_id_matches_across_channels",
        "fill_terms_match_across_channels",
        "open_orders_lists_order_without_symbol",
        "fills_since_reports_fill_without_symbol",
        "lost_answer_is_ambiguous_outcome",
    } <= names


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
