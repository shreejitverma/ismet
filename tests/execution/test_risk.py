"""Each pre-trade check, including that missing information refuses the order."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ismet.calendars import get_calendar
from ismet.execution.risk import RiskConfig, RiskContext, RiskEngine, RiskLimits
from ismet.execution.state import new_order
from ismet.models import AssetClass, Instrument, ListingStatus, Quote, Symbol
from ismet.models.orders import (
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    ShortSaleMark,
    SubmitIntent,
    TimeInForce,
    TradingRules,
)

D = Decimal
NOW = datetime(2026, 3, 9, 15, 0, tzinfo=timezone.utc)  # XNYS open
SYM = Symbol(venue="XNYS", ticker="ACME")
RULES = TradingRules(
    order_types=frozenset(OrderType),
    time_in_force=frozenset(TimeInForce),
    supports_short=True,
    supports_post_only=True,
)
INST = Instrument(
    symbol=SYM,
    name="Acme",
    asset_class=AssetClass.EQUITY,
    currency="USD",
    tick_size="0.01",
    lot_size="1",
    status=ListingStatus.ACTIVE,
)
QUOTE = Quote(
    symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="99.99", ask="100.01", last="100"
)
LIMITS = RiskLimits(
    max_order_quantity=D(1000),
    max_order_notional=D(50000),
    max_position=D(2000),
    max_open_orders=5,
    price_collar_bps=D(100),
)
_n = {"i": 0}


def request(**kw: object) -> OrderRequest:
    _n["i"] += 1
    fields: dict[str, object] = {
        "client_order_id": f"R{_n['i']}",
        "symbol": SYM,
        "side": OrderSide.BUY,
        "quantity": "10",
        "limit_price": "100.00",
    }
    fields.update(kw)
    return OrderRequest.model_validate(fields)


def ctx(req: OrderRequest | None = None, **kw: object) -> RiskContext:
    fields: dict[str, object] = {
        "request": req or request(),
        "instrument": INST,
        "reference": QUOTE,
        "now": NOW,
        "position": D(0),
        "calendar": get_calendar("XNYS"),
        "rules": RULES,
        "gross_notional": D(0),
    }
    fields.update(kw)
    return RiskContext(**fields)  # type: ignore[arg-type]


def config(**kw: object) -> RiskConfig:
    fields: dict[str, object] = {
        "default": LIMITS,
        "duplicate_window_seconds": D(0),
        "allow_short": True,
    }
    fields.update(kw)
    return RiskConfig.model_validate(fields)


def rejected_by(check: str, context: RiskContext, cfg: RiskConfig | None = None) -> str:
    decision = RiskEngine(cfg or config()).check(context)
    assert not decision.approved, "expected a rejection"
    assert decision.check == check, decision
    return decision.reason


def open_order(status: OrderStatus = OrderStatus.ACCEPTED, **kw: object):  # type: ignore[no-untyped-def]
    req = request(**kw)
    state = new_order(
        SubmitIntent(client_order_id=req.client_order_id, ts=NOW, request=req)
    )
    return replace(state, status=status)


def test_a_sound_order_passes() -> None:
    assert RiskEngine(config()).check(ctx()).approved


def test_default_limits_are_required() -> None:
    with pytest.raises(ValueError):
        RiskConfig()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("change", "check"),
    [
        ({"instrument": None}, "instrument"),
        ({"reference": None}, "reference_price"),
        (
            {
                "position": None,
                "request": request(side=OrderSide.SELL, limit_price="99.00"),
            },
            "short_sale",
        ),
        ({"calendar": None}, "session"),
        ({"rules": None}, "order_support"),
    ],
)
def test_missing_information_refuses_the_order(
    change: dict[str, object], check: str
) -> None:
    rejected_by(check, ctx(**change))


def test_position_unknown_fails_a_position_limit_closed() -> None:
    rejected_by("max_position", ctx(position=None))


def test_gross_unknown_fails_a_gross_limit_closed() -> None:
    limits = RiskLimits(max_gross_notional=D(1000))
    rejected_by("max_gross_notional", ctx(gross_notional=None), config(default=limits))


def test_exception_inside_a_check_refuses_the_order() -> None:
    class Broken:
        multiplier = "not a decimal"

        def __getattr__(self, name: str) -> object:
            raise RuntimeError("boom")

    reason = rejected_by("risk_engine_error", ctx(instrument=Broken()))
    assert "boom" in reason


@pytest.mark.parametrize(
    ("terms", "status", "expected"),
    [
        ({"active": False}, ListingStatus.ACTIVE, "instrument"),
        ({}, ListingStatus.HALTED, "instrument"),
        ({}, ListingStatus.SUSPENDED, "instrument"),
        ({}, ListingStatus.DELISTED, "instrument"),
    ],
)
def test_untradable_instruments(
    terms: dict[str, object], status: ListingStatus, expected: str
) -> None:
    instrument = INST.model_copy(update={"status": status, **terms})
    rejected_by(expected, ctx(instrument=instrument))


def test_expired_instrument() -> None:
    instrument = INST.model_copy(update={"expiry": NOW - timedelta(days=1)})
    rejected_by("instrument", ctx(instrument=instrument))


def test_tick_lot_and_size_bounds() -> None:
    rejected_by("tick_size", ctx(request(limit_price="100.001")))
    rejected_by(
        "tick_size",
        ctx(
            request(
                order_type=OrderType.STOP_LIMIT,
                limit_price="100.00",
                stop_price="100.005",
            )
        ),
    )
    rejected_by("lot_size", ctx(request(quantity="1.5")))
    with_min = INST.model_copy(update={"min_quantity": D(50)})
    rejected_by("min_quantity", ctx(instrument=with_min))
    with_max = INST.model_copy(update={"max_quantity": D(5)})
    rejected_by("max_quantity", ctx(instrument=with_max))
    with_notional = INST.model_copy(update={"min_notional": D(1_000_000)})
    rejected_by("min_notional", ctx(instrument=with_notional))


def test_declared_order_support() -> None:
    only_limit = RULES.model_copy(update={"order_types": frozenset({OrderType.LIMIT})})
    rejected_by(
        "order_support",
        ctx(request(order_type=OrderType.MARKET, limit_price=None), rules=only_limit),
    )
    only_day = RULES.model_copy(update={"time_in_force": frozenset({TimeInForce.DAY})})
    rejected_by(
        "order_support", ctx(request(time_in_force=TimeInForce.IOC), rules=only_day)
    )
    no_post = RULES.model_copy(update={"supports_post_only": False})
    rejected_by("order_support", ctx(request(post_only=True), rules=no_post))
    short_ids = RULES.model_copy(update={"client_order_id_max_length": 1})
    rejected_by("order_support", ctx(rules=short_ids))


def test_session_checks() -> None:
    closed = datetime(2026, 3, 8, 15, 0, tzinfo=timezone.utc)
    stale_free = QUOTE.model_copy(update={"exchange_ts": closed, "received_ts": closed})
    reason = rejected_by("session", ctx(now=closed, reference=stale_free))
    assert "continuous trading" in reason
    # Orders meant for later sessions are allowed through when the venue is closed.
    gtc = request(time_in_force=TimeInForce.GTC)
    assert (
        RiskEngine(config()).check(ctx(gtc, now=closed, reference=stale_free)).approved
    )
    relaxed = config(enforce_session=False)
    assert RiskEngine(relaxed).check(ctx(now=closed, reference=stale_free)).approved


def test_calendar_out_of_range_refuses_the_order() -> None:
    calendar = get_calendar("XNYS")
    far = datetime.combine(
        calendar.last_date + timedelta(days=30),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    fresh = QUOTE.model_copy(update={"exchange_ts": far, "received_ts": far})
    assert "covers" in rejected_by("session", ctx(now=far, reference=fresh))


def test_restricted_list_matches_full_symbol_or_ticker() -> None:
    rejected_by(
        "restricted", ctx(), config(restricted_symbols=frozenset({"ACME@XNYS"}))
    )
    rejected_by("restricted", ctx(), config(restricted_symbols=frozenset({"ACME"})))


@pytest.mark.parametrize("entry", ["BRKb", "brkb", "BRKB", "BRKb@XNYS", "brkb@xnys"])
def test_restricted_list_matches_a_mixed_case_ticker(entry: str) -> None:
    symbol = Symbol(venue="XNYS", ticker="BRKb")
    rejected_by(
        "restricted",
        ctx(request(symbol=symbol)),
        config(restricted_symbols=frozenset({entry})),
    )


def test_restricted_list_does_not_refuse_another_ticker() -> None:
    decision = RiskEngine(config(restricted_symbols=frozenset({"BRKb"}))).check(ctx())
    assert decision.approved, decision


def test_stale_reference_refuses_the_order() -> None:
    stale = QUOTE.model_copy(update={"exchange_ts": NOW - timedelta(seconds=6)})
    assert "old" in rejected_by("stale_data", ctx(reference=stale))


@pytest.mark.parametrize(
    ("bid", "ask"),
    [("100.02", "100.01"), (None, None), ("99.99", None), (None, "100.01")],
)
def test_market_order_needs_a_two_sided_uncrossed_book(
    bid: str | None, ask: str | None
) -> None:
    quote = Quote(symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid=bid, ask=ask)
    market = request(order_type=OrderType.MARKET, limit_price=None)
    rejected_by("reference_price", ctx(market, reference=quote))


def test_price_collar_blocks_aggressive_prices_only() -> None:
    assert "bps" in rejected_by("price_collar", ctx(request(limit_price="102.00")))
    assert RiskEngine(config()).check(ctx(request(limit_price="50.00"))).approved
    sell = request(side=OrderSide.SELL, limit_price="98.00")
    rejected_by("price_collar", ctx(sell, position=D(100)))
    ok_sell = request(side=OrderSide.SELL, limit_price="150.00")
    assert RiskEngine(config()).check(ctx(ok_sell, position=D(100))).approved


def test_collar_handles_negative_prices_and_zero_reference() -> None:
    quote = Quote(
        symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="-40.00", ask="-38.00"
    )
    assert (
        RiskEngine(config())
        .check(ctx(request(limit_price="-38.00"), reference=quote))
        .approved
    )
    rejected_by("price_collar", ctx(request(limit_price="-30.00"), reference=quote))
    zero = Quote(symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="0", ask="0")
    assert "zero" in rejected_by(
        "price_collar", ctx(request(limit_price="1.00"), reference=zero)
    )


def test_size_and_notional_limits_include_the_multiplier() -> None:
    rejected_by("max_order_quantity", ctx(request(quantity="1001")))
    rejected_by("max_order_notional", ctx(request(quantity="600")))
    option_like = INST.model_copy(update={"multiplier": D(100)})
    rejected_by(
        "max_order_notional", ctx(request(quantity="10"), instrument=option_like)
    )
    market = request(order_type=OrderType.MARKET, limit_price=None, quantity="600")
    rejected_by("max_order_notional", ctx(market))


def test_position_limit_uses_worst_case_including_open_orders() -> None:
    rejected_by("max_position", ctx(request(quantity="490"), position=D(1600)))
    working = open_order(quantity="400")
    rejected_by(
        "max_position",
        ctx(request(quantity="490"), position=D(1200), open_orders=[working]),
    )
    assert (
        RiskEngine(config())
        .check(ctx(request(quantity="490"), position=D(1200)))
        .approved
    )


def test_reducing_a_position_beyond_the_limit_is_allowed() -> None:
    sell = request(side=OrderSide.SELL, limit_price="99.99", quantity="100")
    assert RiskEngine(config()).check(ctx(sell, position=D(3000))).approved


def test_closing_either_side_to_flat_is_a_reduction_the_gross_limit_never_blocks() -> (
    None
):
    cfg = config(default=RiskLimits(max_gross_notional=D(1000)))
    engine = RiskEngine(cfg)
    buy_to_flat = request(quantity="10", limit_price="100.00")
    assert engine.check(ctx(buy_to_flat, position=D(-10), gross_notional=None)).approved
    sell_to_flat = request(side=OrderSide.SELL, quantity="10", limit_price="100.00")
    assert engine.check(ctx(sell_to_flat, position=D(10), gross_notional=None)).approved
    through_flat = request(quantity="11", limit_price="100.00")
    rejected_by(
        "max_gross_notional",
        ctx(through_flat, position=D(-10), gross_notional=None),
        cfg,
    )


def test_gross_limit() -> None:
    cfg = config(default=RiskLimits(max_gross_notional=D(5000)))
    rejected_by("max_gross_notional", ctx(gross_notional=D(4500)), cfg)
    assert RiskEngine(cfg).check(ctx(gross_notional=D(3000))).approved


def test_open_order_count_limit() -> None:
    orders = [open_order(limit_price="90.00") for _ in range(5)]
    rejected_by("max_open_orders", ctx(open_orders=orders))


def test_unknown_order_in_the_same_symbol_blocks_new_risk() -> None:
    unknown = open_order(OrderStatus.UNKNOWN)
    assert "unknown status" in rejected_by("unknown_order", ctx(open_orders=[unknown]))
    relaxed = config(block_while_unknown=False)
    assert RiskEngine(relaxed).check(ctx(open_orders=[unknown])).approved
    other = open_order(OrderStatus.UNKNOWN, symbol=Symbol(venue="XNYS", ticker="OTHER"))
    assert RiskEngine(config()).check(ctx(open_orders=[other])).approved


def test_self_match_prevention() -> None:
    resting_sell = open_order(side=OrderSide.SELL, limit_price="100.00")
    rejected_by("self_match", ctx(open_orders=[resting_sell]))
    passive = request(limit_price="99.50")
    assert RiskEngine(config()).check(ctx(passive, open_orders=[resting_sell])).approved
    market_sell = request(side=OrderSide.SELL, limit_price="99.99")
    resting_buy = open_order(limit_price="99.99")
    rejected_by(
        "self_match", ctx(market_sell, position=D(100), open_orders=[resting_buy])
    )
    relaxed = config(prevent_self_match=False)
    assert RiskEngine(relaxed).check(ctx(open_orders=[resting_sell])).approved


def test_short_sale_classification() -> None:
    sell = request(side=OrderSide.SELL, limit_price="99.99", quantity="100")
    assert RiskEngine(config()).check(ctx(sell, position=D(100))).approved
    assert "not marked short" in rejected_by("short_sale", ctx(sell, position=D(50)))
    # Open sells count against the long position.
    pending = open_order(side=OrderSide.SELL, limit_price="150.00", quantity="60")
    rejected_by("short_sale", ctx(sell, position=D(100), open_orders=[pending]))
    marked = request(
        side=OrderSide.SELL,
        limit_price="99.99",
        quantity="100",
        short_sale=ShortSaleMark.SHORT,
    )
    assert "locate" in rejected_by("short_sale", ctx(marked, position=D(0)))
    assert (
        RiskEngine(config())
        .check(ctx(marked, position=D(0), locate_available=True))
        .approved
    )
    assert "disabled" in rejected_by(
        "short_sale", ctx(marked, position=D(0)), config(allow_short=False)
    )
    no_short = RULES.model_copy(update={"supports_short": False})
    assert "provider" in rejected_by(
        "short_sale", ctx(marked, position=D(0), rules=no_short)
    )
    exempt = request(
        side=OrderSide.SELL,
        limit_price="99.99",
        quantity="100",
        short_sale=ShortSaleMark.SHORT_EXEMPT,
    )
    assert RiskEngine(config()).check(ctx(exempt, position=D(0))).approved
    reduce = request(
        side=OrderSide.SELL, limit_price="99.99", quantity="100", reduce_only=True
    )
    assert "would become" in rejected_by("reduce_only", ctx(reduce, position=D(50)))


def test_order_rate_window_slides() -> None:
    engine = RiskEngine(config(default=RiskLimits(max_orders_per_second=2)))
    assert engine.check(ctx(now=NOW)).approved
    assert engine.check(ctx(now=NOW + timedelta(milliseconds=100))).approved
    third = engine.check(ctx(now=NOW + timedelta(milliseconds=200)))
    assert not third.approved and third.check == "order_rate"
    assert engine.check(ctx(now=NOW + timedelta(seconds=1, milliseconds=50))).approved


def test_duplicate_detection_in_a_window() -> None:
    engine = RiskEngine(config(duplicate_window_seconds=D(1)))
    assert engine.check(ctx(request(), now=NOW)).approved
    twin = engine.check(ctx(request(), now=NOW + timedelta(milliseconds=100)))
    assert not twin.approved and twin.check == "duplicate_order"
    different = engine.check(ctx(request(quantity="11"), now=NOW))
    assert different.approved
    assert engine.check(ctx(request(), now=NOW + timedelta(seconds=2))).approved


def test_every_applicable_scope_must_pass() -> None:
    tight = RiskLimits(max_order_quantity=D(5))
    by_account = config(per_account={"A": tight})
    rejected_by("max_order_quantity", ctx(request(account="A")), by_account)
    assert RiskEngine(by_account).check(ctx(request(account="B"))).approved
    by_strategy = config(per_strategy={"S": tight})
    assert "strategy:S" in rejected_by(
        "max_order_quantity", ctx(request(strategy="S")), by_strategy
    )
    by_symbol = config(per_symbol={"ACME@XNYS": tight})
    rejected_by("max_order_quantity", ctx(), by_symbol)


def test_config_can_be_replaced() -> None:
    engine = RiskEngine(config())
    tight = config(default=RiskLimits(max_order_quantity=D(1)))
    engine.update_config(tight)
    assert engine.config is tight
    assert not engine.check(ctx()).approved


def test_amend_skips_the_duplicate_check() -> None:
    engine = RiskEngine(config(duplicate_window_seconds=D(1)))
    same = request()
    assert engine.check(ctx(same)).approved
    assert engine.check(ctx(same), is_amend=True).approved
