"""Regressions for defects that made valid market data or safe retries impossible."""

from __future__ import annotations

import decimal
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, ValidationError

from ismet.errors import AmbiguousOutcome, RateLimited, TransportError
from ismet.models import (
    AssetClass,
    Bar,
    Instrument,
    Interval,
    OptionRight,
    Quote,
    Rounding,
    Symbol,
    TickBand,
    TickLadder,
    Trade,
    datetime_from_ns,
    to_decimal,
)
from ismet.models.decimals import is_multiple, midpoint, ratio, round_to_step
from ismet.models.types import Currency, VenueCode
from ismet.refdata import (
    currency_info,
    is_crypto_asset,
    is_iso_currency,
    register_crypto_asset,
    register_private_venue,
    validate_cusip,
    validate_figi,
    validate_isin,
    validate_lei,
    validate_sedol,
    venue_info,
)
from ismet.transport.backoff import ExponentialBackoff, RetryPolicy
from ismet.transport.http import HttpTransport
from ismet.transport.ws import SequenceOutcome, SequenceTracker

D = Decimal
NOW = datetime(2026, 3, 2, 15, 0, tzinfo=timezone.utc)
SYM = Symbol(venue="XNYS", ticker="IBM")
FAST = RetryPolicy(max_attempts=3, backoff=ExponentialBackoff(base=0, jitter=False))


def inst(**kw: object) -> Instrument:
    fields: dict[str, object] = {
        "symbol": SYM,
        "name": "n",
        "currency": "USD",
        "tick_size": "0.01",
    }
    fields.update(kw)
    return Instrument(**fields)  # type: ignore[arg-type]


# Prices, rounding, and precision


@pytest.mark.parametrize(
    ("rounding", "expected"),
    [
        (Rounding.FLOOR, "-37.64"),
        (Rounding.CEILING, "-37.63"),
        (Rounding.TOWARD_ZERO, "-37.63"),
        (Rounding.AWAY_FROM_ZERO, "-37.64"),
    ],
)
def test_negative_price_rounds_in_the_stated_direction(
    rounding: Rounding, expected: str
) -> None:
    assert inst().round_price(D("-37.635"), rounding) == D(expected)


def test_round_half_even_uses_quotient_parity() -> None:
    assert round_to_step(D("10.125"), D("0.01"), Rounding.HALF_EVEN) == D("10.12")
    assert round_to_step(D("10.135"), D("0.01"), Rounding.HALF_EVEN) == D("10.14")
    assert round_to_step(D("10.131"), D("0.01"), Rounding.HALF_EVEN) == D("10.13")
    assert round_to_step(D("10.139"), D("0.01"), Rounding.HALF_EVEN) == D("10.14")
    assert round_to_step(D("10.14"), D("0.01"), Rounding.CEILING) == D("10.14")


def test_rounding_is_independent_of_the_callers_decimal_context() -> None:
    big = D("123456789012.123456789012345678")
    tiny = inst(tick_size="1E-18")
    with decimal.localcontext() as ctx:
        ctx.prec = 5
        assert tiny.round_price(big, Rounding.FLOOR) == big
        assert tiny.is_tick_aligned(big)


def test_midpoint_is_exact_beyond_the_default_context() -> None:
    bid = D("123456789012345.123456789012345678")
    ask = D("123456789012345.123456789012345679")
    quote = Quote(symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid=bid, ask=ask)
    assert quote.mid == D("123456789012345.1234567890123456785")
    assert quote.spread == D("0.000000000000000001")


def test_ratio_rounds_explicitly_and_only_ratio_rounds() -> None:
    assert ratio(D(1), D(3)) == D("0." + "3" * 50)
    assert midpoint(D(1), D(2)) == D("1.5")


@given(
    st.decimals(min_value=-(10**12), max_value=10**12, places=8),
    st.sampled_from([D("0.01"), D("0.05"), D("0.25"), D(1), D("1E-8")]),
)
def test_rounding_directions_bracket_the_value(value: Decimal, step: Decimal) -> None:
    floor = round_to_step(value, step, Rounding.FLOOR)
    ceiling = round_to_step(value, step, Rounding.CEILING)
    assert floor <= value <= ceiling
    assert is_multiple(floor, step) and is_multiple(ceiling, step)
    assert ceiling - floor in (D(0), step)
    nearest = round_to_step(value, step, Rounding.HALF_EVEN)
    assert abs(nearest - value) * 2 <= step


@pytest.mark.parametrize(
    "bad", ["1E+1000000", "0E+999", "1E-1000", "1" * 65, "0." + "1" * 65]
)
def test_magnitudes_outside_the_exact_range_are_rejected(bad: str) -> None:
    with pytest.raises(ValueError):
        to_decimal(bad)


def test_zero_bid_and_negative_prices_are_valid_market_data() -> None:
    assert (
        Quote(symbol=SYM, exchange_ts=NOW, received_ts=NOW, bid="0", ask="0.05").bid
        == 0
    )
    trade = Trade(
        symbol=SYM, exchange_ts=NOW, received_ts=NOW, price="-37.63", size="1"
    )
    assert trade.price == D("-37.63")
    bar = Bar(
        symbol=SYM,
        exchange_ts=NOW,
        received_ts=NOW,
        interval=Interval.D1,
        open="10",
        high="11",
        low="-40",
        close="-37.63",
        volume="1",
    )
    assert bar.low == D(-40)


def test_tick_size_and_lot_size_stay_strictly_positive() -> None:
    with pytest.raises(ValidationError):
        inst(tick_size="0")
    with pytest.raises(ValidationError):
        inst(lot_size="-1")


# Tick ladders

REG_NMS = TickLadder(
    bands=(
        TickBand(from_price=D(0), tick=D("0.0001")),
        TickBand(from_price=D(1), tick=D("0.01")),
    )
)


def test_tick_ladder_selects_band_by_price() -> None:
    instrument = inst(tick_ladder=REG_NMS)
    assert instrument.tick_at(D("0.5")) == D("0.0001")
    assert instrument.tick_at(D(1)) == D("0.01")
    assert instrument.is_tick_aligned(D("0.5001"))
    assert not instrument.is_tick_aligned(D("1.0001"))
    assert instrument.round_price(D("1.0049"), Rounding.FLOOR) == D("1.00")


@given(st.decimals(min_value=0, max_value=100, places=6))
def test_tick_ladder_rounding_always_lands_on_a_valid_tick(price: Decimal) -> None:
    instrument = inst(tick_ladder=REG_NMS)
    for rounding in (Rounding.FLOOR, Rounding.CEILING):
        assert instrument.is_tick_aligned(instrument.round_price(price, rounding))


@pytest.mark.parametrize(
    "bands",
    [
        (),
        (TickBand(from_price=D(1), tick=D("0.01")),),
        (
            TickBand(from_price=D(0), tick=D("0.01")),
            TickBand(from_price=D("1.005"), tick=D("0.01")),
        ),
        (
            TickBand(from_price=D(0), tick=D("0.01")),
            TickBand(from_price=D(5), tick=D("0.05")),
            TickBand(from_price=D(5), tick=D("0.05")),
        ),
    ],
)
def test_invalid_tick_ladders_are_rejected(bands: tuple[TickBand, ...]) -> None:
    with pytest.raises(ValidationError):
        TickLadder(bands=bands)


# Instruments


def test_contract_terms_are_required_by_asset_class() -> None:
    with pytest.raises(ValidationError, match="option is missing"):
        inst(asset_class=AssetClass.OPTION)
    with pytest.raises(ValidationError, match="future is missing"):
        inst(asset_class=AssetClass.FUTURE)
    with pytest.raises(ValidationError, match="apply only to options"):
        inst(strike="100")
    with pytest.raises(ValidationError, match="delisting_date"):
        inst(listing_date="2020-01-02", delisting_date="2019-01-02")
    option = inst(
        asset_class=AssetClass.OPTION,
        option_right=OptionRight.CALL,
        strike="150",
        expiry=NOW + timedelta(days=30),
        underlying=SYM,
        multiplier="100",
    )
    assert option.multiplier == 100


def test_round_quantity_never_rounds_a_size_up() -> None:
    assert inst(lot_size="100").round_quantity(D(250)) == D(200)


# Timestamps


def test_nanoseconds_are_preserved_losslessly() -> None:
    ns = 1_772_463_600_123_456_789
    ts = datetime_from_ns(ns)
    event = Trade(
        symbol=SYM,
        exchange_ts=ts,
        exchange_ts_ns=ns,
        received_ts=ts,
        price="1",
        size="1",
    )
    assert event.exchange_ts_ns == ns
    assert ts.microsecond == 123456
    with pytest.raises(ValidationError, match="does not match"):
        Trade(
            symbol=SYM,
            exchange_ts=NOW,
            exchange_ts_ns=ns,
            received_ts=NOW,
            price="1",
            size="1",
        )


# Codes and identifiers


class Codes(BaseModel):
    venue: VenueCode | None = None
    currency: Currency | None = None


@pytest.mark.parametrize("code", ["NYSE", "XQQQ", "ABCD"])
def test_unregistered_venue_codes_are_rejected(code: str) -> None:
    with pytest.raises(ValidationError, match="unknown venue MIC"):
        Codes(venue=code)


def test_real_mics_including_expired_are_accepted() -> None:
    for mic in ("XNYS", "XNAS", "XNSE", "XBOM", "XLON", "XTKS", "XHKG", "xshg"):
        assert Codes(venue=mic).venue == mic.upper()
    info = venue_info("XNYS")
    assert info is not None and info.country == "US" and info.is_active


def test_private_venues_cannot_shadow_real_mics() -> None:
    assert register_private_venue("zqqq", "a test venue") == "ZQQQ"
    assert Codes(venue="ZQQQ").venue == "ZQQQ"
    assert venue_info("ZQQQ").is_private  # type: ignore[union-attr]
    with pytest.raises(ValueError, match="ISO 10383"):
        register_private_venue("XNYS", "nope")
    with pytest.raises(ValueError, match="4 characters"):
        register_private_venue("TOOLONG", "nope")


@pytest.mark.parametrize("code", ["ZZZ", "XXX", "XTS", "US", "BAD-CODE", "A B"])
def test_unknown_and_non_monetary_currencies_are_rejected(code: str) -> None:
    with pytest.raises(ValidationError):
        Codes(currency=code)


def test_currency_registry_separates_fiat_crypto_and_quote_units() -> None:
    assert Codes(currency="usd").currency == "USD"
    assert Codes(currency="GBX").currency == "GBX"
    assert Codes(currency="USDT").currency == "USDT"
    assert is_iso_currency("JPY") and not is_iso_currency("BTC")
    assert is_crypto_asset("BTC") and not is_crypto_asset("USD")
    info = currency_info("KWD")
    assert info is not None and info.minor_units == 3
    assert register_crypto_asset("newcoin") == "NEWCOIN"
    assert Codes(currency="NEWCOIN").currency == "NEWCOIN"
    assert register_crypto_asset("USD") == "USD"
    assert not is_crypto_asset("USD")
    with pytest.raises(ValueError):
        register_crypto_asset("!")
    assert Codes(currency=register_crypto_asset("t")).currency == "T"
    assert Codes(currency=register_crypto_asset("币安人生")).currency == "币安人生"


@pytest.mark.parametrize(
    ("validate", "good", "bad"),
    [
        (validate_isin, "US0378331005", "US0378331006"),
        (validate_isin, "GB0002634946", "garbage"),
        (validate_cusip, "037833100", "037833101"),
        (validate_cusip, "38259P508", "garbage"),
        (validate_sedol, "2046251", "2046252"),
        (validate_sedol, "B0YBKJ7", "AEIOU12"),
        (validate_figi, "BBG000B9XRY4", "BBG000B9XRY5"),
        (validate_figi, "BBG000BLNNH6", "BSG000BLNNH6"),
        (validate_lei, "5493001KJTIIGC8Y1R12", "5493001KJTIIGC8Y1R13"),
        (validate_lei, "254900LEW5UMQNLF1136", "garbage"),
    ],
)
def test_identifier_check_digits(validate, good: str, bad: str) -> None:  # type: ignore[no-untyped-def]
    assert validate(good.lower()) == good
    with pytest.raises(ValueError):
        validate(bad)


def test_symbol_identity_is_the_listing_key() -> None:
    plain = Symbol(venue="XNAS", ticker="AAPL")
    rich = Symbol(venue="XNAS", ticker="AAPL", isin="US0378331005")
    assert plain == rich and hash(plain) == hash(rich)
    assert len({plain, rich}) == 1
    assert plain != Symbol(venue="XNYS", ticker="AAPL")
    assert plain != "AAPL@XNAS"
    assert plain.same_identifiers(rich)
    other = Symbol(venue="XNAS", ticker="AAPL", isin="GB0002634946")
    assert not rich.same_identifiers(other)
    with pytest.raises(ValidationError):
        Symbol(venue="XNYS", ticker="X", isin="garbage")


# Order safety in the HTTP transport


def counting(handler):  # type: ignore[no-untyped-def]
    calls: list[str] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return handler(request)

    return calls, httpx.MockTransport(wrapped)


def raiser(exc: type[httpx.HTTPError]):  # type: ignore[no-untyped-def]
    def handle(request: httpx.Request) -> httpx.Response:
        raise exc("boom", request=request)

    return handle


@pytest.mark.parametrize("exc", [httpx.ReadTimeout, httpx.ReadError, httpx.WriteError])
async def test_lost_response_on_order_post_is_never_resent(
    exc: type[httpx.HTTPError],
) -> None:
    calls, transport = counting(raiser(exc))
    async with HttpTransport("https://v", transport=transport, retry_policy=FAST) as t:
        with pytest.raises(AmbiguousOutcome, match="may have been sent"):
            await t.post_json("/orders", {"qty": "1"})
    assert calls == ["POST"]


async def test_5xx_on_order_post_is_ambiguous_and_never_resent() -> None:
    calls, transport = counting(lambda _: httpx.Response(503, text="busy"))
    async with HttpTransport("https://v", transport=transport, retry_policy=FAST) as t:
        with pytest.raises(AmbiguousOutcome) as caught:
            await t.post_json("/orders", {})
    assert caught.value.status == 503 and not caught.value.retryable
    assert calls == ["POST"]


@pytest.mark.parametrize("exc", [httpx.ConnectError, httpx.ConnectTimeout])
async def test_undelivered_order_post_is_a_plain_transport_error(
    exc: type[httpx.HTTPError],
) -> None:
    calls, transport = counting(raiser(exc))
    async with HttpTransport("https://v", transport=transport, retry_policy=FAST) as t:
        with pytest.raises(TransportError) as caught:
            await t.post_json("/orders", {})
    assert not isinstance(caught.value, AmbiguousOutcome)
    assert calls == ["POST"]


async def test_explicit_retry_policy_cannot_force_a_resend_of_a_post() -> None:
    calls, transport = counting(raiser(httpx.ReadTimeout))
    async with HttpTransport("https://v", transport=transport) as t:
        with pytest.raises(AmbiguousOutcome):
            await t.request("POST", "/orders", retry_policy=FAST)
    assert calls == ["POST"]


async def test_rate_limited_post_is_not_resent_even_by_a_permissive_policy() -> None:
    calls, transport = counting(lambda _: httpx.Response(429, text="slow down"))
    always = RetryPolicy(
        max_attempts=3,
        backoff=ExponentialBackoff(base=0, jitter=False),
        should_retry=lambda _: True,
    )
    async with HttpTransport("https://v", transport=transport) as t:
        with pytest.raises(RateLimited):
            await t.request("POST", "/orders", retry_policy=always)
    assert calls == ["POST"]


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
async def test_other_state_changing_methods_are_sent_once(method: str) -> None:
    calls, transport = counting(raiser(httpx.ReadTimeout))
    async with HttpTransport("https://v", transport=transport, retry_policy=FAST) as t:
        with pytest.raises(AmbiguousOutcome):
            await t.request(method, "/orders/1")
    assert calls == [method]


async def test_get_and_declared_idempotent_requests_are_retried() -> None:
    calls, transport = counting(raiser(httpx.ReadTimeout))
    async with HttpTransport("https://v", transport=transport, retry_policy=FAST) as t:
        with pytest.raises(TransportError) as caught:
            await t.get_json("/quote")
        assert not isinstance(caught.value, AmbiguousOutcome)
        assert len(calls) == 3
        calls.clear()
        with pytest.raises(TransportError):
            await t.post_json("/search", {}, idempotent=True)
        assert len(calls) == 3


# Sequence tracking


def test_sequence_tracker_distinguishes_gap_duplicate_and_late_delivery() -> None:
    tracker = SequenceTracker()
    outcomes = [tracker.classify(n).outcome for n in (10, 11, 11, 14, 12)]
    assert outcomes == [
        SequenceOutcome.FIRST,
        SequenceOutcome.IN_ORDER,
        SequenceOutcome.DUPLICATE,
        SequenceOutcome.GAP,
        SequenceOutcome.OUT_OF_ORDER,
    ]
    assert tracker.last == 14
    assert (tracker.gaps, tracker.missing) == (1, 2)
    assert (tracker.duplicates, tracker.out_of_order) == (1, 1)
    tracker.reset()
    assert tracker.classify(1).outcome is SequenceOutcome.FIRST
