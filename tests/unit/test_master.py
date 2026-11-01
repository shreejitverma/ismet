from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from ismet.errors import ValidationError
from ismet.models import Adjustment, Bar, Interval, Symbol
from ismet.refdata.master import (
    ActionKind,
    CorporateAction,
    Listing,
    SecurityMaster,
    adjust_bars,
)

D = Decimal
FB = Symbol(venue="XNAS", ticker="FB")
META = Symbol(venue="XNAS", ticker="META")
UTC = timezone.utc


def master() -> SecurityMaster:
    book = SecurityMaster()
    book.add(Listing("SEC-META", FB, date(2012, 5, 18), date(2022, 6, 9)))
    book.add(Listing("SEC-META", META, date(2022, 6, 9)))
    return book


def test_ticker_change_keeps_one_security_identity() -> None:
    book = master()
    assert book.resolve(FB, date(2020, 1, 2)) == "SEC-META"
    assert book.resolve(META, date(2023, 1, 3)) == "SEC-META"
    assert book.resolve(FB, date(2023, 1, 3)) is None
    assert book.symbol_on("SEC-META", date(2020, 1, 2)) == FB
    assert book.symbol_on("SEC-META", date(2023, 1, 3)) == META
    assert book.symbol_on("SEC-META", date(2000, 1, 3)) is None
    assert [lst.symbol for lst in book.history("SEC-META")] == [FB, META]


def test_recycled_ticker_means_different_securities_at_different_dates() -> None:
    book = SecurityMaster()
    ticker = Symbol(venue="XNYS", ticker="ABC")
    book.add(Listing("OLD", ticker, date(2000, 1, 3), date(2010, 1, 4)))
    book.add(Listing("NEW", ticker, date(2015, 1, 5)))
    assert book.resolve(ticker, date(2005, 6, 1)) == "OLD"
    assert book.resolve(ticker, date(2012, 6, 1)) is None
    assert book.resolve(ticker, date(2020, 6, 1)) == "NEW"


def test_knowledge_time_prevents_look_ahead() -> None:
    book = SecurityMaster()
    ticker = Symbol(venue="XNYS", ticker="ABC")
    book.add(Listing("WRONG", ticker, date(2020, 1, 2)))
    corrected_at = datetime(2021, 6, 1, tzinfo=UTC)
    book.add(Listing("RIGHT", ticker, date(2020, 1, 2), known_from=corrected_at))
    assert book.resolve(ticker, date(2020, 6, 1)) == "RIGHT"
    believed_then = datetime(2021, 1, 1, tzinfo=UTC)
    assert book.resolve(ticker, date(2020, 6, 1), known_at=believed_then) == "WRONG"
    assert book.symbol_on("RIGHT", date(2020, 6, 1), known_at=believed_then) is None


def test_universe_includes_delisted_names_on_past_dates() -> None:
    book = master()
    book.add(
        Listing(
            "GONE",
            Symbol(venue="XNAS", ticker="GONE"),
            date(2000, 1, 3),
            date(2015, 1, 5),
        )
    )
    assert book.universe("XNAS", date(2014, 1, 2)) == ["GONE", "SEC-META"]
    assert book.universe("XNAS", date(2016, 1, 4)) == ["SEC-META"]


def test_empty_listing_range_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Listing("X", FB, date(2020, 1, 2), date(2020, 1, 2))


def bar(day: date, close: str, volume: str = "100", **kw: object) -> Bar:
    ts = datetime(day.year, day.month, day.day, 14, 30, tzinfo=UTC)
    fields: dict[str, object] = {
        "symbol": FB,
        "exchange_ts": ts,
        "received_ts": ts,
        "interval": Interval.D1,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "adjustment": Adjustment.RAW,
        "session_date": day,
    }
    fields.update(kw)
    return Bar.model_validate(fields)


def test_four_for_one_split_back_adjusts_prices_and_volume() -> None:
    """AAPL's 4-for-1 split, ex-date 2020-08-31."""
    bars = [bar(date(2020, 8, 28), "499.23"), bar(date(2020, 8, 31), "129.04")]
    split = CorporateAction(ActionKind.SPLIT, date(2020, 8, 31), ratio=D(4))
    before, after = adjust_bars(bars, [split])
    assert before.close == D("124.8075") and before.volume == D(400)
    assert after.close == D("129.04") and after.volume == D(100)
    assert before.adjustment is Adjustment.SPLIT


def test_dividend_uses_the_ratio_method() -> None:
    bars = [bar(date(2026, 3, 6), "100"), bar(date(2026, 3, 9), "99")]
    dividend = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 9), amount=D(1))
    plain = adjust_bars(bars, [dividend])
    assert plain[0].close == D(100)
    total_return = adjust_bars(bars, [dividend], include_dividends=True)
    assert total_return[0].close == D(99)
    assert total_return[0].adjustment is Adjustment.SPLIT_DIVIDEND
    assert total_return[1].close == D(99)


def test_action_factors_compound() -> None:
    bars = [bar(date(2026, 1, 2), "100"), bar(date(2026, 6, 1), "25")]
    actions = [
        CorporateAction(ActionKind.SPLIT, date(2026, 2, 2), ratio=D(2)),
        CorporateAction(ActionKind.SPLIT, date(2026, 3, 2), ratio=D(2)),
    ]
    assert adjust_bars(bars, actions)[0].close == D(25)


def test_explicit_price_factor_for_a_spin_off() -> None:
    bars = [bar(date(2026, 1, 2), "100"), bar(date(2026, 6, 1), "80")]
    spin = CorporateAction(ActionKind.SPIN_OFF, date(2026, 3, 2), price_factor=D("0.8"))
    assert adjust_bars(bars, [spin])[0].close == D(80)


def test_knowledge_time_excludes_actions_not_yet_announced() -> None:
    bars = [bar(date(2026, 1, 2), "100")]
    announced = datetime(2026, 2, 1, tzinfo=UTC)
    split = CorporateAction(
        ActionKind.SPLIT, date(2026, 3, 2), ratio=D(2), known_from=announced
    )
    assert adjust_bars(bars, [split])[0].close == D(50)
    assert adjust_bars(bars, [split], known_at=datetime(2026, 1, 15, tzinfo=UTC))[
        0
    ].close == D(100)


def test_adjusting_adjusted_data_is_refused() -> None:
    with pytest.raises(ValidationError, match="raw bars"):
        adjust_bars([bar(date(2026, 1, 2), "1", adjustment=Adjustment.SPLIT)], [])
    with pytest.raises(ValidationError, match="raw bars"):
        adjust_bars([bar(date(2026, 1, 2), "1", adjustment=Adjustment.UNKNOWN)], [])


def test_invalid_actions_are_rejected() -> None:
    with pytest.raises(ValidationError, match="positive ratio"):
        CorporateAction(ActionKind.SPLIT, date(2026, 3, 2))
    with pytest.raises(ValidationError, match="positive ratio"):
        CorporateAction(ActionKind.STOCK_DIVIDEND, date(2026, 3, 2), ratio=D(0))
    with pytest.raises(ValidationError, match="requires an amount"):
        CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2))


def test_dividend_needs_a_close_before_the_ex_date() -> None:
    bars = [bar(date(2026, 1, 2), "100")]
    action = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(1))
    # The bar precedes the ex-date, and is itself the prior close: fine.
    assert adjust_bars(bars, [action], include_dividends=True)
    later = CorporateAction(ActionKind.CASH_DIVIDEND, date(2025, 3, 2), amount=D(1))
    assert adjust_bars(bars, [later], include_dividends=True)[0].close == D(100)
