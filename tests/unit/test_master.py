from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from fractions import Fraction

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


def test_dividend_needs_the_close_of_the_session_before_the_ex_date() -> None:
    action = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(1))
    stale = [bar(date(2026, 1, 2), "100")]
    with pytest.raises(ValidationError, match=r"2026-02-27.*price_factor"):
        adjust_bars(stale, [action], include_dividends=True)
    explicit = CorporateAction(
        ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(1), price_factor=D("0.99")
    )
    assert adjust_bars(stale, [explicit], include_dividends=True)[0].close == D(99)
    already_past = CorporateAction(
        ActionKind.CASH_DIVIDEND, date(2025, 3, 3), amount=D(1)
    )
    result = adjust_bars(stale, [already_past], include_dividends=True)
    assert result[0].close == D(100)


def test_dividend_scale_comes_from_the_session_before_it_not_an_old_bar() -> None:
    """AAPL: 4-for-1 ex 2020-08-31, then 0.205 ex 2020-11-06 after a 118.03 close."""
    bars = [bar(date(2020, 1, 2), "300"), bar(date(2020, 11, 5), "118.03")]
    actions = [
        CorporateAction(ActionKind.SPLIT, date(2020, 8, 31), ratio=D(4)),
        CorporateAction(ActionKind.CASH_DIVIDEND, date(2020, 11, 6), amount=D("0.205")),
    ]
    adjusted = adjust_bars(bars, actions, include_dividends=True)
    expected = Fraction(300, 4) * (1 - Fraction("0.205") / Fraction("118.03"))
    assert abs(Fraction(adjusted[0].close) - expected) < Fraction(1, 10**40)
    with_a_gap = [bars[0]]
    with pytest.raises(ValidationError, match="2020-11-05"):
        adjust_bars(with_a_gap, actions, include_dividends=True)


def test_a_three_for_one_split_of_300_is_exactly_100() -> None:
    bars = [bar(date(2026, 1, 2), "300", volume="7")]
    split = CorporateAction(ActionKind.SPLIT, date(2026, 3, 2), ratio=D(3))
    adjusted = adjust_bars(bars, [split])[0]
    assert adjusted.close == D(100) and adjusted.volume == D(21)


def test_known_at_ignores_an_announced_split_until_its_ex_date() -> None:
    """Announced 02-01, ex 03-02: the market kept quoting 100 until the ex-date."""
    bars = [bar(date(2026, 2, 13), "100")]
    split = CorporateAction(
        ActionKind.SPLIT,
        date(2026, 3, 2),
        ratio=D(2),
        known_from=datetime(2026, 2, 1, tzinfo=UTC),
    )
    at = datetime(2026, 2, 15, tzinfo=UTC)
    assert adjust_bars(bars, [split], known_at=at)[0].close == D(100)
    new_york_still_march_first = datetime(2026, 3, 2, 0, 30, tzinfo=UTC)
    held = adjust_bars(bars, [split], known_at=new_york_still_march_first)
    assert held[0].close == D(100)
    on_the_day = datetime(2026, 3, 2, 15, tzinfo=UTC)
    assert adjust_bars(bars, [split], known_at=on_the_day)[0].close == D(50)


def test_a_corrected_action_replaces_the_old_record_rather_than_compounding() -> None:
    bars = [bar(date(2026, 1, 2), "120")]
    first = CorporateAction(ActionKind.SPLIT, date(2026, 3, 2), ratio=D(2))
    corrected = CorporateAction(
        ActionKind.SPLIT,
        date(2026, 3, 2),
        ratio=D(3),
        known_from=datetime(2026, 2, 20, tzinfo=UTC),
    )
    assert adjust_bars(bars, [first, corrected])[0].close == D(40)
    before_correction = datetime(2026, 2, 10, tzinfo=UTC)
    held = adjust_bars(bars, [first, corrected], known_at=before_correction)
    assert held[0].close == D(120)
    after_ex = datetime(2026, 3, 5, tzinfo=UTC)
    assert adjust_bars(bars, [first, corrected], known_at=after_ex)[0].close == D(40)


def test_a_duplicate_vendor_record_is_refused() -> None:
    bars = [bar(date(2026, 1, 2), "120")]
    split = CorporateAction(ActionKind.SPLIT, date(2026, 3, 2), ratio=D(2))
    with pytest.raises(ValidationError, match="more than one split"):
        adjust_bars(bars, [split, split])
    dividend = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(1))
    with pytest.raises(ValidationError, match="more than one cash_dividend"):
        adjust_bars(
            [bar(date(2026, 2, 27), "100")],
            [dividend, dividend],
            include_dividends=True,
        )


def test_regular_and_special_dividends_on_one_day_both_apply() -> None:
    bars = [bar(date(2026, 2, 27), "100")]
    regular = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(1))
    special = CorporateAction(ActionKind.CASH_DIVIDEND, date(2026, 3, 2), amount=D(4))
    adjusted = adjust_bars(bars, [regular, special], include_dividends=True)[0]
    assert adjusted.close == D("100") * D("0.99") * D("0.96")


def test_a_narrowing_correction_retracts_the_open_ended_range() -> None:
    """FB was recorded open-ended in 2012; the 2022 rename ended it."""
    book = SecurityMaster()
    book.add(
        Listing(
            "A", FB, date(2012, 5, 18), known_from=datetime(2012, 5, 18, tzinfo=UTC)
        )
    )
    renamed = datetime(2022, 6, 9, tzinfo=UTC)
    book.add(Listing("A", FB, date(2012, 5, 18), date(2022, 6, 9), known_from=renamed))
    assert book.resolve(FB, date(2023, 1, 3)) is None
    assert book.symbol_on("A", date(2023, 1, 3)) is None
    assert book.universe("XNAS", date(2023, 1, 3)) == []
    assert book.resolve(FB, date(2020, 1, 2)) == "A"
    believed_then = datetime(2021, 1, 1, tzinfo=UTC)
    assert book.resolve(FB, date(2023, 1, 3), known_at=believed_then) == "A"
    assert book.universe("XNAS", date(2023, 1, 3), known_at=believed_then) == ["A"]


def test_universe_follows_corrections_and_knowledge_time() -> None:
    book = SecurityMaster()
    ticker = Symbol(venue="XNYS", ticker="ABC")
    book.add(Listing("WRONG", ticker, date(2020, 1, 2)))
    corrected_at = datetime(2021, 6, 1, tzinfo=UTC)
    book.add(Listing("RIGHT", ticker, date(2020, 1, 2), known_from=corrected_at))
    on = date(2020, 6, 1)
    assert book.universe("XNYS", on) == ["RIGHT"]
    assert book.universe("XNYS", on, known_at=datetime(2021, 1, 1, tzinfo=UTC)) == [
        "WRONG"
    ]
    assert book.universe("xnys", on) == ["RIGHT"]
    assert book.symbol_on("WRONG", on) is None


def test_open_ended_listings_that_tie_are_refused_not_decided_by_insertion() -> None:
    book = SecurityMaster()
    ticker = Symbol(venue="XNYS", ticker="ABC")
    book.add(Listing("OLD", ticker, date(2000, 1, 3)))
    book.add(Listing("NEW", ticker, date(2015, 1, 5)))
    assert book.resolve(ticker, date(2005, 6, 1)) == "OLD"
    with pytest.raises(ValidationError, match="conflict"):
        book.resolve(ticker, date(2020, 6, 1))
    with pytest.raises(ValidationError, match="conflict"):
        book.universe("XNYS", date(2020, 6, 1))
    other = Symbol(venue="XNYS", ticker="DEF")
    book.add(Listing("OLD", other, date(2000, 1, 3)))
    with pytest.raises(ValidationError, match="conflict"):
        book.symbol_on("OLD", date(2005, 6, 1))
