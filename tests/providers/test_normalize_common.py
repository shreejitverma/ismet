from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from ismet.errors import TransportError, ValidationError
from ismet.models import (
    Adjustment,
    Bar,
    FeedKind,
    Interval,
    QualityFlag,
    Symbol,
    Timeliness,
    Trade,
)
from ismet.normalize._common import (
    StreamGate,
    check_range,
    datetime_from_ms,
    merge_bars,
    normalizer,
    parse_iso8601_utc,
    register_asset,
    search_rank,
    venue_source,
)
from ismet.refdata import is_crypto_asset

UTC = timezone.utc
T0 = datetime(2026, 11, 1, tzinfo=UTC)
SYM = Symbol(venue="BNCX", ticker="BTCUSDT")


def _bar(opened: datetime, close: str = "1") -> Bar:
    return Bar(
        symbol=SYM,
        exchange_ts=opened,
        received_ts=T0,
        interval=Interval.M1,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1",
        adjustment=Adjustment.RAW,
    )


def _trade(number: int) -> Trade:
    return Trade(
        symbol=SYM,
        exchange_ts=T0,
        received_ts=T0,
        sequence=number,
        price=Decimal(1),
        size=Decimal(1),
    )


@pytest.mark.parametrize(
    ("text", "micros", "ns"),
    [
        ("2026-11-01T01:54:04Z", 0, None),
        ("2026-11-01T01:54:04.5Z", 500_000, None),
        ("2026-11-01T01:54:04.564559Z", 564_559, None),
        ("2026-11-01T01:54:04.596664939Z", 596_664, 596_664_939),
    ],
)
def test_iso8601_keeps_all_digits(text: str, micros: int, ns: int | None) -> None:
    parsed, exact = parse_iso8601_utc(text)
    assert parsed == datetime(2026, 11, 1, 1, 54, 4, micros, tzinfo=UTC)
    if ns is None:
        assert exact is None
    else:
        assert exact is not None
        assert exact % 1_000_000_000 == ns
        assert exact // 1_000_000_000 == int(parsed.timestamp())


@pytest.mark.parametrize(
    "text", ["2026-11-01T01:54:04", "2026-11-01T01:54:04+00:00", "yesterday", ""]
)
def test_iso8601_rejects_anything_but_utc_z(text: str) -> None:
    with pytest.raises(ValueError, match="UTC ISO 8601"):
        parse_iso8601_utc(text)


def test_datetime_from_ms_is_exact() -> None:
    assert datetime_from_ms(1793498640001) == datetime(
        2026, 11, 1, 2, 4, 0, 1000, tzinfo=UTC
    )


def test_venue_source_declares_realtime_venue_api() -> None:
    source = venue_source("binance", "rest:depth")
    assert source.provider == "binance"
    assert source.feed == "rest:depth"
    assert source.kind is FeedKind.VENUE_API
    assert source.timeliness is Timeliness.REALTIME
    assert source.delay_seconds is None


def test_register_asset_accepts_real_codes_and_returns_none_for_others() -> None:
    assert register_asset("zzzq") == "ZZZQ"
    assert is_crypto_asset("ZZZQ")
    assert register_asset("BRL") == "BRL"
    assert not is_crypto_asset("BRL")
    assert register_asset("T") == "T"
    assert register_asset("币安人生") == "币安人生"
    for unrepresentable in ("XTS", "A-B", "two words"):
        assert register_asset(unrepresentable) is None


def test_merge_bars_filters_sorts_and_dedupes_keeping_the_later_page() -> None:
    minute = timedelta(minutes=1)
    stale = _bar(T0 + minute, close="1")
    fresh = _bar(T0 + minute, close="2")
    merged = merge_bars(
        [
            _bar(T0 + 2 * minute),
            stale,
            fresh,
            _bar(T0 - minute),
            _bar(T0),
            _bar(T0 + 3 * minute),
        ],
        T0,
        T0 + 3 * minute,
    )
    assert [b.exchange_ts for b in merged] == [T0, T0 + minute, T0 + 2 * minute]
    assert merged[1].close == Decimal(2)


def test_check_range() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        check_range(datetime(2026, 1, 1), T0)
    with pytest.raises(ValidationError, match="before end"):
        check_range(T0, T0)
    check_range(T0, T0 + timedelta(seconds=1))


def test_search_rank_order() -> None:
    assert search_rank("BTCUSDT", "BTCUSDT", "BTC", "USDT") == 0
    assert search_rank("BTC", "BTCUSDT", "BTC", "USDT") == 1
    assert search_rank("USDT", "BTCUSDT", "BTC", "USDT") == 2
    assert search_rank("TCU", "BTCUSDT", "BTC", "USDT") == 3
    assert search_rank("DOGE", "BTCUSDT", "BTC", "USDT") is None


def test_normalizer_wraps_shape_errors_as_non_retryable() -> None:
    @normalizer("vendor")
    def read(payload: dict[str, str]) -> str:
        return payload["missing"]

    with pytest.raises(TransportError, match="vendor: unreadable payload") as caught:
        read({})
    assert caught.value.retryable is False


def test_normalizer_passes_ismet_errors_through() -> None:
    @normalizer("vendor")
    def read() -> None:
        raise ValidationError("kept")

    with pytest.raises(ValidationError, match="kept"):
        read()


def test_contiguous_gate_flags_a_jump_and_logs_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gate = StreamGate(contiguous=True)
    assert gate.admit("K", 10, _trade(10)).quality == frozenset()  # type: ignore[union-attr]
    assert gate.admit("K", 11, _trade(11)).quality == frozenset()  # type: ignore[union-attr]
    flagged = gate.admit("K", 15, _trade(15))
    assert flagged is not None
    assert flagged.quality == {QualityFlag.AFTER_GAP}
    assert flagged.sequence == 15
    assert "3 events missing" in caplog.text
    assert gate.admit("K", 16, _trade(16)).quality == frozenset()  # type: ignore[union-attr]


def test_gate_drops_repeats_and_flags_late_numbers() -> None:
    for contiguous in (True, False):
        gate = StreamGate(contiguous=contiguous)
        gate.admit("K", 10, _trade(10))
        assert gate.admit("K", 10, _trade(10)) is None
        late = gate.admit("K", 9, _trade(9))
        assert late is not None
        assert late.quality == {QualityFlag.OUT_OF_ORDER}


def test_sparse_gate_ignores_jumps_but_flags_the_first_event_after_reconnect() -> None:
    gate = StreamGate(contiguous=False)
    gate.admit("K", 10, _trade(10))
    assert gate.admit("K", 500, _trade(500)).quality == frozenset()  # type: ignore[union-attr]
    gate.mark_reconnected()
    after = gate.admit("K", 900, _trade(900))
    assert after is not None
    assert after.quality == {QualityFlag.AFTER_GAP}
    assert gate.admit("K", 901, _trade(901)).quality == frozenset()  # type: ignore[union-attr]


def test_reconnect_flag_applies_only_to_keys_already_seen() -> None:
    gate = StreamGate(contiguous=False)
    gate.admit("SEEN", 1, _trade(1))
    gate.mark_reconnected()
    assert gate.admit("NEW", 1, _trade(1)).quality == frozenset()  # type: ignore[union-attr]
    assert gate.admit("SEEN", 2, _trade(2)).quality == {QualityFlag.AFTER_GAP}  # type: ignore[union-attr]


def test_contiguous_gate_trusts_numbers_across_a_reconnect() -> None:
    gate = StreamGate(contiguous=True)
    gate.admit("K", 1, _trade(1))
    gate.mark_reconnected()
    assert gate.admit("K", 2, _trade(2)).quality == frozenset()  # type: ignore[union-attr]


def test_seed_sets_position_without_an_event() -> None:
    gate = StreamGate(contiguous=True)
    assert gate.is_fresh("K")
    gate.seed("K", 10)
    assert not gate.is_fresh("K")
    assert gate.admit("K", 10, _trade(10)) is None
    assert gate.admit("K", 11, _trade(11)).quality == frozenset()  # type: ignore[union-attr]
