from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import SecretStr

from ismet.fix.message import (
    REDACTED,
    FieldFormatError,
    FixMessage,
    FixStreamParser,
    GarbledFrame,
    GarbledMessageError,
    checksum,
    decode,
    encode,
    encode_fields,
    format_utc_timestamp,
    format_value,
    parse_decimal,
    parse_fields,
    parse_utc_timestamp,
)

SOH = "\x01"


def wire(text: str) -> bytes:
    return text.replace("|", SOH).encode("ascii")


def frame(body: str, begin: str = "FIX.4.4") -> bytes:
    """Frame by hand, independently of the codec, so the checks are not circular."""
    raw_body = wire(body)
    head = f"8={begin}|9={len(raw_body)}|".replace("|", SOH).encode("ascii") + raw_body
    total = 0
    for byte in head:
        total += byte
    return head + f"10={total % 256:03d}{SOH}".encode("ascii")


# Length and checksum computed outside the codec, by summing the bytes in a shell.
GOLDEN = wire("8=FIX.4.4|9=20|35=0|49=A|56=B|34=1|10=125|")
GOLDEN_FIELDS = [(35, "0"), (49, "A"), (56, "B"), (34, "1")]


class TestGolden:
    def test_the_golden_frame_has_correct_length_and_checksum(self) -> None:
        body = wire("35=0|49=A|56=B|34=1|")
        assert len(body) == 20
        head = wire("8=FIX.4.4|9=20|") + body
        assert sum(head) % 256 == 125
        assert checksum(head) == 125

    def test_encode_matches_the_bytes(self) -> None:
        assert encode("FIX.4.4", GOLDEN_FIELDS) == GOLDEN

    def test_decode_matches_the_fields(self) -> None:
        message = decode(GOLDEN)
        assert message.begin_string == "FIX.4.4"
        assert list(message.fields) == GOLDEN_FIELDS
        assert message.msg_type == "0"

    @pytest.mark.parametrize("begin", ["FIX.4.2", "FIX.4.4", "FIXT.1.1"])
    def test_hand_framed_messages_decode(self, begin: str) -> None:
        raw = frame("35=D|11=ABC|55=IBM|44=101.2500|", begin)
        message = decode(raw)
        assert message.begin_string == begin
        assert message.get(44) == "101.2500"
        assert encode(begin, message.fields) == raw

    def test_checksum_pads_to_three_digits(self) -> None:
        small = [
            raw
            for first in range(1, 60)
            for raw in [encode("FIX.4.4", [(35, "0"), (58, "x" * first)])]
            if sum(raw[: raw.rindex(b"10=")]) % 256 < 100
        ]
        assert small
        for raw in small:
            assert raw[-7:-4] == b"10=" and raw[-4:-3] == b"0"
            assert len(raw[-4:-1]) == 3
            assert decode(raw).msg_type == "0"

    def test_repeating_groups_keep_order_and_duplicates(self) -> None:
        fields = [
            (35, "AB"),
            (555, "2"),
            (600, "AAA"),
            (624, "1"),
            (600, "BBB"),
            (624, "2"),
        ]
        message = decode(encode("FIX.4.4", fields))
        assert list(message.fields) == fields
        assert message.get(600) == "AAA"

    def test_data_field_may_hold_delimiters(self) -> None:
        payload = "a\x01b=c\x01"
        fields = [(35, "B"), (354, str(len(payload))), (355, payload), (58, "after")]
        message = decode(encode("FIX.4.4", fields))
        assert message.get(355) == payload
        assert message.get(58) == "after"


class TestRejectsMalformed:
    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"", id="empty"),
            pytest.param(wire("9=20|8=FIX.4.4|35=0|10=000|"), id="begin-not-first"),
            pytest.param(wire("8=FOO.4.4|9=5|35=0|10=000|"), id="begin-format"),
            pytest.param(wire("8=FIX.4.4|35=0|9=5|10=000|"), id="length-not-second"),
            pytest.param(wire("8=FIX.4.4|9=x|35=0|10=000|"), id="length-not-number"),
            pytest.param(wire("8=FIX.4.4|9=-5|35=0|10=000|"), id="length-negative"),
            pytest.param(wire("8=FIX.4.4|9=5|49=A|35=0|10=000|"), id="type-not-third"),
            pytest.param(GOLDEN[:-2], id="truncated"),
            pytest.param(GOLDEN + b"x", id="trailing"),
            pytest.param(GOLDEN.replace(b"10=125", b"10=126"), id="bad-checksum"),
            pytest.param(GOLDEN.replace(b"10=125", b"10=12x"), id="alpha-checksum"),
            pytest.param(GOLDEN.replace(b"9=20", b"9=21"), id="wrong-length"),
            pytest.param(GOLDEN.replace(b"9=20", b"9=19"), id="short-length"),
            pytest.param(frame("35=0|broken|"), id="field-without-equals"),
            pytest.param(frame("35=B|354=3|355=abcdefg|"), id="data-length-mismatch"),
            pytest.param(frame("35=B|354=x|355=abc|"), id="data-length-not-number"),
        ],
    )
    def test_garbled_frames_raise_a_typed_error(self, raw: bytes) -> None:
        with pytest.raises(GarbledMessageError):
            decode(raw)

    @pytest.mark.parametrize(
        ("body", "tag"),
        [("35=0|55=|", 55), ("35=0|abc=1|", None), ("35=0|055=1|", None)],
    )
    def test_field_defects_raise_a_typed_error(
        self, body: str, tag: int | None
    ) -> None:
        with pytest.raises(FieldFormatError) as info:
            decode(frame(body))
        assert info.value.tag == tag

    @pytest.mark.parametrize(
        "fields",
        [
            [(49, "A"), (35, "0")],
            [(35, "0"), (58, "a\x01b")],
            [(35, "0"), (58, "")],
            [(35, "0"), (0, "x")],
            [(35, "0"), (355, "abc")],
            [(35, "0"), (354, "9"), (355, "abc")],
            [(35, "0"), (58, "ناصر")],
        ],
    )
    def test_encode_refuses_what_could_not_be_parsed(
        self, fields: list[tuple[int, str]]
    ) -> None:
        with pytest.raises(FieldFormatError):
            encode("FIX.4.4", fields)

    def test_encode_refuses_a_bad_begin_string(self) -> None:
        with pytest.raises(FieldFormatError):
            encode("FIX.4.4\x01", [(35, "0")])

    def test_the_error_never_quotes_frame_contents(self) -> None:
        raw = wire("8=FIX.4.4|9=3|35=A|554=hunter2|10=000|")
        with pytest.raises(GarbledMessageError) as info:
            decode(raw)
        assert "hunter2" not in str(info.value)


class TestValues:
    def test_floats_are_refused(self) -> None:
        with pytest.raises(FieldFormatError, match="floats are refused"):
            format_value(1.5, 44)  # type: ignore[arg-type]
        with pytest.raises(FieldFormatError):
            encode_fields([(35, "D"), (44, 101.25)])  # type: ignore[list-item]

    @pytest.mark.parametrize(
        ("value", "text"),
        [
            (Decimal("0.1"), "0.1"),
            (Decimal("1E+3"), "1000"),
            (Decimal("1.230"), "1.230"),
            (Decimal("-0.00001"), "-0.00001"),
            (
                Decimal("12345678901234567890.123456789"),
                "12345678901234567890.123456789",
            ),
        ],
    )
    def test_decimals_format_exactly_without_exponents(
        self, value: Decimal, text: str
    ) -> None:
        assert format_value(value) == text

    @pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
    def test_non_finite_decimals_are_refused(self, value: Decimal) -> None:
        with pytest.raises(FieldFormatError):
            format_value(value)

    def test_decimals_parse_exactly(self) -> None:
        assert str(parse_decimal("0.1000000000000000055511151231")) == (
            "0.1000000000000000055511151231"
        )

    @pytest.mark.parametrize(
        "text", ["", "1e5", "NaN", "Infinity", "1,5", "+1", "٣", " 1", "1 "]
    )
    def test_bad_decimals_raise(self, text: str) -> None:
        with pytest.raises(FieldFormatError):
            parse_decimal(text, 44)

    def test_timestamps_round_trip_at_millisecond_precision(self) -> None:
        moment = datetime(2026, 3, 9, 14, 30, 15, 123000, tzinfo=timezone.utc)
        assert format_utc_timestamp(moment) == "20260309-14:30:15.123"
        assert parse_utc_timestamp("20260309-14:30:15.123") == moment

    def test_timestamps_accept_the_spec_variants(self) -> None:
        assert parse_utc_timestamp("20260309-14:30:15") == datetime(
            2026, 3, 9, 14, 30, 15, tzinfo=timezone.utc
        )
        assert parse_utc_timestamp("20260309-14:30:15.123456789").microsecond == 123456

    def test_leap_second_does_not_raise(self) -> None:
        assert parse_utc_timestamp("20161231-23:59:60") == datetime(
            2017, 1, 1, tzinfo=timezone.utc
        )

    def test_non_utc_time_is_converted(self) -> None:
        moment = datetime(2026, 3, 9, 10, 0, tzinfo=timezone(timedelta(hours=-4)))
        assert format_utc_timestamp(moment) == "20260309-14:00:00.000"

    def test_naive_time_is_refused(self) -> None:
        with pytest.raises(FieldFormatError):
            format_utc_timestamp(datetime(2026, 3, 9))

    @pytest.mark.parametrize(
        "text", ["", "20260309", "20261309-14:30:15", "20260309-25:00:00", "x"]
    )
    def test_bad_timestamps_raise(self, text: str) -> None:
        with pytest.raises(FieldFormatError):
            parse_utc_timestamp(text, 52)


class TestSecrets:
    def test_repr_masks_sensitive_tags_and_secret_values(self) -> None:
        message = FixMessage(
            "FIX.4.4",
            [
                (35, "A"),
                (553, "alice"),
                (554, SecretStr("hunter2")),
                (925, "newpass"),
                (9999, SecretStr("venue-token")),
            ],
        )
        for rendered in (repr(message), str(message), f"{message}"):
            assert "hunter2" not in rendered
            assert "alice" not in rendered
            assert "newpass" not in rendered
            assert "venue-token" not in rendered
            assert REDACTED in rendered

    def test_a_decoded_password_is_also_masked(self) -> None:
        raw = encode("FIX.4.4", [(35, "A"), (554, SecretStr("hunter2"))])
        assert b"554=hunter2" in raw
        assert "hunter2" not in repr(decode(raw))

    def test_secret_values_stay_wrapped_until_encoding(self) -> None:
        message = FixMessage("FIX.4.4", [(35, "A"), (554, SecretStr("hunter2"))])
        assert isinstance(message.fields[1][1], SecretStr)
        assert b"hunter2" in encode("FIX.4.4", message.fields)


class TestStreamParser:
    def test_one_message_in_one_read(self) -> None:
        assert FixStreamParser().feed(GOLDEN) == [decode(GOLDEN)]

    def test_partial_reads(self) -> None:
        parser = FixStreamParser()
        for index in range(len(GOLDEN) - 1):
            assert parser.feed(GOLDEN[index : index + 1]) == []
        assert parser.feed(GOLDEN[-1:]) == [decode(GOLDEN)]
        assert parser.buffered == 0

    def test_concatenated_messages(self) -> None:
        other = encode("FIX.4.4", [(35, "1"), (112, "T")])
        out = FixStreamParser().feed(GOLDEN + other + GOLDEN)
        assert out == [decode(GOLDEN), decode(other), decode(GOLDEN)]

    def test_garbled_checksum_is_skipped_and_the_next_message_parses(self) -> None:
        bad = GOLDEN.replace(b"10=125", b"10=126")
        out = FixStreamParser().feed(bad + GOLDEN)
        assert isinstance(out[0], GarbledFrame)
        assert out[1:] == [decode(GOLDEN)]

    def test_junk_before_a_message_is_skipped(self) -> None:
        out = FixStreamParser().feed(b"\x00garbage" + SOH.encode() + GOLDEN)
        assert isinstance(out[0], GarbledFrame)
        assert out[1:] == [decode(GOLDEN)]

    def test_a_garbled_frame_with_an_embedded_begin_string_still_resyncs(self) -> None:
        bad = wire("8=FIX.4.4|9=x|35=0|8=FIX.4.4|")
        out = FixStreamParser().feed(bad + GOLDEN)
        assert out[-1] == decode(GOLDEN)
        assert all(isinstance(item, GarbledFrame) for item in out[:-1])

    def test_a_hostile_body_length_is_refused_from_the_header_alone(self) -> None:
        parser = FixStreamParser(max_body_length=4096)
        out = parser.feed(wire("8=FIX.4.4|9=99999999999999|35=0|"))
        assert len(out) == 1
        assert isinstance(out[0], GarbledFrame)
        assert parser.buffered <= 1

    def test_a_length_just_over_the_limit_is_refused(self) -> None:
        parser = FixStreamParser(max_body_length=4096)
        out = parser.feed(wire("8=FIX.4.4|9=4097|35=0|"))
        assert isinstance(out[0], GarbledFrame)

    def test_a_large_declared_length_cannot_grow_the_buffer_past_the_limit(
        self,
    ) -> None:
        limit = 4096
        parser = FixStreamParser(max_body_length=limit)
        parser.feed(wire(f"8=FIX.4.4|9={limit}|35=0|"))
        for _ in range(1000):
            parser.feed(b"x" * 64)
            assert parser.buffered <= parser.max_buffered
        assert parser.max_buffered < limit + 128

    def test_an_endless_begin_string_cannot_grow_the_buffer(self) -> None:
        parser = FixStreamParser(max_body_length=4096)
        out = parser.feed(b"8=" + b"F" * 100_000)
        assert isinstance(out[0], GarbledFrame)
        assert parser.buffered <= 1

    def test_a_stream_with_no_frames_cannot_grow_the_buffer(self) -> None:
        parser = FixStreamParser(max_body_length=4096)
        for _ in range(100):
            parser.feed(b"junk without any delimiter " * 100)
        assert parser.buffered <= parser.max_buffered
        assert parser.buffered <= 1

    def test_an_endless_body_length_cannot_grow_the_buffer(self) -> None:
        parser = FixStreamParser(max_body_length=4096)
        out = parser.feed(b"8=FIX.4.4\x019=" + b"1" * 10_000)
        assert isinstance(out[0], GarbledFrame)
        assert parser.buffered <= 1

    def test_resync_marker_split_across_reads_is_not_lost(self) -> None:
        parser = FixStreamParser()
        bad = GOLDEN.replace(b"10=125", b"10=126")
        split = len(bad) + 1
        stream = bad + GOLDEN
        assert isinstance(parser.feed(stream[:split])[0], GarbledFrame)
        assert parser.feed(stream[split:]) == [decode(GOLDEN)]

    def test_a_field_defect_keeps_the_frame(self) -> None:
        out = FixStreamParser().feed(frame("35=0|55=|"))
        assert isinstance(out[0], FixMessage)
        assert out[0].defect is not None
        assert out[0].defect.reason == 4
        assert out[0].defect.tag == 55

    def test_limit_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            FixStreamParser(max_body_length=0)

    def test_parse_fields_without_a_delimiter_is_garbled(self) -> None:
        with pytest.raises(GarbledMessageError):
            parse_fields(b"35=0")


text_values = st.text(
    alphabet=st.characters(
        min_codepoint=1, max_codepoint=255, exclude_characters="\x01"
    ),
    min_size=1,
    max_size=40,
)
# 8, 9, 10, 35 are framing tags, and length tags need a matching data field.
plain_tags = st.integers(min_value=11, max_value=9999).filter(
    lambda t: (
        t not in {35, 89, 91, 96, 213, 349, 351, 353, 355, 357, 359, 361, 363}
        and t not in {93, 90, 95, 212, 348, 350, 352, 354, 356, 358, 360, 362, 364}
        and t
        not in {365, 445, 446, 618, 619, 621, 622, 1401, 1402, 1403, 1404, 1664, 1665}
    )
)
fields_strategy = st.lists(st.tuples(plain_tags, text_values), max_size=25)
begin_strategy = st.sampled_from(["FIX.4.2", "FIX.4.4", "FIXT.1.1"])
msg_type_strategy = st.sampled_from(["0", "A", "D", "8", "AB", "j"])
decimal_strategy = st.decimals(allow_nan=False, allow_infinity=False, places=8)


class TestProperties:
    @given(begin_strategy, msg_type_strategy, fields_strategy)
    def test_round_trip(
        self, begin: str, msg_type: str, fields: list[tuple[int, str]]
    ) -> None:
        all_fields = [(35, msg_type), *fields]
        raw = encode(begin, all_fields)
        message = decode(raw)
        assert message.begin_string == begin
        assert list(message.fields) == all_fields
        assert encode(begin, message.fields) == raw

    @given(msg_type_strategy, fields_strategy)
    def test_length_and_checksum_are_independently_correct(
        self, msg_type: str, fields: list[tuple[int, str]]
    ) -> None:
        raw = encode("FIX.4.4", [(35, msg_type), *fields])
        first = raw.index(b"\x01")
        second = raw.index(b"\x01", first + 1)
        declared = int(raw[first + 3 : second])
        body_start = second + 1
        assert raw[body_start + declared : body_start + declared + 3] == b"10="
        assert raw[body_start + declared - 1] == 1
        assert int(raw[-4:-1]) == sum(raw[: body_start + declared]) % 256

    @given(decimal_strategy)
    def test_decimals_round_trip_exactly(self, value: Decimal) -> None:
        raw = encode("FIX.4.4", [(35, "D"), (44, value)])
        parsed = parse_decimal(decode(raw).require(44), 44)
        assert parsed == value

    @given(st.binary(max_size=64))
    def test_data_fields_round_trip_arbitrary_bytes(self, payload: bytes) -> None:
        if not payload:
            return
        text = payload.decode("latin-1")
        fields = [(35, "B"), (354, str(len(payload))), (355, text)]
        assert decode(encode("FIX.4.4", fields)).get(355) == text

    @given(
        st.lists(st.tuples(msg_type_strategy, fields_strategy), min_size=1, max_size=5),
        st.lists(st.integers(min_value=1, max_value=97), max_size=30),
    )
    @settings(max_examples=200)
    def test_chunked_stream_at_arbitrary_split_points(
        self,
        messages: list[tuple[str, list[tuple[int, str]]]],
        sizes: list[int],
    ) -> None:
        frames = [encode("FIX.4.4", [(35, t), *f]) for t, f in messages]
        stream = b"".join(frames)
        parser = FixStreamParser()
        out: list[FixMessage | GarbledFrame] = []
        position = 0
        for size in sizes:
            out.extend(parser.feed(stream[position : position + size]))
            position += size
        out.extend(parser.feed(stream[position:]))
        assert out == [decode(f) for f in frames]
        assert parser.buffered == 0

    @given(
        st.lists(st.tuples(msg_type_strategy, fields_strategy), min_size=2, max_size=4),
        st.integers(min_value=0),
        st.lists(st.integers(min_value=1, max_value=50), max_size=20),
    )
    @settings(max_examples=200)
    def test_a_garbled_frame_loses_only_itself_at_any_split(
        self,
        messages: list[tuple[str, list[tuple[int, str]]]],
        victim_seed: int,
        sizes: list[int],
    ) -> None:
        frames = [encode("FIX.4.4", [(35, t), *f]) for t, f in messages]
        victim = victim_seed % len(frames)
        wrong = (int(frames[victim][-4:-1]) + 1) % 256
        damaged = list(frames)
        damaged[victim] = frames[victim][:-4] + f"{wrong:03d}\x01".encode()
        stream = b"".join(damaged)
        parser = FixStreamParser()
        out: list[FixMessage | GarbledFrame] = []
        position = 0
        for size in sizes:
            out.extend(parser.feed(stream[position : position + size]))
            position += size
        out.extend(parser.feed(stream[position:]))
        good = [item for item in out if isinstance(item, FixMessage)]
        assert good == [decode(f) for i, f in enumerate(frames) if i != victim]
        assert any(isinstance(item, GarbledFrame) for item in out)

    @given(st.binary(max_size=2048), st.lists(st.integers(1, 200), max_size=20))
    @settings(max_examples=200)
    def test_arbitrary_bytes_never_raise_and_stay_bounded(
        self, junk: bytes, sizes: list[int]
    ) -> None:
        parser = FixStreamParser(max_body_length=512)
        position = 0
        for size in sizes:
            parser.feed(junk[position : position + size])
            position += size
            assert parser.buffered <= parser.max_buffered
        parser.feed(junk[position:])
        assert parser.buffered <= parser.max_buffered

    @given(st.binary(max_size=512))
    def test_junk_before_good_frames_stays_bounded(self, junk: bytes) -> None:
        parser = FixStreamParser(max_body_length=4096)
        out = parser.feed(junk + b"\x01" + GOLDEN + GOLDEN)
        messages = [m for m in out if isinstance(m, FixMessage)]
        # Junk can itself begin a frame that swallows a copy, but only up to its
        # declared length, and a later reconnect resets the parser.
        assert parser.buffered <= parser.max_buffered
        assert all(m == decode(GOLDEN) for m in messages if m.msg_type == "0")
