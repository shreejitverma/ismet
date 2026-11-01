"""FIX tag=value codec and incremental stream parser.

Spec references (FIXT 1.1 "FIX Transport" document, identical on these points to the
FIX 4.2 and 4.4 session chapters; fiximate.fixtrading.org for field definitions):

- "Data Integrity" and "CheckSum Calculation": BodyLength counts the bytes after the
  BodyLength field's SOH up to and including the SOH before ``10=``. CheckSum is the
  byte sum from the ``8`` of ``8=`` through that same SOH, modulo 256, as three digits.
- "What constitutes a garbled message": BeginString not first or malformed, BodyLength
  not second or wrong, MsgType not third, CheckSum not last or wrong. A garbled message
  is ignored and does not consume a sequence number, so it is reported by the stream
  parser as :class:`GarbledFrame` rather than raised.
- Session test case 14 (a, d): an unknown tag or a tag without a value is not garbled.
  It is answered with a session Reject and does consume a sequence number. The parser
  keeps the frame and records the problem in :attr:`FixMessage.defect`.

Field values are decoded as latin-1, which maps every byte to one code point, so any
frame round-trips byte for byte whatever encoding its Encoded* fields use. Repeating
groups need no schema here: a message is an ordered tuple of fields, and order and
duplicates are preserved. Decimals are formatted and parsed as text and never pass
through ``float``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from pydantic import SecretStr

from ismet.errors import IsmetError

SOH = b"\x01"
_SOH_INT = 1
REDACTED = "**********"

DEFAULT_MAX_BODY_LENGTH = 1 << 20
"""Largest BodyLength accepted, and so the bound on what a stream parser may buffer."""

_MAX_BEGIN_STRING_FIELD = 32
_TRAILER_LENGTH = 7  # "10=xxx" and its SOH
_BEGIN_STRING = re.compile(rb"^FIX(T)?\.\d+\.\d+$")
_DECIMAL = re.compile(r"^-?(\d+(\.\d*)?|\.\d+)$", re.ASCII)
_SEQ_NUM = re.compile(r"^\d{1,18}$", re.ASCII)
_TIMESTAMP = re.compile(
    r"^(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,12}))?$", re.ASCII
)

DATA_FIELDS: Mapping[int, int] = {
    93: 89,  # SignatureLength, Signature
    90: 91,  # SecureDataLen, SecureData
    95: 96,  # RawDataLength, RawData
    212: 213,  # XmlDataLen, XmlData
    348: 349,  # EncodedIssuerLen, EncodedIssuer
    350: 351,  # EncodedSecurityDescLen, EncodedSecurityDesc
    352: 353,  # EncodedListExecInstLen, EncodedListExecInst
    354: 355,  # EncodedTextLen, EncodedText
    356: 357,  # EncodedSubjectLen, EncodedSubject
    358: 359,  # EncodedHeadlineLen, EncodedHeadline
    360: 361,  # EncodedAllocTextLen, EncodedAllocText
    362: 363,  # EncodedUnderlyingIssuerLen, EncodedUnderlyingIssuer
    364: 365,  # EncodedUnderlyingSecurityDescLen, EncodedUnderlyingSecurityDesc
    445: 446,  # EncodedListStatusTextLen, EncodedListStatusText
    618: 619,  # EncodedLegIssuerLen, EncodedLegIssuer
    621: 622,  # EncodedLegSecurityDescLen, EncodedLegSecurityDesc
    1401: 1402,  # EncryptedPasswordLen, EncryptedPassword
    1403: 1404,  # EncryptedNewPasswordLen, EncryptedNewPassword
    1664: 1665,  # EncodedRejectTextLen, EncodedRejectText
}
"""Length tag to data tag. A data value may hold SOH, so its length delimits it.

Taken from the FIX 4.2, 4.4, FIXT 1.1, and 5.0 SP2 repositories, limited to the fields
order entry can meet. Pass ``data_fields`` to the parser for the rest.
"""

SENSITIVE_TAGS = frozenset({89, 91, 96, 553, 554, 925, 1402, 1404})
"""Credentials and opaque auth data, always masked in reprs and diagnostics."""

Field = tuple[int, "str | SecretStr"]


class FixCodecError(IsmetError):
    """A FIX message or field could not be encoded or decoded."""


class GarbledMessageError(FixCodecError):
    """A frame failed the data integrity checks. It carries no frame content."""


class FieldFormatError(FixCodecError):
    """A field value does not have the format its type requires."""

    def __init__(self, message: str, *, tag: int | None = None) -> None:
        self.tag = tag
        super().__init__(message)


@dataclass(frozen=True)
class FieldDefect:
    """A problem in a well-framed message, answered with a session Reject."""

    reason: int
    text: str
    tag: int | None = None


REASON_INVALID_TAG = 0
REASON_TAG_WITHOUT_VALUE = 4


def _text(value: str | SecretStr) -> str:
    return value.get_secret_value() if isinstance(value, SecretStr) else value


class FixMessage:
    """An immutable message: BeginString plus ordered fields starting at MsgType.

    BodyLength and CheckSum are derived, so they are not stored. ``repr`` and ``str``
    mask :data:`SENSITIVE_TAGS` and any :class:`~pydantic.SecretStr` value.
    """

    __slots__ = ("_fields", "_index", "begin_string", "defect")

    def __init__(
        self,
        begin_string: str,
        fields: Iterable[tuple[int, str | SecretStr]],
        *,
        defect: FieldDefect | None = None,
    ) -> None:
        self.begin_string = begin_string
        self._fields: tuple[Field, ...] = tuple(fields)
        self.defect = defect
        index: dict[int, str] = {}
        for tag, value in self._fields:
            index.setdefault(tag, _text(value))
        self._index = index

    @property
    def fields(self) -> tuple[Field, ...]:
        return self._fields

    @property
    def msg_type(self) -> str:
        return self._index.get(35, "")

    def get(self, tag: int, default: str | None = None) -> str | None:
        """The first occurrence of ``tag``, or ``default``."""
        return self._index.get(tag, default)

    def require(self, tag: int) -> str:
        value = self._index.get(tag)
        if value is None:
            raise FieldFormatError(f"required tag {tag} is missing", tag=tag)
        return value

    def __contains__(self, tag: int) -> bool:
        return tag in self._index

    def __iter__(self) -> Iterator[Field]:
        return iter(self._fields)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FixMessage):
            return NotImplemented
        return (
            self.begin_string == other.begin_string
            and [(t, _text(v)) for t, v in self._fields]
            == [(t, _text(v)) for t, v in other._fields]
            and self.defect == other.defect
        )

    def __hash__(self) -> int:
        return hash((self.begin_string, self.msg_type, len(self._fields)))

    def __repr__(self) -> str:
        shown = " ".join(
            f"{tag}={_shown(tag, value)}" for tag, value in self._fields[:64]
        )
        more = "" if len(self._fields) <= 64 else f" (+{len(self._fields) - 64} fields)"
        return f"FixMessage({self.begin_string} {shown}{more})"

    __str__ = __repr__


def _shown(tag: int, value: str | SecretStr) -> str:
    if tag in SENSITIVE_TAGS or isinstance(value, SecretStr):
        return REDACTED
    return value


def format_value(value: str | int | bool | Decimal | SecretStr, tag: int = 0) -> str:
    """Render one value as FIX text. ``float`` is refused, never rounded."""
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "Y" if value else "N"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise FieldFormatError(f"tag {tag}: decimal must be finite", tag=tag)
        return format(value, "f")
    raise FieldFormatError(
        f"tag {tag}: cannot encode {type(value).__name__}; floats are refused so a "
        "price or quantity cannot lose precision",
        tag=tag,
    )


def parse_decimal(text: str, tag: int = 0) -> Decimal:
    """Parse a FIX ``float`` field exactly. Exponents are not FIX and are refused."""
    if not _DECIMAL.match(text):
        raise FieldFormatError(f"tag {tag}: not a FIX decimal", tag=tag)
    try:
        return Decimal(text)
    except InvalidOperation as exc:  # pragma: no cover - the pattern admits only digits
        raise FieldFormatError(f"tag {tag}: not a FIX decimal", tag=tag) from exc


def parse_seq_num(text: str, tag: int = 0) -> int:
    """A sequence number small enough for any store, whatever a peer declares."""
    if not _SEQ_NUM.match(text):
        raise FieldFormatError(f"tag {tag}: not a sequence number", tag=tag)
    return int(text)


def format_utc_timestamp(moment: datetime) -> str:
    """``YYYYMMDD-HH:MM:SS.sss`` in UTC. A naive datetime is refused."""
    if moment.tzinfo is None:
        raise FieldFormatError("a FIX timestamp needs an aware datetime")
    utc = moment.astimezone(timezone.utc)
    return utc.strftime("%Y%m%d-%H:%M:%S.") + f"{utc.microsecond // 1000:03d}"


def parse_utc_timestamp(text: str, tag: int = 0) -> datetime:
    """Parse a UTCTimestamp. Digits past microseconds are truncated, not rounded."""
    match = _TIMESTAMP.match(text)
    if match is None:
        raise FieldFormatError(f"tag {tag}: not a UTC timestamp", tag=tag)
    year, month, day, hour, minute, second, fraction = match.groups()
    micros = int((fraction or "").ljust(6, "0")[:6])
    # The UTCTimestamp type admits 60 for a leap second, which datetime cannot hold.
    leap = int(second) == 60
    try:
        parsed = datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            59 if leap else int(second),
            micros,
            tzinfo=timezone.utc,
        )
    except ValueError as exc:
        raise FieldFormatError(f"tag {tag}: not a UTC timestamp", tag=tag) from exc
    try:
        return parsed + timedelta(seconds=1) if leap else parsed
    except OverflowError as exc:
        raise FieldFormatError(f"tag {tag}: not a UTC timestamp", tag=tag) from exc


def checksum(frame: bytes | bytearray | memoryview) -> int:
    """The FIX CheckSum of ``frame``: its byte sum modulo 256."""
    return sum(frame) % 256


def encode_fields(
    fields: Iterable[tuple[int, str | int | bool | Decimal | SecretStr]],
    *,
    data_fields: Mapping[int, int] = DATA_FIELDS,
) -> bytes:
    """Encode fields as ``tag=value<SOH>`` with no header or trailer.

    Refuses anything that would make the frame unparseable: an invalid tag, an empty
    value, or a SOH anywhere but a data field whose length field says so.
    """
    data_tags = set(data_fields.values())
    out = bytearray()
    previous_tag: int | None = None
    previous_text = ""
    for tag, value in fields:
        if tag <= 0:
            raise FieldFormatError(f"invalid tag {tag}", tag=tag)
        text = format_value(value, tag)
        if not text:
            raise FieldFormatError(f"tag {tag} has no value", tag=tag)
        raw = text.encode("latin-1", "strict") if text.isascii() else _latin1(text, tag)
        if tag in data_tags:
            declared = (
                data_fields.get(previous_tag) if previous_tag is not None else None
            )
            if declared != tag or previous_text != str(len(raw)):
                raise FieldFormatError(
                    f"data tag {tag} must follow its length field with the exact "
                    "byte count",
                    tag=tag,
                )
        elif SOH in raw:
            raise FieldFormatError(f"tag {tag} holds a field delimiter", tag=tag)
        out += f"{tag}=".encode("ascii") + raw + SOH
        previous_tag, previous_text = tag, text
    return bytes(out)


def _latin1(text: str, tag: int) -> bytes:
    try:
        return text.encode("latin-1", "strict")
    except UnicodeEncodeError as exc:
        raise FieldFormatError(
            f"tag {tag} holds characters that are not single bytes; use an Encoded* "
            "field for other encodings",
            tag=tag,
        ) from exc


def encode_frame(begin_string: str, body: bytes) -> bytes:
    """Wrap an encoded body, which must start at MsgType, in header and trailer."""
    if not body.startswith(b"35="):
        raise FieldFormatError("a message body must start with MsgType(35)", tag=35)
    if not _BEGIN_STRING.match(begin_string.encode("ascii", "replace")):
        raise FieldFormatError("invalid BeginString", tag=8)
    head = f"8={begin_string}\x019={len(body)}\x01".encode("ascii")
    frame = head + body
    return frame + f"10={checksum(frame):03d}\x01".encode("ascii")


def encode(
    begin_string: str,
    fields: Iterable[tuple[int, str | int | bool | Decimal | SecretStr]],
    *,
    data_fields: Mapping[int, int] = DATA_FIELDS,
) -> bytes:
    """Encode a whole message. ``fields`` starts at MsgType(35)."""
    return encode_frame(begin_string, encode_fields(fields, data_fields=data_fields))


def parse_fields(
    data: bytes | bytearray | memoryview,
    *,
    data_fields: Mapping[int, int] = DATA_FIELDS,
) -> tuple[list[Field], FieldDefect | None]:
    """Split ``tag=value<SOH>`` bytes into fields and the first field defect found.

    Raises :class:`GarbledMessageError` if the bytes cannot be split at all.
    """
    raw = bytes(data)
    fields: list[Field] = []
    defect: FieldDefect | None = None
    position = 0
    end = len(raw)
    pending_length: int | None = None
    pending_tag: int | None = None
    while position < end:
        equals = raw.find(b"=", position)
        if equals < 0:
            raise GarbledMessageError("field without '='")
        tag_text = raw[position:equals]
        tag = _parse_tag(tag_text)
        value_start = equals + 1
        if pending_length is not None and tag == pending_tag:
            value_end = value_start + pending_length
            if value_end >= end or raw[value_end] != _SOH_INT:
                raise GarbledMessageError("data field does not match its length")
        else:
            value_end = raw.find(SOH, value_start)
            if value_end < 0:
                raise GarbledMessageError("field without a delimiter")
        pending_length, pending_tag = None, None
        value = raw[value_start:value_end].decode("latin-1")
        if tag is None:
            if defect is None:
                defect = FieldDefect(REASON_INVALID_TAG, "invalid tag number")
        else:
            if not value and defect is None:
                defect = FieldDefect(
                    REASON_TAG_WITHOUT_VALUE, f"tag {tag} has no value", tag
                )
            fields.append((tag, value))
            if tag in data_fields:
                if not (value.isascii() and value.isdigit()):
                    raise GarbledMessageError("data length field is not a number")
                pending_length = int(value[:12])
                pending_tag = data_fields[tag]
        position = value_end + 1
    return fields, defect


def _parse_tag(text: bytes) -> int | None:
    if not text.isdigit() or text[:1] == b"0" or len(text) > 9:
        return None
    return int(text)


@dataclass(frozen=True)
class GarbledFrame:
    """Bytes the parser skipped. ``reason`` never quotes the bytes themselves."""

    reason: str


class _NeedMore:
    pass


_NEED_MORE = _NeedMore()


def _scan_frame(
    buf: bytearray, start: int, max_body_length: int
) -> _NeedMore | GarbledMessageError | tuple[bytes, int, int]:
    """Validate the frame at ``start``.

    Returns the BeginString, the body start, and the frame's end. Reads no further
    into a declared body than the buffer holds, and declares a frame garbled as soon as
    it can, so a hostile length cannot make the caller wait on more than the limit.
    """
    available = len(buf) - start
    prefix = bytes(buf[start : start + 2])
    if not b"8=".startswith(prefix):
        return GarbledMessageError("BeginString is not the first field")
    if available < 2:
        return _NEED_MORE
    begin_end = buf.find(SOH, start + 2, start + _MAX_BEGIN_STRING_FIELD)
    if begin_end < 0:
        if available >= _MAX_BEGIN_STRING_FIELD:
            return GarbledMessageError("BeginString field is too long")
        return _NEED_MORE
    begin_string = bytes(buf[start + 2 : begin_end])
    if not _BEGIN_STRING.match(begin_string):
        return GarbledMessageError("BeginString has the wrong format")
    length_start = begin_end + 1
    prefix = bytes(buf[length_start : length_start + 2])
    if not b"9=".startswith(prefix):
        return GarbledMessageError("BodyLength is not the second field")
    if len(prefix) < 2:
        return _NEED_MORE
    digits_limit = len(str(max_body_length)) + 1
    digits_start = length_start + 2
    length_end = buf.find(SOH, digits_start, digits_start + digits_limit)
    if length_end < 0:
        if len(buf) - digits_start >= digits_limit:
            return GarbledMessageError("BodyLength is too long")
        if not bytes(buf[digits_start:]).isdigit() and len(buf) > digits_start:
            return GarbledMessageError("BodyLength is not a number")
        return _NEED_MORE
    digits = bytes(buf[digits_start:length_end])
    if not digits.isdigit():
        return GarbledMessageError("BodyLength is not a number")
    body_length = int(digits)
    if body_length > max_body_length:
        return GarbledMessageError("BodyLength exceeds the limit")
    body_start = length_end + 1
    prefix = bytes(buf[body_start : body_start + 3])
    if not b"35=".startswith(prefix):
        return GarbledMessageError("MsgType is not the third field")
    body_end = body_start + body_length
    frame_end = body_end + _TRAILER_LENGTH
    if len(buf) < frame_end:
        return _NEED_MORE
    if buf[body_end - 1] != _SOH_INT:
        return GarbledMessageError("BodyLength is wrong")
    if buf[body_end : body_end + 3] != b"10=" or buf[frame_end - 1] != _SOH_INT:
        return GarbledMessageError("BodyLength is wrong or CheckSum is not last")
    declared = bytes(buf[body_end + 3 : body_end + 6])
    if not declared.isdigit():
        return GarbledMessageError("CheckSum is not three digits")
    if int(declared) != checksum(memoryview(buf)[start:body_end]):
        return GarbledMessageError("CheckSum is wrong")
    return begin_string, body_start, frame_end


class FixStreamParser:
    """Turns chunks of bytes into messages, across partial reads and concatenation.

    After :meth:`feed` returns, at most one incomplete frame is buffered, and a frame
    longer than ``max_body_length`` is refused from its header alone, so a peer cannot
    cause unbounded allocation. A bad frame yields one :class:`GarbledFrame`, and the
    parser resynchronizes at the next ``<SOH>8=``. A BodyLength that is wrong but within
    the limit stalls the parser until enough bytes arrive to disprove it, which is why
    the session also runs a heartbeat timer.
    """

    def __init__(
        self,
        *,
        max_body_length: int = DEFAULT_MAX_BODY_LENGTH,
        data_fields: Mapping[int, int] = DATA_FIELDS,
    ) -> None:
        if max_body_length <= 0:
            raise ValueError("max_body_length must be positive")
        self._max = max_body_length
        self._data_fields = data_fields
        self._buffer = bytearray()
        self._synced = True

    @property
    def buffered(self) -> int:
        """Bytes held for an incomplete frame."""
        return len(self._buffer)

    @property
    def max_buffered(self) -> int:
        """The most :attr:`buffered` can be after :meth:`feed` returns."""
        return (
            _MAX_BEGIN_STRING_FIELD
            + 3
            + len(str(self._max))
            + 2
            + self._max
            + _TRAILER_LENGTH
        )

    def feed(self, data: bytes) -> list[FixMessage | GarbledFrame]:
        """Consume ``data`` and return what it completes, in order."""
        buf = self._buffer
        buf += data
        out: list[FixMessage | GarbledFrame] = []
        position = 0
        length = len(buf)
        while position < length:
            if not self._synced:
                marker = buf.find(b"\x018=", position)
                if marker < 0:
                    # Keep only what could still become the start of a marker.
                    if buf[length - 1] == _SOH_INT:
                        position = length - 1
                    elif (
                        length - position >= 2 and buf[length - 2 : length] == b"\x018"
                    ):
                        position = length - 2
                    else:
                        position = length
                    break
                position = marker + 1
                self._synced = True
            scanned = _scan_frame(buf, position, self._max)
            if isinstance(scanned, _NeedMore):
                break
            if isinstance(scanned, GarbledMessageError):
                out.append(GarbledFrame(str(scanned)))
                self._synced = False
                continue
            begin_string, body_start, frame_end = scanned
            try:
                fields, defect = parse_fields(
                    memoryview(buf)[body_start : frame_end - _TRAILER_LENGTH],
                    data_fields=self._data_fields,
                )
            except GarbledMessageError as exc:
                out.append(GarbledFrame(str(exc)))
                self._synced = False
                continue
            out.append(FixMessage(begin_string.decode("ascii"), fields, defect=defect))
            position = frame_end
        if position:
            del buf[:position]
        return out


def decode(
    frame: bytes,
    *,
    max_body_length: int = DEFAULT_MAX_BODY_LENGTH,
    data_fields: Mapping[int, int] = DATA_FIELDS,
) -> FixMessage:
    """Decode exactly one frame strictly.

    Raises :class:`GarbledMessageError` for a framing failure, including trailing
    bytes and a truncated frame, and :class:`FieldFormatError` for a field defect.
    """
    buf = bytearray(frame)
    scanned = _scan_frame(buf, 0, max_body_length)
    if isinstance(scanned, GarbledMessageError):
        raise scanned
    if isinstance(scanned, _NeedMore):
        raise GarbledMessageError("frame is truncated")
    begin_string, body_start, frame_end = scanned
    if frame_end != len(buf):
        raise GarbledMessageError("bytes after CheckSum")
    fields, defect = parse_fields(
        memoryview(buf)[body_start : frame_end - _TRAILER_LENGTH],
        data_fields=data_fields,
    )
    if defect is not None:
        raise FieldFormatError(defect.text, tag=defect.tag)
    return FixMessage(begin_string.decode("ascii"), fields)


__all__ = [
    "DATA_FIELDS",
    "DEFAULT_MAX_BODY_LENGTH",
    "REDACTED",
    "SENSITIVE_TAGS",
    "SOH",
    "Field",
    "FieldDefect",
    "FieldFormatError",
    "FixCodecError",
    "FixMessage",
    "FixStreamParser",
    "GarbledFrame",
    "GarbledMessageError",
    "checksum",
    "decode",
    "encode",
    "encode_fields",
    "encode_frame",
    "format_utc_timestamp",
    "format_value",
    "parse_decimal",
    "parse_fields",
    "parse_seq_num",
    "parse_utc_timestamp",
]
