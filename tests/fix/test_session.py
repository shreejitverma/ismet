from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import SecretStr

from ismet.errors import AmbiguousOutcome, ConfigError, TransportError
from ismet.fix.message import format_utc_timestamp
from ismet.fix.session import (
    APPL_VER_FIX50SP2,
    ApplicationRejectError,
    FixSession,
    FixSessionConfig,
    LogonRejectedError,
    SequenceTooLowError,
    SessionDownError,
    SessionFailedError,
    SessionState,
    _DisconnectError,
)
from ismet.fix.store import (
    FixStoreError,
    MemoryFixStore,
    SqliteFixStore,
    StoredMessage,
)

from .acceptor import FixAcceptor
from .conftest import RecordingListener, SessionFactory, eventually, session_config

NEWS = [(58, "x")]


def now_text() -> str:
    return format_utc_timestamp(datetime.now(timezone.utc))


def past_text(seconds: float = 5) -> str:
    return format_utc_timestamp(datetime.now(timezone.utc) - timedelta(seconds=seconds))


def future_text(seconds: float = 5) -> str:
    return format_utc_timestamp(datetime.now(timezone.utc) + timedelta(seconds=seconds))


class TestConfig:
    def test_unsupported_version(self) -> None:
        with pytest.raises(ConfigError, match="unsupported"):
            FixSessionConfig("h", 1, "FIX.4.0", "A", "B")

    def test_fixt_needs_an_application_version(self) -> None:
        with pytest.raises(ConfigError, match="default_appl_ver_id"):
            FixSessionConfig("h", 1, "FIXT.1.1", "A", "B")

    def test_application_version_is_only_for_fixt(self) -> None:
        with pytest.raises(ConfigError, match="only to FIXT"):
            FixSessionConfig("h", 1, "FIX.4.4", "A", "B", default_appl_ver_id="9")

    def test_heartbeat_must_be_positive(self) -> None:
        with pytest.raises(ConfigError):
            FixSessionConfig("h", 1, "FIX.4.4", "A", "B", heartbeat_interval=0)

    @pytest.mark.parametrize("name", ["username", "password"])
    def test_credentials_must_be_secret(self, name: str) -> None:
        with pytest.raises(ConfigError, match="SecretStr"):
            FixSessionConfig("h", 1, "FIX.4.4", "A", "B", **{name: "plain"})

    def test_a_password_in_logon_fields_must_be_secret(self) -> None:
        with pytest.raises(ConfigError, match="SecretStr"):
            FixSessionConfig("h", 1, "FIX.4.4", "A", "B", logon_fields=((554, "pw"),))


class TestLogon:
    async def test_logon_per_version(self, versioned_acceptor: FixAcceptor) -> None:
        acceptor = versioned_acceptor
        listener = RecordingListener()
        session = FixSession(session_config(acceptor), MemoryFixStore(), listener)
        await session.start()
        await session.wait_ready(5.0)
        assert session.state is SessionState.READY
        assert listener.ready == 1
        logon = acceptor.logons[0]
        assert logon.begin_string == acceptor.begin_string
        assert logon.get(98) == "0"
        assert logon.get(108) == "30"
        assert logon.require(34) == "1"
        if acceptor.begin_string == "FIXT.1.1":
            assert logon.get(1137) == "9"
        else:
            assert logon.get(1137) is None
        await session.stop()
        assert session.state is SessionState.STOPPED

    async def test_ready_waits_for_the_test_request_barrier(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.suppress_test_response = True
        session, _ = await make_session(ready=False, logon_timeout=0.3)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("1")))
        assert session.state is SessionState.SYNCING
        with pytest.raises(SessionDownError):
            await session.send_application("D", [(11, "A")])
        assert acceptor.of_type("D") == []

    async def test_logon_answered_with_logout_stops_for_good(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.refuse_logon = "bad password"
        session, listener = await make_session(ready=False)
        with pytest.raises(LogonRejectedError, match="bad password"):
            await session.wait_ready(5.0)
        assert session.state is SessionState.FAILED
        assert acceptor.connections == 1
        assert isinstance(listener.failures[0], LogonRejectedError)

    async def test_first_message_not_a_logon_disconnects(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.first_message_type = "1"
        await make_session(ready=False)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("5")[0].get(58) == "first message not a logon"

    async def test_no_answer_to_logon_times_out_and_reconnects(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.ignore_logon = True
        await make_session(ready=False, logon_timeout=0.2)
        await acceptor.wait_until(lambda: acceptor.connections >= 2)

    async def test_wait_ready_times_out(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.ignore_logon = True
        session, _ = await make_session(ready=False, logon_timeout=5.0)
        with pytest.raises(SessionDownError):
            await session.wait_ready(0.05)

    async def test_logon_too_low_terminates(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        store = MemoryFixStore()
        store.set_next_inbound(100)
        session, listener = await make_session(store=store, ready=False)
        with pytest.raises(SequenceTooLowError):
            await session.wait_ready(5.0)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert "MsgSeqNum too low, expecting 100 but received 1" in (
            acceptor.of_type("5")[0].require(58)
        )
        assert acceptor.connections == 1
        assert isinstance(listener.failures[0], SequenceTooLowError)

    async def test_logon_too_high_is_accepted_then_gap_filled(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.next_out = 11
        acceptor.sent[5] = ("8", [(58, "resent")], past_text())
        session, listener = await make_session()
        assert session.is_ready
        resend = acceptor.of_type("2")[0]
        assert resend.get(7) == "1"
        assert resend.get(16) == "0"
        assert listener.seqs() == [5]
        assert listener.messages[0].get(43) == "Y"

    async def test_reset_on_logon(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        store = MemoryFixStore()
        store.allocate_outbound()
        store.set_next_inbound(50)
        acceptor.next_out = 50
        acceptor.next_in = 2
        session, _ = await make_session(store=store, reset_on_logon=True)
        logon = acceptor.logons[0]
        assert logon.get(141) == "Y"
        assert logon.get(34) == "1"
        assert session.is_ready

    async def test_peer_reset_at_logon_is_obeyed(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        before = len(acceptor.logons)
        await acceptor.reset_sequences_at_logon()
        await acceptor.wait_until(lambda: len(acceptor.logons) > before)
        reply = acceptor.logons[-1]
        assert reply.get(141) == "Y"
        assert reply.get(34) == "1"
        await eventually(lambda: session.next_inbound == 2)

    async def test_peer_reset_with_a_number_other_than_one_is_refused(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        await acceptor.write(acceptor.frame(5, "A", [(98, "0"), (141, "Y")]))
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(373) == "5"

    async def test_unexpected_extra_logon_is_ignored(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send("A", [(98, "0"), (108, "30")])
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        assert session.is_ready


class TestHeartbeats:
    async def test_quiet_link_sends_heartbeats(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session(heartbeat_interval=1)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("0")), timeout=3.0)

    async def test_test_request_gets_its_id_back(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        await acceptor.send("1", [(112, "PING42")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("0")))
        assert acceptor.of_type("0")[0].get(112) == "PING42"

    async def test_test_request_without_an_id_is_rejected(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        await acceptor.send("1", [(58, "no id")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(373) == "1"
        assert acceptor.of_type("3")[0].get(371) == "112"

    async def test_silence_brings_a_test_request_then_a_disconnect(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session(heartbeat_interval=1)
        acceptor.faults.silent = True
        await acceptor.wait_until(
            lambda: any(
                (m.get(112) or "").startswith("TEST") for m in acceptor.received
            ),
            timeout=3.0,
        )
        await eventually(lambda: "missed heartbeats" in listener.downs, timeout=5.0)
        logouts = [m for m in acceptor.received if m.msg_type == "5"]
        assert logouts[0].get(58) == "no response to TestRequest"
        assert acceptor.connections >= 1
        acceptor.faults.silent = False
        await session.wait_ready(5.0)

    async def test_any_message_counts_as_a_sign_of_life(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session(heartbeat_interval=1)
        for _ in range(8):
            await acceptor.send("8", NEWS)
            await asyncio.sleep(0.25)
        assert session.is_ready
        assert acceptor.connections == 1
        assert not any((m.get(112) or "").startswith("TEST") for m in acceptor.received)


class TestInboundSequencing:
    async def test_in_order_messages_are_delivered(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session()
        first = await acceptor.send("8", NEWS)
        second = await acceptor.send("8", NEWS)
        await eventually(lambda: listener.seqs() == [first, second])

    async def test_gap_requests_resend_and_delivers_in_order(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session()
        first = await acceptor.send("8", [(58, "one")])
        acceptor.faults.skip_seq_next = 2
        acceptor.sent[first + 1] = ("8", [(58, "two")], past_text())
        acceptor.sent[first + 2] = ("0", [], past_text())
        fourth = await acceptor.send("8", [(58, "four")])
        await eventually(lambda: listener.seqs() == [first, first + 1, fourth])
        resend = acceptor.of_type("2")[-1]
        assert resend.get(7) == str(first + 1)
        assert resend.get(16) == "0"
        assert [m.get(58) for m in listener.messages] == ["one", "two", "four"]
        # Original copies above the gap were ignored rather than delivered twice.
        assert len(listener.messages) == 3

    async def test_one_gap_makes_one_resend_request(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        acceptor.faults.skip_seq_next = 1
        await acceptor.send("8", NEWS)
        await acceptor.send("8", NEWS)
        await acceptor.send("8", NEWS)
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        assert len(acceptor.of_type("2")) == 1
        assert len(listener.messages) == 3

    async def test_an_unanswered_gap_asks_again_then_drops_the_connection(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session(resend_timeout=0.05, max_resend_requests=2)
        acceptor.faults.silent = True
        expected = acceptor.next_out
        await acceptor.send_raw_seq(expected + 5, "8", NEWS)
        await eventually(
            lambda: "message gap was not filled" in listener.downs, timeout=3.0
        )
        requests = [m for m in acceptor.received if m.msg_type == "2"]
        assert len(requests) == 2

    async def test_garbled_checksum_is_ignored_and_recovered_by_resend(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        acceptor.faults.garble_next = 1
        garbled = await acceptor.send("8", [(58, "was garbled")])
        await asyncio.sleep(0.02)
        assert session.next_inbound == garbled
        assert listener.messages == []
        assert acceptor.of_type("3") == []
        after = await acceptor.send("8", [(58, "after")])
        await eventually(lambda: listener.seqs() == [garbled, after])
        assert listener.messages[0].get(58) == "was garbled"
        assert listener.messages[0].get(43) == "Y"
        assert acceptor.connections == 1

    async def test_too_low_without_possdup_terminates(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        await acceptor.send("8", NEWS)
        await acceptor.send("8", NEWS)
        await eventually(lambda: len(listener.messages) == 2)
        expected = session.next_inbound
        await acceptor.send_raw_seq(2, "8", NEWS)
        await eventually(lambda: session.state is SessionState.FAILED)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("5")[0].get(58) == (
            f"MsgSeqNum too low, expecting {expected} but received 2"
        )
        with pytest.raises(SessionFailedError):
            await session.send_application("D", [(11, "X")])
        await asyncio.sleep(0.05)
        assert acceptor.connections == 1
        assert isinstance(session.failure, SequenceTooLowError)

    async def test_possdup_already_seen_is_dropped_not_reprocessed(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        seq = await acceptor.send("8", [(58, "original")])
        await eventually(lambda: len(listener.messages) == 1)
        await acceptor.send_raw_seq(
            seq, "8", [(58, "dup")], poss_dup=True, orig_sending_time=past_text()
        )
        await acceptor.send_raw_seq(session.next_inbound, "0", [])
        await eventually(lambda: session.next_inbound == seq + 2)
        assert [m.get(58) for m in listener.messages] == ["original"]
        assert session.is_ready

    async def test_possdup_in_sequence_is_processed(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        acceptor.next_out += 1
        await acceptor.send_raw_seq(
            session.next_inbound,
            "8",
            [(58, "first copy to arrive")],
            poss_dup=True,
            orig_sending_time=past_text(),
        )
        await eventually(lambda: len(listener.messages) == 1)
        assert session.is_ready

    async def test_possdup_without_orig_sending_time_is_rejected_and_counted(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        seq = session.next_inbound
        acceptor.next_out += 1
        await acceptor.send_raw_seq(seq, "8", NEWS, poss_dup=True)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        reject = acceptor.of_type("3")[0]
        assert reject.get(373) == "1"
        assert reject.get(371) == "122"
        await eventually(lambda: session.next_inbound == seq + 1)
        assert listener.messages == []
        assert listener.discarded[0][1] == "OrigSendingTime missing"

    async def test_orig_sending_time_after_sending_time_logs_out(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        acceptor.next_out += 1
        await acceptor.send_raw_seq(
            session.next_inbound,
            "8",
            NEWS,
            poss_dup=True,
            orig_sending_time=future_text(),
        )
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("3")[0].get(373) == "10"
        assert listener.messages == []
        assert listener.discarded[0][1] == "OrigSendingTime after SendingTime"
        assert "SendingTime accuracy problem" in acceptor.of_type("5")[0].require(58)

    async def test_possresend_is_forwarded_to_the_application(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session()
        await acceptor.send("8", [(97, "Y"), (58, "maybe seen")])
        await eventually(lambda: len(listener.messages) == 1)
        assert listener.messages[0].get(97) == "Y"

    async def test_inbound_number_advances_only_after_the_handler_returns(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        expected = session.next_inbound
        listener.crash_with = RuntimeError("database down")
        await acceptor.send("8", [(58, "lost to a crash")])
        await eventually(lambda: bool(listener.downs))
        assert session.next_inbound == expected
        await session.wait_ready(5.0)
        await eventually(lambda: len(listener.messages) == 1)
        assert listener.messages[0].get(58) == "lost to a crash"

    async def test_application_reject_sends_a_session_reject_and_advances(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        expected = session.next_inbound
        listener.reject_with = ApplicationRejectError(1, "missing tag", 17)
        await acceptor.send("8", NEWS)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        reject = acceptor.of_type("3")[0]
        assert reject.get(45) == str(expected)
        assert reject.get(373) == "1"
        assert reject.get(371) == "17"
        await eventually(lambda: session.next_inbound == expected + 1)

    async def test_field_defect_is_delivered_then_rejected(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        expected = session.next_inbound
        body = (
            f"35=8\x0149=VENUE\x0156=CLIENT\x0134={expected}\x0152={now_text()}"
            "\x0155=\x01"
        ).encode()
        head = f"8=FIX.4.4\x019={len(body)}\x01".encode() + body
        acceptor.next_out += 1
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        reject = acceptor.of_type("3")[0]
        assert reject.get(373) == "4"
        assert reject.get(371) == "55"
        await eventually(lambda: session.next_inbound == expected + 1)
        assert len(listener.messages) == 1, "it might carry a fill, so it must be read"

    async def test_peer_reject_reaches_the_listener_and_counts(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        await acceptor.send("3", [(45, "1"), (373, "5"), (58, "nope")])
        await eventually(lambda: len(listener.rejects) == 1)
        assert session.next_inbound == acceptor.next_out
        assert session.is_ready


class TestHostileNumbers:
    @pytest.mark.parametrize(
        ("msg_type", "fields", "tag"),
        [
            ("4", [(123, "Y"), (36, "9" * 19)], "36"),
            ("4", [(36, "9" * 19)], "36"),
            ("4", [(123, "Y"), (36, "9" * 5000)], "36"),
            ("2", [(7, "9" * 19), (16, "0")], "7"),
        ],
    )
    async def test_oversized_sequence_numbers_are_rejected(
        self,
        acceptor: FixAcceptor,
        make_session: SessionFactory,
        msg_type: str,
        fields: list[tuple[int, str]],
        tag: str,
    ) -> None:
        session, listener = await make_session()
        await acceptor.send_raw_seq(session.next_inbound, msg_type, fields)
        acceptor.next_out += 1
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == tag
        assert session.is_ready
        assert listener.downs == []

    async def test_a_sending_time_past_the_calendar_is_only_a_bad_time(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        body = (
            f"35=8\x0149=VENUE\x0156=CLIENT\x0134={session.next_inbound}"
            "\x0152=99991231-23:59:60\x01"
        ).encode()
        head = f"8=FIX.4.4\x019={len(body)}\x01".encode() + body
        acceptor.next_out += 1
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(373) == "10"
        assert listener.discarded

    async def test_a_sequence_reset_is_reported_to_the_listener(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session(reset_on_logon=True)
        assert listener.resets == 1


class TestHeaderChecks:
    async def test_wrong_begin_string_logs_out(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        acceptor.faults.wrong_begin_string_next = 1
        await acceptor.send("0", [])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert "Incorrect BeginString" in acceptor.of_type("5")[0].require(58)

    async def test_wrong_comp_id_rejects_and_logs_out(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        acceptor.faults.wrong_comp_id_next = 1
        await acceptor.send("0", [])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("3")[0].get(373) == "9"
        assert "CompID" in acceptor.of_type("5")[0].require(58)

    async def test_wrong_comp_id_on_a_report_is_recorded_as_discarded(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        _, listener = await make_session()
        acceptor.faults.wrong_comp_id_next = 1
        await acceptor.send("8", [(11, "C1"), (150, "F"), (32, "10"), (31, "5")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert [(m.get(11), why) for m, why in listener.discarded] == [
            ("C1", "CompID problem")
        ]

    async def test_sending_time_out_of_range_rejects_and_logs_out(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        acceptor.faults.bad_sending_time_next = 1
        await acceptor.send("0", [])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("3")[0].get(373) == "10"

    async def test_skew_check_can_be_disabled(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session(max_sending_time_skew=None)
        acceptor.faults.bad_sending_time_next = 1
        await acceptor.send("0", [])
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        assert session.is_ready

    async def test_missing_sending_time_rejects(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        body = f"35=0\x0149=VENUE\x0156=CLIENT\x0134={seq}\x01".encode()
        head = f"8=FIX.4.4\x019={len(body)}\x01".encode() + body
        acceptor.next_out += 1
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == "52"
        await eventually(lambda: session.next_inbound == seq + 1)

    @pytest.mark.parametrize("seq_field", ["", "34=abc\x01"])
    async def test_missing_or_bad_msg_seq_num_logs_out(
        self,
        acceptor: FixAcceptor,
        make_session: SessionFactory,
        seq_field: str,
    ) -> None:
        await make_session()
        body = (
            f"35=0\x0149=VENUE\x0156=CLIENT\x01{seq_field}52={now_text()}\x01"
        ).encode()
        head = f"8=FIX.4.4\x019={len(body)}\x01".encode() + body
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert "MsgSeqNum" in acceptor.of_type("5")[0].require(58)


class TestSequenceReset:
    async def test_gap_fill_moves_the_expected_number(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(seq, "4", [(123, "Y"), (36, str(seq + 5))])
        await eventually(lambda: session.next_inbound == seq + 5)

    async def test_gap_fill_above_the_expected_number_requests_a_resend(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        acceptor.faults.silent = False
        await acceptor.send_raw_seq(seq + 3, "4", [(123, "Y"), (36, str(seq + 9))])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("2")))
        assert acceptor.of_type("2")[0].get(7) == str(seq)
        assert session.next_inbound == seq

    async def test_gap_fill_below_the_expected_number_with_possdup_is_ignored(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(
            1,
            "4",
            [(123, "Y"), (36, str(seq + 9))],
            poss_dup=True,
            orig_sending_time=past_text(),
        )
        await asyncio.sleep(0.02)
        assert session.next_inbound == seq
        assert session.is_ready

    async def test_gap_fill_below_the_expected_number_without_possdup_terminates(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(1, "4", [(123, "Y"), (36, str(seq + 9))])
        await eventually(lambda: session.state is SessionState.FAILED)

    async def test_gap_fill_cannot_lower_the_number(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(seq, "4", [(123, "Y"), (36, str(seq))])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        reject = acceptor.of_type("3")[0]
        assert reject.get(373) == "5"
        assert "attempt to lower sequence number" in reject.require(58)
        assert session.next_inbound == seq

    async def test_gap_fill_without_new_seq_no_is_rejected(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send_raw_seq(session.next_inbound, "4", [(123, "Y")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == "36"

    async def test_reset_ignores_the_message_number(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send_raw_seq(9999, "4", [(36, "500")])
        await eventually(lambda: session.next_inbound == 500)
        assert session.state is SessionState.READY

    async def test_reset_to_the_same_number_changes_nothing(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(1, "4", [(36, str(seq))])
        await asyncio.sleep(0.02)
        assert session.next_inbound == seq
        assert acceptor.of_type("3") == []

    async def test_reset_cannot_lower_the_number(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        seq = session.next_inbound
        await acceptor.send_raw_seq(seq, "4", [(36, "1")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(373) == "5"
        assert session.next_inbound == seq

    async def test_reset_without_new_seq_no_is_rejected(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        await make_session()
        await acceptor.send_raw_seq(1, "4", [(58, "no number")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == "36"


class TestServingResendRequests:
    async def _send_two(self, session: FixSession) -> tuple[int, int]:
        first = await session.send_application("D", [(11, "ONE"), (55, "IBM")])
        second = await session.send_application("D", [(11, "TWO"), (55, "IBM")])
        return first, second

    async def test_application_messages_are_resent_with_possdup(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first, _ = await self._send_two(session)
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 2)
        original = acceptor.of_type("D")[0]
        await acceptor.send("2", [(7, str(first)), (16, str(first))])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 3)
        resent = acceptor.of_type("D")[2]
        assert resent.get(34) == str(first)
        assert resent.get(43) == "Y"
        assert resent.get(122) == original.get(52)
        assert resent.get(11) == "ONE"
        assert resent.get(55) == "IBM"

    async def test_admin_messages_are_gap_filled(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first, second = await self._send_two(session)
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 2)
        acceptor.next_in = 1
        await acceptor.send("2", [(7, "1"), (16, "0")])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 4)
        gap_fills = acceptor.of_type("4")
        assert len(gap_fills) == 1
        assert gap_fills[0].get(34) == "1"
        assert gap_fills[0].get(123) == "Y"
        assert gap_fills[0].get(43) == "Y"
        assert gap_fills[0].get(122) is not None
        assert gap_fills[0].get(36) == str(first)
        assert [m.get(34) for m in acceptor.of_type("D")[2:]] == [
            str(first),
            str(second),
        ]

    async def test_trailing_admin_messages_are_gap_filled_to_the_end(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first, _ = await self._send_two(session)
        await session._send_admin("0", [])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("0")))
        heartbeat_seq = acceptor.of_type("0")[0].require(34)
        await acceptor.send("2", [(7, str(first)), (16, "0")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("4")))
        gap = acceptor.of_type("4")[0]
        assert gap.get(34) == heartbeat_seq
        assert gap.get(36) == str(int(heartbeat_seq) + 1)

    async def test_end_above_the_last_message_is_clamped(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first = await session.send_application("D", [(11, "ONE")])
        await acceptor.send("2", [(7, str(first)), (16, "999999")])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 2)
        assert acceptor.of_type("D")[1].get(43) == "Y"

    async def test_nothing_to_resend_is_harmless(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send("2", [(7, "5000"), (16, "0")])
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        assert session.is_ready

    async def test_bad_resend_request_is_rejected(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send("2", [(7, "abc"), (16, "0")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("3")))
        assert acceptor.of_type("3")[0].get(371) == "7"
        await eventually(lambda: session.next_inbound == acceptor.next_out)

    async def test_resend_request_above_the_expected_number_is_served_first(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first = await session.send_application("D", [(11, "ONE")])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 1)
        expected = acceptor.next_out
        await eventually(lambda: session.next_inbound == expected)
        await acceptor.send_raw_seq(
            expected + 2, "2", [(7, str(first)), (16, str(first))]
        )
        await acceptor.wait_until(lambda: bool(acceptor.of_type("2")))
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 2)
        received_types = [m.msg_type for m in acceptor.received]
        assert received_types.index("D", received_types.index("D") + 1) < (
            received_types.index("2")
        )
        assert acceptor.of_type("2")[0].get(7) == str(expected)

    async def test_resent_resend_request_counts_for_sequence_only(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        first = await session.send_application("D", [(11, "ONE")])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 1)
        seq = session.next_inbound
        acceptor.next_out += 1
        await acceptor.send_raw_seq(
            seq,
            "2",
            [(7, str(first)), (16, str(first))],
            poss_dup=True,
            orig_sending_time=past_text(),
        )
        await eventually(lambda: session.next_inbound == seq + 1)
        await asyncio.sleep(0.02)
        assert len(acceptor.of_type("D")) == 1

    async def test_stale_messages_can_be_gap_filled_instead(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session(stale_resend_after=0.0)
        first = await session.send_application("D", [(11, "ONE")])
        await asyncio.sleep(0.01)
        await acceptor.send("2", [(7, str(first)), (16, "0")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("4")))
        assert acceptor.of_type("4")[0].get(34) == str(first)
        assert len(acceptor.of_type("D")) == 1

    async def test_new_messages_cannot_interleave_with_a_resend(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        for index in range(3):
            await session.send_application("D", [(11, f"C{index}")])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 3)
        real_write = session._write
        paused = asyncio.Event()
        resume = asyncio.Event()

        async def pausing_write(frame: bytes) -> None:
            await real_write(frame)
            if b"43=Y" in frame and not paused.is_set():
                paused.set()
                await resume.wait()

        session._write = pausing_write  # type: ignore[method-assign]
        await acceptor.send("2", [(7, "1"), (16, "0")])
        await asyncio.wait_for(paused.wait(), 5.0)
        sending = asyncio.create_task(session.send_application("D", [(11, "NEW")]))
        await asyncio.sleep(0.05)
        assert not sending.done(), "a new message was sent during the resend"
        assert all(m.get(11) != "NEW" for m in acceptor.received)
        resume.set()
        await sending
        await acceptor.wait_until(
            lambda: any(m.get(11) == "NEW" for m in acceptor.received)
        )
        order = [m.get(11) for m in acceptor.of_type("D")]
        assert order == ["C0", "C1", "C2", "C0", "C1", "C2", "NEW"]


class TestOutboundDurability:
    async def test_message_is_stored_before_the_socket_sees_it(
        self, acceptor: FixAcceptor, make_session: SessionFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        session, _ = await make_session(store=store)
        real_write = session._write
        observed: list[int] = []

        async def checking_write(frame: bytes) -> None:
            if b"35=D" in frame:
                last = store.next_outbound() - 1
                stored = store.messages(last, last)
                observed.append(len(stored))
                assert stored[0].msg_type == "D"
            await real_write(frame)

        session._write = checking_write  # type: ignore[method-assign]
        await session.send_application("D", [(11, "A")])
        assert observed == [1]
        session._write = real_write  # type: ignore[method-assign]
        await session.stop()
        store.close()

    async def test_session_down_stores_nothing(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        store = MemoryFixStore()
        session, _ = await make_session(store=store)
        next_seq = store.next_outbound()
        await acceptor.stop()
        await eventually(lambda: not session.is_ready)
        with pytest.raises(SessionDownError) as info:
            await session.send_application("D", [(11, "A")])
        assert isinstance(info.value, TransportError)
        assert not isinstance(info.value, AmbiguousOutcome)
        assert store.next_outbound() in (next_seq, next_seq + 1)
        assert store.messages(next_seq, next_seq + 5) == []

    async def test_write_failure_after_storing_is_ambiguous(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        store = MemoryFixStore()
        session, _ = await make_session(store=store)
        assert session._writer is not None
        session._writer.close()
        with pytest.raises(AmbiguousOutcome):
            await session.send_application("D", [(11, "A")])
        assert [m.msg_type for m in store.messages(1, 50)] == ["D"]

    async def test_store_failure_sends_nothing(
        self, acceptor: FixAcceptor, make_session: SessionFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        session, _ = await make_session(store=store)
        store._conn.execute("DROP TABLE fix_outbound")
        with pytest.raises(FixStoreError):
            await session.send_application("D", [(11, "A")])
        await asyncio.sleep(0.01)
        assert acceptor.of_type("D") == []
        await session.stop()
        store.close()

    async def test_resend_after_a_crash_between_store_and_write(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        """A process dies after storing and before writing, then restarts."""
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        session = FixSession(session_config(acceptor), store, RecordingListener())
        await session.start()
        await session.wait_ready(5.0)

        async def die(frame: bytes) -> None:
            raise _DisconnectError("process died here")

        session._write = die  # type: ignore[method-assign]
        with pytest.raises(AmbiguousOutcome):
            await session.send_application("D", [(11, "CRASHED"), (55, "IBM")])
        await session.stop()
        store.close()
        assert acceptor.of_type("D") == []

        reopened = SqliteFixStore(path, "s")
        again = FixSession(session_config(acceptor), reopened, RecordingListener())
        await again.start()
        await again.wait_ready(5.0)
        await acceptor.wait_until(lambda: "CRASHED" in acceptor.orders)
        resent = acceptor.of_type("D")[0]
        assert resent.get(11) == "CRASHED"
        assert resent.get(43) == "Y"
        # This acceptor asks once per message above its gap, so it may see two copies.
        # What matters is that the order exists once.
        assert list(acceptor.orders) == ["CRASHED"]
        assert acceptor.orders["CRASHED"].arrivals == 1
        await again.stop()
        reopened.close()

    async def test_barrier_is_retried_when_the_peer_drops_it_above_a_gap(
        self, acceptor: FixAcceptor
    ) -> None:
        store = MemoryFixStore()
        store.allocate_outbound()
        store.allocate_outbound()
        session = FixSession(session_config(acceptor), store, RecordingListener())
        await session.start()
        await asyncio.wait_for(session.wait_ready(5.0), 6.0)
        assert len(acceptor.of_type("1")) >= 2
        await session.stop()

    async def test_sequence_numbers_survive_a_restart(
        self, acceptor: FixAcceptor, tmp_path: Path
    ) -> None:
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        listener = RecordingListener()
        session = FixSession(session_config(acceptor), store, listener)
        await session.start()
        await session.wait_ready(5.0)
        await session.send_application("D", [(11, "ONE")])
        await acceptor.send("8", NEWS)
        await eventually(lambda: len(listener.messages) == 2)
        await session.stop()
        store.close()

        out_before = acceptor.next_in
        in_before = acceptor.next_out
        reopened = SqliteFixStore(path, "s")
        assert reopened.next_outbound() == out_before
        assert reopened.next_inbound() == in_before
        logons = len(acceptor.logons)
        again = FixSession(session_config(acceptor), reopened, RecordingListener())
        await again.start()
        await again.wait_ready(5.0)
        assert int(acceptor.logons[logons].require(34)) == out_before
        assert acceptor.resend_requests_answered == 0
        await again.stop()
        reopened.close()


class TestConnectionLoss:
    async def test_reconnects_after_a_drop(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        await acceptor.drop_connection()
        await eventually(lambda: bool(listener.downs))
        await session.wait_ready(5.0)
        assert acceptor.connections == 2
        assert listener.ready == 2

    async def test_message_lost_with_the_connection_is_recovered(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        acceptor.faults.truncate_next = 1
        lost = await acceptor.send("8", [(58, "cut in half")])
        await eventually(lambda: len(listener.messages) == 1)
        assert listener.messages[0].get(58) == "cut in half"
        assert listener.messages[0].require(34) == str(lost)
        assert listener.messages[0].get(43) == "Y"
        assert acceptor.connections == 2
        await session.wait_ready(5.0)

    async def test_backoff_retries_while_the_venue_is_away(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.stop()
        await eventually(lambda: not session.is_ready)
        assert session.state in (
            SessionState.CONNECTING,
            SessionState.DISCONNECTED,
        )
        await asyncio.sleep(0.1)
        await acceptor.restart()
        await session.wait_ready(5.0)

    async def test_gives_up_after_the_attempt_limit(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        port = acceptor.port
        await acceptor.stop()
        session, listener = await make_session(
            ready=False, port=port, max_reconnect_attempts=2
        )
        await eventually(lambda: session.state is SessionState.FAILED)
        with pytest.raises(SessionFailedError, match="gave up"):
            await session.wait_ready(0.1)
        assert isinstance(listener.failures[0], SessionFailedError)

    async def test_wait_ready_right_after_a_drop_waits_for_the_new_connection(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        await acceptor.drop_connection()
        for _ in range(100):
            await session.wait_ready(5.0)
            if listener.downs:
                break
            await asyncio.sleep(0.001)
        await session.wait_ready(5.0)
        assert session.is_ready
        assert acceptor.connections == 2

    async def test_wait_ready_on_a_stopped_session_raises(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await session.stop()
        with pytest.raises(SessionDownError, match="stopped"):
            await session.wait_ready(0.1)


class TestLogout:
    async def test_we_initiate_and_wait_for_the_confirmation(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        await session.stop()
        assert acceptor.of_type("5")[0].get(58) == "session stopping"
        assert acceptor.logout_text == "session stopping"
        # The confirming Logout was read, and its number consumed, before stop returned.
        assert session.next_inbound == acceptor.next_out
        assert listener.downs[-1] == "session stopped"
        assert session.state is SessionState.STOPPED

    async def test_stop_does_not_hang_without_a_confirmation(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session(logout_timeout=0.1)
        acceptor.faults.silent = True
        await asyncio.wait_for(session.stop(), 2.0)
        assert acceptor.of_type("5")[0].get(58) == "session stopping"
        assert listener.downs[-1] == "session stopped"
        assert session.state is SessionState.STOPPED

    async def test_peer_initiates_and_we_confirm(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await acceptor.send("5", [(58, "end of day")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert acceptor.of_type("5")[0].get(58) == "Logout confirmed"
        await acceptor.wait_until(lambda: acceptor.connections == 2)
        await session.wait_ready(5.0)

    async def test_peer_logout_with_a_gap_requests_a_resend_then_confirms(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        acceptor.faults.silent = True
        await acceptor.send_raw_seq(session.next_inbound + 5, "5", [(58, "end of day")])
        await acceptor.wait_until(
            lambda: any(m.msg_type == "5" for m in acceptor.received)
        )
        types = [m.msg_type for m in acceptor.received]
        assert types.index("2") < types.index("5")

    async def test_stop_while_reconnecting_is_prompt(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        port = acceptor.port
        await acceptor.stop()
        session, _ = await make_session(ready=False, port=port)
        await asyncio.wait_for(session.stop(), 1.0)

    async def test_stop_is_idempotent(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await session.stop()
        await session.stop()

    async def test_restart_after_stop(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, _ = await make_session()
        await session.stop()
        await session.start()
        await session.wait_ready(5.0)
        assert acceptor.connections == 2


class TestSecrets:
    async def test_password_goes_on_the_wire_and_nowhere_else(
        self,
        acceptor: FixAcceptor,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG)
        password = "hunter2-do-not-leak"
        token = "venue-token-do-not-leak"
        path = tmp_path / "fix.db"
        store = SqliteFixStore(path, "s")
        config = session_config(
            acceptor,
            username=SecretStr("alice"),
            password=SecretStr(password),
            logon_fields=((9999, SecretStr(token)),),
        )
        session = FixSession(config, store, RecordingListener())
        await session.start()
        await session.wait_ready(5.0)
        await session.send_application("D", [(11, "A")])
        logon = acceptor.logons[0]
        assert logon.get(553) == "alice"
        assert logon.get(554) == password
        assert logon.get(9999) == token

        for rendered in (repr(config), str(config), repr(session)):
            assert password not in rendered
            assert token not in rendered
            assert "alice" not in rendered
        assert password not in repr(logon)
        assert "alice" not in repr(logon)
        assert password not in caplog.text
        assert token not in caplog.text

        await session.stop()
        store.close()
        for file in tmp_path.iterdir():
            contents = file.read_bytes()
            assert password.encode() not in contents, file.name
            assert token.encode() not in contents, file.name

    async def test_a_peer_echoing_the_password_is_scrubbed(
        self,
        acceptor: FixAcceptor,
        make_session: SessionFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG)
        password = "hunter2-do-not-leak"
        acceptor.faults.refuse_logon = f"bad password {password}"
        session, listener = await make_session(
            ready=False, password=SecretStr(password)
        )
        with pytest.raises(LogonRejectedError) as info:
            await session.wait_ready(5.0)
        assert password not in str(info.value)
        assert password not in repr(info.value)
        assert password not in str(listener.failures[0])
        assert password not in caplog.text

    async def test_a_peer_logout_text_is_scrubbed_in_logs(
        self,
        acceptor: FixAcceptor,
        make_session: SessionFactory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.DEBUG)
        password = "hunter2-do-not-leak"
        await make_session(password=SecretStr(password))
        await acceptor.send("5", [(58, f"goodbye {password}")])
        await acceptor.wait_until(lambda: bool(acceptor.of_type("5")))
        assert password not in caplog.text


class TestParametrizedAcrossVersions:
    async def test_gap_and_resend_per_version(
        self, versioned_acceptor: FixAcceptor
    ) -> None:
        acceptor = versioned_acceptor
        listener = RecordingListener()
        session = FixSession(session_config(acceptor), MemoryFixStore(), listener)
        await session.start()
        await session.wait_ready(5.0)
        acceptor.faults.garble_next = 1
        first = await acceptor.send("8", [(58, "garbled")])
        second = await acceptor.send("8", [(58, "good")])
        await eventually(lambda: listener.seqs() == [first, second])
        sent = await session.send_application("D", [(11, "X"), (55, "IBM")])
        await acceptor.send("2", [(7, str(sent)), (16, str(sent))])
        await acceptor.wait_until(lambda: len(acceptor.of_type("D")) == 2)
        assert acceptor.of_type("D")[1].begin_string == acceptor.begin_string
        await session.stop()


class TestResetOnLogonHappensOncePerPeriod:
    async def test_a_reconnect_in_the_same_period_keeps_the_numbers(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session(
            reset_on_logon=True, session_period=lambda _: "day-1"
        )
        await acceptor.drop_connection()
        # Sent while we are away, as a fill would be. Only its number can recover it.
        lost = acceptor.next_out
        acceptor.next_out += 1
        acceptor.sent[lost] = ("8", [(58, "while away")], acceptor._now())
        await acceptor.wait_until(lambda: len(acceptor.logons) == 2)
        assert acceptor.logons[0].get(141) == "Y"
        assert acceptor.logons[1].get(141) is None
        assert acceptor.logons[1].require(34) != "1"
        assert listener.resets == 1
        await eventually(lambda: session.next_inbound == acceptor.next_out)
        assert [m.get(58) for m in listener.messages] == ["while away"]

    async def test_a_new_period_resets_again(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        period = ["day-1"]
        session, listener = await make_session(
            reset_on_logon=True, session_period=lambda _: period[0]
        )
        period[0] = "day-2"
        await acceptor.drop_connection()
        await acceptor.wait_until(lambda: len(acceptor.logons) == 2)
        assert acceptor.logons[1].get(141) == "Y"
        assert acceptor.logons[1].require(34) == "1"
        await session.wait_ready(5.0)
        assert listener.resets == 2

    async def test_a_reset_the_venue_never_confirmed_is_repeated(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        acceptor.faults.ignore_logon = True
        session, _ = await make_session(
            ready=False, reset_on_logon=True, logon_timeout=0.1
        )
        await acceptor.wait_until(lambda: bool(acceptor.logons))
        acceptor.faults.ignore_logon = False
        await session.wait_ready(5.0)
        assert all(logon.get(141) == "Y" for logon in acceptor.logons)
        assert len(acceptor.logons) >= 2

    async def test_the_marker_survives_a_restart(
        self, acceptor: FixAcceptor, make_session: SessionFactory, tmp_path: Path
    ) -> None:
        store = SqliteFixStore(tmp_path / "fix.db", "s")
        first, _ = await make_session(
            store=store, reset_on_logon=True, session_period=lambda _: "day-1"
        )
        await first.stop()
        second, _ = await make_session(
            store=store, reset_on_logon=True, session_period=lambda _: "day-1"
        )
        assert acceptor.logons[-1].get(141) is None
        assert second.is_ready
        await second.stop()
        store.close()


class TestPeerResets:
    async def test_the_answer_to_a_reset_carries_the_whole_logon(
        self, versioned_acceptor: FixAcceptor, begin_string: str
    ) -> None:
        acceptor = versioned_acceptor
        session = FixSession(
            session_config(
                acceptor, username=SecretStr("alice"), logon_fields=((1408, "x"),)
            ),
            MemoryFixStore(),
            RecordingListener(),
        )
        await session.start()
        await session.wait_ready(5.0)
        before = len(acceptor.logons)
        await acceptor.reset_sequences_at_logon()
        await acceptor.wait_until(lambda: len(acceptor.logons) > before)
        reply = acceptor.logons[-1]
        assert reply.get(141) == "Y"
        assert reply.get(34) == "1"
        assert reply.get(553) == "alice"
        assert reply.get(1408) == "x"
        expected = APPL_VER_FIX50SP2 if begin_string == "FIXT.1.1" else None
        assert reply.get(1137) == expected
        await session.stop()

    async def test_no_application_message_can_precede_our_logon(
        self, acceptor: FixAcceptor
    ) -> None:
        entered = asyncio.Event()
        leave = asyncio.Event()

        class Holding(RecordingListener):
            async def on_possible_message_loss(
                self, first_seq: int, last_seq: int | None, reason: str
            ) -> None:
                await super().on_possible_message_loss(first_seq, last_seq, reason)
                entered.set()
                await leave.wait()

        listener = Holding()
        session = FixSession(session_config(acceptor), MemoryFixStore(), listener)
        await session.start()
        await session.wait_ready(5.0)
        unread_from = session.next_inbound
        before = len(acceptor.logons)
        await acceptor.reset_sequences_at_logon()
        await asyncio.wait_for(entered.wait(), 5.0)
        with pytest.raises(SessionDownError):
            await session.send_application("D", [(11, "C1")])
        leave.set()
        await acceptor.wait_until(lambda: len(acceptor.logons) > before)
        assert acceptor.of_type("D") == []
        assert listener.possible_losses[0][0] == unread_from
        await session.wait_ready(5.0)
        await session.stop()

    async def test_a_reset_to_a_higher_number_reports_what_it_skipped(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session()
        expected = session.next_inbound
        await acceptor.send_raw_seq(expected, "4", [(36, "500")])
        await eventually(lambda: session.next_inbound == 500)
        assert listener.possible_losses == [
            (expected, 499, "SequenceReset-Reset skipped these numbers")
        ]


class TestSequenceNumberFormat:
    async def test_leading_zeros_are_the_same_number(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        """The FIX int datatype: "may contain leading zeros (e.g. "00023" = "23")"."""
        session, listener = await make_session()
        seq = acceptor.next_out
        acceptor.next_out += 1
        body = (
            f"35=8\x0149=VENUE\x0156=CLIENT\x0134=000{seq}\x01"
            f"52={format_utc_timestamp(datetime.now(timezone.utc))}\x0158=ok\x01"
        ).encode()
        head = f"8=FIX.4.4\x019={len(body)}\x01".encode() + body
        await acceptor.write(head + f"10={sum(head) % 256:03d}\x01".encode())
        await eventually(lambda: session.next_inbound == seq + 1)
        assert listener.messages[0].get(58) == "ok"


class TestResendProgress:
    async def test_a_slow_recovery_that_progresses_is_not_abandoned(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        session, listener = await make_session(
            resend_timeout=0.2, max_resend_requests=2
        )
        acceptor.faults.silent = True
        start = acceptor.next_out
        await acceptor.send_raw_seq(start + 12, "8", NEWS)
        for offset in range(9):
            await asyncio.sleep(0.1)
            await acceptor.send_raw_seq(start + offset, "8", NEWS)
        await eventually(lambda: session.next_inbound == start + 9)
        assert "message gap was not filled" not in listener.downs


class TestStaleResendDefault:
    async def test_an_old_order_is_gap_filled_rather_than_sent_again(
        self, acceptor: FixAcceptor, make_session: SessionFactory
    ) -> None:
        store = MemoryFixStore()
        long_ago = datetime.now(timezone.utc) - timedelta(minutes=5)
        store.allocate_outbound(
            StoredMessage(0, "D", format_utc_timestamp(long_ago), b"11=OLD\x01")
        )
        session, _ = await make_session(store=store)
        await acceptor.wait_until(lambda: bool(acceptor.of_type("4")))
        fill = acceptor.of_type("4")[0]
        assert (fill.get(34), fill.get(123)) == ("1", "Y")
        assert int(fill.require(36)) >= 2
        assert acceptor.of_type("D") == []
        assert session.is_ready
