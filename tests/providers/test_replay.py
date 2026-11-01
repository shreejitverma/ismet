from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from ismet.testing.replay import (
    ALLOWED_RESPONSE_HEADERS,
    RecordedExchange,
    RecordedSession,
    RecordingTransport,
    ReplayTransport,
    UnrecordedRequestError,
    WsReplayServer,
    exchange_key,
    find_secrets,
    load_exchanges,
    load_session,
    save_exchanges,
    save_session,
)
from ismet.transport.backoff import NO_RETRY
from ismet.transport.http import HttpTransport

from ._support import FIXTURES

MAX_FIXTURE_BYTES = 60_000
URL = "https://venue.example/api?b=2&a=1"
ALL_FIXTURES = sorted(FIXTURES.glob("*/*.json"))


def _exchange(url: str = URL, body: str = "{}", status: int = 200) -> RecordedExchange:
    return RecordedExchange("GET", url, status, {"content-type": "json"}, body)


def test_key_ignores_query_order_and_decodes_it() -> None:
    assert exchange_key("get", URL) == exchange_key(
        "GET", "https://venue.example/api?a=1&b=2"
    )
    assert exchange_key("GET", "https://x/p?t=1%3A2") == exchange_key(
        "GET", "https://x/p?t=1:2"
    )
    assert exchange_key("GET", URL) != exchange_key(
        "GET", "https://venue.example/api?a=1"
    )
    assert exchange_key("GET", URL) != exchange_key("POST", URL)


async def test_unrecorded_request_fails_loudly_through_the_real_transport() -> None:
    replay = ReplayTransport([_exchange()])
    async with HttpTransport(
        "https://venue.example", transport=replay, retry_policy=NO_RETRY
    ) as http:
        with pytest.raises(UnrecordedRequestError, match="no recording for GET"):
            await http.get_json("/api", params={"a": 2})
    assert len(replay.requests) == 1


async def test_recorded_exchange_is_served_with_status_headers_and_body() -> None:
    replay = ReplayTransport([_exchange(body='{"price":1.10}')])
    async with HttpTransport("https://venue.example", transport=replay) as http:
        response = await http.request("GET", "/api", params={"b": 2, "a": 1})
    assert response.status == 200
    assert response.headers["content-type"] == "json"
    assert response.body == b'{"price":1.10}'
    assert str(response.json()["price"]) == "1.10"


async def test_repeats_serve_in_order_then_stick_to_the_last() -> None:
    replay = ReplayTransport([_exchange(body="1"), _exchange(body="2")])
    async with HttpTransport("https://venue.example", transport=replay) as http:
        seen = [await http.get_json("/api?b=2&a=1") for _ in range(3)]
    assert seen == [1, 2, 2]


async def test_recording_keeps_only_allow_listed_headers_and_decoded_body() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "Content-Type": "application/json",
                "Set-Cookie": "__cf_bm=abc; HttpOnly",
                "X-Amz-Cf-Id": "trace",
                "X-MBX-USED-WEIGHT-1M": "20",
            },
            content=b'{"ok":true}',
        )

    recorder = RecordingTransport(httpx.MockTransport(respond))
    async with HttpTransport("https://venue.example", transport=recorder) as http:
        response = await http.request(
            "GET", "/api", headers={"Authorization": "Bearer top-secret"}
        )
    await recorder.aclose()
    assert response.json() == {"ok": True}
    assert "set-cookie" not in response.headers
    recorded = recorder.exchanges[0]
    assert recorded.url == "https://venue.example/api"
    assert recorded.body == '{"ok":true}'
    assert dict(recorded.headers) == {
        "content-type": "application/json",
        "x-mbx-used-weight-1m": "20",
    }
    assert "top-secret" not in repr(recorded)


def test_cassettes_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "a" / "cassette.json"
    exchange = RecordedExchange("GET", URL, 400, {"k": "v"}, "币安", note="trimmed")
    save_exchanges(path, [exchange], "2026-11-01T00:00:00Z")
    assert load_exchanges(path) == [exchange]
    session = RecordedSession("wss://x", ({"a": 1},), ("ack",), ("f1", "f2"))
    save_session(tmp_path / "s.json", session, "2026-11-01T00:00:00Z")
    assert load_session(tmp_path / "s.json") == session


SESSION = RecordedSession(
    "wss://venue.example", ({"sub": 1},), ("ack",), ("one", "two", "three")
)


async def _read(connection, count: int) -> list[str]:  # type: ignore[no-untyped-def]
    return [await connection.recv() for _ in range(count)]


async def test_ws_replays_control_then_frames_and_stays_open() -> None:
    async with WsReplayServer([SESSION]) as server, connect(server.url) as connection:
        await connection.send(json.dumps({"sub": 1}))
        assert await _read(connection, 4) == ["ack", "one", "two", "three"]
        await connection.send(json.dumps({"later": True}))
        await connection.close()
    assert server.connections[0].received[0] == {"sub": 1}
    assert server.unexpected == []


async def test_ws_rejects_an_unrecorded_subscription_loudly() -> None:
    async with WsReplayServer([SESSION]) as server, connect(server.url) as connection:
        await connection.send(json.dumps({"sub": 2}))
        with pytest.raises(ConnectionClosed) as caught:
            await connection.recv()
    assert caught.value.rcvd is not None
    assert caught.value.rcvd.code == 1008
    assert server.unexpected == [{"sub": 2}]


async def test_ws_plan_closes_between_connections() -> None:
    plan = [slice(0, 1), slice(1, None)]
    async with WsReplayServer([SESSION], plan=plan) as server:
        async with connect(server.url) as first:
            await first.send(json.dumps({"sub": 1}))
            assert await _read(first, 2) == ["ack", "one"]
            with pytest.raises(ConnectionClosed):
                await first.recv()
        async with connect(server.url) as second:
            await second.send(json.dumps({"sub": 1}))
            assert await _read(second, 3) == ["ack", "two", "three"]
    assert len(server.connections) == 2


@pytest.mark.parametrize(
    ("text", "name"),
    [
        ('"set-cookie": "a=b"', "credential header"),
        ('"Authorization": "x"', "credential header"),
        ("X-MBX-APIKEY", "credential header"),
        ('{"CB-ACCESS-SIGN": 1}', "credential header"),
        ("__cf_bm=abc", "tracking cookie"),
        ('"api_key": "abc"', "credential field"),
        ("passphrase=abc", "credential field"),
        ("Bearer abcdefgh12345", "bearer token"),
        ("AKIAIOSFODNN7EXAMPLE", "aws key"),
        ("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NSJ9.sig", "json web token"),
        ("-----BEGIN EC PRIVATE KEY-----", "private key"),
        ("client 203.0.113.7 connected", "ip address"),
    ],
)
def test_secret_scanner_catches(text: str, name: str) -> None:
    assert name in find_secrets(text)


def test_secret_scanner_leaves_market_data_alone() -> None:
    assert find_secrets('{"price":"77540.02","size":"0.1","sequence":1}') == []


def test_there_are_fixtures_to_scan() -> None:
    assert {p.parent.name for p in ALL_FIXTURES} == {"binance", "coinbase"}
    assert len(ALL_FIXTURES) > 20


@pytest.mark.parametrize(
    "path", ALL_FIXTURES, ids=lambda p: f"{p.parent.name}/{p.name}"
)
def test_fixture_has_no_secrets_and_is_small(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    assert find_secrets(text) == []
    assert path.stat().st_size <= MAX_FIXTURE_BYTES


@pytest.mark.parametrize(
    "path",
    [p for p in ALL_FIXTURES if p.name.startswith("rest_")],
    ids=lambda p: f"{p.parent.name}/{p.name}",
)
def test_cassette_headers_are_allow_listed(path: Path) -> None:
    for exchange in load_exchanges(path):
        assert set(exchange.headers) <= ALLOWED_RESPONSE_HEADERS
        assert exchange.url.startswith("https://")
        assert exchange.method == "GET"


def test_every_trimmed_response_says_so() -> None:
    notes = [
        exchange.note
        for path in ALL_FIXTURES
        if path.name == "rest_conformance.json"
        for exchange in load_exchanges(path)
        if exchange.note
    ]
    assert len(notes) == 3
    assert all("by recorder" in note for note in notes)
