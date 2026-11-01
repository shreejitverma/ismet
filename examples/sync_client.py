"""Blocking access, for scripts and notebooks."""

from ismet import IsmetSyncClient
from ismet.providers.mock import MockProvider

with IsmetSyncClient([MockProvider()]) as client:
    bars = client.bars("ACME@XMOK", interval="1h", start="2026-03-09", end="2026-03-10")
    print(f"{len(bars)} bars")
    print(bars.to_records()[0]["close"])
    for quote in client.stream_quotes(["ACME"], venue="XMOK"):
        print(quote.bid, quote.ask)
        break
