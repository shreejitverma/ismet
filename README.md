# ISMET: International Stock Market Engine Tool

One typed, async, venue-agnostic Python interface to exchanges, brokers, and market-data providers.
Money is `Decimal`, timestamps are timezone-aware, and every provider is proven by a shared conformance suite.

Pure-Python wheel. Runs the same on Linux, macOS, and Windows, on x86_64 and arm64, Python 3.10 to 3.14.

## Install

```bash
pip install ismet
```

## Quick start

```python
import asyncio
from datetime import datetime, timedelta, timezone

from ismet import IsmetClient
from ismet.providers.mock import MockProvider


async def main() -> None:
    async with IsmetClient([MockProvider()]) as client:
        quote = await client.quote("ACME", venue="XMOK")
        print(quote.bid, quote.ask, quote.exchange_ts)

        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        bars = await client.bars(
            "ACME@XMOK", interval="1m", start=end - timedelta(minutes=30), end=end
        )
        print(len(bars), bars[-1].close)

        async for trade in client.stream_trades(["ACME", "WAYNE"], venue="XMOK"):
            print(trade.symbol, trade.price, trade.side)
            break


asyncio.run(main())
```

Symbols are `TICKER@MIC` strings, or a `Symbol(venue=..., ticker=...)`.
The venue selects the provider; pass `provider=` when several providers serve one venue.

## Configure real providers

`IsmetClient.from_env()` builds the providers named in `ISMET_PROVIDERS` and
reads their credentials from the environment or a config file.
Precedence: explicit argument, environment, config file, default.

```bash
export ISMET_PROVIDERS=alpaca
export ISMET_ALPACA_API_KEY=...      # keys containing KEY/SECRET/TOKEN/PASSWORD/PASSPHRASE are credentials
export ISMET_ALPACA_PAPER=true       # anything else is an option
```

Or in `config.toml` under the OS user config directory (`ismet doctor` prints the path once the CLI lands):

```toml
providers = ["alpaca"]

[provider.alpaca.credentials]
api_key = "..."

[provider.alpaca.options]
paper = true
```

Credentials are wrapped in `SecretStr` and never appear in logs, reprs, or exceptions.

## Capabilities

A provider implements a subset of these protocols; the client checks at call time and raises `NotSupported` with the provider and capability names.

| Capability | Methods |
|---|---|
| `market_data` | `quote`, `order_book` |
| `historical` | `bars` |
| `streaming` | `stream_quotes`, `stream_trades` |
| `reference_data` | `instrument`, `search` |
| `trading`, `account` | milestone M2 |

```python
client.capabilities("mock")
# frozenset({Capability.MARKET_DATA, Capability.HISTORICAL, ...})
```

## Providers

| Provider | Venues | Status |
|---|---|---|
| `mock` | `XMOK` | conformant, deterministic, ships with the package |

Tier 1 targets (M1): Interactive Brokers, Alpaca, Zerodha Kite, Binance, Coinbase, Polygon.
A provider is listed only when it passes `ismet.testing.run_conformance`.

Third-party providers register through the `ismet.providers` entry-point group:

```toml
[project.entry-points."ismet.providers"]
myvenue = "my_package:MyVenueProvider"
```

## Guarantees

- Prices, sizes, and balances are `decimal.Decimal`; floats are rejected at the model boundary.
  Transports decode JSON with `parse_float=Decimal`.
- Every event carries `exchange_ts` and `received_ts`, both timezone-aware, normalised to UTC, with `exchange_ts <= received_ts` enforced.
- HTTP calls get jittered exponential retry, per-endpoint token-bucket rate limits, and a circuit breaker.
  Status codes map to `AuthError`, `RateLimited` (with `retry_after`), `TransportError`, and `VenueError` (raw vendor code preserved).
- WebSocket streams reconnect with backoff, resubscribe through an `on_connect` hook, heartbeat, and apply an explicit backpressure policy (block, drop oldest, drop newest).
- Handshake rejections that cannot succeed on retry (malformed URL, 4xx other than 429) fail fast with `AuthError` for 401/403 or a non-retryable `TransportError`; 429 and 5xx keep reconnecting.
- No shell-outs, no OS-specific paths, no event-loop policy changes.

## Development

```bash
uv sync --extra dev
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run pytest --cov
```

CI runs the suite on Linux, macOS, and Windows for every supported Python version, with `mypy --strict` and a 90 percent coverage gate.

## License

MIT
