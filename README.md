# ISMET: International Stock Market Engine Tool

The safety-critical layer between Python and a market.
One typed, async interface to market data, trading calendars, reference data, and orders, built so an order is never sent twice, a fill is never lost, and a price is never silently wrong.

**Status: alpha.**
The order path has been tested against a simulated venue and through a FIX test double, and never against a real broker's order entry.
Do not trade real money with this yet.
[docs/GAP_MATRIX.md](docs/GAP_MATRIX.md) states exactly what is built, what is proven, and what is not.

Pure-Python wheel on Linux, macOS, and Windows, x86_64 and arm64, Python 3.10 to 3.14.

## Install

```bash
pip install ismet
```

## See it work, with no account

```bash
ISMET_PROVIDERS=coinbase ismet quote BTC-USD@ZCBS
ismet calendar XNSE
python examples/safe_order_path.py
```

The last one runs the order path against the bundled simulated venue:

```text
refused before sending: price_collar: 150.00 is 4998.50 bps through 100.01; default limit 100 bps
filled at 100.01
response lost, status is unknown
after asking the venue: accepted
times sent: 1
refused: end of demo
open order cancelled by the switch: canceled
position from fills: 10
journal chain intact: True
```

## Market data

```python
import asyncio
from datetime import datetime, timedelta, timezone

from ismet import IsmetClient
from ismet.providers.coinbase import CoinbaseProvider


async def main() -> None:
    async with IsmetClient([CoinbaseProvider()]) as client:
        quote = await client.quote("BTC-USD@ZCBS")
        print(quote.bid, quote.ask, quote.book_state.value, quote.flags)

        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        bars = await client.bars(
            "BTC-USD@ZCBS", interval="1m", start=end - timedelta(minutes=30), end=end
        )
        print(len(bars), bars[-1].close, bars.to_pandas().shape)

        async for trade in client.stream_trades(["BTC-USD@ZCBS"]):
            print(trade.price, trade.quality)
            break


asyncio.run(main())
```

Blocking code, including inside a notebook, uses `IsmetSyncClient`, with the same methods.

## Orders

Every order goes through `OrderManager`.

```python
from decimal import Decimal

from ismet import (
    OrderManager, OrderRequest, OrderSide, RiskConfig, RiskEngine, RiskLimits, SqliteJournal,
)

risk = RiskEngine(RiskConfig(default=RiskLimits(
    max_order_quantity=Decimal(1000),
    max_order_notional=Decimal(100_000),
    max_position=Decimal(5000),
    price_collar_bps=Decimal(100),
)))

async with OrderManager(provider, SqliteJournal("orders.db"), risk) as oms:
    state = await oms.submit(OrderRequest(
        client_order_id="strategy-a-000001",
        symbol=symbol,
        side=OrderSide.BUY,
        quantity="10",
        limit_price="100.00",
    ))
```

What happens, and why it matters:

- **Journal first.** The intent is flushed to disk before the network call.
- **Send once.** Orders are never retried automatically, because a timeout does not mean the order failed.
- **Unknown is a state.** A lost response leaves the order `UNKNOWN`, blocks new risk in that symbol, and is resolved by asking the venue by client order id.
- **Fail closed.** No instrument, no reference price, no calendar answer, unverified positions, a stale quote, or an error inside a check all refuse the order.
- **Crash safe.** Restart on the same journal to recover: orders that never reached the send are closed, the rest are looked up, and fills made while down are booked. Tested with real `SIGKILL`.
- **Kill switch.** `ismet kill engage --journal orders.db --reason "..." --operator "..."` stops every process using that journal.
- **Reconciliation.** Orders, fills, and positions are compared with the venue, and a disagreement that means exposure may be wrong halts new orders.
- **Audit trail.** The journal is append-only and hash-chained, and `ismet journal verify` checks it.
- **Live trading is opt-in.** A provider that does not declare itself simulated or paper is treated as live, and it requires `allow_live=True`.

Limits, measured in [docs/BENCHMARKS.md](docs/BENCHMARKS.md): durable writes cap this at tens of orders per second per process, with submit taking tens of milliseconds.
It is not for colocated, sub-millisecond strategies.

## What makes it different

- **Money is exact.** `Decimal` everywhere, in a private exact context that ignores the ambient decimal settings. Floats are rejected.
- **Real markets are representable.** Zero and negative prices, locked and crossed books, and clock skew are valid data, flagged rather than rejected.
- **Identity is validated.** MICs and currencies against ISO snapshots, and ISIN, CUSIP, SEDOL, FIGI, and LEI by check digit.
- **Calendars refuse to guess.** A date outside a venue's sourced data raises `CalendarOutOfRange`, which also refuses the order.
- **History is reproducible.** `BarStore` keeps restated bars and reads data as it stood at a past time. `SecurityMaster` answers "what did this ticker mean then, and what did we know then".
- **Support is proven, not claimed.** A provider is listed only if it passes conformance on recorded responses.

## Providers

| Provider | Venues | Capabilities | Evidence |
|---|---|---|---|
| `binance` | `BNCX` spot | market data, bars, streams, reference | Conformance on recorded responses and live, public data only |
| `coinbase` | `ZCBS` spot | market data, bars, streams, reference | Conformance on recorded responses and live, public data only |
| `mock` | `XMOK` | market data, bars, streams, reference | Deterministic, ships with the package |
| `SimVenue` (`ismet.testing.simvenue`) | `XSIM` | everything, with scriptable faults | Trading conformance |
| `FixProvider` (`ismet.fix`) | Configured per counterparty | order entry over FIX | See the gap matrix |

Interactive Brokers, Alpaca, Zerodha Kite, and Polygon need accounts or API keys to record real fixtures, and an adapter is not shipped without them.

Third-party providers register through the `ismet.providers` entry-point group, and must pass `ismet.testing.run_conformance`, plus `ismet.testing.trading_conformance.run_trading_conformance` for order entry.

## Calendars

```python
from datetime import date, datetime, timezone

from ismet import get_calendar

nyse = get_calendar("XNYS")
nyse.session(date(2026, 11, 27)).early_close  # True: closes at 13:00 New York
nyse.is_open(datetime.now(timezone.utc))
nyse.settlement_date(date(2026, 11, 25), 1)   # T+1 across Thanksgiving

closed = nyse.with_closure(date(2026, 12, 7))  # apply an announced closure at once
```

Bundled: XNYS, XNAS, XNSE, XBOM, XLON, XTKS, XJPX, XHKG, XSHG, XSHE, XETR, XFRA, XPAR, XAMS, XBRU, XMIL, XMAD, XSWX, XSTO, XTSE, XASX, XKRX, XSES, XTAI, XJSE, BVMF, XMEX, XNZE, and 24x7 crypto venues.
Data comes from `exchange_calendars`, and CI proves it matches session by session.
**XNSE, XBOM, XSHG, XSHE, and XSES data ends on 2026-12-31**, because the source's lists end there.
Pre-open, auction, and extended sessions are not modelled.

## Configuration

Precedence: explicit argument, environment, config file, default.

```bash
export ISMET_PROVIDERS=coinbase
export ISMET_ALPACA_API_KEY=...   # KEY, SECRET, TOKEN, PASSWORD, PASSPHRASE mark credentials
```

Credentials are held in `SecretStr`, and they never appear in logs, reprs, or exceptions.
`ismet doctor` reports what is configured, without showing values.

## Documentation

| Document | For |
|---|---|
| [docs/GAP_MATRIX.md](docs/GAP_MATRIX.md) | What exists, the evidence, and what does not exist |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Runbook: halts, crashes, every failure mode |
| [docs/COMPLIANCE.md](docs/COMPLIANCE.md) | Regulatory control mapping, and what stays the firm's responsibility |
| [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md) | Threats, controls, remaining risk |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Measured latency and limits |
| [docs/adr/](docs/adr/) | Why the design is the way it is |
| [SECURITY.md](SECURITY.md) | Reporting a vulnerability |

## Development

```bash
uv sync --extra dev
uv run ruff check . && uv run ruff format --check . && uv run mypy
uv run pytest --cov
```

CI runs on Linux, macOS, and Windows for every supported Python version, with `mypy --strict`, a 90 percent coverage gate, a calendar differential test, performance budgets, a dependency audit, CodeQL, and a fresh-install smoke test.

## Disclaimer

ISMET is software, not financial advice, and using it does not make a firm compliant with any regulation.
You are responsible for your orders.

## License

MIT
