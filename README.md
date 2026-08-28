# ISMET: International Stock Market Engine and Toolkit

`ismet` is a high-performance, unified Python package designed to connect to major global stock exchanges. It provides a standardized interface for fetching historical data and streaming real-time market data.

## Features

- **Unified API**: One interface for multiple global exchanges (NYSE, NASDAQ, NSE, BSE, LSE, JPX, HKEX, SZSE).
- **Hybrid Data Fetching**: Supports both REST (historical/snapshot) and WebSockets (real-time streaming).
- **Standardized Models**: Consistent data structures regardless of the underlying exchange API.
- **Async First**: Built with `httpx` and `websockets` for modern, asynchronous applications.

## Installation

```bash
pip install ismet
```

## Quick Start

```python
import asyncio
from ismet import IsmetClient


async def main():
    client = IsmetClient()

    # Get quote from NYSE
    quote = await client.get_quote("AAPL", exchange="NYSE")
    print(f"{quote.symbol}: {quote.price}")

    # Stream real-time data
    async for trade in client.stream_trades(["AAPL", "TSLA"]):
        print(f"New trade: {trade.symbol} @ {trade.price}")


if __name__ == "__main__":
    asyncio.run(main())
```

## License

MIT
