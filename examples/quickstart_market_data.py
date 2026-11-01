"""Read market data with no account: runs against the bundled mock provider."""

import asyncio
from datetime import datetime, timedelta, timezone

from ismet import IsmetClient, get_calendar
from ismet.providers.mock import MockProvider


async def main() -> None:
    async with IsmetClient([MockProvider()]) as client:
        quote = await client.quote("ACME", venue="XMOK")
        print(
            f"{quote.symbol} bid {quote.bid} ask {quote.ask} ({quote.book_state.value})"
        )

        end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        bars = await client.bars(
            "ACME@XMOK", interval="1m", start=end - timedelta(minutes=30), end=end
        )
        print(f"{len(bars)} bars, last close {bars[-1].close}")

    nyse = get_calendar("XNYS")
    a_saturday = datetime(2026, 3, 7, 12, tzinfo=timezone.utc)
    print(f"NYSE next opens at {nyse.next_open(a_saturday)}")


if __name__ == "__main__":
    asyncio.run(main())
