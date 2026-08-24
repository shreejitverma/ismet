import asyncio
import random
from collections.abc import AsyncIterable
from datetime import datetime, timedelta, timezone

from isme.exchanges.base import BaseExchange
from isme.models.market_data import HistoricalBar, Quote, Trade


class MockExchange(BaseExchange):
    """
    A mock exchange implementation for testing and demonstration purposes.
    Generates simulated market data.
    """

    @property
    def exchange_name(self) -> str:
        return "MOCK"

    async def get_quote(self, symbol: str) -> Quote:
        """Return a simulated quote."""
        price = random.uniform(100, 500)
        return Quote(
            symbol=symbol.upper(),
            exchange=self.exchange_name,
            timestamp=datetime.now(timezone.utc),
            bid_price=price - 0.05,
            ask_price=price + 0.05,
            bid_size=100,
            ask_size=100,
            last_price=price,
        )

    async def get_historical_data(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1d"
    ) -> list[HistoricalBar]:
        """Return a simulated list of historical bars."""
        bars = []
        current = start
        while current <= end:
            price = random.uniform(100, 500)
            bars.append(
                HistoricalBar(
                    symbol=symbol.upper(),
                    exchange=self.exchange_name,
                    timestamp=current,
                    open=price,
                    high=price + 2.0,
                    low=price - 2.0,
                    close=price + 0.5,
                    volume=10000,
                    interval=interval,
                )
            )
            if interval == "1d":
                current += timedelta(days=1)
            else:
                current += timedelta(minutes=1)
        return bars

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterable[Quote]:
        """Simulate a stream of quotes."""
        while True:
            for symbol in symbols:
                yield await self.get_quote(symbol)
            await asyncio.sleep(1)

    async def stream_trades(self, symbols: list[str]) -> AsyncIterable[Trade]:
        """Simulate a stream of trades."""
        while True:
            for symbol in symbols:
                price = random.uniform(100, 500)
                yield Trade(
                    symbol=symbol.upper(),
                    exchange=self.exchange_name,
                    timestamp=datetime.now(timezone.utc),
                    price=price,
                    size=random.randint(1, 100),
                    side=random.choice(["buy", "sell"]),
                )
            await asyncio.sleep(0.5)
