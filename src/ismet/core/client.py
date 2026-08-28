from collections.abc import AsyncIterable
from datetime import datetime

from ismet.exchanges.base import BaseExchange
from ismet.models.market_data import HistoricalBar, Quote, Trade


class IsmetClient:
    """
    The main entry point for the International Stock Market Engine and Toolkit.
    Handles exchange registration and provides a unified interface for data fetching.
    """

    def __init__(self):
        self._exchanges: dict[str, BaseExchange] = {}

    def register_exchange(self, exchange: BaseExchange):
        """Register an exchange implementation."""
        self._exchanges[exchange.exchange_name.upper()] = exchange

    def _get_exchange(self, exchange_name: str) -> BaseExchange:
        """Get a registered exchange by name."""
        name = exchange_name.upper()
        if name not in self._exchanges:
            raise ValueError(
                f"Exchange '{exchange_name}' is not registered or supported."
            )
        return self._exchanges[name]

    async def get_quote(self, symbol: str, exchange: str) -> Quote:
        """Fetch a snapshot quote from a specific exchange."""
        handler = self._get_exchange(exchange)
        return await handler.get_quote(symbol)

    async def get_historical_data(
        self,
        symbol: str,
        exchange: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> list[HistoricalBar]:
        """Fetch historical data from a specific exchange."""
        handler = self._get_exchange(exchange)
        return await handler.get_historical_data(symbol, start, end, interval)

    async def stream_quotes(
        self, symbols: list[str], exchange: str
    ) -> AsyncIterable[Quote]:
        """Stream real-time quotes from a specific exchange."""
        handler = self._get_exchange(exchange)
        async for quote in handler.stream_quotes(symbols):
            yield quote

    async def stream_trades(
        self, symbols: list[str], exchange: str
    ) -> AsyncIterable[Trade]:
        """Stream real-time trades from a specific exchange."""
        handler = self._get_exchange(exchange)
        async for trade in handler.stream_trades(symbols):
            yield trade
