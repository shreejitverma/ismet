from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from datetime import datetime

from ismet.models.market_data import HistoricalBar, Quote, Trade


class BaseExchange(ABC):
    """
    Abstract Base Class for all exchange implementations.
    Defines the standard interface for REST and WebSocket interactions.
    """

    @abstractmethod
    async def get_quote(self, symbol: str) -> Quote:
        """Fetch the latest snapshot quote for a symbol."""
        pass

    @abstractmethod
    async def get_historical_data(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1d"
    ) -> list[HistoricalBar]:
        """Fetch historical OHLCV data."""
        pass

    @abstractmethod
    async def stream_quotes(self, symbols: list[str]) -> AsyncIterable[Quote]:
        """Stream real-time quotes via WebSockets."""
        pass

    @abstractmethod
    async def stream_trades(self, symbols: list[str]) -> AsyncIterable[Trade]:
        """Stream real-time trades via WebSockets."""
        pass

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Return the name of the exchange."""
        pass
