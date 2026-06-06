from abc import ABC, abstractmethod
from typing import AsyncIterable, List, Optional
from datetime import datetime

from isme.models.market_data import Quote, Trade, HistoricalBar

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
        self, 
        symbol: str, 
        start: datetime, 
        end: datetime, 
        interval: str = "1d"
    ) -> List[HistoricalBar]:
        """Fetch historical OHLCV data."""
        pass

    @abstractmethod
    async def stream_quotes(self, symbols: List[str]) -> AsyncIterable[Quote]:
        """Stream real-time quotes via WebSockets."""
        pass

    @abstractmethod
    async def stream_trades(self, symbols: List[str]) -> AsyncIterable[Trade]:
        """Stream real-time trades via WebSockets."""
        pass

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Return the name of the exchange."""
        pass
