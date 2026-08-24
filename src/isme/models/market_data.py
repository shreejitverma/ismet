from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class MarketDataModel(BaseModel):
    """Base model for all market data."""

    symbol: str
    exchange: str
    timestamp: datetime


class Quote(MarketDataModel):
    """Real-time quote data."""

    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    last_price: Optional[float] = None


class Trade(MarketDataModel):
    """Single trade execution data."""

    price: float
    size: int
    side: Optional[str] = None  # buy, sell, or unknown


class HistoricalBar(MarketDataModel):
    """OHLCV bar for historical data."""

    open: float
    high: float
    low: float
    close: float
    volume: int
    interval: str  # 1m, 5m, 1h, 1d, etc.
