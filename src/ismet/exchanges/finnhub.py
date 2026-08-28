from collections.abc import AsyncIterable
from datetime import datetime, timezone

from ismet.exchanges.rest_base import GenericRestExchange
from ismet.models.market_data import HistoricalBar, Quote, Trade


class FinnHubExchange(GenericRestExchange):
    """
    Implementation for the FinnHub API.
    Covers US markets (NYSE, NASDAQ).
    """

    def __init__(self, api_key: str):
        super().__init__(base_url="https://finnhub.io/api/v1", api_key=api_key)

    @property
    def exchange_name(self) -> str:
        return "FINNHUB"

    async def get_quote(self, symbol: str) -> Quote:
        """Fetch a quote from FinnHub."""
        params = {"symbol": symbol.upper(), "token": self.api_key}
        data = await self._request("GET", "/quote", params=params)

        # FinnHub returns c: current, d: change, dp: percent change,
        # h: high, l: low, o: open, pc: prev close
        return Quote(
            symbol=symbol.upper(),
            exchange=self.exchange_name,
            timestamp=datetime.now(timezone.utc),
            bid_price=data.get("h", 0.0),  # Placeholder as FinnHub /quote is simplified
            ask_price=data.get("l", 0.0),  # Placeholder
            bid_size=0,
            ask_size=0,
            last_price=data.get("c", 0.0),
        )

    async def get_historical_data(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        interval: str = "1d",
    ) -> list[HistoricalBar]:
        """Fetch historical data from FinnHub."""
        # Mapping FinnHub resolution: 1, 5, 15, 30, 60, D, W, M
        resolution = "D" if interval == "1d" else "1"

        params = {
            "symbol": symbol.upper(),
            "resolution": resolution,
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "token": self.api_key,
        }

        data = await self._request("GET", "/stock/candle", params=params)

        if data.get("s") != "ok":
            return []

        bars = []
        for i in range(len(data.get("t", []))):
            bars.append(
                HistoricalBar(
                    symbol=symbol.upper(),
                    exchange=self.exchange_name,
                    timestamp=datetime.fromtimestamp(data["t"][i], tz=timezone.utc),
                    open=data["o"][i],
                    high=data["h"][i],
                    low=data["l"][i],
                    close=data["c"][i],
                    volume=data["v"][i],
                    interval=interval,
                )
            )
        return bars

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterable[Quote]:
        """FinnHub requires a WebSocket implementation (not REST)."""
        raise NotImplementedError(
            "FinnHub quotes streaming requires WebSocket implementation."
        )

    async def stream_trades(self, symbols: list[str]) -> AsyncIterable[Trade]:
        """FinnHub requires a WebSocket implementation (not REST)."""
        raise NotImplementedError(
            "FinnHub trades streaming requires WebSocket implementation."
        )
