from datetime import datetime, timedelta, timezone

import pytest

from isme import IsmeClient
from isme.exchanges.mock import MockExchange
from isme.models.market_data import HistoricalBar


@pytest.mark.asyncio
async def test_client_registration():
    client = IsmeClient()
    mock_exchange = MockExchange()
    client.register_exchange(mock_exchange)

    quote = await client.get_quote("AAPL", exchange="MOCK")
    assert quote.symbol == "AAPL"
    assert quote.exchange == "MOCK"
    assert isinstance(quote.bid_price, float)


@pytest.mark.asyncio
async def test_client_historical_data():
    client = IsmeClient()
    mock_exchange = MockExchange()
    client.register_exchange(mock_exchange)

    start = datetime.now(timezone.utc) - timedelta(days=5)
    end = datetime.now(timezone.utc)

    bars = await client.get_historical_data(
        "AAPL", exchange="MOCK", start=start, end=end
    )
    assert len(bars) > 0
    assert isinstance(bars[0], HistoricalBar)
    assert bars[0].symbol == "AAPL"


@pytest.mark.asyncio
async def test_client_invalid_exchange():
    client = IsmeClient()
    with pytest.raises(ValueError, match="Exchange 'INVALID' is not registered"):
        await client.get_quote("AAPL", exchange="INVALID")
