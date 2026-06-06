import asyncio
import json
import websockets
from abc import ABC, abstractmethod
from typing import AsyncIterable, List, Optional, Dict, Any
from isme.exchanges.base import BaseExchange

class GenericWebSocketExchange(BaseExchange, ABC):
    """
    Base class for WebSocket-based exchange implementations.
    Handles connection management and message routing.
    """

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._conn: Optional[websockets.WebSocketClientProtocol] = None

    async def _get_connection(self) -> websockets.WebSocketClientProtocol:
        """Establish or return the WebSocket connection."""
        if self._conn is None or not self._conn.open:
            self._conn = await websockets.connect(self.ws_url)
            await self._on_connect()
        return self._conn

    async def _on_connect(self):
        """Hook for post-connection logic (e.g., authentication)."""
        pass

    async def _send(self, message: Dict[str, Any]):
        """Send a JSON message over the WebSocket."""
        conn = await self._get_connection()
        await conn.send(json.dumps(message))

    async def _listen(self) -> AsyncIterable[Dict[str, Any]]:
        """Listen for incoming messages."""
        conn = await self._get_connection()
        async for message in conn:
            yield json.loads(message)

    async def close(self):
        """Close the WebSocket connection."""
        if self._conn and self._conn.open:
            await self._conn.close()

    @abstractmethod
    def _parse_message(self, message: Dict[str, Any]) -> Optional[Any]:
        """Convert a raw WebSocket message into a standardized model."""
        pass
