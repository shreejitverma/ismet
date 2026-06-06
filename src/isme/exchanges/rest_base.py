import httpx
from typing import Optional, Dict, Any
from isme.exchanges.base import BaseExchange

class GenericRestExchange(BaseExchange):
    """
    Base class for REST-based exchange implementations.
    Provides common HTTP client management and utility methods.
    """

    def __init__(
        self, 
        base_url: str, 
        api_key: Optional[str] = None,
        timeout: float = 10.0
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout
            )
        return self._client

    async def _request(
        self, 
        method: str, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Perform an HTTP request with error handling."""
        client = await self._get_client()
        
        # Merge headers (e.g., for API Key)
        request_headers = headers or {}
        if self.api_key:
            # Common patterns, can be overridden in subclasses
            request_headers.setdefault("Authorization", f"Bearer {self.api_key}")
            request_headers.setdefault("X-API-Key", self.api_key)

        response = await client.request(
            method=method,
            url=endpoint,
            params=params,
            headers=request_headers
        )
        
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Wrap in a custom exception if needed
            raise RuntimeError(f"API request failed: {e.response.text}") from e
            
        return response.json()

    async def close(self):
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
