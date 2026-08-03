import pytest
import httpx
from unittest.mock import AsyncMock, patch
from src.client.cache_client import CacheClient
from src.client.retry import RetryConfig

@pytest.mark.asyncio
async def test_client_retry_on_failure():
    retry_config = RetryConfig(max_retries=2, base_delay=0.01)
    client = CacheClient(["http://node1", "http://node2"], retry_config=retry_config)
    
    # We must enter the context manager to initialize the httpx client
    async with client as c:
        # Mock the internal httpx request to fail once, then succeed
        c._client.request = AsyncMock(side_effect=[
            httpx.ConnectError("Connection failed"),
            httpx.Response(200, json={"value": "success"}, request=httpx.Request("GET", "http://node2/cache/test_key"))
        ])
        
        val = await c.get("test_key")
        
        # It should retry and succeed, returning the value
        assert val == "success"
        
        # Should have called request twice
        assert c._client.request.call_count == 2
        
        # Node should have cycled
        assert c._current_node_index == 1

@pytest.mark.asyncio
async def test_client_context_manager():
    client = CacheClient(["http://node1"])
    async with client as c:
        assert c._client is not None
        assert not c._client.is_closed
        
    # After exiting, client should be closed
    assert c._client.is_closed
