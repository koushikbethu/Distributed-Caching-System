import httpx
import structlog
from typing import Optional, Any
from src.client.retry import RetryConfig, retry_async

logger = structlog.get_logger()

class CacheClient:
    """Client library for the distributed cache system.
    
    Features:
    - Connection pooling via httpx
    - Automatic retry with exponential backoff
    - Cluster topology awareness (discovers all nodes)
    - Automatic failover on node failure
    
    Usage:
        async with CacheClient(["http://localhost:8001"]) as client:
            await client.set("key", "value", ttl=60)
            value = await client.get("key")
    """
    
    def __init__(self, nodes: list[str], retry_config: RetryConfig = None,
                 pool_size: int = 10, timeout: float = 5.0):
        self._nodes = list(nodes)  # List of REST addresses
        self._current_node_index = 0
        self._retry_config = retry_config or RetryConfig(
            max_retries=3,
            retryable_exceptions=(httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError)
        )
        self._client: Optional[httpx.AsyncClient] = None
        self._pool_size = pool_size
        self._timeout = timeout
    
    async def __aenter__(self) -> 'CacheClient':
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=self._pool_size, max_keepalive_connections=self._pool_size)
        )
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()
    
    def _get_base_url(self) -> str:
        """Get current node URL. Cycles through nodes on failure."""
        if not self._nodes:
            raise RuntimeError("No nodes available")
        return self._nodes[self._current_node_index]
    
    def _cycle_node(self) -> None:
        """Move to the next node in the list (round-robin failover)."""
        if self._nodes:
            self._current_node_index = (self._current_node_index + 1) % len(self._nodes)
            logger.info("cycled_node", new_node=self._nodes[self._current_node_index])
    
    async def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        """Make an HTTP request with retry and failover logic.
        On connection error, cycle to next node and retry."""
        async def _do_request():
            if not self._client:
                raise RuntimeError("Client not initialized. Use 'async with' context manager.")
            
            url = f"{self._get_base_url()}{path}"
            try:
                response = await self._client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                self._cycle_node()
                raise e

        return await retry_async(_do_request, self._retry_config)

    async def get(self, key: str) -> Optional[str]:
        """Get a value by key. Returns None if not found."""
        try:
            response = await self._request("GET", f"/cache/{key}")
            return response.json().get("value")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise
    
    async def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """Set a key-value pair. Returns True if successful."""
        payload = {"value": value}
        if ttl is not None:
            payload["ttl"] = ttl
        
        await self._request("PUT", f"/cache/{key}", json=payload)
        return True
    
    async def delete(self, key: str) -> bool:
        """Delete a key. Returns True if the key existed."""
        try:
            await self._request("DELETE", f"/cache/{key}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise
    
    async def exists(self, key: str) -> bool:
        """Check if a key exists."""
        try:
            await self._request("GET", f"/cache/{key}")
            return True
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise
    
    async def ping(self) -> bool:
        """Ping the cluster to check connectivity."""
        try:
            await self._request("GET", "/health")
            return True
        except Exception:
            return False
    
    async def cluster_status(self) -> dict:
        """Get cluster status information."""
        response = await self._request("GET", "/cluster/status")
        return response.json()
    
    async def discover_nodes(self) -> list[str]:
        """Discover all nodes in the cluster via /cluster/nodes endpoint.
        Updates internal node list for failover."""
        response = await self._request("GET", "/cluster/nodes")
        nodes_info = response.json()
        
        new_nodes = []
        for node in nodes_info:
            if "rest_url" in node:
                new_nodes.append(node["rest_url"])
                
        if new_nodes:
            self._nodes = new_nodes
            if self._current_node_index >= len(self._nodes):
                self._current_node_index = 0
            logger.info("nodes_discovered", nodes=self._nodes)
            
        return self._nodes
