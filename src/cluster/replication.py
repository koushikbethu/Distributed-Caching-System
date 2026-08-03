import asyncio
from typing import Optional
import httpx
import structlog
from src.config import Settings
from src.core.store import CacheStore
from src.core.entry import CacheEntry
from src.cluster.hash_ring import HashRing
from src.metrics.prometheus import CacheMetrics

logger = structlog.get_logger()

class ReplicationManager:
    """Leader-follower replication manager."""
    def __init__(self, settings: Settings, store: CacheStore, 
                 hash_ring: HashRing, local_node_id: str, metrics: CacheMetrics):
        self._settings = settings
        self._store = store
        self._hash_ring = hash_ring
        self._local_node_id = local_node_id
        self._metrics = metrics
        self._sequence_number: int = 0
        self._peer_clients: dict[str, httpx.AsyncClient] = {}  # node_id -> client
        self._node_addresses: dict[str, str] = {}  # node_id -> rest_address
        self._lock = asyncio.Lock()
    
    def _get_client(self, node_id: str) -> httpx.AsyncClient:
        if node_id not in self._peer_clients:
            self._peer_clients[node_id] = httpx.AsyncClient(timeout=2.0)
        return self._peer_clients[node_id]

    async def replicate_write(self, key: str, value: str, ttl: Optional[int] = None) -> None:
        """Replicate a SET operation to follower nodes."""
        targets = self._get_replication_targets(key)
        entry = self._store.get_entry(key)
        if not entry:
            return
            
        payload = entry.to_dict()
        for target in targets:
            address = self._node_addresses.get(target)
            if not address:
                continue
            client = self._get_client(target)
            try:
                await client.post(f"{address}/internal/replicate", json=payload)
            except Exception as e:
                logger.warning("replicate_write_failed", target=target, key=key, error=str(e))
                
    async def replicate_delete(self, key: str) -> None:
        """Replicate a DELETE operation to follower nodes."""
        targets = self._get_replication_targets(key)
        payload = {"key": key, "delete": True}
        for target in targets:
            address = self._node_addresses.get(target)
            if not address:
                continue
            client = self._get_client(target)
            try:
                await client.post(f"{address}/internal/replicate", json=payload)
            except Exception as e:
                logger.warning("replicate_delete_failed", target=target, key=key, error=str(e))
                
    async def handle_replicate(self, data: dict) -> bool:
        """Handle incoming replication from leader. Apply to local store."""
        try:
            if data.get("delete"):
                return self._store.delete(data["key"])
            else:
                entry = CacheEntry.from_dict(data)
                self._store.set_entry(entry)
                return True
        except Exception as e:
            logger.error("handle_replicate_error", error=str(e))
            return False
            
    async def full_sync_to(self, target_node_id: str) -> int:
        """Full sync: send all local entries to a target node. Returns count of synced entries."""
        address = self._node_addresses.get(target_node_id)
        if not address:
            return 0
            
        entries = self._store.get_all_entries()
        payload = [e.to_dict() for e in entries.values()]
        
        client = self._get_client(target_node_id)
        try:
            resp = await client.post(f"{address}/internal/full-sync", json=payload, timeout=10.0)
            if resp.status_code == 200:
                return len(payload)
        except Exception as e:
            logger.error("full_sync_failed", target=target_node_id, error=str(e))
        return 0
        
    async def handle_full_sync(self, entries: list[dict]) -> int:
        """Handle incoming full sync. Apply all entries to local store. Returns count."""
        count = 0
        for e_dict in entries:
            try:
                entry = CacheEntry.from_dict(e_dict)
                self._store.set_entry(entry)
                count += 1
            except Exception:
                pass
        return count
        
    def update_node_address(self, node_id: str, rest_address: str) -> None:
        """Update the REST address for a peer node."""
        self._node_addresses[node_id] = rest_address
        
    def remove_node(self, node_id: str) -> None:
        """Remove a node from replication targets."""
        self._node_addresses.pop(node_id, None)
        client = self._peer_clients.pop(node_id, None)
        if client:
            asyncio.create_task(client.aclose())
            
    def _get_replication_targets(self, key: str) -> list[str]:
        """Get the follower node IDs for a given key."""
        nodes = self._hash_ring.get_nodes(key, self._settings.replication_factor)
        return [n for n in nodes if n != self._local_node_id]

__all__ = ["ReplicationManager"]
