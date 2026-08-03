import asyncio
from typing import Optional
import httpx
import structlog
from src.core.store import CacheStore
from src.core.entry import CacheEntry
from src.cluster.hash_ring import HashRing
from src.cluster.replication import ReplicationManager
from src.metrics.prometheus import CacheMetrics

logger = structlog.get_logger()

class Rebalancer:
    """Handles key migration when cluster topology changes."""
    def __init__(self, store: CacheStore, hash_ring: HashRing, 
                 local_node_id: str, replication_manager: ReplicationManager,
                 metrics: CacheMetrics):
        self._store = store
        self._hash_ring = hash_ring
        self._local_node_id = local_node_id
        self._replication = replication_manager
        self._metrics = metrics
        
    async def on_node_joined(self, new_node_id: str, new_node_address: str) -> int:
        """Called when a new node joins. Migrate keys that now belong to the new node."""
        logger.info("rebalancer_node_joined", new_node_id=new_node_id)
        migrated = 0
        entries_to_migrate = []
        
        all_entries = self._store.get_all_entries()
        for key, entry in all_entries.items():
            primary_node = self._hash_ring.get_node(key)
            if primary_node == new_node_id:
                entries_to_migrate.append(entry)
                
        if entries_to_migrate:
            # Batch size could be added here
            migrated = await self._migrate_keys_to(new_node_address, entries_to_migrate)
            # Delete local if they are no longer even a replica?
            # For simplicity, if migrated successfully, delete from local if we are not a replica
            for entry in entries_to_migrate:
                replicas = self._hash_ring.get_nodes(entry.key, 3) # assuming RF=3 or from settings
                if self._local_node_id not in replicas:
                    self._store.delete(entry.key)
                    
        return migrated
        
    async def on_node_left(self, dead_node_id: str) -> None:
        """Called when a node leaves/dies. 
        The hash ring has already been updated.
        Check if any keys we hold should be re-replicated to new targets."""
        logger.info("rebalancer_node_left", dead_node_id=dead_node_id)
        # To be implemented: scan local keys and ensure they have enough replicas
        
    async def _migrate_keys_to(self, target_address: str, entries: list[CacheEntry]) -> int:
        """Send a batch of entries to target node via HTTP."""
        if not entries:
            return 0
            
        payload = [e.to_dict() for e in entries]
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(f"{target_address}/internal/full-sync", json=payload)
                if resp.status_code == 200:
                    return len(payload)
        except Exception as e:
            logger.error("migrate_keys_failed", target=target_address, error=str(e))
            
        return 0

__all__ = ["Rebalancer"]
