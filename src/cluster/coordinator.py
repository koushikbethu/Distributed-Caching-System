import asyncio
from typing import Optional
import httpx
import structlog

from src.config import Settings
from src.core.store import CacheStore
from src.metrics.prometheus import CacheMetrics
from src.cluster.node import NodeInfo, NodeState
from src.cluster.hash_ring import HashRing
from src.cluster.gossip import GossipProtocol
from src.cluster.replication import ReplicationManager
from src.cluster.rebalancer import Rebalancer

logger = structlog.get_logger()

class ClusterCoordinator:
    """Central coordinator for the distributed cache cluster."""
    
    def __init__(self, settings: Settings, store: CacheStore, metrics: CacheMetrics):
        self._settings = settings
        self._store = store
        self._metrics = metrics
        self._local_node = NodeInfo(
            node_id=settings.node_id,
            host=settings.host,
            rest_port=settings.rest_port,
            grpc_port=settings.grpc_port
        )
        self._hash_ring = HashRing(vnodes_per_node=settings.vnodes_per_node)
        self._replication = ReplicationManager(settings, store, self._hash_ring, settings.node_id, metrics)
        self._rebalancer = Rebalancer(store, self._hash_ring, settings.node_id, self._replication, metrics)
        self._gossip = GossipProtocol(
            local_node=self._local_node,
            settings=settings,
            on_node_alive=self._on_node_alive,
            on_node_suspect=self._on_node_suspect,
            on_node_dead=self._on_node_dead,
            metrics=metrics
        )
        self._client = httpx.AsyncClient(timeout=5.0)
        
    async def start(self) -> None:
        """Start the cluster coordinator."""
        self._hash_ring.add_node(self._local_node.node_id)
        await self._gossip.start()
        
        seeds = [s for s in self._settings.seed_nodes.split(",") if s]
        if seeds:
            await self._gossip.join(seeds)
            
        logger.info("cluster_coordinator_started", node_id=self._local_node.node_id)
        
    async def stop(self) -> None:
        """Graceful shutdown. Leave cluster, stop gossip."""
        await self._gossip.stop()
        await self._client.aclose()
        logger.info("cluster_coordinator_stopped")
        
    async def handle_get(self, key: str) -> tuple[Optional[str], bool]:
        """Handle GET request."""
        if self.is_key_local(key):
            return self._store.get(key), True
            
        primary = self._hash_ring.get_node(key)
        if not primary or primary == self._local_node.node_id:
            return None, False
            
        members = self._gossip.get_members()
        target_node = members.get(primary)
        if not target_node:
            return None, False
            
        try:
            resp = await self._client.get(f"{target_node.rest_address}/cache/{key}")
            if resp.status_code == 200:
                return resp.json().get("value"), False
        except Exception as e:
            logger.error("forward_get_error", target=primary, key=key, error=str(e))
            
        return None, False

    async def handle_set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """Handle SET request."""
        if self.is_key_local(key):
            success = self._store.set(key, value, ttl)
            if success:
                asyncio.create_task(self._replication.replicate_write(key, value, ttl))
            return success
            
        primary = self._hash_ring.get_node(key)
        if not primary:
            return False
            
        members = self._gossip.get_members()
        target_node = members.get(primary)
        if not target_node:
            return False
            
        try:
            payload = {"value": value}
            if ttl is not None:
                payload["ttl"] = ttl
            resp = await self._client.put(f"{target_node.rest_address}/cache/{key}", json=payload)
            return resp.status_code == 200
        except Exception as e:
            logger.error("forward_set_error", target=primary, key=key, error=str(e))
            
        return False
        
    async def handle_delete(self, key: str) -> bool:
        """Handle DELETE request."""
        if self.is_key_local(key):
            success = self._store.delete(key)
            if success:
                asyncio.create_task(self._replication.replicate_delete(key))
            return success
            
        primary = self._hash_ring.get_node(key)
        if not primary:
            return False
            
        members = self._gossip.get_members()
        target_node = members.get(primary)
        if not target_node:
            return False
            
        try:
            resp = await self._client.delete(f"{target_node.rest_address}/cache/{key}")
            return resp.status_code == 200
        except Exception as e:
            logger.error("forward_delete_error", target=primary, key=key, error=str(e))
            
        return False
        
    async def handle_exists(self, key: str) -> bool:
        """Handle EXISTS request."""
        if self.is_key_local(key):
            return self._store.exists(key)
            
        primary = self._hash_ring.get_node(key)
        if not primary:
            return False
            
        members = self._gossip.get_members()
        target_node = members.get(primary)
        if not target_node:
            return False
            
        try:
            resp = await self._client.head(f"{target_node.rest_address}/cache/{key}")
            return resp.status_code == 200
        except Exception as e:
            logger.error("forward_exists_error", target=primary, key=key, error=str(e))
            
        return False
        
    def is_key_local(self, key: str) -> bool:
        """Check if a key belongs to this node based on consistent hashing."""
        node = self._hash_ring.get_node(key)
        return node == self._local_node.node_id
        
    def get_cluster_status(self) -> dict:
        """Return full cluster status."""
        return {
            "local_node": self._local_node.to_dict(),
            "members": {k: v.to_dict() for k, v in self._gossip.get_members().items()},
            "ring_nodes": self._hash_ring.get_node_count()
        }
        
    def get_node_info(self) -> NodeInfo:
        """Return local node info."""
        return self._local_node
        
    @property
    def gossip(self) -> GossipProtocol:
        return self._gossip
        
    @property
    def hash_ring(self) -> HashRing:
        return self._hash_ring
        
    @property
    def replication(self) -> ReplicationManager:
        return self._replication
        
    @property
    def rebalancer(self) -> Rebalancer:
        return self._rebalancer
        
    async def _on_node_alive(self, node_info: NodeInfo) -> None:
        """Called when a new node is detected as alive."""
        self._hash_ring.add_node(node_info.node_id)
        self._replication.update_node_address(node_info.node_id, node_info.rest_address)
        await self._rebalancer.on_node_joined(node_info.node_id, node_info.rest_address)
        
    async def _on_node_suspect(self, node_info: NodeInfo) -> None:
        """Called when a node is suspected of failure."""
        logger.warning("node_suspect", node_id=node_info.node_id)
        
    async def _on_node_dead(self, node_info: NodeInfo) -> None:
        """Called when a node is confirmed dead."""
        self._hash_ring.remove_node(node_info.node_id)
        self._replication.remove_node(node_info.node_id)
        await self._rebalancer.on_node_left(node_info.node_id)

__all__ = ["ClusterCoordinator"]
