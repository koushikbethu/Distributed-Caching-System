import asyncio
import random
import time
from typing import Callable, Optional
import httpx
import structlog
from src.config import Settings
from src.metrics.prometheus import CacheMetrics
from src.cluster.node import NodeInfo, NodeState

logger = structlog.get_logger()

class GossipProtocol:
    """SWIM-lite gossip protocol for failure detection and membership management."""
    def __init__(self, local_node: NodeInfo, settings: Settings, 
                 on_node_alive: Callable, on_node_suspect: Callable,
                 on_node_dead: Callable, metrics: CacheMetrics):
        self._members: dict[str, NodeInfo] = {}  # node_id -> NodeInfo
        self._local_node = local_node
        self._settings = settings
        self._running = False
        self._gossip_task: Optional[asyncio.Task] = None
        self._suspect_timers: dict[str, asyncio.Task] = {}
        # Callbacks for membership changes
        self._on_node_alive = on_node_alive
        self._on_node_suspect = on_node_suspect
        self._on_node_dead = on_node_dead
        self._metrics = metrics
        
        # Add self to members
        self._members[self._local_node.node_id] = self._local_node
        
    async def start(self) -> None:
        """Start the gossip protocol background task."""
        if self._running:
            return
        self._running = True
        self._gossip_task = asyncio.create_task(self._gossip_loop())
        logger.info("gossip_started", node_id=self._local_node.node_id)
        
    async def stop(self) -> None:
        """Stop the gossip protocol."""
        self._running = False
        if self._gossip_task:
            self._gossip_task.cancel()
            try:
                await self._gossip_task
            except asyncio.CancelledError:
                pass
        for timer in self._suspect_timers.values():
            timer.cancel()
        logger.info("gossip_stopped")
        
    async def join(self, seed_addresses: list[str]) -> None:
        """Join cluster by contacting seed nodes via HTTP."""
        if not seed_addresses:
            return
            
        async with httpx.AsyncClient(timeout=2.0) as client:
            for seed in seed_addresses:
                try:
                    parts = seed.split(':')
                    host = parts[0]
                    target_port = parts[1] if len(parts) > 1 else str(self._settings.rest_port)
                    target = f"http://{host}:{target_port}/internal/join"
                    resp = await client.post(target, json={"node_info": self._local_node.to_dict()})
                    if resp.status_code == 200:
                        data = resp.json()
                        self._merge_membership(data.get("members", []))
                        logger.info("joined_cluster_via_seed", seed=seed)
                        break
                except Exception as e:
                    logger.warning("failed_to_join_seed", seed=seed, error=str(e))
                    
    async def _gossip_loop(self) -> None:
        """Main gossip loop - runs every gossip_interval_ms."""
        interval = self._settings.gossip_interval_ms / 1000.0
        while self._running:
            try:
                await asyncio.sleep(interval)
                peers = [m for id, m in self._members.items() if id != self._local_node.node_id and m.state in (NodeState.ALIVE, NodeState.SUSPECT)]
                if not peers:
                    continue
                target = random.choice(peers)
                
                # Send ping
                success = await self._send_ping(target)
                if not success:
                    # Indirect ping
                    k_peers = random.sample(peers, min(3, len(peers)))
                    indirect_success = False
                    for peer in k_peers:
                        if peer.node_id == target.node_id:
                            continue
                        res = await self._send_ping_req(peer, target.node_id)
                        if res:
                            indirect_success = True
                            break
                    if not indirect_success:
                        await self._mark_suspect(target.node_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("gossip_loop_error", error=str(e))
                
    async def _send_ping(self, target: NodeInfo) -> bool:
        """Send direct ping."""
        self._metrics.record_gossip_message("ping")
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                payload = {
                    "sender": self._local_node.to_dict(),
                    "membership_updates": [m.to_dict() for m in self._members.values()]
                }
                resp = await client.post(f"{target.rest_address}/internal/ping", json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    self._merge_membership(data.get("membership_updates", []))
                    return True
        except Exception:
            pass
        return False
        
    async def _send_ping_req(self, peer: NodeInfo, target_id: str) -> bool:
        """Send indirect ping request."""
        self._metrics.record_gossip_message("ping_req")
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                payload = {
                    "target_node_id": target_id,
                    "sender": self._local_node.to_dict()
                }
                resp = await client.post(f"{peer.rest_address}/internal/ping-req", json=payload)
                return resp.status_code == 200 and resp.json().get("success", False)
        except Exception:
            pass
        return False
        
    async def handle_ping(self, sender_info: dict, membership_updates: list[dict]) -> dict:
        """Handle incoming ping. Return own state + membership updates."""
        self._merge_membership([sender_info] + membership_updates)
        return {
            "sender": self._local_node.to_dict(),
            "membership_updates": [m.to_dict() for m in self._members.values()]
        }
        
    async def handle_ping_req(self, target_node_id: str, sender_info: dict) -> bool:
        """Handle indirect ping request. Ping target and return result."""
        self._merge_membership([sender_info])
        target = self._members.get(target_node_id)
        if target:
            return await self._send_ping(target)
        return False
        
    def _merge_membership(self, updates: list[dict]) -> None:
        """Merge remote membership info with local state."""
        for update in updates:
            node = NodeInfo.from_dict(update)
            if node.node_id == self._local_node.node_id:
                if node.state in (NodeState.SUSPECT, NodeState.DEAD) and node.incarnation >= self._local_node.incarnation:
                    self._local_node.incarnation = node.incarnation + 1
                continue
                
            existing = self._members.get(node.node_id)
            if not existing:
                self._members[node.node_id] = node
                if node.state == NodeState.ALIVE:
                    asyncio.create_task(self._on_node_alive(node))
            else:
                # State priority: ALIVE (1) < SUSPECT (2) < DEAD (3)
                state_priority = {NodeState.ALIVE: 1, NodeState.SUSPECT: 2, NodeState.DEAD: 3, NodeState.LEAVING: 0}
                new_prio = state_priority.get(node.state, 0)
                old_prio = state_priority.get(existing.state, 0)

                if node.incarnation > existing.incarnation or (node.incarnation == existing.incarnation and new_prio > old_prio):
                    old_state = existing.state
                    self._members[node.node_id] = node

                    if old_state != NodeState.ALIVE and node.state == NodeState.ALIVE:
                        asyncio.create_task(self._on_node_alive(node))
                    elif old_state != NodeState.SUSPECT and node.state == NodeState.SUSPECT:
                        asyncio.create_task(self._mark_suspect(node.node_id))
                    elif old_state != NodeState.DEAD and node.state == NodeState.DEAD:
                        asyncio.create_task(self._on_node_dead(node))
                        
    async def _mark_suspect(self, node_id: str) -> None:
        node = self._members.get(node_id)
        if node and node.state != NodeState.DEAD:
            node.state = NodeState.SUSPECT
            logger.warning("node_suspect", suspect_id=node_id)
            await self._on_node_suspect(node)
            if node_id not in self._suspect_timers:
                self._suspect_timers[node_id] = asyncio.create_task(self._start_suspicion_timer(node_id))

    async def _start_suspicion_timer(self, node_id: str) -> None:
        """Start a timer for a suspect node. If timer expires, mark DEAD."""
        try:
            await asyncio.sleep(self._settings.suspicion_timeout_ms / 1000.0)
            node = self._members.get(node_id)
            if node and node.state == NodeState.SUSPECT:
                node.state = NodeState.DEAD
                logger.warning("node_dead", dead_id=node_id)
                await self._on_node_dead(node)
        except asyncio.CancelledError:
            pass
        finally:
            self._suspect_timers.pop(node_id, None)
            
    def get_members(self) -> dict[str, NodeInfo]:
        """Get current membership list."""
        return self._members.copy()
        
    def get_alive_members(self) -> list[NodeInfo]:
        """Get all ALIVE members (excluding self)."""
        return [m for id, m in self._members.items() if id != self._local_node.node_id and m.state == NodeState.ALIVE]
        
    @property
    def member_count(self) -> int:
        return len(self._members)

__all__ = ["GossipProtocol"]
