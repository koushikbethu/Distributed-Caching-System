import hashlib
import bisect
import threading
from typing import Optional

class HashRing:
    """Consistent hashing ring with virtual nodes."""
    def __init__(self, vnodes_per_node: int = 150):
        self.vnodes_per_node = vnodes_per_node
        self._ring: list[int] = []
        self._nodes: dict[int, str] = {}
        self._physical_nodes: set[str] = set()
        self._lock = threading.RLock()
        
    def _hash(self, key: str) -> int:
        """Hash a key to a 32-bit integer using MD5."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest()[:8], 16)
        
    def add_node(self, node_id: str) -> set[str]:
        """Add node, return set of node_ids whose key ranges are affected."""
        affected_nodes = set()
        with self._lock:
            if node_id in self._physical_nodes:
                return affected_nodes
            
            for i in range(self.vnodes_per_node):
                vnode_key = f"{node_id}:{i}"
                h = self._hash(vnode_key)
                
                if self._ring:
                    idx = bisect.bisect(self._ring, h)
                    if idx < len(self._ring):
                        affected_nodes.add(self._nodes[self._ring[idx]])
                    else:
                        affected_nodes.add(self._nodes[self._ring[0]])
                
                self._ring.append(h)
                self._nodes[h] = node_id
                self._ring.sort()
            
            self._physical_nodes.add(node_id)
            affected_nodes.discard(node_id)
        return affected_nodes

    def remove_node(self, node_id: str) -> set[str]:
        """Remove node, return set of affected node_ids."""
        affected_nodes = set()
        with self._lock:
            if node_id not in self._physical_nodes:
                return affected_nodes
            
            self._physical_nodes.remove(node_id)
            
            new_ring = []
            new_nodes = {}
            for h in self._ring:
                if self._nodes[h] == node_id:
                    idx = bisect.bisect(self._ring, h)
                    next_node_id = None
                    for offset in range(len(self._ring)):
                        next_idx = (idx + offset) % len(self._ring)
                        candidate_id = self._nodes[self._ring[next_idx]]
                        if candidate_id != node_id:
                            next_node_id = candidate_id
                            break
                    if next_node_id:
                        affected_nodes.add(next_node_id)
                else:
                    new_ring.append(h)
                    new_nodes[h] = self._nodes[h]
                    
            self._ring = new_ring
            self._nodes = new_nodes
            
        return affected_nodes

    def get_node(self, key: str) -> Optional[str]:
        """Get the primary node for a key."""
        with self._lock:
            if not self._ring:
                return None
            h = self._hash(key)
            idx = bisect.bisect(self._ring, h)
            if idx == len(self._ring):
                idx = 0
            return self._nodes[self._ring[idx]]

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Get N distinct physical nodes for a key (for replication).
        Walk the ring clockwise, skipping duplicate physical nodes."""
        with self._lock:
            if not self._ring:
                return []
            
            h = self._hash(key)
            idx = bisect.bisect(self._ring, h)
            if idx == len(self._ring):
                idx = 0
                
            nodes = []
            seen = set()
            start_idx = idx
            
            while len(nodes) < count and len(seen) < len(self._physical_nodes):
                node_id = self._nodes[self._ring[idx]]
                if node_id not in seen:
                    nodes.append(node_id)
                    seen.add(node_id)
                idx = (idx + 1) % len(self._ring)
                if idx == start_idx:
                    break
                    
            return nodes

    def get_all_nodes(self) -> set[str]:
        """Get all registered physical nodes."""
        with self._lock:
            return self._physical_nodes.copy()

    def get_node_count(self) -> int:
        """Number of physical nodes."""
        with self._lock:
            return len(self._physical_nodes)

__all__ = ["HashRing"]
