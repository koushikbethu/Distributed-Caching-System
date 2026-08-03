import pytest
from src.cluster.rebalancer import Rebalancer
from src.cluster.replication import ReplicationManager
from unittest.mock import AsyncMock

@pytest.fixture
def rebalancer(settings, store, hash_ring, metrics):
    rep_manager = ReplicationManager(settings, store, hash_ring, "node1", metrics)
    return Rebalancer(store, hash_ring, "node1", rep_manager, metrics)

@pytest.mark.asyncio
async def test_keys_migrate_on_node_join(rebalancer, store, hash_ring):
    hash_ring.add_node("node1")
    
    # Add a bunch of keys that hash to node1
    for i in range(100):
        store.set(f"key{i}", f"val{i}")
        
    # Simulate a new node joining
    hash_ring.add_node("node2")
    
    # Mock _migrate_keys_to to just return success without HTTP
    rebalancer._migrate_keys_to = AsyncMock(return_value=1)
    
    # on_node_joined should identify keys that now belong to node2
    migrated = await rebalancer.on_node_joined("node2", "http://node2:8000")
    
    # Some keys should have been migrated
    assert migrated > 0
    assert rebalancer._migrate_keys_to.called

@pytest.mark.asyncio
async def test_hash_ring_rebalance(hash_ring):
    hash_ring.add_node("node1")
    hash_ring.add_node("node2")
    hash_ring.add_node("node3")
    
    # Simulate a node join/leave, verify nodes shift but keys remain accessible (hash ring properties)
    mapping = {}
    for i in range(1000):
        key = f"key{i}"
        mapping[key] = hash_ring.get_node(key)
        
    hash_ring.remove_node("node2")
    
    moved = 0
    for i in range(1000):
        key = f"key{i}"
        if mapping[key] != hash_ring.get_node(key):
            moved += 1
            
    assert moved > 0
