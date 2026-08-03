import pytest
from unittest.mock import AsyncMock, patch
from src.cluster.coordinator import ClusterCoordinator

@pytest.fixture
def coordinator(settings, store, metrics):
    return ClusterCoordinator(settings, store, metrics)

@pytest.mark.asyncio
async def test_single_node_set_get(coordinator):
    # Hash ring only has local node
    coordinator.hash_ring.add_node(coordinator.get_node_info().node_id)
    
    # SET local
    success = await coordinator.handle_set("my_key", "my_val")
    assert success is True
    
    # GET local
    val, is_local = await coordinator.handle_get("my_key")
    assert val == "my_val"
    assert is_local is True

@pytest.mark.asyncio
async def test_coordinator_routes_to_correct_node(coordinator):
    # Add another node
    coordinator.hash_ring.add_node("test_node")
    coordinator.hash_ring.add_node("remote_node")
    
    # Mock gossip members
    coordinator.gossip._members["remote_node"] = type("Node", (), {"rest_address": "http://remote:8000"})
    
    # Find a key that hashes to remote_node
    remote_key = None
    for i in range(1000):
        key = f"key_{i}"
        if coordinator.hash_ring.get_node(key) == "remote_node":
            remote_key = key
            break
            
    assert remote_key is not None
    assert coordinator.is_key_local(remote_key) is False
    
    # Mock HTTP client
    coordinator._client.get = AsyncMock()
    coordinator._client.get.return_value.status_code = 200
    coordinator._client.get.return_value.json = lambda: {"value": "remote_val"}
    
    val, is_local = await coordinator.handle_get(remote_key)
    assert val == "remote_val"
    assert is_local is False
    coordinator._client.get.assert_called_once_with(f"http://remote:8000/cache/{remote_key}")

@pytest.mark.asyncio
async def test_cluster_status(coordinator):
    status = coordinator.get_cluster_status()
    assert "local_node" in status
    assert "members" in status
    assert "ring_nodes" in status
