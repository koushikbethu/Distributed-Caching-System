import pytest
from src.cluster.replication import ReplicationManager
from src.core.store import CacheStore

@pytest.fixture
def rep_manager(settings, store, hash_ring, metrics):
    return ReplicationManager(settings, store, hash_ring, settings.node_id, metrics)

@pytest.mark.asyncio
async def test_handle_replicate_applies_to_store(rep_manager, store):
    # Replicate SET
    payload = {
        "key": "rep_key",
        "value": "rep_val",
        "access_count": 1,
        "created_at": 100.0,
        "last_accessed": 100.0,
        "ttl": None,
        "expires_at": None,
    }
    success = await rep_manager.handle_replicate(payload)
    assert success is True
    assert store.get("rep_key") == "rep_val"
    
    # Replicate DELETE
    delete_payload = {"key": "rep_key", "delete": True}
    success = await rep_manager.handle_replicate(delete_payload)
    assert success is True
    assert store.get("rep_key") is None

@pytest.mark.asyncio
async def test_handle_full_sync(rep_manager, store):
    entries = [
        {"key": "k1", "value": "v1", "access_count": 1, "created_at": 100.0, "last_accessed": 100.0, "ttl": None, "expires_at": None},
        {"key": "k2", "value": "v2", "access_count": 1, "created_at": 100.0, "last_accessed": 100.0, "ttl": None, "expires_at": None},
    ]
    
    count = await rep_manager.handle_full_sync(entries)
    assert count == 2
    assert store.size() == 2
    assert store.get("k1") == "v1"
    assert store.get("k2") == "v2"
