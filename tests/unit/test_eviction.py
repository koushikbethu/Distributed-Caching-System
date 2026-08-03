import pytest
from src.core.eviction import create_eviction_policy, LRUPolicy, LFUPolicy

def test_lru_evicts_least_recently_used():
    policy = LRUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    policy.on_insert("k3")
    
    # Exceed capacity is managed by CacheStore in practice, but let's test policy tracking
    # If store reaches capacity, it calls evict()
    assert len(policy) == 3
    evicted = policy.evict()
    assert evicted == "k1"
    
def test_lru_access_prevents_eviction():
    policy = LRUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    policy.on_insert("k3")
    
    # Access k1, making it most recently used
    policy.on_access("k1")
    evicted = policy.evict()
    assert evicted == "k2"  # Now k2 is least recently used

def test_lru_on_delete():
    policy = LRUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    policy.on_delete("k1")
    assert "k1" not in policy
    evicted = policy.evict()
    assert evicted == "k2"

def test_lfu_evicts_least_frequently_used():
    policy = LFUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    policy.on_insert("k3")
    
    policy.on_access("k1")
    policy.on_access("k2")
    
    # k3 has lowest frequency (1)
    evicted = policy.evict()
    assert evicted == "k3"

def test_lfu_frequency_tracking():
    policy = LFUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    policy.on_insert("k3")
    
    # k1 accessed 3 times, k2 accessed 2 times, k3 accessed 1 time
    policy.on_access("k1")
    policy.on_access("k1")
    policy.on_access("k2")
    
    evicted1 = policy.evict()
    assert evicted1 == "k3"
    evicted2 = policy.evict()
    assert evicted2 == "k2"
    evicted3 = policy.evict()
    assert evicted3 == "k1"

def test_lfu_tie_breaking():
    policy = LFUPolicy(max_keys=3)
    policy.on_insert("k1")
    policy.on_insert("k2")
    
    # Both have frequency 1
    # LRU within frequency means k1 is evicted first
    evicted = policy.evict()
    assert evicted == "k1"

def test_create_eviction_policy_factory():
    lru = create_eviction_policy("lru", 10)
    assert isinstance(lru, LRUPolicy)
    
    lfu = create_eviction_policy("lfu", 10)
    assert isinstance(lfu, LFUPolicy)

def test_invalid_policy_name():
    # Falls back to LRU
    policy = create_eviction_policy("unknown", 10)
    assert isinstance(policy, LRUPolicy)
