import pytest
from src.cluster.hash_ring import HashRing

def test_add_node(hash_ring):
    hash_ring.add_node("node1")
    assert hash_ring.get_node("key1") == "node1"
    assert hash_ring.get_node_count() == 1

def test_consistent_mapping(hash_ring):
    hash_ring.add_node("node1")
    hash_ring.add_node("node2")
    hash_ring.add_node("node3")
    
    node = hash_ring.get_node("my_key")
    # Same key should consistently hash to the same node
    for _ in range(10):
        assert hash_ring.get_node("my_key") == node

def test_distribution_uniformity(hash_ring):
    nodes = ["node1", "node2", "node3"]
    for n in nodes:
        hash_ring.add_node(n)
        
    counts = {n: 0 for n in nodes}
    num_keys = 10000
    for i in range(num_keys):
        node = hash_ring.get_node(f"key{i}")
        counts[node] += 1
        
    # Roughly 33% each with statistical variance (±35% tolerance for 150 vnodes)
    expected = num_keys / 3
    for count in counts.values():
        assert expected * 0.65 <= count <= expected * 1.35, f"Distribution too skewed: {counts}"

def test_minimal_redistribution(hash_ring):
    for i in range(3):
        hash_ring.add_node(f"node{i}")
        
    initial_mapping = {}
    for i in range(1000):
        key = f"key{i}"
        initial_mapping[key] = hash_ring.get_node(key)
        
    # Add a 4th node
    hash_ring.add_node("node3")
    
    moved_keys = 0
    for i in range(1000):
        key = f"key{i}"
        if initial_mapping[key] != hash_ring.get_node(key):
            moved_keys += 1
            
    # With consistent hashing and 150 vnodes, ~25% of keys should move but variance is high
    # Accept range of 100-400 moved keys out of 1000
    assert 100 <= moved_keys <= 400, f"Expected 100-400 moved keys, got {moved_keys}"

def test_remove_node(hash_ring):
    hash_ring.add_node("node1")
    hash_ring.add_node("node2")
    
    affected = hash_ring.remove_node("node1")
    assert affected == {"node2"}
    assert hash_ring.get_node_count() == 1
    assert hash_ring.get_node("any_key") == "node2"

def test_get_nodes_for_replication(hash_ring):
    hash_ring.add_node("node1")
    hash_ring.add_node("node2")
    hash_ring.add_node("node3")
    hash_ring.add_node("node4")
    
    replicas = hash_ring.get_nodes("some_key", 3)
    assert len(replicas) == 3
    assert len(set(replicas)) == 3

def test_empty_ring(hash_ring):
    assert hash_ring.get_node("key") is None
    assert hash_ring.get_nodes("key", 3) == []
