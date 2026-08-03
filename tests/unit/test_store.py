import pytest
import threading
from src.core.store import CacheStore
from src.core.entry import CacheEntry
import time

def test_set_and_get(store):
    assert store.set("key1", "val1")
    assert store.get("key1") == "val1"

def test_get_nonexistent(store):
    assert store.get("nonexistent") is None

def test_delete(store):
    store.set("key1", "val1")
    assert store.delete("key1") is True
    assert store.get("key1") is None
    assert store.delete("nonexistent") is False

def test_exists(store):
    store.set("key1", "val1")
    assert store.exists("key1") is True
    assert store.exists("nonexistent") is False

def test_overwrite(store):
    store.set("key1", "val1")
    store.set("key1", "val2")
    assert store.get("key1") == "val2"

def test_size_and_keys(store):
    store.set("key1", "val1")
    store.set("key2", "val2")
    assert store.size() == 2
    keys = store.keys()
    assert set(keys) == {"key1", "key2"}

def test_thread_safety(store):
    def worker(worker_id):
        for i in range(100):
            key = f"key_{worker_id}_{i}"
            store.set(key, f"val_{i}")
            assert store.get(key) == f"val_{i}"
            
    threads = []
    for i in range(10):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    assert store.size() == 1000

def test_set_entry_for_replication(store):
    entry = CacheEntry(key="rep_key", value="rep_val")
    store.set_entry(entry)
    assert store.get("rep_key") == "rep_val"
    fetched_entry = store.get_entry("rep_key")
    assert fetched_entry.key == "rep_key"
    assert fetched_entry.value == "rep_val"
