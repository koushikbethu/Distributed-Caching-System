import pytest
import time
from src.core.entry import CacheEntry
from src.core.ttl import TTLManager
from src.core.store import CacheStore

def test_entry_expires():
    entry = CacheEntry("k1", "v1", ttl=0.1)
    time.sleep(0.15)
    assert entry.is_expired() is True

def test_entry_no_ttl_never_expires():
    entry = CacheEntry("k1", "v1")
    assert entry.is_expired() is False

def test_ttl_manager_tracks_expiry():
    manager = TTLManager()
    now = time.time()
    manager.add("k1", now + 0.1)
    manager.add("k2", now + 0.5)
    
    assert not manager.get_expired_keys()
    
    time.sleep(0.15)
    expired = manager.get_expired_keys()
    assert expired == ["k1"]

def test_ttl_manager_remove():
    manager = TTLManager()
    now = time.time()
    manager.add("k1", now + 0.1)
    manager.remove("k1")
    
    time.sleep(0.15)
    assert not manager.get_expired_keys()

def test_store_get_expired_returns_none(store):
    store.set("k1", "v1", ttl=0.1)
    time.sleep(0.15)
    assert store.get("k1") is None

def test_store_exists_expired_returns_false(store):
    store.set("k1", "v1", ttl=0.1)
    time.sleep(0.15)
    assert store.exists("k1") is False

def test_reaper_thread_cleans_expired():
    store = CacheStore(reaper_interval=0.1)
    store.start()
    store.set("k1", "v1", ttl=0.2)
    time.sleep(0.4)
    # The reaper should have deleted it by now
    assert store.size() == 0
    store.stop()
