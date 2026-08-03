"""Main thread-safe in-memory key-value store."""
import threading
import time
from typing import Optional, List, Dict
import structlog

from src.core.entry import CacheEntry
from src.core.eviction import create_eviction_policy, EvictionPolicy
from src.core.ttl import TTLManager, ReaperThread
from src.metrics.prometheus import default_metrics

logger = structlog.get_logger(__name__)

class CacheStore:
    """Thread-safe in-memory key-value store with eviction and TTL support."""
    
    def __init__(self, max_keys: int = 10000, eviction_policy: str = "lru",
                 default_ttl: int = 0, reaper_interval: float = 1.0):
        self.max_keys = max_keys
        self.default_ttl = default_ttl
        self._store: Dict[str, CacheEntry] = {}
        self._eviction: EvictionPolicy = create_eviction_policy(eviction_policy, max_keys)
        self._ttl_manager = TTLManager()
        self._lock = threading.RLock()
        self._reaper = ReaperThread(self._ttl_manager, self._on_expire, reaper_interval)
        
        logger.info("cache_store_initialized", max_keys=max_keys, policy=eviction_policy, default_ttl=default_ttl)

    def _on_expire(self, key: str) -> None:
        """Callback for the TTL reaper thread."""
        with self._lock:
            if key in self._store:
                entry = self._store[key]
                if entry.is_expired():
                    self.delete(key)
                    logger.debug("key_expired", key=key)

    def get(self, key: str) -> Optional[str]:
        """
        Get value by key. Returns None if not found or expired.
        Performs lazy TTL check. Records hit/miss metrics.
        """
        start_time = time.time()
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                default_metrics.record_miss()
                default_metrics.record_operation("get", time.time() - start_time)
                return None
                
            if entry.is_expired():
                self.delete(key)
                default_metrics.record_miss()
                default_metrics.record_operation("get", time.time() - start_time)
                return None
                
            entry.touch()
            self._eviction.on_access(key)
            default_metrics.record_hit()
            default_metrics.record_operation("get", time.time() - start_time)
            return entry.value

    def set(self, key: str, value: str, ttl: Optional[int] = None) -> bool:
        """
        Set key-value pair. Applies eviction if at capacity.
        Uses default_ttl if ttl not specified and default_ttl > 0.
        Records operation metrics.
        """
        start_time = time.time()
        with self._lock:
            if key not in self._store and len(self._store) >= self.max_keys:
                evicted_key = self._eviction.evict()
                if evicted_key:
                    self._store.pop(evicted_key, None)
                    self._ttl_manager.remove(evicted_key)
                    default_metrics.record_eviction(self._eviction.__class__.__name__)
            
            effective_ttl = ttl if ttl is not None else (self.default_ttl if self.default_ttl > 0 else None)
            entry = CacheEntry(key=key, value=value, ttl=effective_ttl)
            
            is_new = key not in self._store
            self._store[key] = entry
            
            if is_new:
                self._eviction.on_insert(key)
            else:
                self._eviction.on_access(key)
                
            if entry.expires_at is not None:
                self._ttl_manager.add(key, entry.expires_at)
                
            default_metrics.set_keys_count(len(self._store))
            default_metrics.record_operation("set", time.time() - start_time)
            return True

    def delete(self, key: str) -> bool:
        """Delete key. Returns True if key existed."""
        start_time = time.time()
        with self._lock:
            if key in self._store:
                self._store.pop(key)
                self._eviction.on_delete(key)
                self._ttl_manager.remove(key)
                default_metrics.set_keys_count(len(self._store))
                default_metrics.record_operation("delete", time.time() - start_time)
                return True
            default_metrics.record_operation("delete", time.time() - start_time)
            return False

    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return False
            if entry.is_expired():
                self.delete(key)
                return False
            return True

    def keys(self) -> List[str]:
        """Return all non-expired keys."""
        with self._lock:
            valid_keys = []
            now = time.time()
            for key, entry in list(self._store.items()):
                if entry.expires_at is not None and entry.expires_at < now:
                    self.delete(key)
                else:
                    valid_keys.append(key)
            return valid_keys

    def size(self) -> int:
        """Return count of non-expired keys."""
        return len(self.keys())

    def get_entry(self, key: str) -> Optional[CacheEntry]:
        """Get full CacheEntry (for replication). No side effects."""
        with self._lock:
            entry = self._store.get(key)
            if entry and not entry.is_expired():
                return entry
            return None

    def get_all_entries(self) -> Dict[str, CacheEntry]:
        """Get all entries (for bulk sync). No side effects."""
        with self._lock:
            now = time.time()
            return {
                k: v for k, v in self._store.items() 
                if v.expires_at is None or v.expires_at >= now
            }

    def set_entry(self, entry: CacheEntry) -> None:
        """Set entry directly (used by replication). Bypasses eviction count."""
        with self._lock:
            key = entry.key
            self._store[key] = entry
            self._eviction.on_insert(key)
            if entry.expires_at is not None:
                self._ttl_manager.add(key, entry.expires_at)
            default_metrics.set_keys_count(len(self._store))

    def start(self) -> None:
        """Start the TTL reaper thread."""
        self._reaper.start()

    def stop(self) -> None:
        """Stop the TTL reaper thread."""
        self._reaper.stop()
        self._reaper.join(timeout=2.0)

__all__ = ["CacheStore"]
