"""TTL management with lazy expiry and background reaper."""
import heapq
import threading
import time
from typing import List, Callable, Dict
import structlog

logger = structlog.get_logger(__name__)

class TTLManager:
    """Manages time-to-live for cache entries using a min-heap."""
    
    def __init__(self):
        self._heap: List[tuple[float, str]] = []
        self._removed: set[str] = set()
        self._lock = threading.Lock()
        
    def add(self, key: str, expires_at: float) -> None:
        """Add a key with an expiration time."""
        with self._lock:
            heapq.heappush(self._heap, (expires_at, key))
            self._removed.discard(key)
            
    def remove(self, key: str) -> None:
        """Mark a key as removed (lazy deletion from heap)."""
        with self._lock:
            self._removed.add(key)
            
    def is_expired(self, key: str) -> bool:
        """
        Check if a specific key is expired. 
        Note: This doesn't actually check the heap efficiently for arbitrary keys.
        Usually caller knows expires_at or we rely on Reaper to clean up.
        This function just returns False as it's meant to be implemented differently or relies on CacheEntry.
        """
        return False

    def get_expired_keys(self) -> List[str]:
        """Get all currently expired keys (for reaper) and remove them from heap."""
        expired = []
        now = time.time()
        with self._lock:
            while self._heap and self._heap[0][0] <= now:
                expires_at, key = heapq.heappop(self._heap)
                if key in self._removed:
                    self._removed.remove(key)
                    continue
                expired.append(key)
        return expired
        
    def clear(self) -> None:
        """Clear all TTL data."""
        with self._lock:
            self._heap.clear()
            self._removed.clear()


class ReaperThread(threading.Thread):
    """Background thread that periodically reaps expired keys."""
    
    def __init__(self, ttl_manager: TTLManager, on_expire: Callable[[str], None], interval: float = 1.0):
        super().__init__(daemon=True, name="TTLReaperThread")
        self.ttl_manager = ttl_manager
        self.on_expire = on_expire
        self.interval = interval
        self._stop_event = threading.Event()
        
    def stop(self) -> None:
        """Stop the reaper thread."""
        self._stop_event.set()
        
    def run(self) -> None:
        """Run the background reap cycle."""
        logger.info("reaper_thread_started", interval=self.interval)
        while not self._stop_event.is_set():
            try:
                expired_keys = self.ttl_manager.get_expired_keys()
                if expired_keys:
                    logger.debug("reaping_keys", count=len(expired_keys))
                    for key in expired_keys:
                        self.on_expire(key)
            except Exception as e:
                logger.error("reaper_thread_error", exc_info=True, error=str(e))
                
            self._stop_event.wait(self.interval)
        logger.info("reaper_thread_stopped")

__all__ = ["TTLManager", "ReaperThread"]
