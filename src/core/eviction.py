"""Pluggable eviction policies for cache storage."""
from abc import ABC, abstractmethod
from typing import Optional
from collections import OrderedDict
import structlog

logger = structlog.get_logger(__name__)

class EvictionPolicy(ABC):
    """Abstract base class for eviction policies."""
    
    @abstractmethod
    def on_access(self, key: str) -> None:
        """Called when a key is accessed."""
        pass

    @abstractmethod
    def on_insert(self, key: str) -> None:
        """Called when a new key is inserted."""
        pass

    @abstractmethod
    def on_delete(self, key: str) -> None:
        """Called when a key is deleted."""
        pass

    @abstractmethod
    def evict(self) -> Optional[str]:
        """Evict a key based on the policy. Returns the evicted key, or None if empty."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear all state from the policy."""
        pass

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of keys tracked."""
        pass

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """Check if a key is tracked by the policy."""
        pass

class LRUPolicy(EvictionPolicy):
    """Least Recently Used (LRU) eviction policy."""
    
    def __init__(self, max_keys: int):
        self.max_keys = max_keys
        self._keys: OrderedDict[str, None] = OrderedDict()
    
    def on_access(self, key: str) -> None:
        if key in self._keys:
            self._keys.move_to_end(key)
            
    def on_insert(self, key: str) -> None:
        self._keys[key] = None
        self._keys.move_to_end(key)
        
    def on_delete(self, key: str) -> None:
        self._keys.pop(key, None)
        
    def evict(self) -> Optional[str]:
        if not self._keys:
            return None
        key, _ = self._keys.popitem(last=False)
        logger.debug("evicted_key", policy="lru", key=key)
        return key
        
    def clear(self) -> None:
        self._keys.clear()
        
    def __len__(self) -> int:
        return len(self._keys)
        
    def __contains__(self, key: str) -> bool:
        return key in self._keys

class LFUPolicy(EvictionPolicy):
    """Least Frequently Used (LFU) eviction policy."""
    
    def __init__(self, max_keys: int):
        self.max_keys = max_keys
        self._key_to_freq: dict[str, int] = {}
        self._freq_to_keys: dict[int, OrderedDict[str, None]] = {}
        self._min_freq: int = 0
        
    def on_access(self, key: str) -> None:
        if key not in self._key_to_freq:
            return
            
        freq = self._key_to_freq[key]
        self._freq_to_keys[freq].pop(key, None)
        
        if not self._freq_to_keys[freq] and self._min_freq == freq:
            self._min_freq += 1
            
        new_freq = freq + 1
        self._key_to_freq[key] = new_freq
        if new_freq not in self._freq_to_keys:
            self._freq_to_keys[new_freq] = OrderedDict()
        self._freq_to_keys[new_freq][key] = None
        
    def on_insert(self, key: str) -> None:
        self._key_to_freq[key] = 1
        if 1 not in self._freq_to_keys:
            self._freq_to_keys[1] = OrderedDict()
        self._freq_to_keys[1][key] = None
        self._min_freq = 1
        
    def on_delete(self, key: str) -> None:
        if key not in self._key_to_freq:
            return
        freq = self._key_to_freq.pop(key)
        self._freq_to_keys[freq].pop(key, None)
        
    def evict(self) -> Optional[str]:
        if not self._key_to_freq:
            return None
            
        while self._min_freq in self._freq_to_keys and not self._freq_to_keys[self._min_freq]:
            self._min_freq += 1
            if self._min_freq > max(self._freq_to_keys.keys(), default=0):
                return None
                
        keys_dict = self._freq_to_keys.get(self._min_freq)
        if not keys_dict:
            return None
            
        key, _ = keys_dict.popitem(last=False)
        self._key_to_freq.pop(key, None)
        logger.debug("evicted_key", policy="lfu", key=key)
        return key

    def clear(self) -> None:
        self._key_to_freq.clear()
        self._freq_to_keys.clear()
        self._min_freq = 0
        
    def __len__(self) -> int:
        return len(self._key_to_freq)
        
    def __contains__(self, key: str) -> bool:
        return key in self._key_to_freq

def create_eviction_policy(name: str, max_keys: int) -> EvictionPolicy:
    """Factory function to create an eviction policy by name."""
    if name.lower() == "lfu":
        return LFUPolicy(max_keys)
    return LRUPolicy(max_keys)

__all__ = ["EvictionPolicy", "LRUPolicy", "LFUPolicy", "create_eviction_policy"]
