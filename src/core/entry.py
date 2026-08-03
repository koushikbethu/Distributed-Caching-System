"""CacheEntry representing a single cached item."""
import time
from dataclasses import dataclass, field, asdict
from typing import Optional, Any

@dataclass
class CacheEntry:
    """Dataclass representing a single cached item."""
    key: str
    value: str
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 1
    ttl: Optional[int] = None  # seconds
    expires_at: Optional[float] = None
    
    def __post_init__(self):
        """Set expires_at based on ttl if not provided."""
        if self.ttl and self.ttl > 0 and self.expires_at is None:
            self.expires_at = self.created_at + self.ttl
    
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def touch(self) -> None:
        """Update last accessed time and access count."""
        self.last_accessed = time.time()
        self.access_count += 1
    
    def to_dict(self) -> dict:
        """Convert entry to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> 'CacheEntry':
        """Create an entry from a dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

__all__ = ["CacheEntry"]
