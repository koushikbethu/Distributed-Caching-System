"""Configuration management for the distributed cache system."""
import sys
import structlog
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Pydantic Settings class for configuration management."""
    model_config = SettingsConfigDict(env_prefix="DCACHE_")
    
    node_id: str = "node1"
    host: str = "0.0.0.0"
    rest_port: int = 8000
    grpc_port: int = 50051
    max_keys: int = 10000
    eviction_policy: str = "lru"  # "lru" or "lfu"
    default_ttl: int = 0  # 0 means no expiry
    replication_factor: int = 2
    gossip_interval_ms: int = 1000
    suspicion_timeout_ms: int = 5000
    seed_nodes: str = ""  # comma-separated "host:grpc_port" pairs
    metrics_port: int = 9090
    vnodes_per_node: int = 150
    reaper_interval_seconds: float = 1.0
    log_level: str = "INFO"
    
    @property
    def seed_node_list(self) -> list[str]:
        """Parse comma-separated seed nodes into a list."""
        if not self.seed_nodes:
            return []
        return [s.strip() for s in self.seed_nodes.split(",") if s.strip()]

def setup_logging(level: str) -> None:
    """Configure structured logging using structlog."""
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(structlog.stdlib.logging, level.upper(), structlog.stdlib.logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )

__all__ = ["Settings", "setup_logging"]
