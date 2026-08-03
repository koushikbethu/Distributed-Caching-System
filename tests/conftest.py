import pytest
from prometheus_client import CollectorRegistry
from src.config import Settings
from src.metrics.prometheus import CacheMetrics
from src.core.store import CacheStore
from src.cluster.hash_ring import HashRing

@pytest.fixture
def settings():
    return Settings(
        node_id="test_node",
        host="127.0.0.1",
        rest_port=8000,
        grpc_port=50051,
        max_keys=1000,
        eviction_policy="lru",
        vnodes_per_node=3
    )

@pytest.fixture
def metrics():
    registry = CollectorRegistry()
    return CacheMetrics(registry=registry)

@pytest.fixture
def store(settings):
    # Returning a CacheStore instance without starting the reaper
    return CacheStore(
        max_keys=settings.max_keys,
        eviction_policy=settings.eviction_policy,
        default_ttl=settings.default_ttl,
        reaper_interval=settings.reaper_interval_seconds
    )

@pytest.fixture
def hash_ring(settings):
    return HashRing(vnodes_per_node=settings.vnodes_per_node)
