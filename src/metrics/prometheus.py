"""Prometheus metrics using the prometheus_client library."""
import time
from typing import Optional
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest

class CacheMetrics:
    """Metrics collector for the distributed cache system."""
    
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry()
        
        self.hits = Counter('dcache_hits_total', 'Total cache hits', registry=self.registry)
        self.misses = Counter('dcache_misses_total', 'Total cache misses', registry=self.registry)
        self.evictions = Counter('dcache_evictions_total', 'Total evictions', ['policy'], registry=self.registry)
        self.keys_total = Gauge('dcache_keys_total', 'Current number of keys', registry=self.registry)
        self.memory_bytes = Gauge('dcache_memory_bytes', 'Estimated memory usage', registry=self.registry)
        self.operation_duration = Histogram(
            'dcache_operation_duration_seconds', 'Operation latency',
            ['operation'],
            buckets=(0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1),
            registry=self.registry
        )
        self.cluster_nodes = Gauge('dcache_cluster_nodes_total', 'Cluster nodes by state', ['state'], registry=self.registry)
        self.replication_lag = Histogram('dcache_replication_lag_seconds', 'Replication lag', registry=self.registry)
        self.gossip_messages = Counter('dcache_gossip_messages_total', 'Gossip messages sent', ['type'], registry=self.registry)
        self.requests_total = Counter('dcache_requests_total', 'Total API requests', ['method', 'endpoint', 'status'], registry=self.registry)
        self.request_duration = Histogram('dcache_request_duration_seconds', 'API request latency', ['method', 'endpoint'], registry=self.registry)

    def record_hit(self) -> None:
        """Record a cache hit."""
        self.hits.inc()
        
    def record_miss(self) -> None:
        """Record a cache miss."""
        self.misses.inc()
        
    def record_eviction(self, policy: str) -> None:
        """Record an eviction with the given policy."""
        self.evictions.labels(policy=policy).inc()
        
    def record_operation(self, operation: str, duration: float) -> None:
        """Record duration of a cache operation (get, set, delete)."""
        self.operation_duration.labels(operation=operation).observe(duration)
        
    def set_keys_count(self, count: int) -> None:
        """Set the current total keys gauge."""
        self.keys_total.set(count)
        
    def set_memory_bytes(self, size: int) -> None:
        """Set the estimated memory usage gauge."""
        self.memory_bytes.set(size)
        
    def set_cluster_nodes(self, state: str, count: int) -> None:
        """Set the number of nodes in a given cluster state."""
        self.cluster_nodes.labels(state=state).set(count)
        
    def record_replication_lag(self, lag: float) -> None:
        """Record lag in replication in seconds."""
        self.replication_lag.observe(lag)
        
    def record_gossip_message(self, msg_type: str) -> None:
        """Record a gossip message of a specific type."""
        self.gossip_messages.labels(type=msg_type).inc()
        
    def record_request(self, method: str, endpoint: str, status: int, duration: float) -> None:
        """Record an API request duration and status."""
        self.requests_total.labels(method=method, endpoint=endpoint, status=str(status)).inc()
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)
        
    def generate_metrics(self) -> bytes:
        """Generate metrics output in Prometheus format."""
        return generate_latest(self.registry)

# Module-level default instance for ease of use
default_metrics = CacheMetrics()

__all__ = ["CacheMetrics", "default_metrics"]
