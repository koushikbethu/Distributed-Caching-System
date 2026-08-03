import time
from fastapi import FastAPI, HTTPException, Request, Response
import structlog

from src.config import Settings
from src.metrics.prometheus import CacheMetrics
from src.cluster.coordinator import ClusterCoordinator
from src.cluster.node import NodeInfo
from src.api.models import (
    SetRequest, GetResponse, DeleteResponse, ExistsResponse,
    SetResponse, NodeInfoResponse, ClusterStatusResponse,
    HealthResponse, ErrorResponse, PingRequest, PingResponse,
    PingReqRequest, ReplicateRequest, FullSyncRequest, JoinRequest
)
from src.api.middleware import RequestIdMiddleware, LoggingMiddleware, MetricsMiddleware

logger = structlog.get_logger()

def create_app(coordinator: ClusterCoordinator, settings: Settings, metrics: CacheMetrics) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Distributed Cache API",
        description="Production-grade distributed caching system",
        version="1.0.0"
    )
    
    # Add middleware (order matters, bottom to top in starlette)
    app.add_middleware(MetricsMiddleware, metrics=metrics)
    app.add_middleware(LoggingMiddleware)
    app.add_middleware(RequestIdMiddleware)
    
    # Store coordinator and metrics in app.state
    app.state.coordinator = coordinator
    app.state.settings = settings
    app.state.metrics = metrics
    app.state.start_time = time.time()
    
    # === PUBLIC CACHE ENDPOINTS ===
    
    @app.put("/cache/{key}", response_model=SetResponse)
    async def set_key(key: str, request: SetRequest):
        """Set a key-value pair in the cache."""
        result = await coordinator.handle_set(key, request.value, request.ttl)
        return SetResponse(key=key, stored=result, node_id=settings.node_id)
    
    @app.get("/cache/{key}", response_model=GetResponse)
    async def get_key(key: str):
        """Get a value by key."""
        value, is_local = await coordinator.handle_get(key)
        if value is None:
            raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
        return GetResponse(key=key, value=value, found=True, node_id=settings.node_id)
    
    @app.delete("/cache/{key}", response_model=DeleteResponse)
    async def delete_key(key: str):
        """Delete a key from the cache."""
        result = await coordinator.handle_delete(key)
        return DeleteResponse(key=key, deleted=result)
    
    @app.head("/cache/{key}")
    async def exists_key(key: str):
        """Check if a key exists. Returns 200 if exists, 404 if not."""
        exists = await coordinator.handle_exists(key)
        if not exists:
            raise HTTPException(status_code=404)
        return Response(status_code=200)
    
    # === CLUSTER ENDPOINTS ===
    
    @app.get("/cluster/status", response_model=ClusterStatusResponse)
    async def cluster_status():
        """Get cluster topology and health info."""
        status = coordinator.get_cluster_status()
        
        nodes_info = []
        for n_id, n_info in status.get("nodes", {}).items():
            nodes_info.append(NodeInfoResponse(
                node_id=n_info.node_id,
                host=n_info.host,
                rest_port=n_info.rest_port,
                grpc_port=n_info.grpc_port,
                state=n_info.state.name,
                last_heartbeat=n_info.last_heartbeat
            ))
            
        return ClusterStatusResponse(
            node_id=settings.node_id,
            state="ALIVE",
            cluster_size=len(nodes_info),
            nodes=nodes_info,
            total_keys=status.get("total_keys", 0),
            eviction_policy=settings.eviction_policy
        )
    
    @app.get("/cluster/nodes")
    async def list_nodes():
        """List all nodes and their states."""
        members = coordinator.gossip.get_members()
        return {"nodes": {n_id: n.to_dict() for n_id, n in members.items()}}
    
    # === OBSERVABILITY ENDPOINTS ===
    
    @app.get("/metrics")
    async def prometheus_metrics():
        """Prometheus metrics endpoint."""
        return Response(
            content=metrics.generate_metrics(),
            media_type="text/plain; version=0.0.4; charset=utf-8"
        )
    
    @app.get("/health", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        uptime = time.time() - app.state.start_time
        return HealthResponse(
            status="healthy",
            node_id=settings.node_id,
            uptime_seconds=uptime
        )
    
    # === INTERNAL ENDPOINTS (used by gossip/replication, not for public use) ===
    # Security Note: In production, these should be secured (e.g., via mTLS or internal tokens)
    
    @app.post("/internal/ping", response_model=PingResponse)
    async def internal_ping(request: PingRequest):
        """Handle gossip ping from peer node."""
        result = await coordinator.gossip.handle_ping(
            request.sender, request.membership_updates
        )
        return PingResponse(**result)
    
    @app.post("/internal/ping-req")
    async def internal_ping_req(request: PingReqRequest):
        """Handle indirect ping request."""
        success = await coordinator.gossip.handle_ping_req(
            request.target_node_id, request.sender
        )
        return {"success": success}
    
    @app.post("/internal/join")
    async def internal_join(request: JoinRequest):
        """Handle node join request."""
        # Add the joining node to gossip membership
        node_info = NodeInfo.from_dict(request.node_info)
        
        # Typically handled by coordinator, mock if needed:
        if hasattr(coordinator, '_on_node_alive'):
            await coordinator._on_node_alive(node_info)
            
        # Return current membership
        members = coordinator.gossip.get_members()
        return {"members": [m.to_dict() for m in members.values()]}
    
    @app.post("/internal/replicate")
    async def internal_replicate(request: ReplicateRequest):
        """Handle replication from leader node."""
        result = await coordinator.replication.handle_replicate(request.model_dump())
        return {"success": result}
    
    @app.post("/internal/full-sync")
    async def internal_full_sync(request: FullSyncRequest):
        """Handle full sync from another node."""
        count = await coordinator.replication.handle_full_sync(request.entries)
        return {"synced": count}
    
    return app

__all__ = ["create_app"]
