from pydantic import BaseModel, Field
from typing import Optional, Any

class SetRequest(BaseModel):
    value: str = Field(..., description="Value to cache")
    ttl: Optional[int] = Field(None, description="Time-to-live in seconds", ge=1)

class GetResponse(BaseModel):
    key: str
    value: Optional[str] = None
    found: bool = True
    node_id: Optional[str] = None  # Which node served the response

class DeleteResponse(BaseModel):
    key: str
    deleted: bool

class ExistsResponse(BaseModel):
    key: str
    exists: bool

class SetResponse(BaseModel):
    key: str
    stored: bool
    node_id: Optional[str] = None

class NodeInfoResponse(BaseModel):
    node_id: str
    host: str
    rest_port: int
    grpc_port: int
    state: str
    last_heartbeat: float

class ClusterStatusResponse(BaseModel):
    node_id: str
    state: str
    cluster_size: int
    nodes: list[NodeInfoResponse]
    total_keys: int
    eviction_policy: str

class HealthResponse(BaseModel):
    status: str = "healthy"
    node_id: str = ""
    uptime_seconds: float = 0.0

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None

# Internal models for inter-node communication
class PingRequest(BaseModel):
    sender: dict
    membership_updates: list[dict] = []

class PingResponse(BaseModel):
    sender: dict
    membership_updates: list[dict] = []
    ack: bool = True

class PingReqRequest(BaseModel):
    target_node_id: str
    sender: dict

class ReplicateRequest(BaseModel):
    key: str
    value: str
    ttl: Optional[int] = None
    operation: str = "set"  # "set" or "delete"
    sequence_number: int = 0

class FullSyncRequest(BaseModel):
    entries: list[dict]
    source_node_id: str

class JoinRequest(BaseModel):
    node_info: dict
