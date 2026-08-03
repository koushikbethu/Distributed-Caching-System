import enum
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

class NodeState(enum.Enum):
    ALIVE = "alive"
    SUSPECT = "suspect"
    DEAD = "dead"
    LEAVING = "leaving"

@dataclass
class NodeInfo:
    node_id: str
    host: str
    rest_port: int
    grpc_port: int
    state: NodeState = NodeState.ALIVE
    last_heartbeat: float = field(default_factory=time.time)
    incarnation: int = 0
    
    @property
    def grpc_address(self) -> str:
        return f"{self.host}:{self.grpc_port}"
    
    @property
    def rest_address(self) -> str:
        return f"http://{self.host}:{self.rest_port}"
    
    def to_dict(self) -> dict:
        d = asdict(self)
        d['state'] = self.state.value
        return d
    
    @classmethod
    def from_dict(cls, data: dict) -> 'NodeInfo':
        data = data.copy()
        if isinstance(data.get('state'), str):
            data['state'] = NodeState(data['state'])
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

__all__ = ["NodeState", "NodeInfo"]
