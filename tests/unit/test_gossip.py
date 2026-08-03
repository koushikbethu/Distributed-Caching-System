import pytest
import asyncio
from src.cluster.gossip import GossipProtocol
from src.cluster.node import NodeInfo, NodeState

@pytest.fixture
def local_node():
    return NodeInfo("node1", "127.0.0.1", 8000, 50051)

@pytest.fixture
def gossip(local_node, settings, metrics):
    async def noop_alive(n): pass
    async def noop_suspect(n): pass
    async def noop_dead(n): pass
    return GossipProtocol(local_node, settings, noop_alive, noop_suspect, noop_dead, metrics)

@pytest.mark.asyncio
async def test_handle_ping_returns_ack(gossip, local_node):
    sender = NodeInfo("node2", "127.0.0.1", 8001, 50052)
    res = await gossip.handle_ping(sender.to_dict(), [])
    assert "membership_updates" in res
    assert len(res["membership_updates"]) == 2 # self + sender

@pytest.mark.asyncio
async def test_membership_merge_higher_incarnation_wins(gossip, local_node):
    node2_dict = {"node_id": "node2", "host": "127.0.0.1", "rest_port": 8001, "grpc_port": 50052, "state": "alive", "incarnation": 1, "last_heartbeat": 0}
    gossip._merge_membership([node2_dict])
    
    # Update with higher incarnation
    node2_dict["state"] = "dead"
    node2_dict["incarnation"] = 2
    gossip._merge_membership([node2_dict])
    
    members = gossip.get_members()
    assert members["node2"].state == NodeState.DEAD
    assert members["node2"].incarnation == 2

@pytest.mark.asyncio
async def test_membership_merge_suspect_overrides_alive(gossip, local_node):
    node2_dict = {"node_id": "node2", "host": "127.0.0.1", "rest_port": 8001, "grpc_port": 50052, "state": "alive", "incarnation": 1, "last_heartbeat": 0}
    gossip._merge_membership([node2_dict])
    
    # Same incarnation, SUSPECT overrides ALIVE
    node2_dict["state"] = "suspect"
    gossip._merge_membership([node2_dict])
    
    members = gossip.get_members()
    assert members["node2"].state == NodeState.SUSPECT

@pytest.mark.asyncio
async def test_get_alive_members(gossip, local_node):
    node2_dict = {"node_id": "node2", "host": "127.0.0.1", "rest_port": 8001, "grpc_port": 50052, "state": "alive", "incarnation": 1, "last_heartbeat": 0}
    node3_dict = {"node_id": "node3", "host": "127.0.0.1", "rest_port": 8002, "grpc_port": 50053, "state": "dead", "incarnation": 1, "last_heartbeat": 0}
    gossip._merge_membership([node2_dict, node3_dict])
    
    alive = gossip.get_alive_members()
    assert len(alive) == 1
    assert alive[0].node_id == "node2"

@pytest.mark.asyncio
async def test_get_members_excludes_self(gossip, local_node):
    alive = gossip.get_alive_members()
    assert len(alive) == 0
