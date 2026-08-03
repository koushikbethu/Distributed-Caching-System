# Distributed Cache Architecture

This document provides an in-depth view of the system architecture, component design, and trade-offs made in the distributed cache.

## System Architecture Overview

```mermaid
graph TD
    Client((Client))
    
    subgraph Node A
    GatewayA[FastAPI Gateway]
    CoordA[Cluster Coordinator]
    StoreA[Local Cache Store]
    RingA[Consistent Hash Ring]
    GossipA[Gossip Protocol]
    RepA[Replication Manager]
    
    GatewayA --> CoordA
    CoordA --> RingA
    CoordA --> StoreA
    CoordA --> GossipA
    CoordA --> RepA
    end
    
    subgraph Node B
    CoordB[Cluster Coordinator]
    end
    
    Client --> GatewayA
    GossipA <..> |UDP/HTTP Ping| CoordB
    RepA ==> |Async Replicate| CoordB
```

## Component Deep Dives

### Consistent Hashing
Instead of standard modular hashing (`hash(key) % N`) which causes massive data redistribution when $N$ changes, we use **Consistent Hashing**.
- **Virtual Nodes (vnodes)**: We assign 150 virtual nodes per physical node on the ring. This greatly improves the statistical distribution of keys, ensuring uniform load balancing.
- **Math**: When a node joins or leaves, only $O(K/N)$ keys need to move, where $K$ is total keys and $N$ is total nodes.
- **Ring Implementation**: Implemented using a sorted list and `bisect` for $O(\log V)$ lookups where $V$ is total virtual nodes.

```mermaid
circle
    "Hash Ring"
    "Node A (v1)"
    "Node B (v1)"
    "Node A (v2)"
    "Node C (v1)"
```

### Replication Strategy
We utilize a **Leader-Follower** (Primary-Backup) replication model for keys based on their hash placement.
- **Primary Node**: The first node found on the hash ring for a given key.
- **Replica Nodes**: The next $R-1$ distinct physical nodes on the ring.
- **Async Replication**: Writes are acknowledged immediately by the primary after local commit, and replicated asynchronously to followers to minimize write latency.
- **State Transfer**: When a new node joins, it requests a full sync from the primary node to rebuild its replica state.

### Gossip Protocol (SWIM-lite)
Cluster membership and failure detection use a decentralized, weakly-consistent gossip protocol inspired by SWIM.
- **Direct Pings**: Every interval, a node randomly selects a peer and sends a `ping`.
- **Indirect Pings**: If the direct `ping` fails, the node asks $k$ other peers to send a `ping-req` to the target.
- **Suspicion Mechanism**: Nodes that fail direct and indirect pings are marked `SUSPECT`. If they don't refute this within the timeout, they are marked `DEAD`.
- **Incarnation Numbers**: Used to resolve state conflicts. A node can increment its incarnation number to refute a false `SUSPECT` status.

### Eviction Policies
The cache supports pluggable eviction policies to bound memory usage:
- **O(1) LRU**: Implemented using a doubly-linked list backed by Python's `OrderedDict`. Accesses and insertions move keys to the end, evictions pop from the front.
- **O(1) LFU**: Implemented using frequency buckets. We maintain a dictionary of frequencies mapping to `OrderedDict`s of keys, tracking the global minimum frequency. This avoids $O(\log N)$ priority queues.

### TTL Management
We use a **Hybrid Approach**:
1. **Lazy Expiration**: Keys are checked for expiration on `GET` requests.
2. **Background Reaper**: A background asyncio task periodically scans a small sample of keys and evicts expired ones to prevent memory bloat from unaccessed keys.

## Design Tradeoff Analysis

### CAP Theorem Positioning
This system is strictly an **AP (Available and Partition-tolerant)** system.
- **Availability**: Any node can accept reads/writes. If the primary is down, requests are routed to the next available node on the ring.
- **Eventual Consistency**: Because replication is asynchronous, stale reads can occur during network partitions or immediately following a write.

### Gossip vs Centralized Coordination (e.g., ZooKeeper/etcd)
We chose Gossip to avoid a single point of failure and remove the need for deploying a separate consensus cluster (like etcd/ZooKeeper). While cluster state converges slower ($O(\log N)$ rounds), it dramatically simplifies operational overhead.

### REST + gRPC Dual Interface
- **REST**: Used for the public client-facing gateway due to ubiquity and ease of debugging.
- **gRPC (Internal)**: Built for low-latency, strongly-typed internal node-to-node communication (e.g., replication payloads and gossip pings). *(Note: Current implementation uses HTTP/REST internally for simplicity, but the architecture accommodates gRPC via `grpc_port`)*.

### Thread Safety
We use `threading.RLock()` in the `HashRing` rather than standard `Lock()` to allow re-entrant methods, though standard `asyncio.Lock()` is used in the `ReplicationManager` since the primary data plane operates within an asyncio event loop.
