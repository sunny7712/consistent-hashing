# Consistent Hashing in Python

This repository contains a robust, Python implementation of a consistent hashing ring. This algorithm is fundamental for building scalable distributed systems, such as distributed caches (like Memcached or Redis clusters), databases, and load balancers.

The primary goal of consistent hashing is to minimize key remapping when the number of nodes (servers) changes, ensuring that only a fraction of keys (approximately `1/N`, where `N` is the number of nodes) need to be moved when a node joins or leaves the cluster.

---

## Features

* **Dynamic Node Management**: Add and remove nodes from the ring at runtime with `add_node()` and `remove_node()`.
* **Virtual Nodes (vnodes)**: Uses virtual nodes to ensure a more uniform key distribution across all physical nodes.
* **Weighted Nodes**: Assign different weights to physical nodes (e.g., based on server capacity), and the library will proportionally assign vnodes.
* **Replication Support**: Find `R` distinct physical nodes for any key using `get_nodes_for_key()` to support fault tolerance.
* **Fast Lookups**: Key lookups are performed in $O(\log V)$ time (where $V$ is the total number of vnodes) using binary search (`bisect`).
* **Deterministic Hashing**: Uses SHA-256 for deterministic key and vnode mapping.
* **Hash Collision Handling**: Deterministically resolves rare hash collisions for vnode positions.

---

## Files in This Repository

* `consistent_hash.py`: The core library containing the `ConsistentHashRing` class. This is the only file you need to import into your own project.
* `test_consistent_hash.py`: A complete `unittest` suite that validates correctness, consistency, and the "minimal movement" property of the ring.
* `simulation.py`: A standalone simulation script that measures key distribution and the impact of adding/removing nodes.

---

## How to Use

### 1. As a Library in Your Project

Simply copy `consistent_hash.py` into your project and import the `ConsistentHashRing` class.

```python
from consistent_hash import ConsistentHashRing

# 1. Initialize the ring
# (100 vnodes per node, 3 replicas by default)
ring = ConsistentHashRing(vnode_count=100, replication_factor=3)

# 2. Add your cache servers
ring.add_node("cache-server-1")
ring.add_node("cache-server-2", weight=1.5) # This node gets 1.5x vnodes
ring.add_node("cache-server-3")

# 3. Find the primary node for a key
key = "user:profile:12345"
primary_node = ring.get_node(key)
print(f"The key '{key}' belongs to: {primary_node}")
# Output: The key 'user:profile:12345' belongs to: cache-server-2

# 4. Find all nodes for replication
replica_nodes = ring.get_nodes_for_key(key)
print(f"Store replicas on: {replica_nodes}")
# Output: Store replicas on: ['cache-server-2', 'cache-server-3', 'cache-server-1']

# 5. Handle a node failure
ring.remove_node("cache-server-3")
print("\nCache server 3 failed...")

# The key is automatically remapped to the next node
new_primary_node = ring.get_node(key)
print(f"The key '{key}' now belongs to: {new_primary_node}")
# Output: The key 'user:profile:12345' now belongs to: cache-server-1
```

## API Overview

### ConsistentHashRing

#### `__init__(self, vnode_count=100, replication_factor=3, hash_function=None)`
Creates a new ring.

* `vnode_count`: The default number of virtual nodes to create per physical node.
* `replication_factor`: The default number of distinct nodes to return for replication.
* `hash_function`: An optional `Callable[[str], int]` to override the default SHA-256 hash.

#### `add_node(self, node_id, weight=1.0)`
Adds a physical node to the ring.

* `node_id (str)`: A unique ID for the node.
* `weight (float)`: A multiplier for the vnode_count. A node with weight=2.0 will get twice as many vnodes as a node with weight=1.0.

#### `remove_node(self, node_id)`
Removes a physical node and all its vnodes from the ring. Keys will be automatically remapped to their next successor.

#### `get_node(self, key)`
Returns the single primary physical node ID (str) responsible for the given key.

#### `get_nodes_for_key(self, key, replica_count=None)`
Returns a list of distinct physical node IDs for storing replicas.

* `key (str)`: The key to map.
* `replica_count (int, optional)`: The number of distinct nodes to find. Defaults to self.replication_factor.

## Future Improvements

While this implementation is a correct and functional data structure, several key considerations must be addressed to make it truly production-ready in a large-scale, concurrent environment.

* **Thread Safety**: The current `ConsistentHashRing` class is **not thread-safe**. If a single instance of the ring is shared between multiple threads, read operations (`get_node`) can race with write operations (`add_node`, `remove_node`), leading to crashes or incorrect results. A production implementation would require an internal lock (like `threading.Lock`) to protect access to the shared `ring` and `vnode_map` data structures.

* **Distributed State Management**: This library only manages its *local* state. In a real distributed system, multiple clients (routers) would each have their own instance of this class. A mechanism is required to ensure all instances have an identical view of the ring's state.
    * **The Problem**: If `Router-A` adds a new node but `Router-B` doesn't, they will have different rings and map the same key to different servers, leading to catastrophic cache misses and data inconsistency.
    * **The Solution**: The "source of truth" for the node list must be externalized. This is typically solved using a service discovery tool like **ZooKeeper**, **etcd**, or **Consul**. All router instances would "watch" the coordination service for changes to the node list and update their local rings accordingly, ensuring all clients converge on the same ring state.