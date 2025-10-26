from typing import Any, Callable, Optional, List, Dict, Set
from hashlib import sha256
import bisect
import threading


class ConsistentHashRing:
    def __init__(
        self,
        vnode_count: int = 100,
        replication_factor: int = 3,
        hash_function: Optional[Callable[[str], int]] = None,
    ):
        self.vnode_count = vnode_count
        self.replication_factor = replication_factor

        if hash_function is None:
            self.hash_function = self._hash_to_int
        else:
            self.hash_function = hash_function

        self.ring: List[int] = []
        self.vnode_map: Dict[int, str] = {}
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()

    def _hash_to_int(self, key: str) -> int:
        data = key.encode("utf-8")
        digest = sha256(data).digest()
        first_8_bytes = digest[:8]
        return int.from_bytes(first_8_bytes, byteorder="big")

    def add_node(self, node_id: str, weight: float = 1.0) -> None:
        with self.lock:
            if node_id in self.nodes:
                print(f"Warning: Node '{node_id}' already exists.")
                return

            self.nodes[node_id] = {"weight": weight}
            total_vnodes = int(self.vnode_count * weight)
            for i in range(total_vnodes):
                vnode_key = f"{node_id}-{i}"
                vnode_hash = self.hash_function(vnode_key)
                collision_count = 0
                while vnode_hash in self.vnode_map:
                    collision_count += 1
                    print(
                        f"Warning: Hash collision detected for {vnode_key}. Retrying with salt."
                    )
                    vnode_hash = self.hash_function(f"{vnode_key}_{collision_count}")
                self.ring.append(vnode_hash)
                self.vnode_map[vnode_hash] = node_id
            self.ring.sort()
            print(f"Node '{node_id}' added with {total_vnodes} virtual nodes.")

    def get_node(self, key: str) -> Optional[str]:
        with self.lock:
            if not self.ring:
                print("Warning: No nodes in the hash ring.")
                return None

            key_hash = self.hash_function(key)
            idx = bisect.bisect_left(self.ring, key_hash) % len(self.ring)
            vnode_hash = self.ring[idx]
            return self.vnode_map[vnode_hash]

    def get_nodes_for_key(
        self, key: str, replica_count: Optional[int] = None
    ) -> List[str]:
        with self.lock:
            if not self.ring:
                print("Warning: No nodes in the hash ring.")
                return []

            if replica_count is None:
                replica_count = self.replication_factor
            if replica_count > len(self.nodes):
                print(
                    "Warning: Requested replica count exceeds number of available nodes. Adjusting to maximum available nodes."
                )
                replica_count = len(self.nodes)
            key_hash = self.hash_function(key)
            idx = bisect.bisect_left(self.ring, key_hash) % len(self.ring)

            selected_nodes = set()
            result = []
            while len(result) < replica_count:
                vnode_hash = self.ring[idx]
                node_id = self.vnode_map[vnode_hash]
                if node_id not in selected_nodes:
                    selected_nodes.add(node_id)
                    result.append(node_id)
                idx = (idx + 1) % len(self.ring)

        return result

    def remove_node(self, node_id: str) -> None:
        with self.lock:
            if node_id not in self.nodes:
                print(f"Warning: Node '{node_id}' does not exist.")
                return

            node_info = self.nodes[node_id]
            weight = node_info.get("weight", 1.0)
            total_vnodes = int(self.vnode_count * weight)
            hashes_to_remove: Set[int] = set()
            for i in range(total_vnodes):
                vnode_key = f"{node_id}-{i}"
                vnode_hash = self.hash_function(vnode_key)
                collision_count = 0

                # Handle potential collisions during removal
                while (
                    vnode_hash in self.vnode_map
                    and self.vnode_map[vnode_hash] != node_id
                ):
                    collision_count += 1
                    vnode_hash = self.hash_function(f"{vnode_key}_{collision_count}")
                if (
                    vnode_hash in self.vnode_map
                    and self.vnode_map[vnode_hash] == node_id
                ):
                    hashes_to_remove.add(vnode_hash)
                else:
                    # This should not happen if add_node and remove_node are symmetric
                    print(
                        f"Warning: Virtual node for '{vnode_key}' not found during removal."
                    )
            new_ring: List[int] = []
            new_vnode_map: Dict[int, str] = {}
            for vnode_hash in self.ring:
                if vnode_hash not in hashes_to_remove:
                    new_ring.append(vnode_hash)
                    new_vnode_map[vnode_hash] = self.vnode_map[vnode_hash]

            self.ring = new_ring
            self.vnode_map = new_vnode_map
            del self.nodes[node_id]
            print(
                f"Node '{node_id}' removed with {len(hashes_to_remove)} virtual nodes from the hash ring."
            )
