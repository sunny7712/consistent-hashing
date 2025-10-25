import random
import statistics
from collections import Counter
from consistent_hash_ring import ConsistentHashRing


def run_simulation(N, M, VNODE_COUNT, R):
    """
    Runs a full simulation of the consistent hash ring.

    Args:
        N (int): Number of initial nodes.
        M (int): Number of keys to test.
        VNODE_COUNT (int): Number of virtual nodes per physical node.
        R (int): Replication factor.
    """
    print("--- Consistent Hashing Simulation ---")
    print(f"Initial Nodes (N): {N}")
    print(f"Keys (M):          {M}")
    print(f"VNodes per Node:   {VNODE_COUNT}")
    print(f"Replication (R):   {R}")
    print("---------------------------------------")

    # 1. Create a large set of random keys
    keys = [f"key-{random.randint(0, M*10)}" for _ in range(M)]

    # 2. Create the ring and add initial nodes
    ring = ConsistentHashRing(vnode_count=VNODE_COUNT, replication_factor=R)
    nodes = [f"node-{i}" for i in range(N)]
    for node_id in nodes:
        ring.add_node(node_id)

    print(f"\nRing created with {N} nodes.")

    # 3. --- Measure initial distribution ---
    print("\n--- Measuring Initial Distribution ---")
    key_distribution = Counter()
    for key in keys:
        node = ring.get_node(key)
        key_distribution[node] += 1

    print("Key distribution per node:")
    for node_id in nodes:
        count = key_distribution.get(node_id, 0)
        percentage = (count / M) * 100
        print(f"  {node_id}: {count} keys ({percentage:.2f}%)")

    # Calculate distribution stats
    counts = key_distribution.values()
    mean = statistics.mean(counts)
    stddev = statistics.stdev(counts)
    print(f"\nStats (N={N}):")
    print(f"  Mean:   {mean:.2f} keys per node")
    print(f"  StdDev: {stddev:.2f} ({(stddev / mean) * 100:.2f}% of mean)")
    print(f"  Min:    {min(counts)} keys")
    print(f"  Max:    {max(counts)} keys")

    # --- 4. Measure movement on NODE JOIN ---
    print("\n--- Measuring Movement (Node Join) ---")

    # Store initial mapping (State 1)
    initial_mapping = {key: ring.get_node(key) for key in keys}

    # Add a new node
    new_node_id = f"node-{N}"
    print(f"Adding node '{new_node_id}'...")
    ring.add_node(new_node_id)

    # Check remapping and store intermediate mapping (State 2)
    intermediate_mapping = {}
    moved_count_join = 0
    for key in keys:
        new_node = ring.get_node(key)
        intermediate_mapping[key] = new_node  # Store State 2

        if initial_mapping[key] != new_node:
            moved_count_join += 1
            # In a join, keys should only move to the new node
            assert new_node == new_node_id, "Key moved to an unexpected node"

    moved_fraction = moved_count_join / M
    expected_fraction = 1.0 / (N + 1)

    print(f"Keys remapped: {moved_count_join} / {M} ({moved_fraction:.4f})")
    print(f"Expected:      ~{expected_fraction:.4f} (1 / {N+1})")

    # --- 5. Measure movement on NODE REMOVE ---
    print("\n--- Measuring Movement (Node Remove) ---")

    # We'll remove the node we just added
    print(f"Removing node '{new_node_id}'...")
    ring.remove_node(new_node_id)

    moved_count_remove = 0
    consistency_errors = 0

    for key in keys:
        # Get final mapping (State 3)
        final_node = ring.get_node(key)

        # Check 1: Did keys move *off* the new node?
        if intermediate_mapping[key] != final_node:
            moved_count_remove += 1
            # They should only have moved *if* they were on the removed node
            assert (
                intermediate_mapping[key] == new_node_id
            ), "A key moved from a non-removed node"

        # Check 2: Did the ring return to its *original* state?
        if initial_mapping[key] != final_node:
            consistency_errors += 1
            print(f"ERROR: Key {key} did not return to original state.")

    print(f"Keys remapped *off* '{new_node_id}': {moved_count_remove} / {M}")
    print(f"Total consistency errors: {consistency_errors}")

    # The real tests:
    assert consistency_errors == 0, "Ring did not return to its original state"
    assert (
        moved_count_join == moved_count_remove
    ), "Move-on count does not equal move-off count"

    print("\nSimulation complete. Ring is consistent.")


if __name__ == "__main__":
    # --- Configurable Parameters ---
    NUM_NODES = 10
    NUM_KEYS = 100000
    VNODES_PER_NODE = 100
    REPLICATION_FACTOR = 3
    # -------------------------------

    run_simulation(NUM_NODES, NUM_KEYS, VNODES_PER_NODE, REPLICATION_FACTOR)
