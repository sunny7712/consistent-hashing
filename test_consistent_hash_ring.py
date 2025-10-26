import unittest
from consistent_hash_ring import ConsistentHashRing
import threading
import random


class TestConsistentHashRing(unittest.TestCase):

    def setUp(self):
        """
        This method is called before each test.
        It creates a fresh ring for each test case.
        """
        self.nodes = ["node-a", "node-b", "node-c", "node-d"]
        self.ring = ConsistentHashRing(vnode_count=100, replication_factor=3)
        for node_id in self.nodes:
            self.ring.add_node(node_id)

    def test_lookup_consistency(self):
        """
        Tests that the same key consistently maps to the same node.
        """
        print("\nRunning test_lookup_consistency...")
        key = "my-test-key-123"

        # 1. Get the node for the key
        first_node = self.ring.get_node(key)

        # 2. Check that it's not None
        self.assertIsNotNone(first_node)
        print(f"Key '{key}' mapped to '{first_node}'")

        # 3. Loop 100 times and check that the node is *always* the same
        for i in range(100):
            node = self.ring.get_node(key)
            self.assertEqual(node, first_node)

        print("Consistency test passed.")

    def test_replication_distinct_nodes(self):
        """
        Tests that get_nodes_for_key returns R distinct nodes.
        """
        print("\nRunning test_replication_distinct_nodes...")
        key = "another-key-456"
        R = 3

        # 1. Get the nodes for the key
        nodes = self.ring.get_nodes_for_key(key, R)
        print(f"Replicas for '{key}': {nodes}")

        # 2. Assert that we got R nodes back
        self.assertEqual(len(nodes), R)

        # 3. Assert that all nodes in the list are unique
        # (Compare the length of the list to the length of a set of the list)
        self.assertEqual(len(set(nodes)), R)

        print("Replication distinctness test passed.")


    def test_minimal_movement_on_node_join(self):
        """
        Tests that when a new node is added, only a fraction
        of keys (approx. 1 / (N+1)) are remapped.
        """
        print("\nRunning test_minimal_movement_on_node_join...")

        # 1. Generate a large set of test keys
        num_keys = 10000
        keys = [f"key-{i}" for i in range(num_keys)]

        # 2. Get the initial mapping for all keys with N nodes
        initial_mapping = {}
        for key in keys:
            initial_mapping[key] = self.ring.get_node(key)

        N = len(self.nodes)  # N = 4

        # 3. Add a new node
        new_node = "node-e"
        print(f"Adding new node '{new_node}'...")
        self.ring.add_node(new_node)

        # 4. Get the new mapping
        moved_count = 0
        for key in keys:
            new_node_for_key = self.ring.get_node(key)
            if initial_mapping[key] != new_node_for_key:
                moved_count += 1

                # We also expect keys to move *only* to the new node
                self.assertEqual(new_node_for_key, new_node)

        # 5. Check the fraction of moved keys
        moved_fraction = moved_count / num_keys

        # Expected fraction is 1 / (N+1) = 1 / 5 = 0.2
        expected_fraction = 1.0 / (N + 1)

        print(f"Nodes changed from {N} to {N+1}.")
        print(f"Keys moved: {moved_count} / {num_keys} ({moved_fraction:.4f})")
        print(f"Expected fraction: {expected_fraction:.4f}")

        # Allow a reasonable margin of error (e.g., 50%)
        # With 100 vnodes, the distribution isn't perfect, so we need
        # a wide margin for the test to be stable.
        self.assertLess(
            moved_fraction, expected_fraction * 1.5, "Moved fraction is too high"
        )
        self.assertGreater(
            moved_fraction, expected_fraction * 0.5, "Moved fraction is too low"
        )
        print("Minimal movement on join test passed.")

    def test_minimal_movement_on_node_remove(self):
        """
        Tests that when a node is removed, its keys are
        distributed among the remaining nodes.
        """
        print("\nRunning test_minimal_movement_on_node_remove...")

        # 1. Generate keys
        num_keys = 10000
        keys = [f"key-{i}" for i in range(num_keys)]

        # 2. Get initial mapping
        initial_mapping = {}
        for key in keys:
            initial_mapping[key] = self.ring.get_node(key)

        N = len(self.nodes)  # N = 4
        node_to_remove = "node-c"

        # 3. Get the set of keys that *belonged* to the removed node
        keys_on_removed_node = {
            k for k, v in initial_mapping.items() if v == node_to_remove
        }
        print(f"Keys on removed node '{node_to_remove}': {len(keys_on_removed_node)}")

        # 4. Remove the node
        print(f"Removing node '{node_to_remove}'...")
        self.ring.remove_node(node_to_remove)

        # 5. Check the new mapping
        moved_count = 0
        remap_distribution = {}

        for key in keys:
            new_node_for_key = self.ring.get_node(key)
            old_node = initial_mapping[key]

            if old_node != new_node_for_key:
                moved_count += 1

                # Assert that only keys from the removed node were moved
                self.assertEqual(old_node, node_to_remove)

                # Track where they moved to
                remap_distribution[new_node_for_key] = (
                    remap_distribution.get(new_node_for_key, 0) + 1
                )

        print(f"Total keys moved: {moved_count}")
        self.assertEqual(moved_count, len(keys_on_removed_node))

        print("Distribution of remapped keys:")
        for node, count in remap_distribution.items():
            print(f"  -> {node}: {count} keys")

        # Assert that the keys were remapped to the *remaining* nodes
        self.assertNotIn(node_to_remove, remap_distribution)
        self.assertEqual(len(remap_distribution.keys()), N - 1)
        print("Minimal movement on remove test passed.")

    def test_concurrency(self):
        """
        Tests that the ring remains consistent under concurrent access.
        """
        print("\nRunning test_concurrency...")
        num_threads = 10
        nodes_to_add = [f"node-{i}" for i in range(100, 100 + num_threads)]
        nodes_to_remove = random.sample(self.nodes, 2)

        def worker(ring, node_id_to_add, node_id_to_remove):
            ring.add_node(node_id_to_add)
            ring.remove_node(node_id_to_remove)
            for i in range(100):
                ring.get_node(f"key-{i}")
                ring.get_nodes_for_key(f"key-{i}")

        threads = []
        for i in range(num_threads):
            # Each thread gets a unique node to add, and one of the two nodes to remove
            node_to_remove_for_thread = nodes_to_remove[i % len(nodes_to_remove)]
            t = threading.Thread(target=worker, args=(self.ring, nodes_to_add[i], node_to_remove_for_thread))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Check for consistency after concurrent operations
        # The final set of nodes should be the initial nodes, minus the ones we removed, plus the ones we added.
        expected_nodes = set(self.nodes) - set(nodes_to_remove) | set(nodes_to_add)
        self.assertEqual(set(self.ring.nodes.keys()), expected_nodes)

        # Verify that the ring structure is not corrupted
        self.assertTrue(len(self.ring.ring) > 0)
        self.assertTrue(len(self.ring.vnode_map) > 0)
        self.assertEqual(len(self.ring.ring), len(self.ring.vnode_map))

        print("Concurrency test passed.")


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)
