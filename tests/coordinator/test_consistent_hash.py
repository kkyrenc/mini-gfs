from coordinator.consistent_hash import ConsistentHash
from coordinator.metadata import ChunkServerInfo, ChunkServerStatus
import pytest

class TestConsistentHash:
    @pytest.fixture
    def setup_consistent_hash(self):
        # Initially set virtual nodes to 100
        ch = ConsistentHash(virtual_nodes=100)
        return ch

    def test_add_node_and_unique_physical_nodes(self, setup_consistent_hash):
        ch = setup_consistent_hash
        # Add two physical nodes
        node1 = ChunkServerInfo(
            address="10.0.0.1",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks=set(),
            last_update=0
        )
        node2 = ChunkServerInfo(
            address="10.0.0.2",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks=set(),
            last_update=0
        )
        ch.add_node(node1)
        ch.add_node(node2)

        # Request three nodes，should only return two physical nodes since there are only two nodes
        nodes = ch.get_nodes("some_key", 3)
        assert len(set(nodes)) == 2, "Should return two unique physical nodes."

    def test_remove_node_and_redistribution(self, setup_consistent_hash):
        ch = setup_consistent_hash
        # Add node then remove one node
        node1 = ChunkServerInfo(
            address="10.0.0.1",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks=set(),
            last_update=0
        )
        node2 = ChunkServerInfo(
            address="10.0.0.2",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks=set(),
            last_update=0
        )
        ch.add_node(node1)
        ch.add_node(node2)
        ch.remove_node("10.0.0.1")

        remaining_nodes = ch.get_nodes("some_key", 1)
        assert len(remaining_nodes) == 1, "Should only remain one node after removal."
        assert remaining_nodes[0] == node2, "Remaining node should be node2 after node1 is removed."

    def test_distributive_property_for_multiple_keys(self, setup_consistent_hash):
        ch = setup_consistent_hash
        # Add multiple nodes
        nodes = [
            ChunkServerInfo(
                address=f"10.0.0.{i}",
                status=ChunkServerStatus.HEALTHY,
                remains=1000,
                chunks=set(),
                last_update=0
            ) for i in range(1, 5)
        ]
        for node in nodes:
            ch.add_node(node)

        # Check if keys are hased evenly
        key_node_mapping = {}
        for i in range(100):  # Randomly generate 100 values
            key = f"key_{i}"
            node = ch.get_nodes(key, 1)[0]
            key_node_mapping[key] = node

        # Check if hash evenly
        node_distribution = {node: list(key_node_mapping.values()).count(node) for node in nodes}
        assert all(count > 10 for count in node_distribution.values()), "Keys should be evenly distributed among nodes."
