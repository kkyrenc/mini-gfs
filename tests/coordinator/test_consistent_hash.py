from coordinator.consistent_hash import ConsistentHash
from coordinator.metadata import ChunkServerInfo, ChunkServerStatus
import pytest

class TestConsistentHash:
    @pytest.fixture
    def setup_consistent_hash(self):
        # Initialize with 100 virtual nodes
        ch = ConsistentHash(virtual_nodes=100)
        return ch

    def test_add_and_get_node(self, setup_consistent_hash):
        ch = setup_consistent_hash
        node1 = ChunkServerInfo(
            address="10.0.0.1", status=ChunkServerStatus.HEALTHY, remains=1000, last_update=0)
        node2 = ChunkServerInfo(
            address="10.0.0.2", status=ChunkServerStatus.HEALTHY, remains=1000, last_update=0)
        
        # Add two nodes
        ch.add_node(node1)
        ch.add_node(node2)

        # Try hash some key to one of exist nodes
        nodes = ch.get_nodes("some_key", 1)
        assert len(nodes) == 1, "Should only return one node"
        assert nodes[0] in [node1, node2], "The returned node should be one of the added nodes."

        nodes = ch.get_nodes("some_key", 2)
        assert len(nodes) == 2, "Should return two node"
        assert all(node in [node1, node2] for node in nodes), "All returned nodes should be one of the added nodes."

        nodes = ch.get_nodes("some_key", 3)
        assert len(nodes) == 2, "Should only return two node since we only have two"
        assert all(node in [node1, node2] for node in nodes), "All returned nodes should be one of the added nodes."

    def test_remove_node(self, setup_consistent_hash):
        ch = setup_consistent_hash
        node1 = ChunkServerInfo(
            address="10.0.0.1", status=ChunkServerStatus.HEALTHY, remains=1000, last_update=0)
        node2 = ChunkServerInfo(
            address="10.0.0.2", status=ChunkServerStatus.HEALTHY, remains=1000, last_update=0)
        
        ch.add_node(node1)
        ch.add_node(node2)
        
        # Remove one node
        ch.remove_node("10.0.0.1")

        # Ensure all key will be mapping to another node after removing
        remaining_nodes = ch.get_nodes("some_key")
        assert len(remaining_nodes) == 1, "Should only remain one node"
        assert remaining_nodes[0] == node2, "The remaining node should be node2 after node1 is removed."
