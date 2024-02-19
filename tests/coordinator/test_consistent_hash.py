from coordinator.consistent_hash import ConsistentHash
from coordinator.metadata import ChunkServerInfo, ChunkServerStatus, ChunkInfo
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
        ch.add_node(node1, lambda x, y, z: None)
        ch.add_node(node2, lambda x, y, z: None)

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
        ch.add_node(node1, lambda x, y, z: None)
        ch.add_node(node2, lambda x, y, z: None)
        ch.remove_node("10.0.0.1", lambda x, y: None)

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
            ch.add_node(node, lambda x, y, z: None)

        # Check if keys are hased evenly
        key_node_mapping = {}
        for i in range(100):  # Randomly generate 100 values
            key = f"key_{i}"
            node = ch.get_nodes(key, 1)[0]
            key_node_mapping[key] = node

        # Check if hash evenly
        node_distribution = {node: list(key_node_mapping.values()).count(node) for node in nodes}
        assert all(count > 10 for count in node_distribution.values()), "Keys should be evenly distributed among nodes."

    def test_data_migration_on_new_node_addition(self, setup_consistent_hash, mocker):
        ch = setup_consistent_hash
        node1 = ChunkServerInfo(
            address="10.0.0.1",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks={ChunkInfo(chunk_handle="chunk1")},
            last_update=0
        )        
        # Mock the migrate_func to track if it's called correctly
        mock_migrate_func = mocker.Mock()
        ch.add_node(node1, mock_migrate_func)

        node2 = ChunkServerInfo(
            address="10.0.0.2",
            status=ChunkServerStatus.HEALTHY,
            remains=1000,
            chunks=set(),
            last_update=0
        )
        # Add the second node and check if migration occurs
        ch.add_node(node2, migrate_func=mock_migrate_func)
        
        # Verify migrate_func was called with correct parameters
        mock_migrate_func.assert_called_with(node1, node2, mocker.ANY)  # mocker.ANY to ignore checking specific ChunkInfo object

    def test_replica_distribution_after_node_removal(self, setup_consistent_hash, mocker):
        ch = setup_consistent_hash
        # Mock the distribute_func to track redistribution
        mock_distribute_func = mocker.Mock()
        # Add multiple nodes
        for i in range(1, 4):  # Adding three nodes
            ch.add_node(ChunkServerInfo(
                address=f"10.0.0.{i}",
                status=ChunkServerStatus.HEALTHY,
                remains=1000,
                chunks=set(),
                last_update=0
            ), mock_distribute_func)

        # Assume each node initially manages one chunk
        for node in ch.nodes:
            node.chunks.add(ChunkInfo(chunk_handle=f"chunk_{node.address}"))

        # Remove a node and trigger redistribution
        ch.remove_node("10.0.0.1", distribute_func=mock_distribute_func)

        # Verify distribute_func was called for each chunk that was managed by the removed node
        assert mock_distribute_func.call_count == 1, "Should redistribute data from the removed node."