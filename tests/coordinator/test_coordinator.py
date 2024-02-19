import pytest
from coordinator.coordinator import Coordinator
from coordinator import metadata
import time

class TestCoordinator:
    @pytest.fixture
    def coordinator(self):
        return Coordinator(hearbeat_check_interval=1)

    @pytest.fixture
    def setup_chunk_servers(self, coordinator):
        # Setup multiple chunk servers for testing file write
        for i in range(1, 5):
            chunk_server = metadata.ChunkServerInfo(
                address=f"127.0.0.{i}:8000",
                status=metadata.ChunkServerStatus.HEALTHY,
                remains=1000,
                chunks=set(),
                last_update=0
            ) 
            coordinator.activate_chunk_server(chunk_server)
            # Simulate each server reporting available space and being healthy
        return coordinator

    def test_register_chunk_server(self, coordinator):
        addr = "127.0.0.1:8000"
        coordinator.register_chunk_server(addr)
        assert addr in coordinator.chunk_servers
        assert coordinator.chunk_servers[addr].address == addr

    def test_unregister_chunk_server(self, coordinator):
        addr = "127.0.0.1:8000"
        coordinator.register_chunk_server(addr)
        coordinator.unregister_chunk_server(addr)
        assert addr not in coordinator.chunk_servers

    def test_heartbeat(self, coordinator):
        addr = "127.0.0.1:8000"
        coordinator.register_chunk_server(addr)
        initial_remains = 100
        coordinator.heartbeat(addr, remains=initial_remains)
        assert coordinator.chunk_servers[addr].remains == initial_remains

    def test_heartbeat_check(self, coordinator):
        addr = "127.0.0.1:8000"
        coordinator.register_chunk_server(addr)
        # Mock heartbeat timeout
        coordinator.chunk_servers[addr].last_update = time.time() - 2
        coordinator.chunk_servers[addr].status = metadata.ChunkServerStatus.HEALTHY
        coordinator.heartbeat_check()
        assert coordinator.chunk_servers[addr].status == metadata.ChunkServerStatus.SUSPECT
        time.sleep(2)  # Ensure timeout happens
        coordinator.heartbeat_check()
        assert coordinator.chunk_servers[addr].status == metadata.ChunkServerStatus.FAILED

    def test_start_stop_heartbeat_check(self, coordinator):
        coordinator.start_heartbeat_check()
        assert coordinator.is_heartbeat_checking is True
        coordinator.stop_heartbeat_check()
        assert coordinator.is_heartbeat_checking is False

    def test_write_file(self, setup_chunk_servers):
        coordinator = setup_chunk_servers
        file_stem = "test_file"
        file_suffix = "txt"
        chunk_num = 5
        replica_count = 3

        # Write a file and get the replicas mapping
        replicas = coordinator.write_file(file_stem, file_suffix, chunk_num, replica_count)
        
        # Check if the correct number of chunks and replicas are created
        assert len(replicas) == chunk_num, "Incorrect number of chunks created."
        for chunk_handle, server_addresses in replicas.items():
            assert len(server_addresses) == replica_count, f"Incorrect number of replicas for chunk {chunk_handle}."

        # Check if each chunk is assigned to different chunk servers to ensure redundancy
        all_servers = set()
        for server_addresses in replicas.values():
            for address in server_addresses:
                all_servers.add(address)
        assert len(all_servers) > 1, "Replicas should be distributed across multiple servers."

        # Verify file versioning
        file_name = f"{file_stem}.{file_suffix}"
        assert coordinator.files[file_name].version == 1, "File versioning failed."

        # Test updating the same file
        new_replicas = coordinator.write_file(file_stem, file_suffix, chunk_num, replica_count)
        assert coordinator.files[file_name].version == 2, "File versioning on update failed."
        assert new_replicas != replicas, "New write operation did not generate new chunk locations."