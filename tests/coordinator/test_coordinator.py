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
    
    @pytest.fixture
    def setup_files(self, coordinator):
        # Setup file with chunks for testing
        file_stem = "test_file"
        file_suffix = "txt"
        file_name = f"{file_stem}.{file_suffix}"
        chunks = [metadata.ChunkInfo(chunk_handle=f"{file_stem}_chunk{i}") for i in range(3)]
        coordinator.filetable[file_name] = metadata.FileInfo(file_name=file_name, version=1, chunks=chunks)
        return coordinator, file_stem, file_suffix, file_name, chunks

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
        assert coordinator.filetable[file_name].version == 1, "File versioning failed."

        # Test updating the same file
        new_replicas = coordinator.write_file(file_stem, file_suffix, chunk_num, replica_count)
        assert coordinator.filetable[file_name].version == 2, "File versioning on update failed."
        assert new_replicas != replicas, "New write operation did not generate new chunk locations."
    
    def test_get_file(self, setup_chunk_servers):
        coordinator = setup_chunk_servers
        file_stem = "test_get_file"
        file_suffix = "txt"
        chunk_num = 3
        replica_count = 2  # Assuming a smaller replica count for simplicity

        # Write a file to setup initial conditions
        coordinator.write_file(file_stem, file_suffix, chunk_num, replica_count)

        # Test getting file information
        file_info = coordinator.get_file(file_stem, file_suffix)

        # Verify that the file information is correctly returned
        assert file_info is not None, "get_file should return information for an existing file."
        assert len(file_info) == chunk_num, f"Expected {chunk_num} chunks, got {len(file_info)}."

        # Verify that each chunk has the correct number of replicas
        for chunk_handle, servers in file_info:
            assert len(servers) == replica_count, f"Chunk {chunk_handle} should have {replica_count} replicas."

        # Verify that the chunk servers are correctly reported
        for chunk_handle, servers in file_info:
            for server_addr in servers:
                assert server_addr.startswith("127.0.0."), "Chunk server address format is incorrect."
                assert server_addr.endswith(":8000"), "Chunk server port is incorrect."

        # Verify that the chunk handles are correctly formed
        for chunk_handle, _ in file_info:
            assert chunk_handle.startswith(file_stem), "Chunk handle does not start with the file stem."
            assert chunk_handle.endswith(file_suffix), "Chunk handle does not end with the file suffix."

    def test_fetch_file_info(self, setup_files):
        coordinator, file_stem, file_suffix, file_name, chunks = setup_files
        # Fetch file info for existing file
        file_info = coordinator.fetch_file_info(file_stem, file_suffix)
        assert file_info is not None, "Should have fetched file info."
        assert file_info.file_name == file_name, "Fetched file info has incorrect file name."
        assert len(file_info.chunks) == 3, "Fetched file info has incorrect number of chunks."
        assert file_info.chunks == chunks, "Fetched file info has incorrect chunks."

        # Fetch file info for non-existing file
        non_existing_info = coordinator.fetch_file_info("non_existing_file", "txt")
        assert non_existing_info is None, "Should return None for non-existing file."

    def test_delete_file(self, setup_files):
        coordinator, file_stem, file_suffix, file_name, chunks = setup_files
        # Ensure file exists before deletion
        assert file_name in coordinator.filetable, "File should exist before deletion."

        # Delete the file
        coordinator.delete_file(file_stem, file_suffix)
        assert file_name not in coordinator.filetable, "File should be deleted."
        for chunk in chunks:
            assert chunk.chunk_handle not in coordinator.chunktable, "Chunk should be deleted."