import pytest
from coordinator.coordinator import Coordinator
from coordinator import metadata
import time

class TestCoordinator:
    @pytest.fixture
    def coordinator(self):
        return Coordinator(hearbeat_check_interval=1)

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
