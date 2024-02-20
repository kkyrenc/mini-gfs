import pytest
from unittest.mock import MagicMock
from coordinator.coordinator_service import CoordinatorService

@pytest.fixture
def mocked_coordinator_service():
    service = CoordinatorService()
    service.coordinator = MagicMock()
    return service

def test_register_chunk_server(mocked_coordinator_service):
    addr = "127.0.0.1:5000"
    mocked_coordinator_service.register_chunk_server(addr)
    mocked_coordinator_service.coordinator.register_chunk_server.assert_called_once_with(addr)

def test_unregister_chunk_server(mocked_coordinator_service):
    addr = "127.0.0.1:5000"
    mocked_coordinator_service.unregister_chunk_server(addr)
    mocked_coordinator_service.coordinator.unregister_chunk_server.assert_called_once_with(addr)

def test_heartbeat(mocked_coordinator_service):
    addr = "127.0.0.1:5000"
    remains = 100
    mocked_coordinator_service.heartbeat(addr, remains)
    mocked_coordinator_service.coordinator.heartbeat.assert_called_once_with(addr, remains)

def test_write_file(mocked_coordinator_service):
    file_stem = "test_file"
    file_suffix = "txt"
    chunk_num = 5
    replica = 3
    mocked_coordinator_service.write_file(file_stem, file_suffix, chunk_num, replica)
    mocked_coordinator_service.coordinator.write_file.assert_called_once_with(file_stem, file_suffix, chunk_num, replica)

def test_get_file(mocked_coordinator_service):
    file_stem = "test_file"
    file_suffix = "txt"
    mocked_coordinator_service.get_file(file_stem, file_suffix)
    mocked_coordinator_service.coordinator.get_file.assert_called_once_with(file_stem, file_suffix)

def test_fetch_file_info(mocked_coordinator_service):
    file_stem = "test_file"
    file_suffix = "txt"
    mocked_coordinator_service.fetch_file_info(file_stem, file_suffix)
    mocked_coordinator_service.coordinator.fetch_file_info.assert_called_once_with(file_stem, file_suffix)

def test_delete_file(mocked_coordinator_service):
    file_stem = "test_file"
    file_suffix = "txt"
    mocked_coordinator_service.delete_file(file_stem, file_suffix)
    mocked_coordinator_service.coordinator.delete_file.assert_called_once_with(file_stem, file_suffix)
