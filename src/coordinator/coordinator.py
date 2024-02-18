import logging
from typing import Dict, List
from coordinator import metadata
from coordinator.consistent_hash import ConsistentHash
import time
from threading import RLock, Timer


class Coordinator:
    """
    Coordinator class for managing chunk servers in a distributed file system.

    Attributes:
        logger (logging.Logger): Logger for the coordinator.
        consistent_hash (ConsistentHash): Consistent hashing mechanism for chunk server distribution.
        chunk_servers (Dict[str, metadata.ChunkServerInfo]): Dictionary mapping chunk server addresses to their info.
        file_chunks_mapping (Dict[str, List[str]]): Mapping of file identifiers to their chunk identifiers.
        chunk_locations_mapping (Dict[str, List[str]]): Mapping of chunk identifiers to their server addresses.
        heartbeat_check_interval (int): Interval in seconds for heartbeat checks to detect chunk server availability.
        is_heartbeat_checking (bool): Flag indicating whether heartbeat checks are currently being performed.
        _lock (RLock): Reentrant lock for thread-safe operations.
    """

    def __init__(self, hearbeat_check_interval: int = 10) -> None:
        """
        Initializes the Coordinator with a specified heartbeat check interval.

        Args:
            hearbeat_check_interval (int): Interval in seconds for heartbeat checks.
        """
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.consistent_hash: ConsistentHash = ConsistentHash()
        self.chunk_servers: Dict[str, metadata.ChunkServerInfo] = {}
        self.file_chunks_mapping: Dict[str, List[str]] = {}
        self.chunk_locations_mapping: Dict[str, List[str]] = {}
        self.heartbeat_check_interval = hearbeat_check_interval
        # Set to False initially since we haven't check heartbet now
        self.is_heartbeat_checking = False
        self._lock = RLock()

    def register_chunk_server(self, addr: str) -> None:
        """
        Registers a new chunk server with the given address.

        Args:
            addr (str): The address of the chunk server to register.

        Raises:
            KeyError: If the chunk server already exists.
        """
        self.logger.info(f"Registering chunk server for: {addr}...")

        with self._lock:
            if addr in self.chunk_servers:
                self.logger.error(f"Chunk server {addr} already exists.")
                raise KeyError(f"Chunk server {addr} already exists.")
            
            chunk_server = metadata.ChunkServerInfo(
                address=addr,
                status=metadata.ChunkServerStatus.INITIAL,
                remains=0,
                last_update=time.time()
            )

            # Initially chunk server is not active, so we won't add it to
            # our consistenty hash.
            self.chunk_servers[addr] = chunk_server

        self.logger.info(f"Registration chunk server for: {addr} completed.")

    def unregister_chunk_server(self, addr: str) -> None:
        """
        Unregisters the chunk server with the given address.

        Args:
            addr (str): The address of the chunk server to unregister.
        """
        self.logger.info(f"Unregistering chunk server for: {addr}...")

        with self._lock:
            if addr not in self.chunk_servers:
                self.logger.warning(f"Chunk server: {addr} does not exist.")
                return

            self.deactivate_chunk_server(addr=addr)
            del self.chunk_servers[addr]

        self.logger.info(f"Unregistration chunk server for: {addr} completed.")

    def heartbeat(self, addr: str, remains: int) -> None:
        """
        Processes a heartbeat signal from a chunk server.

        Args:
            addr (str): The address of the chunk server sending the heartbeat.
            remains (int): The remaining storage capacity reported by the chunk server.
        """
        with self._lock:
            if addr not in self.chunk_servers:
                self.logger.warning(f"Received unknown heartbeat from {addr}, ignored.")
                return
            
            self.logger.info(f"Received heartbeat from {addr}.")
            chunk_server_info = self.chunk_servers[addr]

            # Update chunk server's information
            chunk_server_info.last_update = time.time()
            chunk_server_info.remains = remains

    def activate_chunk_server(self, chunk_server: metadata.ChunkServerInfo) -> None:
        """
        Activates a chunk server, making it available for storing chunks.

        Args:
            chunk_server (metadata.ChunkServerInfo): The chunk server to activate.
        """
        self.logger.info(f"Activating chunk server {chunk_server.address}")
        with self._lock:
            self.consistent_hash.add_node(chunk_server)
        self.rebalance()
        self.logger.info(f"Chunk server {chunk_server.address} activated.")

    def deactivate_chunk_server(self, addr: str) -> None:
        """
        Deactivates a chunk server, removing it from the pool of available servers.

        Args:
            addr (str): The address of the chunk server to deactivate.
        """
        self.logger.info(f"Deactivating chunk server {addr}")
        with self._lock:
            self.consistent_hash.remove_node(addr)
        self.rebalance()
        self.logger.info(f"Chunk server {addr} deactivated.")

    def rebalance(self) -> None:
        """
        Rebalances the distribution of chunks across the available chunk servers.
        TODO: Implement the rebalance logic.
        """
        ...

    def heartbeat_check(self) -> None:
        """
        Periodically checks the status of each chunk server based on their heartbeat signals.
        """
        current_time = time.time()

        with self._lock:
            for chunk_server_info in self.chunk_servers.values():
                if current_time - chunk_server_info.last_update <= self.heartbeat_check_interval:
                    # Chunk Server is healthy
                    if chunk_server_info.status is metadata.ChunkServerStatus.FAILED \
                        or chunk_server_info.status is metadata.ChunkServerStatus.INITIAL:
                        # Activate intial node or resume failed node
                        self.activate_chunk_server(chunk_server_info)
                    chunk_server_info.status = metadata.ChunkServerStatus.HEALTHY
                else:
                    # Hearbeat timeout
                    if chunk_server_info.status is metadata.ChunkServerStatus.HEALTHY:
                        self.logger.warning(f"Chunk server {chunk_server_info.address} heartbeat timed out, suspecting.")
                        chunk_server_info.status = metadata.ChunkServerStatus.SUSPECT
                    elif chunk_server_info.status is metadata.ChunkServerStatus.SUSPECT:
                        self.logger.warning(f"Chunk server {chunk_server_info.address} heartbeat timed out, failed.")
                        chunk_server_info.status = metadata.ChunkServerStatus.FAILED
                        self.deactivate_chunk_server(chunk_server_info.address)

        if self.is_heartbeat_checking:
            Timer(self.heartbeat_check_interval, self.heartbeat_check).start()

    def start_heartbeat_check(self) -> None:
        """
        Starts the periodic heartbeat checks.
        """
        self.logger.info("Heartbeat check starting...")
        self.is_heartbeat_checking = True
        Timer(self.heartbeat_check_interval, self.heartbeat_check).start()
        self.logger.info("Heartbeat check started.")

    def stop_heartbeat_check(self) -> None:
        """
        Stops the periodic heartbeat checks.
        """
        self.logger.info("Heartbeat check stopping...")
        self.is_heartbeat_checking = False
        self.logger.info("Heartbeat check stopped.")