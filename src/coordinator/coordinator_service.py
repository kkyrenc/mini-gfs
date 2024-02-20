import zerorpc
from coordinator.coordinator import Coordinator
from coordinator import metadata
from typing import List, Dict, Optional, Tuple
from utils.decorator_utils import DecoratorUtils


class CoordinatorService:
    """
    An RPC service class that provides coordinator functionalities for a distributed file system.

    This service exposes methods as RPC endpoints using the zerorpc framework, enabling clients to interact with the distributed file system's coordinator for various operations such as registering chunk servers, writing files, etc.
    """
    def __init__(self) -> None:
        """
        Initializes the CoordinatorService with a Coordinator instance.
        """
        self.coordinator = Coordinator()

    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def register_chunk_server(self, addr: str) -> Optional[str]:
        """
        Registers a chunk server with the given address in the distributed file system.

        Args:
            addr: The address of the chunk server to register.

        Returns:
            An optional string indicating the result of the registration.
        """
        return self.coordinator.register_chunk_server(addr)
        
    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def unregister_chunk_server(self, addr: str) -> None:
        """
        Unregisters a chunk server with the given address from the distributed file system.

        Args:
            addr: The address of the chunk server to unregister.
        """
        return self.coordinator.unregister_chunk_server(addr)
    
    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def heartbeat(self, addr: str, remains: int) -> None:
        """
        Processes a heartbeat signal from a chunk server, indicating it's alive and operational.

        Args:
            addr: The address of the chunk server sending the heartbeat.
            remains: The remaining storage capacity of the chunk server.
        """
        return self.coordinator.heartbeat(addr, remains)
    
    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def write_file(
        self,
        file_stem: str,
        file_suffix: str,
        chunk_num: int,
        replica: int
    ) -> Dict[str, List[str]]:
        """
        Writes a file with the specified attributes to the distributed file system.

        Args:
            file_stem: The stem part of the file name.
            file_suffix: The file suffix.
            chunk_num: The number of chunks the file is divided into.
            replica: The number of replicas for each chunk.

        Returns:
            A dictionary mapping chunk handles to lists of chunk server addresses where the chunks are stored.
        """
        return self.coordinator.write_file(file_stem, file_suffix, chunk_num, replica)
    
    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def get_file(
            self, file_stem: str, file_suffix: str) -> Optional[List[Tuple[str, List[str]]]]:
        """
        Retrieves the locations of all chunks for a given file from the distributed file system.

        Args:
            file_stem: The stem part of the file name.
            file_suffix: The file suffix.

        Returns:
            An optional list of tuples, each containing a chunk handle and a list of chunk server addresses where the chunk is stored.
        """
        return self.coordinator.get_file(file_stem, file_suffix)
    
    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def fetch_file_info(self, file_stem: str, file_suffix: str) -> Optional[metadata.FileInfo]:
        """
        Fetches metadata information for a specific file from the distributed file system.

        Args:
            file_stem: The stem part of the file name.
            file_suffix: The file suffix.

        Returns:
            An optional FileInfo object containing metadata about the file.
        """
        return self.coordinator.fetch_file_info(file_stem, file_suffix)

    @DecoratorUtils.exception_logging_decorator
    @DecoratorUtils.timing_decorator
    def delete_file(self, file_stem: str, file_suffix: str) -> None:
        """
        Deletes a specified file and its chunks from the distributed file system.

        Args:
            file_stem: The stem part of the file name to be deleted.
            file_suffix: The suffix part of the file name to be deleted.
        """
        return self.coordinator.delete_file(file_stem, file_suffix)
    

def main():
    """
    The main entry point of the CoordinatorService.
    """
    s = zerorpc.Server(CoordinatorService())
    s.bind("tcp://0.0.0.0:4242")  # Listen on port 4242
    s.run()

if __name__ == "__main__":
    main()