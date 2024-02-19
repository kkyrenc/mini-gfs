import hashlib
from typing import Optional, List, Callable, Set
from coordinator.metadata import ChunkServerInfo, ChunkInfo
from sortedcontainers import SortedDict

class ConsistentHash:
    """
    A class that implements consistent hashing with support for virtual nodes.
    
    Attributes:
        nodes (set): A set of all real nodes.
        ring (SortedDict): The hash ring, where keys are node hash values and values are node info.
        virtual_nodes (int): The number of virtual nodes per real node.
    """

    def __init__(self, virtual_nodes: int = 20) -> None:
        """
        Initializes an instance of ConsistentHash.
        
        Args:
            virtual_nodes (int): The number of virtual nodes per real node, default is 20.
        """
        self.nodes: Set[ChunkServerInfo] = set()
        self.ring: SortedDict[int, ChunkServerInfo] = SortedDict()
        self.virtual_nodes: int = virtual_nodes

    def hash(self, key: str) -> int:
        """
        Computes the MD5 hash value for a given key.
        
        Args:
            key (str): The key to hash.
        
        Returns:
            int: The hash value of the key.
        """
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
    
    def find_predecessor(self, node_hash: int) -> Optional[ChunkServerInfo]:
        """
        Finds the direct predecessor of a given node hash on the hash ring.
        If the exact hash is not found, finds the closest hash that is less than the given hash.

        Args:
            node_hash (int): The hash of the node.

        Returns:
            Optional[ChunkServerInfo]: The predecessor of the given node hash.
        """
        if not self.ring:
            return None

        # Find suitable position
        index = self.ring.bisect_left(node_hash)
        keys = list(self.ring.keys())
        
        if index == 0:
            predecessor_hash = keys[-1]  # Ring structure, return the last one
        else:
            predecessor_hash = keys[index - 1]

        return self.ring[predecessor_hash]

    def find_successor(self, node_hash: int) -> Optional[ChunkServerInfo]:
        """
        Finds the direct successor of a given node hash on the hash ring.
        If the exact hash is not found, finds the closest hash that is greater than the given hash.

        Args:
            node_hash (int): The hash of the node.

        Returns:
            Optional[ChunkServerInfo]: The successor of the given node hash.
        """
        if not self.ring:
            return None

        keys = self.ring.keys()
        # Using bisect_right to find the position where the node_hash would fit
        # This position corresponds to the first node hash that is greater than the given node_hash
        index = keys.bisect_right(node_hash)

        # If the index is equal to the length of the keys, it means we've wrapped around the ring
        # and the successor is the first node in the ring
        if index == len(keys):
            successor_hash = keys[0]
        else:
            successor_hash = keys[index]

        return self.ring[successor_hash]

    def migrate_data_to_new_node(
        self,
        new_node: ChunkServerInfo,
        migrate_func: Callable[[ChunkServerInfo, ChunkServerInfo, ChunkInfo], None]
    ) -> None:
        """Identifies the data items that need to be migrated to the newly added 
        node and performs the migration.

        Args:
            new_node (ChunkServerInfo): The new added node
            migrate_func (Callable[[ChunkServerInfo, ChunkServerInfo, ChunkInfo], None]):
                Migrate function which performs real logic of migrating.
        """
        for i in range(self.virtual_nodes):
            new_node_hash = self.hash(f"{new_node.address}_{i}")
            predecessor = self.find_predecessor(new_node_hash)
            if not predecessor:
                continue
            
            # Assuming each ChunkServerInfo object has a method to return all chunk handles it manages
            # and a way to remove chunks from its collection.
            affected_chunks = [chunk for chunk in predecessor.chunks if self.hash(chunk.chunk_handle) <= new_node_hash]

            # Migrate affected chunks to the new node
            for chunk in affected_chunks:
                migrate_func(predecessor, new_node, chunk)
                predecessor.chunks.remove(chunk)
                new_node.chunks.add(chunk)

    def add_node(
        self,
        node: ChunkServerInfo,
        migrate_func: Callable[[ChunkServerInfo, ChunkServerInfo, ChunkInfo], None]
    ) -> None:
        """
        Adds a new node to the hash ring.
        
        Args:
            node (ChunkServerInfo): The node information to add.
        """
        self.migrate_data_to_new_node(node, migrate_func)
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node.address}_{i}"
            node_hash = self.hash(virtual_node_key)
            self.ring[node_hash] = node
        self.nodes.add(node)
    
    def distribute_data_from_removed_node(
        self,
        removed_node: ChunkServerInfo,
        distribute_func: Callable[[ChunkInfo, List[ChunkServerInfo]], None],
        replica_count: int = 3
    ) -> None:
        """
        Identifies the data items that were managed by the removed node and
        performs the migration to ensure data is not lost.

        Args:
            removed_node (ChunkServerInfo): The node being removed from the hash ring.
            distribute_func (Callable): A function that handles the distribution chunks.
                This function takes three arguments: the source node, the target node, and the data item.
        """
        affected_chunks = removed_node.chunks
        for chunk in affected_chunks:
            replica_locations = self.get_nodes(chunk.chunk_handle, replica_count)
            distribute_func(chunk, replica_locations)

    def remove_node(
        self,
        node_addr: str,
        distribute_func: Callable[[ChunkInfo, List[ChunkServerInfo]], None],
        replica_count: int = 3
    ) -> None:
        """
        Removes a node from the hash ring.
        
        Args:
            node_addr (str): The address of the node to remove.
        """
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node_addr}_{i}"
            node_hash = self.hash(virtual_node_key)
            if node_hash in self.ring:
                del self.ring[node_hash]
        # Remove the node from the set of nodes
        node_to_remove = next((node for node in self.nodes if node.address == node_addr), None)
        if node_to_remove:
            self.nodes.remove(node_to_remove)
            self.distribute_data_from_removed_node(node_to_remove, distribute_func, replica_count)

    def get_nodes(self, key: str, replica_count: int = 3) -> List[Optional[ChunkServerInfo]]:
        """
        Retrieves the corresponding nodes for a given key from the hash ring,
        intended for storing replicas of the key.

        Args:
            key (str): The key to find the corresponding nodes for.
            replica_count (int): The number of replicas (nodes) to retrieve.

        Returns:
            List[Optional[ChunkServerInfo]]: The list of node information for the replicas.
        """
        if not self.ring:
            return [None] * replica_count

        key_hash = self.hash(key)
        nodes = []
        unique_physical_nodes = set()

        start_index = self.ring.bisect_right(key_hash)
        ring_keys = list(self.ring.keys())

        # Adjust the iteration to directly use the ring, minimizing list conversions
        for i in range(start_index, start_index + len(self.ring) * 2):  # Loop through twice as a safeguard
            wrapped_index = i % len(ring_keys)
            node_hash = ring_keys[wrapped_index]
            node = self.ring[node_hash]

            # Check for unique physical nodes to ensure replicas are not on the same physical node
            if node.address not in unique_physical_nodes:
                nodes.append(node)
                unique_physical_nodes.add(node.address)

            # Break once enough replicas are found
            if len(nodes) == replica_count:
                break

        # It's possible that the number of unique physical nodes is less than replica_count,
        # in which case, we've done our best to distribute replicas across available nodes.
        return nodes
