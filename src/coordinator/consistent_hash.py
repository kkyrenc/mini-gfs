import hashlib
from typing import Optional, List
from coordinator.metadata import ChunkServerInfo
from sortedcontainers import SortedDict

class ConsistentHash:
    """
    A class that implements consistent hashing with support for virtual nodes.
    
    Attributes:
        nodes (set): A set of all real nodes.
        ring (dict): The hash ring, where keys are node hash values and values are node info.
        virtual_nodes (int): The number of virtual nodes per real node.
    """

    def __init__(self, virtual_nodes: int = 20) -> None:
        """
        Initializes an instance of ConsistentHash.
        
        Args:
            virtual_nodes (int): The number of virtual nodes per real node, default is 20.
        """
        self.nodes = set()
        self.ring = SortedDict()
        self.virtual_nodes = virtual_nodes

    def hash(self, key: str) -> int:
        """
        Computes the MD5 hash value for a given key.
        
        Args:
            key (str): The key to hash.
        
        Returns:
            int: The hash value of the key.
        """
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
    
    def add_node(self, node: ChunkServerInfo) -> None:
        """
        Adds a new node to the hash ring.
        
        Args:
            node (ChunkServerInfo): The node information to add.
        """
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node.address}_{i}"
            node_hash = self.hash(virtual_node_key)
            self.ring[node_hash] = node
        self.nodes.add(node)
    
    def remove_node(self, node_addr: str) -> None:
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
