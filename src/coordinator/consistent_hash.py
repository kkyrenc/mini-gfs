import hashlib
from typing import Optional
from coordinator.chunk_server_info import ChunkServerInfo

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
        self.ring = {}
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

    def get_node(self, key: str) -> Optional[ChunkServerInfo]:
        """
        Retrieves the corresponding node for a given key from the hash ring.
        
        Args:
            key (str): The key to find the corresponding node for.
        
        Returns:
            Optional[ChunkServerInfo]: The node information, or None if no nodes are in the ring.
        """
        if not self.ring:
            return None
        
        key_hash = self.hash(key)
        # Find the nearest node hash greater than or equal to the key hash
        nearest_node_hash = min(
            (hash_val for hash_val in self.ring.keys() if hash_val >= key_hash),
            default=min(self.ring.keys(), default=None)
        )
        
        if nearest_node_hash is not None:
            return self.ring[nearest_node_hash]
        return None
