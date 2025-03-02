import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        
        # Add all nodes to the hash ring
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str) -> None:
        """
        Add a new node to the hash ring
        
        Args:
            node: Node identifier to add
        """
        # Create virtual nodes for the physical node
        for i in range(self.virtual_nodes):
            virtual_node = f"{node}:{i}"
            # Calculate hash for the virtual node
            key = self._hash(virtual_node)
            # Map virtual node to physical node
            self.hash_ring[key] = node
            self.sorted_keys.append(key)
        
        # Keep the keys sorted for efficient lookup
        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring
        
        Args:
            node: Node identifier to remove
        """
        # Find all keys that belong to the node
        keys_to_remove = []
        for key, n in self.hash_ring.items():
            if n == node:
                keys_to_remove.append(key)
        
        # Remove the node from hash_ring and sorted_keys
        for key in keys_to_remove:
            del self.hash_ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
        if not self.hash_ring:
            return ""
        
        # Calculate hash of the key
        hash_key = self._hash(key)
        
        # Find the first node in the ring that comes after the key's hash
        pos = bisect(self.sorted_keys, hash_key)
        
        # If we've gone past the end, wrap around to the first node
        if pos >= len(self.sorted_keys):
            pos = 0
            
        # Return the node at this position
        return self.hash_ring[self.sorted_keys[pos]]
        
    def _hash(self, key: str) -> int:
        """
        Generate a hash value for a key
        
        Args:
            key: The key to hash
            
        Returns:
            The hash value
        """
        # Using md5 for consistent distribution
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    