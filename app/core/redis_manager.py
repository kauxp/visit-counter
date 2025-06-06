import redis
from typing import Dict, List, Optional, Any
from .consistent_hash import ConsistentHash
from .config import settings

class RedisManager:
    def __init__(self):
        """Initialize Redis connection pools and consistent hashing"""
        self.connection_pools: Dict[str, redis.ConnectionPool] = {}
        self.redis_clients: Dict[str, redis.Redis] = {}
        
        # Parse Redis nodes from comma-separated string
        redis_nodes = [node.strip() for node in settings.REDIS_NODES.split(",") if node.strip()]
        self.consistent_hash = ConsistentHash(redis_nodes, settings.VIRTUAL_NODES)
        
        # TODO: Initialize connection pools for each Redis node
        # 1. Create connection pools for each Redis node
        # 2. Initialize Redis clients
        for node in redis_nodes:
            try:
                pool = redis.ConnectionPool.from_url(node)
                self.connection_pools[node] = pool
                self.redis_clients[node] = redis.Redis(connection_pool=pool)
            except Exception as e:
                print(f"Failed to initialize Redis connection for {node}: {str(e)}")
        pass

    async def get_connection(self, key: str) -> redis.Redis:
        """
        Get Redis connection for the given key using consistent hashing
        
        Args:
            key: The key to determine which Redis node to use
            
        Returns:
            Redis client for the appropriate node
        """
        # TODO: Implement getting the appropriate Redis connection
        # 1. Use consistent hashing to determine which node should handle this key
        # 2. Return the Redis client for that node

        node = self.consistent_hash.get_node(key)
        return self.redis_clients[node]

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Increment a counter in Redis
        
        Args:
            key: The key to increment
            amount: Amount to increment by
            
        Returns:
            New value of the counter
        """
        # TODO: Implement incrementing a counter
        # 1. Get the appropriate Redis connection
        # 2. Increment the counter
        # 3. Handle potential failures and retries
        conn = await self.get_connection(key)
        return conn.incrby(key, amount)

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value for a key from Redis
        
        Args:
            key: The key to get
            
        Returns:
            Tuple of (value of the key or 0 if not found, shard identifier)
        """
        # TODO: Implement getting a value
        # 1. Get the appropriate Redis connection
        # 2. Retrieve the value
        # 3. Handle potential failures and retries
        node = self.consistent_hash.get_node(key)
        print(node)
        conn = await self.get_connection(key)
        count = conn.get(key)
        
        # Extract a unique identifier from the Redis node URL
        # Parse the hostname from URL to get a unique identifier for each Redis node
        host = node.split("//")[1].split(":")[0]  # This extracts the hostname (redis1, redis2, etc.)
        
        return int(count) if count else 0, f"{host}"
