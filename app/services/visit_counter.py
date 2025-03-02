from typing import Dict, List, Any
import asyncio
from datetime import datetime
import time
from ..core.redis_manager import RedisManager
from ..core.mem_cache import MemCache

class VisitCounterService:
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(VisitCounterService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        if not VisitCounterService._initialized:
            self.redis_manager = RedisManager()
            self.mem_manager = MemCache()
            self.buffer = {}
            self.last_flush_time = time.time()
            self.flush_interval = 30  # seconds
            self.background_task = None
            VisitCounterService._initialized = True
    
    async def start_background_tasks(self):
        """Start background task for periodic buffer flushing"""
        if self.background_task is None:
            self.background_task = asyncio.create_task(self._periodic_flush())
    
    async def _periodic_flush(self):
        """Periodically flush buffer to Redis"""
        while True:
            await asyncio.sleep(self.flush_interval)
            await self._flush_buffer()
    
    async def _flush_buffer(self):
        """Flush the buffer to Redis"""
        if not self.buffer:
            return
            
        buffer_copy = self.buffer.copy()
        self.buffer = {}
        self.last_flush_time = time.time()
        
        for page_id, count in buffer_copy.items():
            await self.redis_manager.increment(page_id, count)
            
    async def flush_all(self):
        """Manually flush all buffered counts to Redis"""
        await self._flush_buffer()
        
    def get_buffer_status(self):
        """Get the current buffer status"""
        return {
            "buffer_size": len(self.buffer),
            "buffer_contents": self.buffer,
            "time_since_last_flush": time.time() - self.last_flush_time
        }

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # Store in memory buffer
        self.buffer[page_id] = self.buffer.get(page_id, 0) + 1
        # Update in-memory cache for read consistency
        self.mem_manager.increment(page_id)

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        dp_data, via = self.mem_manager.get(page_id)

        if (dp_data is None) or ((dp_data is not None) and ((datetime.now() - dp_data["on"]).seconds > 5)):
            # Cache miss - flush buffer for this page_id before reading from Redis
            if page_id in self.buffer:
                page_count = self.buffer.pop(page_id)
                await self.redis_manager.increment(page_id, page_count)
            
            counts, via = await self.redis_manager.get(page_id)
            self.mem_manager.set_page_visit(page_id, counts)
            return counts, via
        else:
            counts = dp_data["count"]
            return counts, via
