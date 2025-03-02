from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager
from ..core.mem_cache import MemCache

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis manager"""
        self.redis_manager = RedisManager()
        self.mem_manager = MemCache()

    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page
        
        Args:
            page_id: Unique identifier for the page
        """
        # self.mem_manager.increment(page_id)
        await self.redis_manager.increment(page_id)

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        # return self.mem_manager.get(page_id)
        counts, via = await self.redis_manager.get(page_id)
        print(f"counts: {counts}, via: {via}")
        return counts, via
