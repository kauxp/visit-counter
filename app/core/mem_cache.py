from collections import defaultdict


class MemCache:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MemCache, cls).__new__(cls)
            cls._instance.dp = defaultdict(int)
        
        return cls._instance

    def increment(self, page_id: str, count: int = 1) -> None:
        self.dp[page_id] += 1

    def get(self, page_id: str) -> None:
        return self.dp[page_id], "in_memory"