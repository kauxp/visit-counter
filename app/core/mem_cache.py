from collections import defaultdict
from datetime import datetime

class MemCache:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MemCache, cls).__new__(cls)
            cls._instance.dp = defaultdict(dict)
        
        return cls._instance

    def increment(self, page_id: str, count: int = 1) -> None:

        if page_id not in self.dp:
            self.set_page(page_id)

        self.dp[page_id] = {
            "count": self.dp[page_id].get("count", 0) + count,
            "on": datetime.now()
        }

    def set_page_visit(self, page_id: str, counts: int) -> None:

        if page_id not in self.dp:
            self.set_page(page_id)

        self.dp[page_id] = {
            "count": counts,
            "on": datetime.now()
        }


    def set_page(self, page_id: str) -> None:
        self.dp[page_id] = {
            "count": 0,
            "on": datetime.now()
        }

    def get(self, page_id: str) -> dict | str:
        data = self.dp[page_id]
        if data:
            return data, "in_memory"
        else :
            return None, "in_memory"