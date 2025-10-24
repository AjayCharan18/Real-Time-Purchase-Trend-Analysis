import threading
import time
from typing import Any, Callable, Dict, Optional


class CacheEntry:
    __slots__ = ("data", "timestamp")

    def __init__(self, data: Any = None, timestamp: float = 0.0):
        self.data = data
        self.timestamp = timestamp


class CacheManager:
    def __init__(self, ttl: int = 60, default_limit: int = 100):
        self.ttl = ttl
        self.default_limit = default_limit
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, key: str, loader: Callable[[], Any], ttl: Optional[int] = None) -> Any:
        expiry = ttl if ttl is not None else self.ttl
        now = time.time()
        with self._lock:
            entry = self._cache.get(key)
            if entry and (now - entry.timestamp) < expiry:
                return entry.data

        data = loader()
        with self._lock:
            self._cache[key] = CacheEntry(data, now)
        return data

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = CacheEntry(value, time.time())

    def invalidate(self, key: Optional[str] = None) -> None:
        with self._lock:
            if key is None:
                self._cache.clear()
            else:
                self._cache.pop(key, None)

    def preload(self, items: Dict[str, Callable[[], Any]]) -> None:
        for key, loader in items.items():
            try:
                self.get(key, loader)
            except Exception:
                # Cache warm-ups should not crash the application
                continue
