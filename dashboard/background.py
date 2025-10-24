import threading
from typing import Dict, Callable

from cache_utils import CacheManager


class CacheWarmupTask:
    def __init__(self, cache_manager: CacheManager, loaders: Dict[str, Callable[[], object]]):
        self.cache_manager = cache_manager
        self.loaders = loaders

    def run(self) -> None:
        for key, loader in self.loaders.items():
            try:
                self.cache_manager.get(key, loader)
            except Exception:
                continue

    def start_async(self) -> None:
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
