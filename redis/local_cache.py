import threading
import time
from collections import OrderedDict
from typing import Any, Optional


class LocalCache:
    """
    线程安全的本地内存缓存，支持 TTL 和 LRU 淘汰策略。
    
    此缓存用于降低对远端 Redis 的高频读取延迟，特别适合 Hot Key 场景。
    
    Attributes:
        ttl_ms: 缓存条目存活时间（毫秒）
        max_size: 缓存最大条目数量
        _cache: 内部存储，使用 OrderedDict 实现 LRU
        _lock: 线程锁
    """

    def __init__(self, ttl_ms: int, max_size: int):
        """
        初始化本地缓存。
        
        Args:
            ttl_ms: 缓存存活时间（毫秒），必须大于 0
            max_size: 最大缓存条目数量，必须大于 0
            
        Raises:
            ValueError: 如果 ttl_ms 或 max_size 小于等于 0
        """
        if ttl_ms <= 0:
            raise ValueError("ttl_ms must be greater than 0")
        if max_size <= 0:
            raise ValueError("max_size must be greater than 0")
        
        self.ttl_ms = ttl_ms
        self.max_size = max_size
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: Any) -> Optional[Any]:
        """
        从缓存中获取值。
        
        如果缓存命中且未过期，返回对应值并更新 LRU 顺序。
        如果缓存未命中或已过期，返回 None。
        
        Args:
            key: 缓存键
            
        Returns:
            缓存值或 None
        """
        with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            current_time = time.time() * 1000
            
            if current_time - entry["timestamp"] > self.ttl_ms:
                del self._cache[key]
                return None
            
            self._cache.move_to_end(key)
            return entry["value"]

    def set(self, key: Any, value: Any) -> None:
        """
        设置缓存值。
        
        如果缓存已达到最大容量，会淘汰最久未使用的条目（LRU）。
        
        Args:
            key: 缓存键
            value: 缓存值
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
            elif len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            
            self._cache[key] = {
                "value": value,
                "timestamp": time.time() * 1000
            }

    def delete(self, key: Any) -> bool:
        """
        删除缓存条目。
        
        Args:
            key: 缓存键
            
        Returns:
            如果键存在并被删除返回 True，否则返回 False
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> int:
        """
        清空所有缓存条目。
        
        Returns:
            被清除的条目数量
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def __len__(self) -> int:
        """
        返回缓存中的条目数量。
        
        注意：此方法不会清理过期条目，仅返回当前存储的条目数。
        
        Returns:
            缓存中的条目数量
        """
        with self._lock:
            return len(self._cache)

    def __contains__(self, key: Any) -> bool:
        """
        检查键是否存在且未过期。
        
        Args:
            key: 缓存键
            
        Returns:
            如果键存在且未过期返回 True，否则返回 False
        """
        return self.get(key) is not None
