from typing import Callable, TypeVar, Any, NoReturn, List, Union
from typing import Optional

from cachetools import TTLCache, Cache, LRUCache
from cachetools.keys import hashkey

from redis.typing import ResponseT

T = TypeVar('T')


def ensure_string(key):
    if isinstance(key, bytes):
        return key.decode('utf-8')
    elif isinstance(key, str):
        return key
    else:
        raise TypeError("Key must be either a string or bytes")


class CacheMixin:
    def __init__(self,
                 use_cache: bool,
                 connection_pool: "ConnectionPool",
                 cache: Optional[Cache] = None,
                 cache_size: int = 128,
                 cache_ttl: int = 300,
                 ) -> None:
        self.use_cache = use_cache
        if not use_cache:
            return
        if cache is not None:
            self.cache = cache
        else:
            self.cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)
        self.keys_mapping = LRUCache(maxsize=10000)
        self.wrap_connection_pool(connection_pool)
        self.connections = []

    def cached_call(self,
                    func: Callable[..., ResponseT],
                    *args,
                    **options) -> ResponseT:
        if not self.use_cache:
            return func(*args, **options)

        print(f'Cached call with args {args} and options {options}')

        keys = None
        if 'keys' in options:
            keys = options['keys']
            if not isinstance(keys, list):
                raise TypeError("Cache keys must be a list.")
        if not keys:
            return func(*args, **options)
        print(f'keys {keys}')

        cache_key = hashkey(*args)

        for conn in self.connections:
            conn.process_invalidation_messages()

        for key in keys:
            if key in self.keys_mapping:
                if cache_key not in self.keys_mapping[key]:
                    self.keys_mapping[key].append(cache_key)
            else:
                self.keys_mapping[key] = [cache_key]

        if cache_key in self.cache:
            result = self.cache[cache_key]
            print(f'Cached call for {args} yields cached result {result}')
            return result

        result = func(*args, **options)
        self.cache[cache_key] = result
        print(f'Cached call for {args} yields computed result {result}')
        return result

    def get_cache_entry(self, *args: Any) -> Any:
        cache_key = hashkey(*args)
        return self.cache.get(cache_key, None)

    def invalidate_cache_entry(self, *args: Any) -> None:
        cache_key = hashkey(*args)
        if cache_key in self.cache:
            self.cache.pop(cache_key)

    def wrap_connection_pool(self, connection_pool: "ConnectionPool"):
        if not self.use_cache:
            return
        if connection_pool is None:
            return
        original_maker = connection_pool.make_connection
        connection_pool.make_connection = lambda: self._make_connection(original_maker)

    def _make_connection(self, original_maker: Callable[[], "Connection"]):
        conn = original_maker()
        original_disconnect = conn.disconnect
        conn.disconnect = lambda: self._wrapped_disconnect(conn, original_disconnect)
        self.add_connection(conn)
        return conn

    def _wrapped_disconnect(self, connection: "Connection",
                            original_disconnect: Callable[[], NoReturn]):
        original_disconnect()
        self.remove_connection(connection)

    def add_connection(self, conn):
        print(f'Tracking connection {conn} {id(conn)}')
        conn.register_connect_callback(self._on_connect)
        self.connections.append(conn)

    def _on_connect(self, conn):
        conn.send_command("CLIENT", "TRACKING", "ON")
        response = conn.read_response()
        print(f"Client tracking response {response}")
        conn._parser.set_invalidation_push_handler(self._cache_invalidation_process)

    def _cache_invalidation_process(
        self, data: List[Union[str, Optional[List[str]]]]
    ) -> None:
        """
        Invalidate (delete) all redis commands associated with a specific key.
        `data` is a list of strings, where the first string is the invalidation message
        and the second string is the list of keys to invalidate.
        (if the list of keys is None, then all keys are invalidated)
        """
        print(f'Invalidation {data}')
        if data[1] is None:
            self.cache.clear()
        else:
            for key in data[1]:
                normalized_key = ensure_string(key)
                print(f'Invalidating normalized key {normalized_key}')
                if normalized_key in self.keys_mapping:
                    for cache_key in self.keys_mapping[normalized_key]:
                        print(f'Invalidating cache key {cache_key}')
                        self.cache.pop(cache_key)

    def remove_connection(self, conn):
        print(f'Untracking connection {conn} {id(conn)}')
        self.connections.remove(conn)
