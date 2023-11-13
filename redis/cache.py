import random
import time
from collections import OrderedDict, defaultdict


class _Cache:
    def __init__(self, max_size: int, ttl: int, eviction_policy: str, **kwargs):
        self.max_size = max_size
        self.ttl = ttl
        self.eviction_policy = eviction_policy
        self.cache = OrderedDict()
        self.key_commands_map = defaultdict(set)
        self.commands_ttl_list = []

    def set(self, command, response):
        keys_in_command = self.get_keys_from_command(command)
        if len(self.cache) >= self.max_size:
            self._evict()
        self.cache[command] = {
            "response": response,
            "keys": keys_in_command,
            "created_time": time.monotonic(),
        }
        if self.eviction_policy == "lfu":
            self.cache[command]["access_count"] = 0
        self._update_key_commands_map(keys_in_command, command)
        self.commands_ttl_list.append(command)

    def get(self, command):
        if command in self.cache:
            if self._is_expired(command):
                del self.cache[command]
                keys_in_command = self.cache[command]["keys"]
                self._del_key_commands_map(keys_in_command, command)
                return None
            self._update_access(command)
            return self.cache[command]["response"]
        return None

    def delete(self, command):
        if command in self.cache:
            keys_in_command = self.cache[command]["keys"]
            self._del_key_commands_map(keys_in_command, command)
            self.commands_ttl_list.remove(command)
            del self.cache[command]

    def delete_many(self, commands):
        pass

    def flush(self):
        self.cache.clear()
        self.key_commands_map.clear()
        self.commands_ttl_list = []

    def _is_expired(self, command):
        if self.ttl == 0:
            return False
        return time.monotonic() - self.cache[command]["created_time"] > self.ttl

    def _update_access(self, command):
        if self.eviction_policy == "lru":
            self.cache.move_to_end(command)
        elif self.eviction_policy == "lfu":
            self.cache[command]["access_count"] = (
                self.cache.get(command, {}).get("access_count", 0) + 1
            )
            self.cache.move_to_end(command)
        elif self.eviction_policy == "random":
            pass  # Random eviction doesn't require updates

    def _evict(self):
        if self._is_expired(self.commands_ttl_list[0]):
            self.delete(self.commands_ttl_list[0])
        elif self.eviction_policy == "lru":
            self.cache.popitem(last=False)
        elif self.eviction_policy == "lfu":
            min_access_command = min(
                self.cache, key=lambda k: self.cache[k].get("access_count", 0)
            )
            self.cache.pop(min_access_command)
        elif self.eviction_policy == "random":
            random_command = random.choice(list(self.cache.keys()))
            self.cache.pop(random_command)

    def _update_key_commands_map(self, keys, command):
        for key in keys:
            self.key_commands_map[key].add(command)

    def _del_key_commands_map(self, keys, command):
        for key in keys:
            self.key_commands_map[key].remove(command)

    def invalidate(self, key):
        if key in self.key_commands_map:
            for command in self.key_commands_map[key]:
                self.delete(command)

    def get_keys_from_command(self, command):
        # Implement your function to extract keys from a Redis command here
        pass
