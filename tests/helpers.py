import logging
import time
from time import sleep
from typing import Callable
from unittest.mock import MagicMock

from redis.cache import CacheConfig, CacheKey, DefaultCache
from redis.commands.metadata import RequestPolicy, ResponsePolicy
from redis.observability.attributes import CSCRefreshResult


def wait_for_condition(
    predicate: Callable[[], bool],
    timeout: float = 5.0,
    check_interval: float = 0.01,
    error_message: str = "Timeout waiting for condition",
) -> None:
    """
    Poll a condition until it becomes True or timeout is reached.

    Args:
        predicate: A callable that returns True when the condition is met
        timeout: Maximum time to wait in seconds (default: 5s). A passing
            condition returns as soon as it holds, so a long deadline only
            costs time on a stalled runner.
        check_interval: Time to sleep between checks in seconds (default: 0.01s)
        error_message: Error message to raise if timeout occurs

    Raises:
        AssertionError: If the condition is not met within the timeout period

    Example:
        # Wait for circuit breaker to open
        wait_for_condition(
            lambda: cb2.state == CBState.OPEN,
            timeout=5.0,
            error_message="Timeout waiting for cb2 to open"
        )

        # Wait for failover strategy to select a specific database
        wait_for_condition(
            lambda: client.command_executor.active_database is mock_db,
            timeout=5.0,
            error_message="Timeout waiting for active database to change"
        )
    """
    # Bounded by elapsed time, not by an iteration count: a slow predicate or a
    # late wakeup must not stretch or shorten the deadline.
    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        if predicate():
            logging.debug(f"Condition met after {attempt} attempts")
            return
        if time.monotonic() >= deadline:
            raise AssertionError(error_message)
        attempt += 1
        sleep(check_interval)


def get_cache_key(*keys) -> CacheKey:
    """The cache key of a ``GET`` - or, given several keys, a ``GET`` naming them all."""
    return CacheKey(command="GET", redis_keys=keys, redis_args=("GET", *keys))


def wait_for_refreshes(refresher, error_message="refreshes never completed") -> None:
    """
    Wait until a ``CacheRefresher`` has nothing pending.

    An invalidation submits its keys inside the read that picked it up, so by the time that
    read returns they are pending. Empty means every accepted refresh has finished or was
    cancelled; callers assert which by what the cache holds.
    """
    wait_for_condition(
        lambda: not refresher._pending, timeout=2, error_message=error_message
    )


class FakeRefresherPool:
    """
    The pool surface a ``CacheRefresher`` uses, recording what it is asked for.

    Hands out ``connection``: a mock by default, whose refresh round trip can be held or
    failed through its ``send_command`` and ``read_response``, or a real proxy a test
    assigns.
    """

    def __init__(self, cache=None):
        self.cache = cache if cache is not None else DefaultCache(CacheConfig())
        self.connection = MagicMock()
        self.connection._refresh_entry.side_effect = self._refresh_entry
        # What the mock connection reports for a refresh that did not raise.
        self.refresh_result = CSCRefreshResult.SUCCESS
        self.get_connection_error = None
        # Every connection in use and the pool at its limit.
        self.full = False
        self.released = []

    def _get_connection(self, if_available=False):
        assert if_available, "a refresh must never wait for a pool connection"
        if self.full:
            return None
        if self.get_connection_error is not None:
            error, self.get_connection_error = self.get_connection_error, None
            raise error
        return self.connection

    def release(self, connection):
        self.released.append(connection)

    def _refresh_entry(self, cache_key, is_current=None):
        self.connection.send_command(*cache_key.redis_args, keys=cache_key.redis_keys)
        self.connection.read_response()
        return self.refresh_result


def get_expected_command_policies(changes_in_defaults={}):
    default_cmd_policies = {
        "core": {
            "keys": [
                "keys",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
            "acl setuser": [
                "acl setuser",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
            "exists": ["exists", RequestPolicy.MULTI_SHARD, ResponsePolicy.AGG_SUM],
            "config resetstat": [
                "config resetstat",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
            "slowlog len": [
                "slowlog len",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.AGG_SUM,
            ],
            "scan": ["scan", RequestPolicy.SPECIAL, ResponsePolicy.SPECIAL],
            "latency history": [
                "latency history",
                RequestPolicy.ALL_NODES,
                ResponsePolicy.SPECIAL,
            ],
            "memory doctor": [
                "memory doctor",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.SPECIAL,
            ],
            "randomkey": [
                "randomkey",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.SPECIAL,
            ],
            "mget": [
                "mget",
                RequestPolicy.MULTI_SHARD,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "function restore": [
                "function restore",
                RequestPolicy.ALL_SHARDS,
                ResponsePolicy.ALL_SUCCEEDED,
            ],
        },
        "json": {
            "debug": [
                "debug",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "get": [
                "get",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "ft": {
            "search": [
                "search",
                RequestPolicy.DEFAULT_KEYLESS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
            "create": [
                "create",
                RequestPolicy.DEFAULT_KEYLESS,
                ResponsePolicy.DEFAULT_KEYLESS,
            ],
        },
        "bf": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "madd": [
                "madd",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "cf": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "mexists": [
                "mexists",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "tdigest": {
            "add": [
                "add",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "min": [
                "min",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "ts": {
            "create": [
                "create",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "info": [
                "info",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
        "topk": {
            "list": [
                "list",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
            "query": [
                "query",
                RequestPolicy.DEFAULT_KEYED,
                ResponsePolicy.DEFAULT_KEYED,
            ],
        },
    }
    default_cmd_policies.update(changes_in_defaults)
    return default_cmd_policies
