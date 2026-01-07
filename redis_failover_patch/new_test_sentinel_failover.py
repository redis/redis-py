import pytest
from unittest.mock import Mock
from redis.sentinel import Sentinel

def test_switch_master_event_updates_master():
    sentinel = Sentinel([('127.0.0.1', 26379)])
    client = sentinel.master_for('mymaster')
    # initial connection
    client.connection_pool.connection_kwargs = {'host': '127.0.0.1', 'port': 6379}

    # switch-master
    sentinel._handle_switch_master('mymaster', '127.0.0.2', 6380)

    # Assert client updated to new master
    assert client.connection_pool.connection_kwargs['host'] == '127.0.0.2'
    assert client.connection_pool.connection_kwargs['port'] == 6380
def test_rapid_consecutive_failovers():
    sentinel = Sentinel([('127.0.0.1', 26379)])
    client = sentinel.master_for('mymaster')
    client.connection_pool.connection_kwargs = {'host': '127.0.0.1', 'port': 6379}

    # First failover
    sentinel._handle_switch_master('mymaster', '127.0.0.2', 6380)
    # Second failover immediately
    sentinel._handle_switch_master('mymaster', '127.0.0.3', 6381)

    assert client.connection_pool.connection_kwargs['host'] == '127.0.0.3'
    assert client.connection_pool.connection_kwargs['port'] == 6381

def test_async_client_follow_switch_master():
    sentinel = Sentinel([('127.0.0.1', 26379)], socket_timeout=0.1)
    async_client = sentinel.master_for('mymaster', redis_class=Mock)
    async_client.connection_pool.connection_kwargs = {'host': '127.0.0.1', 'port': 6379}

    sentinel._handle_switch_master('mymaster', '127.0.0.2', 6380)
    assert async_client.connection_pool.connection_kwargs['host'] == '127.0.0.2'
    assert async_client.connection_pool.connection_kwargs['port'] == 6380
def test_missed_events_recovery():
    sentinel = Sentinel([('127.0.0.1', 26379)])
    client = sentinel.master_for('mymaster')
    client.connection_pool.connection_kwargs = {'host': '127.0.0.1', 'port': 6379}
    # Simulate missed event recovery
    # Assume internal method that fetches current master from Sentinel
    sentinel.discover_master = Mock(return_value=('127.0.0.4', 6382))
    sentinel._handle_missed_switch_master('mymaster')
    assert client.connection_pool.connection_kwargs['host'] == '127.0.0.4'
    assert client.connection_pool.connection_kwargs['port'] == 6382
