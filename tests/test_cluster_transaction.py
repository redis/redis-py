import threading
from typing import Tuple
from unittest.mock import patch, Mock

import pytest

import redis
from redis import CrossSlotTransactionError, ConnectionPool, RedisClusterException
from redis.backoff import NoBackoff
from redis.client import Redis
from redis.cluster import PRIMARY, ClusterNode, NodesManager, RedisCluster
from redis.retry import Retry

from .conftest import skip_if_server_version_lt


def _find_source_and_target_node_for_slot(
    r: RedisCluster, slot: int
) -> Tuple[ClusterNode, ClusterNode]:
    """Returns a pair of ClusterNodes, where the first node is the
    one that owns the slot and the second is a possible target
    for that slot, i.e. a primary node different from the first
    one.
    """
    node_migrating = r.nodes_manager.get_node_from_slot(slot)
    assert node_migrating, f"No node could be found that owns slot #{slot}"

    available_targets = [
        n
        for n in r.nodes_manager.startup_nodes.values()
        if node_migrating.name != n.name and n.server_type == PRIMARY
    ]

    assert available_targets, f"No possible target nodes for slot #{slot}"
    return node_migrating, available_targets[0]


class TestClusterTransaction:
    @pytest.mark.onlycluster
    def test_pipeline_is_true(self, r):
        "Ensure pipeline instances are not false-y"
        with r.pipeline(transaction=True) as pipe:
            assert pipe

    @pytest.mark.onlycluster
    def test_pipeline_empty_transaction(self, r):
        r["a"] = 0

        with r.pipeline(transaction=True) as pipe:
            assert pipe.execute() == []

    @pytest.mark.onlycluster
    def test_executes_transaction_against_cluster(self, r):
        with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foo}baz", "value2")
            tx.set("{foo}bad", "value3")
            tx.get("{foo}bar")
            tx.get("{foo}baz")
            tx.get("{foo}bad")
            assert tx.execute() == [
                b"OK",
                b"OK",
                b"OK",
                b"value1",
                b"value2",
                b"value3",
            ]

        r.flushall()

        tx = r.pipeline(transaction=True)
        tx.set("{foo}bar", "value1")
        tx.set("{foo}baz", "value2")
        tx.set("{foo}bad", "value3")
        tx.get("{foo}bar")
        tx.get("{foo}baz")
        tx.get("{foo}bad")
        assert tx.execute() == [b"OK", b"OK", b"OK", b"value1", b"value2", b"value3"]

    @pytest.mark.onlycluster
    def test_throws_exception_on_different_hash_slots(self, r):
        with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foobar}baz", "value2")

            with pytest.raises(
                CrossSlotTransactionError,
                match="All keys involved in a cluster transaction must map to the same slot",
            ):
                tx.execute()

    @pytest.mark.onlycluster
    def test_throws_exception_with_watch_on_different_hash_slots(self, r):
        with r.pipeline(transaction=True) as tx:
            with pytest.raises(
                RedisClusterException,
                match="WATCH - all keys must map to the same key slot",
            ):
                tx.watch("key1", "key2")

    @pytest.mark.onlycluster
    def test_transaction_with_watched_keys(self, r):
        r["a"] = 0

        with r.pipeline(transaction=True) as pipe:
            pipe.watch("a")
            a = pipe.get("a")
            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert pipe.execute() == [b"OK"]

    @pytest.mark.onlycluster
    def test_retry_transaction_during_unfinished_slot_migration(self, r):
        """
        When a transaction is triggered during a migration, MovedError
        or AskError may appear (depends on the key being already migrated
        or the key not existing already). The patch on parse_response
        simulates such an error, but the slot cache is not updated
        (meaning the migration is still ongogin) so the pipeline eventually
        fails as if it was retried but the migration is not yet complete.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with (
            patch.object(Redis, "parse_response") as parse_response,
            patch.object(
                NodesManager, "_update_moved_slots"
            ) as manager_update_moved_slots,
        ):

            def ask_redirect_effect(connection, *args, **options):
                if "MULTI" in args:
                    return
                elif "EXEC" in args:
                    raise redis.exceptions.ExecAbortError()

                raise redis.exceptions.AskError(f"{slot} {node_importing.name}")

            parse_response.side_effect = ask_redirect_effect

            with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.AskError) as ex:
                    pipe.execute()

                assert str(ex.value).startswith(
                    "Command # 1 (SET book val) of pipeline caused error:"
                    f" {slot} {node_importing.name}"
                )

            manager_update_moved_slots.assert_called()

    @pytest.mark.onlycluster
    def test_retry_transaction_during_slot_migration_successful(self, r):
        """
        If a MovedError or AskError appears when calling EXEC and no key is watched,
        the pipeline is retried after updating the node manager slot table. If the
        migration was completed, the transaction may then complete successfully.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with (
            patch.object(Redis, "parse_response") as parse_response,
            patch.object(
                NodesManager, "_update_moved_slots"
            ) as manager_update_moved_slots,
        ):

            def ask_redirect_effect(conn, *args, **options):
                # first call should go here, we trigger an AskError
                if f"{conn.host}:{conn.port}" == node_migrating.name:
                    if "MULTI" in args:
                        return
                    elif "EXEC" in args:
                        raise redis.exceptions.ExecAbortError()

                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")
                # if the slot table is updated, the next call will go here
                elif f"{conn.host}:{conn.port}" == node_importing.name:
                    if "EXEC" in args:
                        return [
                            "MOCK_OK"
                        ]  # mock value to validate this section was called
                    return
                else:
                    assert False, f"unexpected node {conn.host}:{conn.port} was called"

            def update_moved_slot():  # simulate slot table update
                ask_error = r.nodes_manager._moved_exception
                assert ask_error is not None, "No AskError was previously triggered"
                assert f"{ask_error.host}:{ask_error.port}" == node_importing.name
                r.nodes_manager._moved_exception = None
                r.nodes_manager.slots_cache[slot] = [node_importing]

            parse_response.side_effect = ask_redirect_effect
            manager_update_moved_slots.side_effect = update_moved_slot

            result = None
            with r.pipeline(transaction=True) as pipe:
                pipe.multi()
                pipe.set(key, "val")
                result = pipe.execute()

            assert result and "MOCK_OK" in result, "Target node was not called"

    @pytest.mark.onlycluster
    def test_retry_transaction_with_watch_after_slot_migration(self, r):
        """
        If a MovedError or AskError appears when calling WATCH, the client
        must attempt to recover itself before proceeding and no WatchError
        should appear.
        """
        key = "book"
        slot = r.keyslot(key)
        r.reinitialize_steps = 1

        # force a MovedError on the first call to pipe.watch()
        # by switching the node that owns the slot to another one
        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        r.nodes_manager.slots_cache[slot] = [node_importing]

        with r.pipeline(transaction=True) as pipe:
            pipe.watch(key)
            pipe.multi()
            pipe.set(key, "val")
            assert pipe.execute() == [b"OK"]

    @pytest.mark.onlycluster
    def test_retry_transaction_with_watch_during_slot_migration(self, r):
        """
        If a MovedError or AskError appears when calling EXEC and keys were
        being watched before the migration started, a WatchError should appear.
        These errors imply resetting the connection and connecting to a new node,
        so watches are lost anyway and the client code must be notified.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with patch.object(Redis, "parse_response") as parse_response:

            def ask_redirect_effect(conn, *args, **options):
                if f"{conn.host}:{conn.port}" == node_migrating.name:
                    # we simulate the watch was sent before the migration started
                    if "WATCH" in args:
                        return b"OK"
                    # but the pipeline was triggered after the migration started
                    elif "MULTI" in args:
                        return
                    elif "EXEC" in args:
                        raise redis.exceptions.ExecAbortError()

                    raise redis.exceptions.AskError(f"{slot} {node_importing.name}")
                # we should not try to connect to any other node
                else:
                    assert False, f"unexpected node {conn.host}:{conn.port} was called"

            parse_response.side_effect = ask_redirect_effect

            with r.pipeline(transaction=True) as pipe:
                pipe.watch(key)
                pipe.multi()
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.WatchError) as ex:
                    pipe.execute()

                assert str(ex.value).startswith(
                    "Slot rebalancing occurred while watching keys"
                )

    @pytest.mark.onlycluster
    def test_retry_transaction_on_connection_error(self, r, mock_connection):
        key = "book"
        slot = r.keyslot(key)

        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection]
        mock_pool._lock = threading.RLock()

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        node_importing.redis_connection.connection_pool = mock_pool
        r.nodes_manager.slots_cache[slot] = [node_importing]
        r.reinitialize_steps = 1

        with r.pipeline(transaction=True) as pipe:
            pipe.set(key, "val")
            assert pipe.execute() == [b"OK"]

    @pytest.mark.onlycluster
    def test_retry_transaction_on_connection_error_with_watched_keys(
        self, r, mock_connection
    ):
        key = "book"
        slot = r.keyslot(key)

        mock_connection.read_response.side_effect = redis.exceptions.ConnectionError(
            "Conn error"
        )
        mock_connection.retry = Retry(NoBackoff(), 0)
        mock_pool = Mock(spec=ConnectionPool)
        mock_pool.get_connection.return_value = mock_connection
        mock_pool._available_connections = [mock_connection]
        mock_pool._lock = threading.RLock()

        _node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)
        node_importing.redis_connection.connection_pool = mock_pool
        r.nodes_manager.slots_cache[slot] = [node_importing]
        r.reinitialize_steps = 1

        with r.pipeline(transaction=True) as pipe:
            pipe.watch(key)
            pipe.multi()
            pipe.set(key, "val")
            assert pipe.execute() == [b"OK"]

    @pytest.mark.onlycluster
    def test_exec_error_raised(self, r):
        hashkey = "{key}"
        r[f"{hashkey}:c"] = "a"
        with r.pipeline(transaction=True) as pipe:
            pipe.set(f"{hashkey}:a", 1).set(f"{hashkey}:b", 2)
            pipe.lpush(f"{hashkey}:c", 3).set(f"{hashkey}:d", 4)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()
            assert str(ex.value).startswith(
                "Command # 3 (LPUSH {key}:c 3) of pipeline caused error: "
            )

            # make sure the pipe was restored to a working state
            assert pipe.set(f"{hashkey}:z", "zzz").execute() == [b"OK"]
            assert r[f"{hashkey}:z"] == b"zzz"

    @pytest.mark.onlycluster
    def test_parse_error_raised(self, r):
        hashkey = "{key}"
        with r.pipeline(transaction=True) as pipe:
            # the zrem is invalid because we don't pass any keys to it
            pipe.set(f"{hashkey}:a", 1).zrem(f"{hashkey}:b").set(f"{hashkey}:b", 2)
            with pytest.raises(redis.ResponseError) as ex:
                pipe.execute()

            assert str(ex.value).startswith(
                "Command # 2 (ZREM {key}:b) of pipeline caused error: wrong number"
            )

            # make sure the pipe was restored to a working state
            assert pipe.set(f"{hashkey}:z", "zzz").execute() == [b"OK"]
            assert r[f"{hashkey}:z"] == b"zzz"

    @pytest.mark.onlycluster
    def test_transaction_callable(self, r):
        hashkey = "{key}"
        r[f"{hashkey}:a"] = 1
        r[f"{hashkey}:b"] = 2
        has_run = []

        def my_transaction(pipe):
            a_value = pipe.get(f"{hashkey}:a")
            assert a_value in (b"1", b"2")
            b_value = pipe.get(f"{hashkey}:b")
            assert b_value == b"2"

            # silly run-once code... incr's "a" so WatchError should be raised
            # forcing this all to run again. this should incr "a" once to "2"
            if not has_run:
                r.incr(f"{hashkey}:a")
                has_run.append("it has")

            pipe.multi()
            pipe.set(f"{hashkey}:c", int(a_value) + int(b_value))

        result = r.transaction(my_transaction, f"{hashkey}:a", f"{hashkey}:b")
        assert result == [b"OK"]
        assert r[f"{hashkey}:c"] == b"4"

    @pytest.mark.onlycluster
    @skip_if_server_version_lt("2.0.0")
    def test_transaction_discard(self, r):
        hashkey = "{key}"

        # pipelines enabled as transactions can be discarded at any point
        with r.pipeline(transaction=True) as pipe:
            pipe.watch(f"{hashkey}:key")
            pipe.set(f"{hashkey}:key", "someval")
            pipe.discard()

            assert not pipe._execution_strategy._watching
            assert not pipe.command_stack
