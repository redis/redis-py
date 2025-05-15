from typing import Tuple
from unittest.mock import patch

import pytest

import redis
from redis import CrossSlotTransactionError, RedisClusterException
from redis.asyncio import RedisCluster, Redis
from redis.asyncio.cluster import ClusterNode, NodesManager
from redis.cluster import PRIMARY
from tests.test_asyncio.conftest import create_redis


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

@pytest.mark.onlycluster
class TestClusterTransaction:
    @pytest.mark.onlycluster
    async def test_pipeline_is_true(self, r) -> None:
        "Ensure pipeline instances are not false-y"
        async with r.pipeline(transaction=True) as pipe:
            assert pipe

    @pytest.mark.onlycluster
    async def test_pipeline_empty_transaction(self, r):
        await r.set('a', 0)

        async with r.pipeline(transaction=True) as pipe:
            assert await pipe.execute() == []

    @pytest.mark.onlycluster
    async def test_executes_transaction_against_cluster(self, r) -> None:
        async with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foo}baz", "value2")
            tx.set("{foo}bad", "value3")
            tx.get("{foo}bar")
            tx.get("{foo}baz")
            tx.get("{foo}bad")
            assert await tx.execute() == [
                True,
                True,
                True,
                b"value1",
                b"value2",
                b"value3",
            ]

        await r.flushall()

        tx = r.pipeline(transaction=True)
        tx.set("{foo}bar", "value1")
        tx.set("{foo}baz", "value2")
        tx.set("{foo}bad", "value3")
        tx.get("{foo}bar")
        tx.get("{foo}baz")
        tx.get("{foo}bad")
        assert await tx.execute() == [True, True, True, b"value1", b"value2", b"value3"]

    @pytest.mark.onlycluster
    async def test_throws_exception_on_different_hash_slots(self, r):
        async with r.pipeline(transaction=True) as tx:
            tx.set("{foo}bar", "value1")
            tx.set("{foobar}baz", "value2")

            with pytest.raises(
                CrossSlotTransactionError,
                match="All keys involved in a cluster transaction must map to the same slot",
            ):
                await tx.execute()

    @pytest.mark.onlycluster
    async def test_throws_exception_with_watch_on_different_hash_slots(self, r):
        async with r.pipeline(transaction=True) as tx:
            with pytest.raises(
                RedisClusterException,
                match="WATCH - all keys must map to the same key slot",
            ):
                await tx.watch("key1", "key2")

    @pytest.mark.onlycluster
    async def test_transaction_with_watched_keys(self, r):
        await r.set("a", 0)

        async with r.pipeline(transaction=True) as pipe:
            await pipe.watch("a")
            a = await pipe.get("a")

            pipe.multi()
            pipe.set("a", int(a) + 1)
            assert await pipe.execute() == [True]

    @pytest.mark.onlycluster
    async def test_retry_transaction_during_unfinished_slot_migration(self, r):
        """
        When a transaction is triggered during a migration, MovedError
        or AskError may appear (depends on the key being already migrated
        or the key not existing already). The patch on parse_response
        simulates such an error, but the slot cache is not updated
        (meaning the migration is still ongoing) so the pipeline eventually
        fails as if it was retried but the migration is not yet complete.
        """
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with patch.object(ClusterNode, "parse_response") as parse_response, patch.object(
            NodesManager, "_update_moved_slots"
        ) as manager_update_moved_slots:

            def ask_redirect_effect(connection, *args, **options):
                if "MULTI" in args:
                    return
                elif "EXEC" in args:
                    raise redis.exceptions.ExecAbortError()

                raise redis.exceptions.AskError(f"{slot} {node_importing.name}")

            parse_response.side_effect = ask_redirect_effect

            async with r.pipeline(transaction=True) as pipe:
                pipe.set(key, "val")
                with pytest.raises(redis.exceptions.AskError) as ex:
                    await pipe.execute()

                assert str(ex.value).startswith(
                    "Command # 1 (SET book val) of pipeline caused error:"
                    f" {slot} {node_importing.name}"
                )

            manager_update_moved_slots.assert_called()

    @pytest.mark.onlycluster
    async def test_retry_transaction_during_slot_migration_successful(self, create_redis):
        """
        If a MovedError or AskError appears when calling EXEC and no key is watched,
        the pipeline is retried after updating the node manager slot table. If the
        migration was completed, the transaction may then complete successfully.
        """
        r = await create_redis(flushdb=False)
        key = "book"
        slot = r.keyslot(key)
        node_migrating, node_importing = _find_source_and_target_node_for_slot(r, slot)

        with patch.object(ClusterNode, "parse_response") as parse_response, patch.object(
            NodesManager, "_update_moved_slots"
        ) as manager_update_moved_slots:

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
                            "OK"
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
            async with r.pipeline(transaction=True) as pipe:
                pipe.multi()
                pipe.set(key, "val")
                result = await pipe.execute()

            assert result and True in result, "Target node was not called"
