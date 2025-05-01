from __future__ import annotations

from collections.abc import Sequence
from typing import (
    Any,
    AsyncIterator,
    Iterable,
    List,
    Mapping,
    Optional,
)

from ..utils import gather
from ...commands.cluster import (
    ClusterDataAccessCommands, ClusterManagementCommands,
    ClusterMultiKeyCommands, READ_COMMANDS,
)
from ...commands.core import (
    AsyncACLCommands,
    AsyncDataAccessCommands,
    AsyncFunctionCommands,
    AsyncManagementCommands,
    AsyncModuleCommands,
    AsyncScriptCommands,
)
from ...commands.helpers import list_or_args
from ...commands.redismodules import AsyncRedisModuleCommands
from ...typing import AnyKeyT, EncodableT, KeyT, KeysT, PatternT


class AsyncClusterMultiKeyCommands(ClusterMultiKeyCommands):
    """
    A class containing commands that handle more than one key
    """

    async def mget_nonatomic(self, keys: KeysT, *args: KeyT) -> List[Optional[Any]]:
        """
        Splits the keys into different slots and then calls MGET
        for the keys of every slot. This operation will not be atomic
        if keys belong to more than one slot.

        Returns a list of values ordered identically to ``keys``

        For more information see https://redis.io/commands/mget
        """

        # Concatenate all keys into a list
        keys = list_or_args(keys, args)

        # Split keys into slots
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Execute commands using a pipeline
        res = await self._execute_pipeline_by_slot("MGET", slots_to_keys)

        # Reorder keys in the order the user provided & return
        return self._reorder_keys_by_command(keys, slots_to_keys, res)

    async def mset_nonatomic(self, mapping: Mapping[AnyKeyT, EncodableT]) -> List[bool]:
        """
        Sets key/values based on a mapping. Mapping is a dictionary of
        key/value pairs. Both keys and values should be strings or types that
        can be cast to a string via str().

        Splits the keys into different slots and then calls MSET
        for the keys of every slot. This operation will not be atomic
        if keys belong to more than one slot.

        For more information see https://redis.io/commands/mset
        """

        # Partition the keys by slot
        slots_to_pairs = self._partition_pairs_by_slot(mapping)

        # Execute commands using a pipeline & return list of replies
        return await self._execute_pipeline_by_slot("MSET", slots_to_pairs)

    async def _split_command_across_slots(self, command: str, *keys: KeyT) -> int:
        """
        Runs the given command once for the keys
        of each slot. Returns the sum of the return values.
        """

        # Partition the keys by slot
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Sum up the reply from each command
        return sum(await self._execute_pipeline_by_slot(command, slots_to_keys))

    async def _execute_pipeline_by_slot(
        self, command: str, slots_to_args: Mapping[int, Iterable[EncodableT]]
    ) -> List[Any]:
        read_from_replicas = self.read_from_replicas and command in READ_COMMANDS
        pipe = self.pipeline()
        [
            pipe.execute_command(
                command,
                *slot_args,
                target_nodes=[
                    self.nodes_manager.get_node_from_slot(slot, read_from_replicas)
                ],
            )
            for slot, slot_args in slots_to_args.items()
        ]
        return await pipe.execute()


class AsyncClusterManagementCommands(
    ClusterManagementCommands, AsyncManagementCommands
):
    """
    A class for Redis Cluster management commands

    The class inherits from Redis's core ManagementCommands class and do the
    required adjustments to work with cluster mode
    """

    async def cluster_delslots(self, *slots: EncodableT) -> Sequence[bool]:
        """
        Set hash slots as unbound in the cluster.
        It determines by it self what node the slot is in and sends it there

        Returns a list of the results for each processed slot.

        For more information see https://redis.io/commands/cluster-delslots
        """
        return await gather(
            *(
                self.execute_command("CLUSTER DELSLOTS", slot)
                for slot in slots
            )
        )


class AsyncClusterDataAccessCommands(
    ClusterDataAccessCommands, AsyncDataAccessCommands
):
    """
    A class for Redis Cluster Data Access Commands

    The class inherits from Redis's core DataAccessCommand class and do the
    required adjustments to work with cluster mode
    """

    async def scan_iter(
        self,
        match: Optional[PatternT] = None,
        count: Optional[int] = None,
        _type: Optional[str] = None,
        **kwargs,
    ) -> AsyncIterator:
        # Do the first query with cursor=0 for all nodes
        cursors, data = await self.scan(match=match, count=count, _type=_type, **kwargs)
        for value in data:
            yield value

        cursors = {name: cursor for name, cursor in cursors.items() if cursor != 0}
        if cursors:
            # Get nodes by name
            nodes = {name: self.get_node(node_name=name) for name in cursors.keys()}

            # Iterate over each node till its cursor is 0
            kwargs.pop("target_nodes", None)
            while cursors:
                for name, cursor in cursors.items():
                    cur, data = await self.scan(
                        cursor=cursor,
                        match=match,
                        count=count,
                        _type=_type,
                        target_nodes=nodes[name],
                        **kwargs,
                    )
                    for value in data:
                        yield value
                    cursors[name] = cur[name]

                cursors = {
                    name: cursor for name, cursor in cursors.items() if cursor != 0
                }


class AsyncRedisClusterCommands(
    AsyncClusterMultiKeyCommands,
    AsyncClusterManagementCommands,
    AsyncACLCommands,
    AsyncClusterDataAccessCommands,
    AsyncScriptCommands,
    AsyncFunctionCommands,
    AsyncModuleCommands,
    AsyncRedisModuleCommands,
):
    """
    A class for all Redis Cluster commands

    For key-based commands, the target node(s) will be internally determined
    by the keys' hash slot.
    Non-key-based commands can be executed with the 'target_nodes' argument to
    target specific nodes. By default, if target_nodes is not specified, the
    command will be executed on the default cluster node.

    :param :target_nodes: type can be one of the followings:
        - nodes flag: ALL_NODES, PRIMARIES, REPLICAS, RANDOM
        - 'ClusterNode'
        - 'list(ClusterNodes)'
        - 'dict(any:clusterNodes)'

    for example:
        r.cluster_info(target_nodes=RedisCluster.ALL_NODES)
    """
