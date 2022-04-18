import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NoReturn,
    Optional,
    Union,
)

from redis.compat import Literal
from redis.crc import key_slot
from redis.exceptions import RedisClusterException, RedisError
from redis.typing import (
    AnyKeyT,
    ClusterCommandsProtocol,
    EncodableT,
    KeysT,
    KeyT,
    PatternT,
)

from .core import (
    ACLCommands,
    AsyncACLCommands,
    AsyncDataAccessCommands,
    AsyncFunctionCommands,
    AsyncManagementCommands,
    AsyncScriptCommands,
    DataAccessCommands,
    FunctionCommands,
    ManagementCommands,
    PubSubCommands,
    ScriptCommands,
)
from .helpers import list_or_args
from .redismodules import RedisModuleCommands

if TYPE_CHECKING:
    from redis.asyncio.cluster import TargetNodesT


class ClusterMultiKeyCommands(ClusterCommandsProtocol):
    """
    A class containing commands that handle more than one key
    """

    def _partition_keys_by_slot(self, keys):
        """
        Split keys into a dictionary that maps a slot to
        a list of keys.
        """
        slots_to_keys = {}
        for key in keys:
            k = self.encoder.encode(key)
            slot = key_slot(k)
            slots_to_keys.setdefault(slot, []).append(key)

        return slots_to_keys

    def mget_nonatomic(self, keys, *args):
        """
        Splits the keys into different slots and then calls MGET
        for the keys of every slot. This operation will not be atomic
        if keys belong to more than one slot.

        Returns a list of values ordered identically to ``keys``

        For more information see https://redis.io/commands/mget
        """

        from redis.client import EMPTY_RESPONSE

        options = {}
        if not args:
            options[EMPTY_RESPONSE] = []

        # Concatenate all keys into a list
        keys = list_or_args(keys, args)
        # Split keys into slots
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Call MGET for every slot and concatenate
        # the results
        # We must make sure that the keys are returned in order
        all_results = {}
        for slot_keys in slots_to_keys.values():
            slot_values = self.execute_command("MGET", *slot_keys, **options)

            slot_results = dict(zip(slot_keys, slot_values))
            all_results.update(slot_results)

        # Sort the results
        vals_in_order = [all_results[key] for key in keys]
        return vals_in_order

    def mset_nonatomic(self, mapping):
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
        slots_to_pairs = {}
        for pair in mapping.items():
            # encode the key
            k = self.encoder.encode(pair[0])
            slot = key_slot(k)
            slots_to_pairs.setdefault(slot, []).extend(pair)

        # Call MSET for every slot and concatenate
        # the results (one result per slot)
        res = []
        for pairs in slots_to_pairs.values():
            res.append(self.execute_command("MSET", *pairs))

        return res

    def _split_command_across_slots(self, command, *keys):
        """
        Runs the given command once for the keys
        of each slot. Returns the sum of the return values.
        """
        # Partition the keys by slot
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Sum up the reply from each command
        total = 0
        for slot_keys in slots_to_keys.values():
            total += self.execute_command(command, *slot_keys)

        return total

    def exists(self, *keys):
        """
        Returns the number of ``names`` that exist in the
        whole cluster. The keys are first split up into slots
        and then an EXISTS command is sent for every slot

        For more information see https://redis.io/commands/exists
        """
        return self._split_command_across_slots("EXISTS", *keys)

    def delete(self, *keys):
        """
        Deletes the given keys in the cluster.
        The keys are first split up into slots
        and then an DEL command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were deleted.

        For more information see https://redis.io/commands/del
        """
        return self._split_command_across_slots("DEL", *keys)

    def touch(self, *keys):
        """
        Updates the last access time of given keys across the
        cluster.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were touched.

        For more information see https://redis.io/commands/touch
        """
        return self._split_command_across_slots("TOUCH", *keys)

    def unlink(self, *keys):
        """
        Remove the specified keys in a different thread.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were unlinked.

        For more information see https://redis.io/commands/unlink
        """
        return self._split_command_across_slots("UNLINK", *keys)


class AsyncClusterMultiKeyCommands(ClusterCommandsProtocol):
    """
    A class containing commands that handle more than one key
    """

    def _partition_keys_by_slot(self, keys: Iterable[KeyT]) -> Dict[int, List[KeyT]]:
        """
        Split keys into a dictionary that maps a slot to
        a list of keys.
        """
        slots_to_keys = {}
        for key in keys:
            k = self.encoder.encode(key)
            slot = key_slot(k)
            slots_to_keys.setdefault(slot, []).append(key)

        return slots_to_keys

    async def mget_nonatomic(self, keys: KeysT, *args) -> List[Optional[Any]]:
        """
        Splits the keys into different slots and then calls MGET
        for the keys of every slot. This operation will not be atomic
        if keys belong to more than one slot.

        Returns a list of values ordered identically to ``keys``

        For more information see https://redis.io/commands/mget
        """

        from redis.client import EMPTY_RESPONSE

        options = {}
        if not args:
            options[EMPTY_RESPONSE] = []

        # Concatenate all keys into a list
        keys = list_or_args(keys, args)
        # Split keys into slots
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Call MGET for every slot and concatenate
        # the results
        # We must make sure that the keys are returned in order
        all_values = await asyncio.gather(
            *[
                self.execute_command("MGET", *slot_keys, **options)
                for slot_keys in slots_to_keys.values()
            ]
        )

        all_results = {}
        for slot_keys, slot_values in zip(slots_to_keys.values(), all_values):
            all_results.update(dict(zip(slot_keys, slot_values)))

        # Sort the results
        vals_in_order = [all_results[key] for key in keys]
        return vals_in_order

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
        slots_to_pairs = {}
        for pair in mapping.items():
            # encode the key
            k = self.encoder.encode(pair[0])
            slot = key_slot(k)
            slots_to_pairs.setdefault(slot, []).extend(pair)

        # Call MSET for every slot and concatenate
        # the results (one result per slot)
        return await asyncio.gather(
            *[self.execute_command("MSET", *pairs) for pairs in slots_to_pairs.values()]
        )

    async def _split_command_across_slots(self, command: str, *keys: KeyT) -> int:
        """
        Runs the given command once for the keys
        of each slot. Returns the sum of the return values.
        """
        # Partition the keys by slot
        slots_to_keys = self._partition_keys_by_slot(keys)

        # Sum up the reply from each command
        return sum(
            await asyncio.gather(
                *[
                    self.execute_command(command, *slot_keys)
                    for slot_keys in slots_to_keys.values()
                ]
            )
        )

    def exists(self, *keys: KeyT) -> Awaitable:
        """
        Returns the number of ``names`` that exist in the
        whole cluster. The keys are first split up into slots
        and then an EXISTS command is sent for every slot

        For more information see https://redis.io/commands/exists
        """
        return self._split_command_across_slots("EXISTS", *keys)

    def delete(self, *keys: KeyT) -> Awaitable:
        """
        Deletes the given keys in the cluster.
        The keys are first split up into slots
        and then an DEL command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were deleted.

        For more information see https://redis.io/commands/del
        """
        return self._split_command_across_slots("DEL", *keys)

    def touch(self, *keys: KeyT) -> Awaitable:
        """
        Updates the last access time of given keys across the
        cluster.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were touched.

        For more information see https://redis.io/commands/touch
        """
        return self._split_command_across_slots("TOUCH", *keys)

    def unlink(self, *keys: KeyT) -> Awaitable:
        """
        Remove the specified keys in a different thread.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were unlinked.

        For more information see https://redis.io/commands/unlink
        """
        return self._split_command_across_slots("UNLINK", *keys)


class ClusterManagementCommands(ManagementCommands):
    """
    A class for Redis Cluster management commands

    The class inherits from Redis's core ManagementCommands class and do the
    required adjustments to work with cluster mode
    """

    def slaveof(self, *args, **kwargs):
        """
        Make the server a replica of another instance, or promote it as master.

        For more information see https://redis.io/commands/slaveof
        """
        raise RedisClusterException("SLAVEOF is not supported in cluster mode")

    def replicaof(self, *args, **kwargs):
        """
        Make the server a replica of another instance, or promote it as master.

        For more information see https://redis.io/commands/replicaof
        """
        raise RedisClusterException("REPLICAOF is not supported in cluster" " mode")

    def swapdb(self, *args, **kwargs):
        """
        Swaps two Redis databases.

        For more information see https://redis.io/commands/swapdb
        """
        raise RedisClusterException("SWAPDB is not supported in cluster" " mode")


class AsyncClusterManagementCommands(AsyncManagementCommands):
    """
    A class for Redis Cluster management commands

    The class inherits from Redis's core ManagementCommands class and do the
    required adjustments to work with cluster mode
    """

    def slaveof(self, *args, **kwargs) -> NoReturn:
        """
        Make the server a replica of another instance, or promote it as master.

        For more information see https://redis.io/commands/slaveof
        """
        raise RedisClusterException("SLAVEOF is not supported in cluster mode")

    def replicaof(self, *args, **kwargs) -> NoReturn:
        """
        Make the server a replica of another instance, or promote it as master.

        For more information see https://redis.io/commands/replicaof
        """
        raise RedisClusterException("REPLICAOF is not supported in cluster" " mode")

    def swapdb(self, *args, **kwargs) -> NoReturn:
        """
        Swaps two Redis databases.

        For more information see https://redis.io/commands/swapdb
        """
        raise RedisClusterException("SWAPDB is not supported in cluster" " mode")


class ClusterDataAccessCommands(DataAccessCommands):
    """
    A class for Redis Cluster Data Access Commands

    The class inherits from Redis's core DataAccessCommand class and do the
    required adjustments to work with cluster mode
    """

    def stralgo(
        self,
        algo,
        value1,
        value2,
        specific_argument="strings",
        len=False,
        idx=False,
        minmatchlen=None,
        withmatchlen=False,
        **kwargs,
    ):
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.

        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.

        For more information see https://redis.io/commands/stralgo
        """
        target_nodes = kwargs.pop("target_nodes", None)
        if specific_argument == "strings" and target_nodes is None:
            target_nodes = "default-node"
        kwargs.update({"target_nodes": target_nodes})
        return super().stralgo(
            algo,
            value1,
            value2,
            specific_argument,
            len,
            idx,
            minmatchlen,
            withmatchlen,
            **kwargs,
        )

    def scan_iter(
        self,
        match: Union[PatternT, None] = None,
        count: Union[int, None] = None,
        _type: Union[str, None] = None,
        **kwargs,
    ) -> Iterator:
        # Do the first query with cursor=0 for all nodes
        cursors, data = self.scan(match=match, count=count, _type=_type, **kwargs)
        yield from data

        cursors = {name: cursor for name, cursor in cursors.items() if cursor != 0}
        if cursors:
            # Get nodes by name
            nodes = {name: self.get_node(node_name=name) for name in cursors.keys()}

            # Iterate over each node till its cursor is 0
            kwargs.pop("target_nodes", None)
            while cursors:
                for name, cursor in cursors.items():
                    cur, data = self.scan(
                        cursor=cursor,
                        match=match,
                        count=count,
                        _type=_type,
                        target_nodes=nodes[name],
                        **kwargs,
                    )
                    yield from data
                    cursors[name] = cur[name]

                cursors = {
                    name: cursor for name, cursor in cursors.items() if cursor != 0
                }


class AsyncClusterDataAccessCommands(AsyncDataAccessCommands):
    """
    A class for Redis Cluster Data Access Commands

    The class inherits from Redis's core DataAccessCommand class and do the
    required adjustments to work with cluster mode
    """

    def stralgo(
        self,
        algo: Literal["LCS"],
        value1: KeyT,
        value2: KeyT,
        specific_argument: Union[Literal["strings"], Literal["keys"]] = "strings",
        len: bool = False,
        idx: bool = False,
        minmatchlen: Optional[int] = None,
        withmatchlen: bool = False,
        **kwargs,
    ) -> Awaitable:
        """
        Implements complex algorithms that operate on strings.
        Right now the only algorithm implemented is the LCS algorithm
        (longest common substring). However new algorithms could be
        implemented in the future.

        ``algo`` Right now must be LCS
        ``value1`` and ``value2`` Can be two strings or two keys
        ``specific_argument`` Specifying if the arguments to the algorithm
        will be keys or strings. strings is the default.
        ``len`` Returns just the len of the match.
        ``idx`` Returns the match positions in each string.
        ``minmatchlen`` Restrict the list of matches to the ones of a given
        minimal length. Can be provided only when ``idx`` set to True.
        ``withmatchlen`` Returns the matches with the len of the match.
        Can be provided only when ``idx`` set to True.

        For more information see https://redis.io/commands/stralgo
        """
        target_nodes = kwargs.pop("target_nodes", None)
        if specific_argument == "strings" and target_nodes is None:
            target_nodes = "default-node"
        kwargs.update({"target_nodes": target_nodes})
        return super().stralgo(
            algo,
            value1,
            value2,
            specific_argument,
            len,
            idx,
            minmatchlen,
            withmatchlen,
            **kwargs,
        )

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


class RedisClusterCommands(
    ClusterMultiKeyCommands,
    ClusterManagementCommands,
    ACLCommands,
    PubSubCommands,
    ClusterDataAccessCommands,
    ScriptCommands,
    FunctionCommands,
    RedisModuleCommands,
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

    def cluster_myid(self, target_node):
        """
        Returns the node’s id.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information check https://redis.io/commands/cluster-myid/
        """
        return self.execute_command("CLUSTER MYID", target_nodes=target_node)

    def cluster_addslots(self, target_node, *slots):
        """
        Assign new hash slots to receiving node. Sends to specified node.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-addslots
        """
        return self.execute_command(
            "CLUSTER ADDSLOTS", *slots, target_nodes=target_node
        )

    def cluster_addslotsrange(self, target_node, *slots):
        """
        Similar to the CLUSTER ADDSLOTS command.
        The difference between the two commands is that ADDSLOTS takes a list of slots
        to assign to the node, while ADDSLOTSRANGE takes a list of slot ranges
        (specified by start and end slots) to assign to the node.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-addslotsrange
        """
        return self.execute_command(
            "CLUSTER ADDSLOTSRANGE", *slots, target_nodes=target_node
        )

    def cluster_countkeysinslot(self, slot_id):
        """
        Return the number of local keys in the specified hash slot
        Send to node based on specified slot_id

        For more information see https://redis.io/commands/cluster-countkeysinslot
        """
        return self.execute_command("CLUSTER COUNTKEYSINSLOT", slot_id)

    def cluster_count_failure_report(self, node_id):
        """
        Return the number of failure reports active for a given node
        Sends to a random node

        For more information see https://redis.io/commands/cluster-count-failure-reports
        """
        return self.execute_command("CLUSTER COUNT-FAILURE-REPORTS", node_id)

    def cluster_delslots(self, *slots):
        """
        Set hash slots as unbound in the cluster.
        It determines by it self what node the slot is in and sends it there

        Returns a list of the results for each processed slot.

        For more information see https://redis.io/commands/cluster-delslots
        """
        return [self.execute_command("CLUSTER DELSLOTS", slot) for slot in slots]

    def cluster_delslotsrange(self, *slots):
        """
        Similar to the CLUSTER DELSLOTS command.
        The difference is that CLUSTER DELSLOTS takes a list of hash slots to remove
        from the node, while CLUSTER DELSLOTSRANGE takes a list of slot ranges to remove
        from the node.

        For more information see https://redis.io/commands/cluster-delslotsrange
        """
        return self.execute_command("CLUSTER DELSLOTSRANGE", *slots)

    def cluster_failover(self, target_node, option=None):
        """
        Forces a slave to perform a manual failover of its master
        Sends to specified node

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-failover
        """
        if option:
            if option.upper() not in ["FORCE", "TAKEOVER"]:
                raise RedisError(
                    f"Invalid option for CLUSTER FAILOVER command: {option}"
                )
            else:
                return self.execute_command(
                    "CLUSTER FAILOVER", option, target_nodes=target_node
                )
        else:
            return self.execute_command("CLUSTER FAILOVER", target_nodes=target_node)

    def cluster_info(self, target_nodes=None):
        """
        Provides info about Redis Cluster node state.
        The command will be sent to a random node in the cluster if no target
        node is specified.

        For more information see https://redis.io/commands/cluster-info
        """
        return self.execute_command("CLUSTER INFO", target_nodes=target_nodes)

    def cluster_keyslot(self, key):
        """
        Returns the hash slot of the specified key
        Sends to random node in the cluster

        For more information see https://redis.io/commands/cluster-keyslot
        """
        return self.execute_command("CLUSTER KEYSLOT", key)

    def cluster_meet(self, host, port, target_nodes=None):
        """
        Force a node cluster to handshake with another node.
        Sends to specified node.

        For more information see https://redis.io/commands/cluster-meet
        """
        return self.execute_command(
            "CLUSTER MEET", host, port, target_nodes=target_nodes
        )

    def cluster_nodes(self):
        """
        Get Cluster config for the node.
        Sends to random node in the cluster

        For more information see https://redis.io/commands/cluster-nodes
        """
        return self.execute_command("CLUSTER NODES")

    def cluster_replicate(self, target_nodes, node_id):
        """
        Reconfigure a node as a slave of the specified master node

        For more information see https://redis.io/commands/cluster-replicate
        """
        return self.execute_command(
            "CLUSTER REPLICATE", node_id, target_nodes=target_nodes
        )

    def cluster_reset(self, soft=True, target_nodes=None):
        """
        Reset a Redis Cluster node

        If 'soft' is True then it will send 'SOFT' argument
        If 'soft' is False then it will send 'HARD' argument

        For more information see https://redis.io/commands/cluster-reset
        """
        return self.execute_command(
            "CLUSTER RESET", b"SOFT" if soft else b"HARD", target_nodes=target_nodes
        )

    def cluster_save_config(self, target_nodes=None):
        """
        Forces the node to save cluster state on disk

        For more information see https://redis.io/commands/cluster-saveconfig
        """
        return self.execute_command("CLUSTER SAVECONFIG", target_nodes=target_nodes)

    def cluster_get_keys_in_slot(self, slot, num_keys):
        """
        Returns the number of keys in the specified cluster slot

        For more information see https://redis.io/commands/cluster-getkeysinslot
        """
        return self.execute_command("CLUSTER GETKEYSINSLOT", slot, num_keys)

    def cluster_set_config_epoch(self, epoch, target_nodes=None):
        """
        Set the configuration epoch in a new node

        For more information see https://redis.io/commands/cluster-set-config-epoch
        """
        return self.execute_command(
            "CLUSTER SET-CONFIG-EPOCH", epoch, target_nodes=target_nodes
        )

    def cluster_setslot(self, target_node, node_id, slot_id, state):
        """
        Bind an hash slot to a specific node

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-setslot
        """
        if state.upper() in ("IMPORTING", "NODE", "MIGRATING"):
            return self.execute_command(
                "CLUSTER SETSLOT", slot_id, state, node_id, target_nodes=target_node
            )
        elif state.upper() == "STABLE":
            raise RedisError('For "stable" state please use ' "cluster_setslot_stable")
        else:
            raise RedisError(f"Invalid slot state: {state}")

    def cluster_setslot_stable(self, slot_id):
        """
        Clears migrating / importing state from the slot.
        It determines by it self what node the slot is in and sends it there.

        For more information see https://redis.io/commands/cluster-setslot
        """
        return self.execute_command("CLUSTER SETSLOT", slot_id, "STABLE")

    def cluster_replicas(self, node_id, target_nodes=None):
        """
        Provides a list of replica nodes replicating from the specified primary
        target node.

        For more information see https://redis.io/commands/cluster-replicas
        """
        return self.execute_command(
            "CLUSTER REPLICAS", node_id, target_nodes=target_nodes
        )

    def cluster_slots(self, target_nodes=None):
        """
        Get array of Cluster slot to node mappings

        For more information see https://redis.io/commands/cluster-slots
        """
        return self.execute_command("CLUSTER SLOTS", target_nodes=target_nodes)

    def cluster_links(self, target_node):
        """
        Each node in a Redis Cluster maintains a pair of long-lived TCP link with each
        peer in the cluster: One for sending outbound messages towards the peer and one
        for receiving inbound messages from the peer.

        This command outputs information of all such peer links as an array.

        For more information see https://redis.io/commands/cluster-links
        """
        return self.execute_command("CLUSTER LINKS", target_nodes=target_node)

    def readonly(self, target_nodes=None):
        """
        Enables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.

        For more information see https://redis.io/commands/readonly
        """
        if target_nodes == "replicas" or target_nodes == "all":
            # read_from_replicas will only be enabled if the READONLY command
            # is sent to all replicas
            self.read_from_replicas = True
        return self.execute_command("READONLY", target_nodes=target_nodes)

    def readwrite(self, target_nodes=None):
        """
        Disables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.

        For more information see https://redis.io/commands/readwrite
        """
        # Reset read from replicas flag
        self.read_from_replicas = False
        return self.execute_command("READWRITE", target_nodes=target_nodes)


class AsyncRedisClusterCommands(
    AsyncClusterMultiKeyCommands,
    AsyncClusterManagementCommands,
    AsyncACLCommands,
    AsyncClusterDataAccessCommands,
    AsyncScriptCommands,
    AsyncFunctionCommands,
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

    def cluster_myid(self, target_node: "TargetNodesT") -> Awaitable:
        """
        Returns the node’s id.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information check https://redis.io/commands/cluster-myid/
        """
        return self.execute_command("CLUSTER MYID", target_nodes=target_node)

    def cluster_addslots(
        self, target_node: "TargetNodesT", *slots: EncodableT
    ) -> Awaitable:
        """
        Assign new hash slots to receiving node. Sends to specified node.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-addslots
        """
        return self.execute_command(
            "CLUSTER ADDSLOTS", *slots, target_nodes=target_node
        )

    def cluster_addslotsrange(
        self, target_node: "TargetNodesT", *slots: EncodableT
    ) -> Awaitable:
        """
        Similar to the CLUSTER ADDSLOTS command.
        The difference between the two commands is that ADDSLOTS takes a list of slots
        to assign to the node, while ADDSLOTSRANGE takes a list of slot ranges
        (specified by start and end slots) to assign to the node.

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-addslotsrange
        """
        return self.execute_command(
            "CLUSTER ADDSLOTSRANGE", *slots, target_nodes=target_node
        )

    def cluster_countkeysinslot(self, slot_id: int) -> Awaitable:
        """
        Return the number of local keys in the specified hash slot
        Send to node based on specified slot_id

        For more information see https://redis.io/commands/cluster-countkeysinslot
        """
        return self.execute_command("CLUSTER COUNTKEYSINSLOT", slot_id)

    def cluster_count_failure_report(self, node_id: str) -> Awaitable:
        """
        Return the number of failure reports active for a given node
        Sends to a random node

        For more information see https://redis.io/commands/cluster-count-failure-reports
        """
        return self.execute_command("CLUSTER COUNT-FAILURE-REPORTS", node_id)

    async def cluster_delslots(self, *slots: EncodableT) -> List[bool]:
        """
        Set hash slots as unbound in the cluster.
        It determines by it self what node the slot is in and sends it there

        Returns a list of the results for each processed slot.

        For more information see https://redis.io/commands/cluster-delslots
        """
        return await asyncio.gather(
            *[self.execute_command("CLUSTER DELSLOTS", slot) for slot in slots]
        )

    def cluster_delslotsrange(self, *slots: EncodableT) -> Awaitable:
        """
        Similar to the CLUSTER DELSLOTS command.
        The difference is that CLUSTER DELSLOTS takes a list of hash slots to remove
        from the node, while CLUSTER DELSLOTSRANGE takes a list of slot ranges to remove
        from the node.

        For more information see https://redis.io/commands/cluster-delslotsrange
        """
        return self.execute_command("CLUSTER DELSLOTSRANGE", *slots)

    def cluster_failover(
        self, target_node: "TargetNodesT", option: Optional[str] = None
    ) -> Awaitable:
        """
        Forces a slave to perform a manual failover of its master
        Sends to specified node

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-failover
        """
        if option:
            if option.upper() not in ["FORCE", "TAKEOVER"]:
                raise RedisError(
                    f"Invalid option for CLUSTER FAILOVER command: {option}"
                )
            else:
                return self.execute_command(
                    "CLUSTER FAILOVER", option, target_nodes=target_node
                )
        else:
            return self.execute_command("CLUSTER FAILOVER", target_nodes=target_node)

    def cluster_info(self, target_nodes: Optional["TargetNodesT"] = None) -> Awaitable:
        """
        Provides info about Redis Cluster node state.
        The command will be sent to a random node in the cluster if no target
        node is specified.

        For more information see https://redis.io/commands/cluster-info
        """
        return self.execute_command("CLUSTER INFO", target_nodes=target_nodes)

    def cluster_keyslot(self, key: str) -> Awaitable:
        """
        Returns the hash slot of the specified key
        Sends to random node in the cluster

        For more information see https://redis.io/commands/cluster-keyslot
        """
        return self.execute_command("CLUSTER KEYSLOT", key)

    def cluster_meet(
        self, host: str, port: int, target_nodes: Optional["TargetNodesT"] = None
    ) -> Awaitable:
        """
        Force a node cluster to handshake with another node.
        Sends to specified node.

        For more information see https://redis.io/commands/cluster-meet
        """
        return self.execute_command(
            "CLUSTER MEET", host, port, target_nodes=target_nodes
        )

    def cluster_nodes(self) -> Awaitable:
        """
        Get Cluster config for the node.
        Sends to random node in the cluster

        For more information see https://redis.io/commands/cluster-nodes
        """
        return self.execute_command("CLUSTER NODES")

    def cluster_replicate(
        self, target_nodes: "TargetNodesT", node_id: str
    ) -> Awaitable:
        """
        Reconfigure a node as a slave of the specified master node

        For more information see https://redis.io/commands/cluster-replicate
        """
        return self.execute_command(
            "CLUSTER REPLICATE", node_id, target_nodes=target_nodes
        )

    def cluster_reset(
        self, soft: bool = True, target_nodes: Optional["TargetNodesT"] = None
    ) -> Awaitable:
        """
        Reset a Redis Cluster node

        If 'soft' is True then it will send 'SOFT' argument
        If 'soft' is False then it will send 'HARD' argument

        For more information see https://redis.io/commands/cluster-reset
        """
        return self.execute_command(
            "CLUSTER RESET", b"SOFT" if soft else b"HARD", target_nodes=target_nodes
        )

    def cluster_save_config(
        self, target_nodes: Optional["TargetNodesT"] = None
    ) -> Awaitable:
        """
        Forces the node to save cluster state on disk

        For more information see https://redis.io/commands/cluster-saveconfig
        """
        return self.execute_command("CLUSTER SAVECONFIG", target_nodes=target_nodes)

    def cluster_get_keys_in_slot(self, slot: int, num_keys: int) -> Awaitable:
        """
        Returns the number of keys in the specified cluster slot

        For more information see https://redis.io/commands/cluster-getkeysinslot
        """
        return self.execute_command("CLUSTER GETKEYSINSLOT", slot, num_keys)

    def cluster_set_config_epoch(
        self, epoch: int, target_nodes: Optional["TargetNodesT"] = None
    ) -> Awaitable:
        """
        Set the configuration epoch in a new node

        For more information see https://redis.io/commands/cluster-set-config-epoch
        """
        return self.execute_command(
            "CLUSTER SET-CONFIG-EPOCH", epoch, target_nodes=target_nodes
        )

    def cluster_setslot(
        self, target_node: "TargetNodesT", node_id: str, slot_id: int, state: str
    ) -> Awaitable:
        """
        Bind an hash slot to a specific node

        :target_node: 'ClusterNode'
            The node to execute the command on

        For more information see https://redis.io/commands/cluster-setslot
        """
        if state.upper() in ("IMPORTING", "NODE", "MIGRATING"):
            return self.execute_command(
                "CLUSTER SETSLOT", slot_id, state, node_id, target_nodes=target_node
            )
        elif state.upper() == "STABLE":
            raise RedisError('For "stable" state please use ' "cluster_setslot_stable")
        else:
            raise RedisError(f"Invalid slot state: {state}")

    def cluster_setslot_stable(self, slot_id: int) -> Awaitable:
        """
        Clears migrating / importing state from the slot.
        It determines by it self what node the slot is in and sends it there.

        For more information see https://redis.io/commands/cluster-setslot
        """
        return self.execute_command("CLUSTER SETSLOT", slot_id, "STABLE")

    def cluster_replicas(
        self, node_id: str, target_nodes: Optional["TargetNodesT"] = None
    ) -> Awaitable:
        """
        Provides a list of replica nodes replicating from the specified primary
        target node.

        For more information see https://redis.io/commands/cluster-replicas
        """
        return self.execute_command(
            "CLUSTER REPLICAS", node_id, target_nodes=target_nodes
        )

    def cluster_slots(self, target_nodes: Optional["TargetNodesT"] = None) -> Awaitable:
        """
        Get array of Cluster slot to node mappings

        For more information see https://redis.io/commands/cluster-slots
        """
        return self.execute_command("CLUSTER SLOTS", target_nodes=target_nodes)

    def cluster_links(self, target_node: "TargetNodesT") -> Awaitable:
        """
        Each node in a Redis Cluster maintains a pair of long-lived TCP link with each
        peer in the cluster: One for sending outbound messages towards the peer and one
        for receiving inbound messages from the peer.

        This command outputs information of all such peer links as an array.

        For more information see https://redis.io/commands/cluster-links
        """
        return self.execute_command("CLUSTER LINKS", target_nodes=target_node)

    def readonly(self, target_nodes: Optional["TargetNodesT"] = None) -> Awaitable:
        """
        Enables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.

        For more information see https://redis.io/commands/readonly
        """
        if target_nodes == "replicas" or target_nodes == "all":
            # read_from_replicas will only be enabled if the READONLY command
            # is sent to all replicas
            self.read_from_replicas = True
        return self.execute_command("READONLY", target_nodes=target_nodes)

    def readwrite(self, target_nodes: Optional["TargetNodesT"] = None) -> Awaitable:
        """
        Disables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.

        For more information see https://redis.io/commands/readwrite
        """
        # Reset read from replicas flag
        self.read_from_replicas = False
        return self.execute_command("READWRITE", target_nodes=target_nodes)
