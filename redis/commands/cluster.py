from redis.exceptions import (
    ConnectionError,
    DataError,
    RedisError,
)
from redis.crc import key_slot
from .core import DataAccessCommands
from .helpers import list_or_args


class ClusterMultiKeyCommands:
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
            slot_values = self.execute_command(
                'MGET', *slot_keys, **options)

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
            res.append(self.execute_command('MSET', *pairs))

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
        """
        return self._split_command_across_slots('EXISTS', *keys)

    def delete(self, *keys):
        """
        Deletes the given keys in the cluster.
        The keys are first split up into slots
        and then an DEL command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were deleted.
        """
        return self._split_command_across_slots('DEL', *keys)

    def touch(self, *keys):
        """
        Updates the last access time of given keys across the
        cluster.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were touched.
        """
        return self._split_command_across_slots('TOUCH', *keys)

    def unlink(self, *keys):
        """
        Remove the specified keys in a different thread.

        The keys are first split up into slots
        and then an TOUCH command is sent for every slot

        Non-existant keys are ignored.
        Returns the number of keys that were unlinked.
        """
        return self._split_command_across_slots('UNLINK', *keys)


class ClusterManagementCommands:
    """
    Redis Cluster management commands

    Commands with the 'target_nodes' argument can be executed on specified
    nodes. By default, if target_nodes is not specified, the command will be
    executed on the default cluster node.

    :param :target_nodes: type can be one of the followings:
        - nodes flag: 'all', 'primaries', 'replicas', 'random'
        - 'ClusterNode'
        - 'list(ClusterNodes)'
        - 'dict(any:clusterNodes)'

    for example:
        primary = r.get_primaries()[0]
        r.bgsave(target_nodes=primary)
        r.bgsave(target_nodes='primaries')
    """
    def bgsave(self, schedule=True, target_nodes=None):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        pieces = []
        if schedule:
            pieces.append("SCHEDULE")
        return self.execute_command('BGSAVE',
                                    *pieces,
                                    target_nodes=target_nodes)

    def client_getname(self, target_nodes=None):
        """
        Returns the current connection name from all nodes.
        The result will be a dictionary with the IP and
        connection name.
        """
        return self.execute_command('CLIENT GETNAME',
                                    target_nodes=target_nodes)

    def client_getredir(self, target_nodes=None):
        """Returns the ID (an integer) of the client to whom we are
        redirecting tracking notifications.

        see: https://redis.io/commands/client-getredir
        """
        return self.execute_command('CLIENT GETREDIR',
                                    target_nodes=target_nodes)

    def client_id(self, target_nodes=None):
        """Returns the current connection id"""
        return self.execute_command('CLIENT ID',
                                    target_nodes=target_nodes)

    def client_info(self, target_nodes=None):
        """
        Returns information and statistics about the current
        client connection.
        """
        return self.execute_command('CLIENT INFO',
                                    target_nodes=target_nodes)

    def client_kill_filter(self, _id=None, _type=None, addr=None,
                           skipme=None, laddr=None, user=None,
                           target_nodes=None):
        """
        Disconnects client(s) using a variety of filter options
        :param id: Kills a client by its unique ID field
        :param type: Kills a client by type where type is one of 'normal',
        'master', 'slave' or 'pubsub'
        :param addr: Kills a client by its 'address:port'
        :param skipme: If True, then the client calling the command
        will not get killed even if it is identified by one of the filter
        options. If skipme is not provided, the server defaults to skipme=True
        :param laddr: Kills a client by its 'local (bind) address:port'
        :param user: Kills a client for a specific user name
        """
        args = []
        if _type is not None:
            client_types = ('normal', 'master', 'slave', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT KILL type must be one of %r" % (
                    client_types,))
            args.extend((b'TYPE', _type))
        if skipme is not None:
            if not isinstance(skipme, bool):
                raise DataError("CLIENT KILL skipme must be a bool")
            if skipme:
                args.extend((b'SKIPME', b'YES'))
            else:
                args.extend((b'SKIPME', b'NO'))
        if _id is not None:
            args.extend((b'ID', _id))
        if addr is not None:
            args.extend((b'ADDR', addr))
        if laddr is not None:
            args.extend((b'LADDR', laddr))
        if user is not None:
            args.extend((b'USER', user))
        if not args:
            raise DataError("CLIENT KILL <filter> <value> ... ... <filter> "
                            "<value> must specify at least one filter")
        return self.execute_command('CLIENT KILL', *args,
                                    target_nodes=target_nodes)

    def client_kill(self, address, target_nodes=None):
        "Disconnects the client at ``address`` (ip:port)"
        return self.execute_command('CLIENT KILL', address,
                                    target_nodes=target_nodes)

    def client_list(self, _type=None, target_nodes=None):
        """
        Returns a list of currently connected clients to the entire cluster.
        If type of client specified, only that type will be returned.
        :param _type: optional. one of the client types (normal, master,
         replica, pubsub)
        """
        if _type is not None:
            client_types = ('normal', 'master', 'replica', 'pubsub')
            if str(_type).lower() not in client_types:
                raise DataError("CLIENT LIST _type must be one of %r" % (
                    client_types,))
            return self.execute_command('CLIENT LIST',
                                        b'TYPE',
                                        _type,
                                        target_noes=target_nodes)
        return self.execute_command('CLIENT LIST',
                                    target_nodes=target_nodes)

    def client_pause(self, timeout, target_nodes=None):
        """
        Suspend all the Redis clients for the specified amount of time
        :param timeout: milliseconds to pause clients
        """
        if not isinstance(timeout, int):
            raise DataError("CLIENT PAUSE timeout must be an integer")
        return self.execute_command('CLIENT PAUSE', str(timeout),
                                    target_nodes=target_nodes)

    def client_reply(self, reply, target_nodes=None):
        """Enable and disable redis server replies.
        ``reply`` Must be ON OFF or SKIP,
            ON - The default most with server replies to commands
            OFF - Disable server responses to commands
            SKIP - Skip the response of the immediately following command.

        Note: When setting OFF or SKIP replies, you will need a client object
        with a timeout specified in seconds, and will need to catch the
        TimeoutError.
              The test_client_reply unit test illustrates this, and
              conftest.py has a client with a timeout.
        See https://redis.io/commands/client-reply
        """
        replies = ['ON', 'OFF', 'SKIP']
        if reply not in replies:
            raise DataError('CLIENT REPLY must be one of %r' % replies)
        return self.execute_command("CLIENT REPLY", reply,
                                    target_nodes=target_nodes)

    def client_setname(self, name, target_nodes=None):
        "Sets the current connection name"
        return self.execute_command('CLIENT SETNAME', name,
                                    target_nodes=target_nodes)

    def client_trackinginfo(self, target_nodes=None):
        """
        Returns the information about the current client connection's
        use of the server assisted client side cache.
        See https://redis.io/commands/client-trackinginfo
        """
        return self.execute_command('CLIENT TRACKINGINFO',
                                    target_nodes=target_nodes)

    def client_unblock(self, client_id, error=False, target_nodes=None):
        """
        Unblocks a connection by its client id.
        If ``error`` is True, unblocks the client with a special error message.
        If ``error`` is False (default), the client is unblocked using the
        regular timeout mechanism.
        """
        args = ['CLIENT UNBLOCK', int(client_id)]
        if error:
            args.append(b'ERROR')
        return self.execute_command(*args, target_nodes=target_nodes)

    def client_unpause(self, target_nodes=None):
        """
        Unpause all redis clients
        """
        return self.execute_command('CLIENT UNPAUSE',
                                    target_nodes=target_nodes)

    def command(self, target_nodes=None):
        """
        Returns dict reply of details about all Redis commands.
        """
        return self.execute_command('COMMAND', target_nodes=target_nodes)

    def command_count(self, target_nodes=None):
        """
        Returns Integer reply of number of total commands in this Redis server.
        """
        return self.execute_command('COMMAND COUNT', target_nodes=target_nodes)

    def config_get(self, pattern="*", target_nodes=None):
        """
        Return a dictionary of configuration based on the ``pattern``
        """
        return self.execute_command('CONFIG GET',
                                    pattern,
                                    target_nodes=target_nodes)

    def config_resetstat(self, target_nodes=None):
        """Reset runtime statistics"""
        return self.execute_command('CONFIG RESETSTAT',
                                    target_nodes=target_nodes)

    def config_rewrite(self, target_nodes=None):
        """
        Rewrite config file with the minimal change to reflect running config.
        """
        return self.execute_command('CONFIG REWRITE',
                                    target_nodes=target_nodes)

    def config_set(self, name, value, target_nodes=None):
        "Set config item ``name`` with ``value``"
        return self.execute_command('CONFIG SET',
                                    name,
                                    value,
                                    target_nodes=target_nodes)

    def dbsize(self, target_nodes=None):
        """
        Sums the number of keys in the target nodes' DB.

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        return self.execute_command('DBSIZE',
                                    target_nodes=target_nodes)

    def debug_object(self, key):
        raise NotImplementedError(
            "DEBUG OBJECT is intentionally not implemented in the client."
        )

    def debug_segfault(self):
        raise NotImplementedError(
            "DEBUG SEGFAULT is intentionally not implemented in the client."
        )

    def echo(self, value, target_nodes):
        """Echo the string back from the server"""
        return self.execute_command('ECHO', value,
                                    target_nodes=target_nodes)

    def flushall(self, asynchronous=False, target_nodes=None):
        """
        Delete all keys in the database.
        In cluster mode this method is the same as flushdb

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(b'ASYNC')
        return self.execute_command('FLUSHALL',
                                    *args,
                                    target_nodes=target_nodes)

    def flushdb(self, asynchronous=False, target_nodes=None):
        """
        Delete all keys in the database.

        ``asynchronous`` indicates whether the operation is
        executed asynchronously by the server.
        """
        args = []
        if asynchronous:
            args.append(b'ASYNC')
        return self.execute_command('FLUSHDB',
                                    *args,
                                    target_nodes=target_nodes)

    def info(self, section=None, target_nodes=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        if section is None:
            return self.execute_command('INFO',
                                        target_nodes=target_nodes)
        else:
            return self.execute_command('INFO',
                                        section,
                                        target_nodes=target_nodes)

    def keys(self, pattern='*', target_nodes=None):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern, target_nodes=target_nodes)

    def lastsave(self, target_nodes=None):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command('LASTSAVE',
                                    target_nodes=target_nodes)

    def memory_doctor(self):
        raise NotImplementedError(
            "MEMORY DOCTOR is intentionally not implemented in the client."
        )

    def memory_help(self):
        raise NotImplementedError(
            "MEMORY HELP is intentionally not implemented in the client."
        )

    def memory_malloc_stats(self, target_nodes=None):
        """Return an internal statistics report from the memory allocator."""
        return self.execute_command('MEMORY MALLOC-STATS',
                                    target_nodes=target_nodes)

    def memory_purge(self, target_nodes=None):
        """Attempts to purge dirty pages for reclamation by allocator"""
        return self.execute_command('MEMORY PURGE',
                                    target_nodes=target_nodes)

    def memory_stats(self, target_nodes=None):
        """Return a dictionary of memory stats"""
        return self.execute_command('MEMORY STATS',
                                    target_nodes=target_nodes)

    def memory_usage(self, key, samples=None):
        """
        Return the total memory usage for key, its value and associated
        administrative overheads.

        For nested data structures, ``samples`` is the number of elements to
        sample. If left unspecified, the server's default is 5. Use 0 to sample
        all elements.
        """
        args = []
        if isinstance(samples, int):
            args.extend([b'SAMPLES', samples])
        return self.execute_command('MEMORY USAGE', key, *args)

    def object(self, infotype, key):
        """Return the encoding, idletime, or refcount about the key"""
        return self.execute_command('OBJECT', infotype, key, infotype=infotype)

    def ping(self, target_nodes=None):
        """
        Ping the cluster's servers.
        If no target nodes are specified, sent to all nodes and returns True if
         the ping was successful across all nodes.
        """
        return self.execute_command('PING',
                                    target_nodes=target_nodes)

    def randomkey(self, target_nodes=None):
        """
        Returns the name of a random key"
        """
        return self.execute_command('RANDOMKEY', target_nodes=target_nodes)

    def save(self, target_nodes=None):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE', target_nodes=target_nodes)

    def scan(self, cursor=0, match=None, count=None, _type=None,
             target_nodes=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        pieces = [cursor]
        if match is not None:
            pieces.extend([b'MATCH', match])
        if count is not None:
            pieces.extend([b'COUNT', count])
        if _type is not None:
            pieces.extend([b'TYPE', _type])
        return self.execute_command('SCAN', *pieces, target_nodes=target_nodes)

    def scan_iter(self, match=None, count=None, _type=None, target_nodes=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` provides a hint to Redis about the number of keys to
            return per batch.

        ``_type`` filters the returned values by a particular Redis type.
            Stock Redis instances allow for the following types:
            HASH, LIST, SET, STREAM, STRING, ZSET
            Additionally, Redis modules can expose other types as well.
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match,
                                     count=count, _type=_type,
                                     target_nodes=target_nodes)
            yield from data

    def shutdown(self, save=False, nosave=False, target_nodes=None):
        """Shutdown the Redis server.  If Redis has persistence configured,
        data will be flushed before shutdown.  If the "save" option is set,
        a data flush will be attempted even if there is no persistence
        configured.  If the "nosave" option is set, no data flush will be
        attempted.  The "save" and "nosave" options cannot both be set.
        """
        if save and nosave:
            raise DataError('SHUTDOWN save and nosave cannot both be set')
        args = ['SHUTDOWN']
        if save:
            args.append('SAVE')
        if nosave:
            args.append('NOSAVE')
        try:
            self.execute_command(*args, target_nodes=target_nodes)
        except ConnectionError:
            # a ConnectionError here is expected
            return
        raise RedisError("SHUTDOWN seems to have failed.")

    def slowlog_get(self, num=None, target_nodes=None):
        """
        Get the entries from the slowlog. If ``num`` is specified, get the
        most recent ``num`` items.
        """
        args = ['SLOWLOG GET']
        if num is not None:
            args.append(num)

        return self.execute_command(*args,
                                    target_nodes=target_nodes)

    def slowlog_len(self, target_nodes=None):
        "Get the number of items in the slowlog"
        return self.execute_command('SLOWLOG LEN',
                                    target_nodes=target_nodes)

    def slowlog_reset(self, target_nodes=None):
        "Remove all items in the slowlog"
        return self.execute_command('SLOWLOG RESET',
                                    target_nodes=target_nodes)

    def stralgo(self, algo, value1, value2, specific_argument='strings',
                len=False, idx=False, minmatchlen=None, withmatchlen=False,
                target_nodes=None):
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
        """
        # check validity
        supported_algo = ['LCS']
        if algo not in supported_algo:
            raise DataError("The supported algorithms are: %s"
                            % (', '.join(supported_algo)))
        if specific_argument not in ['keys', 'strings']:
            raise DataError("specific_argument can be only"
                            " keys or strings")
        if len and idx:
            raise DataError("len and idx cannot be provided together.")

        pieces = [algo, specific_argument.upper(), value1, value2]
        if len:
            pieces.append(b'LEN')
        if idx:
            pieces.append(b'IDX')
        try:
            int(minmatchlen)
            pieces.extend([b'MINMATCHLEN', minmatchlen])
        except TypeError:
            pass
        if withmatchlen:
            pieces.append(b'WITHMATCHLEN')
        if specific_argument == 'strings' and target_nodes is None:
            target_nodes = 'default-node'
        return self.execute_command('STRALGO', *pieces, len=len, idx=idx,
                                    minmatchlen=minmatchlen,
                                    withmatchlen=withmatchlen,
                                    target_nodes=target_nodes)

    def time(self, target_nodes=None):
        """
        Returns the server time as a 2-item tuple of ints:
        (seconds since epoch, microseconds into this second).
        """
        return self.execute_command('TIME', target_nodes=target_nodes)

    def wait(self, num_replicas, timeout, target_nodes=None):
        """
        Redis synchronous replication
        That returns the number of replicas that processed the query when
        we finally have at least ``num_replicas``, or when the ``timeout`` was
        reached.

        If more than one target node are passed the result will be summed up
        """
        return self.execute_command('WAIT', num_replicas,
                                    timeout,
                                    target_nodes=target_nodes)


class ClusterPubSubCommands:
    """
    Redis PubSub commands for RedisCluster use.
    see https://redis.io/topics/pubsub
    """
    def publish(self, channel, message, target_nodes=None):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.execute_command('PUBLISH', channel, message,
                                    target_nodes=target_nodes)

    def pubsub_channels(self, pattern='*', target_nodes=None):
        """
        Return a list of channels that have at least one subscriber
        """
        return self.execute_command('PUBSUB CHANNELS', pattern,
                                    target_nodes=target_nodes)

    def pubsub_numpat(self, target_nodes=None):
        """
        Returns the number of subscriptions to patterns
        """
        return self.execute_command('PUBSUB NUMPAT', target_nodes=target_nodes)

    def pubsub_numsub(self, *args, target_nodes=None):
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``
        """
        return self.execute_command('PUBSUB NUMSUB', *args,
                                    target_nodes=target_nodes)


class ClusterCommands(ClusterManagementCommands, ClusterMultiKeyCommands,
                      ClusterPubSubCommands, DataAccessCommands):
    """
    Redis Cluster commands

    Commands with the 'target_nodes' argument can be executed on specified
    nodes. By default, if target_nodes is not specified, the command will be
    executed on the default cluster node.

    :param :target_nodes: type can be one of the followings:
        - nodes flag: 'all', 'primaries', 'replicas', 'random'
        - 'ClusterNode'
        - 'list(ClusterNodes)'
        - 'dict(any:clusterNodes)'

    for example:
        r.cluster_info(target_nodes='all')
    """
    def cluster_addslots(self, target_node, *slots):
        """
        Assign new hash slots to receiving node. Sends to specified node.

        :target_node: 'ClusterNode'
            The node to execute the command on
        """
        return self.execute_command('CLUSTER ADDSLOTS', *slots,
                                    target_nodes=target_node)

    def cluster_countkeysinslot(self, slot_id):
        """
        Return the number of local keys in the specified hash slot
        Send to node based on specified slot_id
        """
        return self.execute_command('CLUSTER COUNTKEYSINSLOT', slot_id)

    def cluster_count_failure_report(self, node_id):
        """
        Return the number of failure reports active for a given node
        Sends to a random node
        """
        return self.execute_command('CLUSTER COUNT-FAILURE-REPORTS', node_id)

    def cluster_delslots(self, *slots):
        """
        Set hash slots as unbound in the cluster.
        It determines by it self what node the slot is in and sends it there

        Returns a list of the results for each processed slot.
        """
        return [
            self.execute_command('CLUSTER DELSLOTS', slot)
            for slot in slots
        ]

    def cluster_failover(self, target_node, option=None):
        """
        Forces a slave to perform a manual failover of its master
        Sends to specified node

        :target_node: 'ClusterNode'
            The node to execute the command on
        """
        if option:
            if option.upper() not in ['FORCE', 'TAKEOVER']:
                raise RedisError(
                    'Invalid option for CLUSTER FAILOVER command: {0}'.format(
                        option))
            else:
                return self.execute_command('CLUSTER FAILOVER', option,
                                            target_nodes=target_node)
        else:
            return self.execute_command('CLUSTER FAILOVER',
                                        target_nodes=target_node)

    def cluster_info(self, target_nodes=None):
        """
        Provides info about Redis Cluster node state.
        The command will be sent to a random node in the cluster if no target
        node is specified.
        """
        return self.execute_command('CLUSTER INFO', target_nodes=target_nodes)

    def cluster_keyslot(self, key):
        """
        Returns the hash slot of the specified key
        Sends to random node in the cluster
        """
        return self.execute_command('CLUSTER KEYSLOT', key)

    def cluster_meet(self, host, port, target_nodes=None):
        """
        Force a node cluster to handshake with another node.
        Sends to specified node.
        """
        return self.execute_command('CLUSTER MEET', host, port,
                                    target_nodes=target_nodes)

    def cluster_nodes(self):
        """
        Force a node cluster to handshake with another node

        Sends to random node in the cluster
        """
        return self.execute_command('CLUSTER NODES')

    def cluster_replicate(self, target_nodes, node_id):
        """
        Reconfigure a node as a slave of the specified master node
        """
        return self.execute_command('CLUSTER REPLICATE', node_id,
                                    target_nodes=target_nodes)

    def cluster_reset(self, soft=True, target_nodes=None):
        """
        Reset a Redis Cluster node

        If 'soft' is True then it will send 'SOFT' argument
        If 'soft' is False then it will send 'HARD' argument
        """
        return self.execute_command('CLUSTER RESET',
                                    b'SOFT' if soft else b'HARD',
                                    target_nodes=target_nodes)

    def cluster_save_config(self, target_nodes=None):
        """
        Forces the node to save cluster state on disk
        """
        return self.execute_command('CLUSTER SAVECONFIG',
                                    target_nodes=target_nodes)

    def cluster_get_keys_in_slot(self, slot, num_keys):
        """
        Returns the number of keys in the specified cluster slot
        """
        return self.execute_command('CLUSTER GETKEYSINSLOT', slot, num_keys)

    def cluster_set_config_epoch(self, epoch, target_nodes=None):
        """
        Set the configuration epoch in a new node
        """
        return self.execute_command('CLUSTER SET-CONFIG-EPOCH', epoch,
                                    target_nodes=target_nodes)

    def cluster_setslot(self, target_node, node_id, slot_id, state):
        """
        Bind an hash slot to a specific node

        :target_node: 'ClusterNode'
            The node to execute the command on
        """
        if state.upper() in ('IMPORTING', 'NODE', 'MIGRATING'):
            return self.execute_command('CLUSTER SETSLOT', slot_id, state,
                                        node_id, target_nodes=target_node)
        elif state.upper() == 'STABLE':
            raise RedisError('For "stable" state please use '
                             'cluster_setslot_stable')
        else:
            raise RedisError('Invalid slot state: {0}'.format(state))

    def cluster_setslot_stable(self, slot_id):
        """
        Clears migrating / importing state from the slot.
        It determines by it self what node the slot is in and sends it there.
        """
        return self.execute_command('CLUSTER SETSLOT', slot_id, 'STABLE')

    def cluster_replicas(self, node_id, target_nodes=None):
        """
        Provides a list of replica nodes replicating from the specified primary
        target node.
        """
        return self.execute_command('CLUSTER REPLICAS', node_id,
                                    target_nodes=target_nodes)

    def cluster_slots(self, target_nodes=None):
        """
        Get array of Cluster slot to node mappings
        """
        return self.execute_command('CLUSTER SLOTS', target_nodes=target_nodes)

    def readonly(self, target_nodes=None):
        """
        Enables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.
         """
        if target_nodes == 'replicas' or target_nodes == 'all':
            # read_from_replicas will only be enabled if the READONLY command
            # is sent to all replicas
            self.read_from_replicas = True
        return self.execute_command('READONLY', target_nodes=target_nodes)

    def readwrite(self, target_nodes=None):
        """
        Disables read queries.
        The command will be sent to the default cluster node if target_nodes is
        not specified.
        """
        # Reset read from replicas flag
        self.read_from_replicas = False
        return self.execute_command('READWRITE', target_nodes=target_nodes)
