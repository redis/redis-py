from redis.exceptions import (
    ConnectionError,
    DataError,
    RedisError,
)
from redis.crc import key_slot
from .core import DataAccessCommands, PubSubCommands
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

    def config_get(self, pattern="*", target_nodes=None):
        """Return a dictionary of configuration based on the ``pattern``"""
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
        If no target nodes are specified, send to the entire cluster and sum
         the results.

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
        Delete all keys in the database on all hosts.
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

    def migrate(self, host, source_node, port, keys, destination_db, timeout,
                copy=False, replace=False, auth=None):
        """
        Migrate 1 or more keys from the source_node Redis server to a different
        server specified by the ``host``, ``port`` and ``destination_db``.

        The ``timeout``, specified in milliseconds, indicates the maximum
        time the connection between the two servers can be idle before the
        command is interrupted.

        If ``copy`` is True, the specified ``keys`` are NOT deleted from
        the source server.

        If ``replace`` is True, this operation will overwrite the keys
        on the destination server if they exist.

        If ``auth`` is specified, authenticate to the destination server with
        the password provided.
        """
        keys = list_or_args(keys, [])
        if not keys:
            raise DataError('MIGRATE requires at least one key')
        pieces = []
        if copy:
            pieces.append(b'COPY')
        if replace:
            pieces.append(b'REPLACE')
        if auth:
            pieces.append(b'AUTH')
            pieces.append(auth)
        pieces.append(b'KEYS')
        pieces.extend(keys)
        return self.execute_command('MIGRATE', host, port, '', destination_db,
                                    timeout, *pieces,
                                    target_nodes=source_node)

    def object(self, infotype, key):
        """Return the encoding, idletime, or refcount about the key"""
        return self.execute_command('OBJECT', infotype, key, infotype=infotype)

    def ping(self, target_nodes=None):
        """
        Ping the cluster's servers.
        If no target nodes are specified, sent to all nodes and returns True if
         the ping was successful across all nodes.

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        return self.execute_command('PING',
                                    target_nodes=target_nodes)

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')

    def shutdown(self, save=False, nosave=False):
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
            self.execute_command(*args)
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

        In cluster mode the WAIT command will be sent to all primaries
        and the result will be summed up
        """
        return self.execute_command('WAIT', num_replicas,
                                    timeout,
                                    target_nodes=target_nodes)


class ClusterCommands(ClusterManagementCommands, ClusterMultiKeyCommands,
                      DataAccessCommands, PubSubCommands):
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

    def cluster_info(self, target_node=None):
        """
        Provides info about Redis Cluster node state.
        The command will be sent to a random node in the cluster if no target
        node is specified.

        :target_node: 'ClusterNode'
            The node to execute the command on
        """
        return self.execute_command('CLUSTER INFO', target_nodes=target_node)

    def cluster_keyslot(self, key):
        """
        Returns the hash slot of the specified key
        Sends to random node in the cluster
        """
        return self.execute_command('CLUSTER KEYSLOT', key)

    def cluster_meet(self, target_nodes, host, port):
        """
        Force a node cluster to handshake with another node.
        Sends to specified node.

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
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

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        return self.execute_command('CLUSTER REPLICATE', node_id,
                                    target_nodes=target_nodes)

    def cluster_reset(self, target_nodes, soft=True):
        """
        Reset a Redis Cluster node

        If 'soft' is True then it will send 'SOFT' argument
        If 'soft' is False then it will send 'HARD' argument

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        return self.execute_command('CLUSTER RESET',
                                    b'SOFT' if soft else b'HARD',
                                    target_nodes=target_nodes)

    def cluster_save_config(self, target_nodes):
        """
        Forces the node to save cluster state on disk

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        return self.execute_command('CLUSTER SAVECONFIG',
                                    target_nodes=target_nodes)

    def cluster_get_keys_in_slot(self, slot, num_keys):
        """
        Returns the number of keys in the specified cluster slot
        """
        return self.execute_command('CLUSTER GETKEYSINSLOT', slot, num_keys)

    def cluster_set_config_epoch(self, target_nodes, epoch):
        """
        Set the configuration epoch in a new node

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
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

    def cluster_replicas(self, node_id):
        """
        Provides a list of replica nodes replicating from the specified primary
        target node.
        Sends to random node in the cluster.
        """
        return self.execute_command('CLUSTER REPLICAS', node_id)

    def cluster_slots(self):
        """
        Get array of Cluster slot to node mappings

        Sends to random node in the cluster
        """
        return self.execute_command('CLUSTER SLOTS')

    def readonly(self, target_nodes=None):
        """
        Enables read queries.
        The command will be sent to all replica nodes if target_nodes is not
        specified.

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
         """
        self.read_from_replicas = True
        return self.execute_command('READONLY', target_nodes=target_nodes)

    def readwrite(self, target_nodes=None):
        """
        Disables read queries.
        The command will be sent to all replica nodes if target_nodes is not
        specified.

        :target_nodes: 'ClusterNode' or 'list(ClusterNodes)'
            The node/s to execute the command on
        """
        # Reset read from replicas flag
        self.read_from_replicas = False
        return self.execute_command('READWRITE', target_nodes=target_nodes)
