import asyncio
import copy
import logging
import random
import socket

from redis.asyncio.client import Redis
from redis.asyncio.connection import ConnectionPool, DefaultParser, Encoder, parse_url
from redis.asyncio.parser import CommandsParser
from redis.client import CaseInsensitiveDict
from redis.cluster import (
    PRIMARY,
    READ_COMMANDS,
    REPLICA,
    SLOT_ID,
    AbstractRedisCluster,
    LoadBalancer,
    cleanup_kwargs,
    get_node_name,
)
from redis.commands import AsyncRedisClusterCommands
from redis.crc import REDIS_CLUSTER_HASH_SLOTS, key_slot
from redis.exceptions import (
    AskError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ClusterError,
    ConnectionError,
    MasterDownError,
    MovedError,
    RedisClusterException,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.utils import dict_merge, str_if_bytes

log = logging.getLogger(__name__)


async def get_connection(redis_node, *args, **options):
    return redis_node.connection or await redis_node.connection_pool.get_connection(
        args[0], **options
    )


class ClusterParser(DefaultParser):
    EXCEPTION_CLASSES = dict_merge(
        DefaultParser.EXCEPTION_CLASSES,
        {
            "ASK": AskError,
            "TRYAGAIN": TryAgainError,
            "MOVED": MovedError,
            "CLUSTERDOWN": ClusterDownError,
            "CROSSSLOT": ClusterCrossSlotError,
            "MASTERDOWN": MasterDownError,
        },
    )


class RedisCluster(AbstractRedisCluster, AsyncRedisClusterCommands):
    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a Redis client object configured from the given URL

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - `redis://` creates a TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/redis>
        - `rediss://` creates a SSL wrapped TCP socket connection. See more at:
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>
        - ``unix://``: creates a Unix Domain Socket connection.

        The username, password, hostname, path and all querystring values
        are passed through urllib.parse.unquote in order to replace any
        percent-encoded values with their corresponding characters.

        There are several ways to specify a database number. The first value
        found will be used:

            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// or rediss:// schemes, the path argument
               of the url, e.g. redis://localhost/0
            3. A ``db`` keyword argument to this function.

        If none of these options are specified, the default db=0 is used.

        All querystring options are cast to their appropriate Python types.
        Boolean arguments can be specified with string values "True"/"False"
        or "Yes"/"No". Values that cannot be properly cast cause a
        ``ValueError`` to be raised. Once parsed, the querystring arguments
        and keyword arguments are passed to the ``ConnectionPool``'s
        class initializer. In the case of conflicting arguments, querystring
        arguments always win.

        """
        return cls(url=url, **kwargs)

    def __init__(
        self,
        host=None,
        port=6379,
        startup_nodes=None,
        cluster_error_retry_attempts=3,
        require_full_coverage=False,
        reinitialize_steps=10,
        read_from_replicas=False,
        url=None,
        **kwargs,
    ):
        """
         Initialize a new RedisCluster client.

         :startup_nodes: 'list[ClusterNode]'
             List of nodes from which initial bootstrapping can be done
         :host: 'str'
             Can be used to point to a startup node
         :port: 'int'
             Can be used to point to a startup node
         :require_full_coverage: 'bool'
            When set to False (default value): the client will not require a
            full coverage of the slots. However, if not all slots are covered,
            and at least one node has 'cluster-require-full-coverage' set to
            'yes,' the server will throw a ClusterDownError for some key-based
            commands. See -
            https://redis.io/topics/cluster-tutorial#redis-cluster-configuration-parameters
            When set to True: all slots must be covered to construct the
            cluster client. If not all slots are covered, RedisClusterException
            will be thrown.
        :read_from_replicas: 'bool'
             Enable read from replicas in READONLY mode. You can read possibly
             stale data.
             When set to true, read commands will be assigned between the
             primary and its replications in a Round-Robin manner.
        :cluster_error_retry_attempts: 'int'
             Retry command execution attempts when encountering ClusterDownError
             or ConnectionError
        :reinitialize_steps: 'int'
            Specifies the number of MOVED errors that need to occur before
            reinitializing the whole cluster topology. If a MOVED error occurs
            and the cluster does not need to be reinitialized on this current
            error handling, only the MOVED slot will be patched with the
            redirected node.
            To reinitialize the cluster on every MOVED error, set
            reinitialize_steps to 1.
            To avoid reinitializing the cluster on moved errors, set
            reinitialize_steps to 0.

         :**kwargs:
             Extra arguments that will be sent into Redis instance when created
             (See Official redis-py doc for supported kwargs
         [https://github.com/andymccurdy/redis-py/blob/master/redis/client.py])
             Some kwargs are not supported and will raise a
             RedisClusterException:
                 - db (Redis do not support database SELECT in cluster mode)
        """
        if startup_nodes is None:
            startup_nodes = []

        if "db" in kwargs:
            # Argument 'db' is not possible to use in cluster mode
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        # Get the startup node/s
        from_url = False
        if url is not None:
            from_url = True
            url_options = parse_url(url)
            if "path" in url_options:
                raise RedisClusterException(
                    "RedisCluster does not currently support Unix Domain "
                    "Socket connections"
                )
            if "db" in url_options and url_options["db"] != 0:
                # Argument 'db' is not possible to use in cluster mode
                raise RedisClusterException(
                    "A ``db`` querystring option can only be 0 in cluster mode"
                )
            kwargs.update(url_options)
            host = kwargs.get("host")
            port = kwargs.get("port", port)
            startup_nodes.append(ClusterNode(host, port))
        elif host is not None and port is not None:
            startup_nodes.append(ClusterNode(host, port))
        elif len(startup_nodes) == 0:
            # No startup node was provided
            raise RedisClusterException(
                "RedisCluster requires at least one node to discover the "
                "cluster. Please provide one of the followings:\n"
                "1. host and port, for example:\n"
                " RedisCluster(host='localhost', port=6379)\n"
                "2. list of startup nodes, for example:\n"
                " RedisCluster(startup_nodes=[ClusterNode('localhost', 6379),"
                " ClusterNode('localhost', 6378)])"
            )
        log.debug(f"startup_nodes : {startup_nodes}")
        # Update the connection arguments
        # Whenever a new connection is established, RedisCluster's on_connect
        # method should be run
        kwargs.update({"redis_connect_func": self.on_connect})
        kwargs = cleanup_kwargs(**kwargs)

        self.encoder = Encoder(
            kwargs.get("encoding", "utf-8"),
            kwargs.get("encoding_errors", "strict"),
            kwargs.get("decode_responses", False),
        )
        self.cluster_error_retry_attempts = cluster_error_retry_attempts
        self.command_flags = self.__class__.COMMAND_FLAGS.copy()
        self.node_flags = self.__class__.NODE_FLAGS.copy()
        self.read_from_replicas = read_from_replicas
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        self.nodes_manager = None
        self.nodes_manager = NodesManager(
            startup_nodes=startup_nodes,
            from_url=from_url,
            require_full_coverage=require_full_coverage,
            **kwargs,
        )

        self.cluster_response_callbacks = CaseInsensitiveDict(
            self.__class__.CLUSTER_COMMANDS_RESPONSE_CALLBACKS
        )
        self.result_callbacks = CaseInsensitiveDict(self.__class__.RESULT_CALLBACKS)
        self.commands_parser = CommandsParser()
        self._initialize = True
        self._lock = asyncio.Lock()

    async def initialize(self, force=False):
        if self._initialize or force:
            self._initialize = False
            await self.nodes_manager.initialize()
            await self.commands_parser.initialize(self)
        return self

    async def __aenter__(self):
        return await self.initialize()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def __await__(self):
        return self.initialize().__await__()

    def __del__(self):
        try:
            loop = asyncio.get_event_loop()
            coro = self.close()
            if loop.is_running():
                loop.create_task(coro)
            else:
                loop.run_until_complete(coro)
        except Exception:
            pass

    async def on_connect(self, connection):
        """
        Initialize the connection, authenticate and select a database and send
         READONLY if it is set during object initialization.
        """
        connection.set_parser(ClusterParser)
        await connection.on_connect()

        if self.read_from_replicas:
            # Sending READONLY command to server to configure connection as
            # readonly. Since each cluster node may change its server type due
            # to a failover, we should establish a READONLY connection
            # regardless of the server type. If this is a primary connection,
            # READONLY would not affect executing write commands.
            await connection.send_command("READONLY")
            if str_if_bytes(await connection.read_response()) != "OK":
                raise ConnectionError("READONLY command failed")

    async def get_redis_connection(self, node):
        if not node.redis_connection:
            async with self._lock:
                if not node.redis_connection:
                    await self.nodes_manager.create_redis_connections([node])
        return node.redis_connection

    def get_node(self, host=None, port=None, node_name=None):
        return self.nodes_manager.get_node(host, port, node_name)

    def get_primaries(self):
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_replicas(self):
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self):
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_nodes(self):
        return list(self.nodes_manager.nodes_cache.values())

    def get_node_from_key(self, key, replica=False):
        """
        Get the node that holds the key's slot.
        If replica set to True but the slot doesn't have any replicas, None is
        returned.
        """
        slot = self.keyslot(key)
        slot_cache = self.nodes_manager.slots_cache.get(slot)
        if slot_cache is None or len(slot_cache) == 0:
            raise SlotNotCoveredError(f'Slot "{slot}" is not covered by the cluster.')
        if replica and len(self.nodes_manager.slots_cache[slot]) < 2:
            return None
        elif replica:
            node_idx = 1
        else:
            # primary
            node_idx = 0

        return slot_cache[node_idx]

    def get_default_node(self):
        """
        Get the cluster's default node
        """
        return self.nodes_manager.default_node

    def set_default_node(self, node):
        """
        Set the default node of the cluster.
        :param node: 'ClusterNode'
        :return True if the default node was set, else False
        """
        if node is None or self.get_node(node_name=node.name) is None:
            log.info(
                "The requested node does not exist in the cluster, so "
                "the default node was not changed."
            )
            return False
        self.nodes_manager.default_node = node
        log.info(f"Changed the default cluster node to {node}")
        return True

    def set_response_callback(self, command, callback):
        """Set a custom Response Callback"""
        self.cluster_response_callbacks[command] = callback

    async def _determine_nodes(self, *args, **kwargs):
        command = args[0]
        nodes_flag = kwargs.pop("nodes_flag", None)
        if nodes_flag is not None:
            # nodes flag passed by the user
            command_flag = nodes_flag
        else:
            # get the nodes group for this command if it was predefined
            command_flag = self.command_flags.get(command)
        if command_flag:
            log.debug(f"Target node/s for {command}: {command_flag}")
        if command_flag == self.__class__.RANDOM:
            # return a random node
            return [self.get_random_node()]
        elif command_flag == self.__class__.PRIMARIES:
            # return all primaries
            return self.get_primaries()
        elif command_flag == self.__class__.REPLICAS:
            # return all replicas
            return self.get_replicas()
        elif command_flag == self.__class__.ALL_NODES:
            # return all nodes
            return self.get_nodes()
        elif command_flag == self.__class__.DEFAULT_NODE:
            # return the cluster's default node
            return [self.nodes_manager.default_node]
        elif command in self.__class__.SEARCH_COMMANDS[0]:
            return [self.nodes_manager.default_node]
        else:
            # get the node that holds the key's slot
            slot = await self.determine_slot(*args)
            node = await self.nodes_manager.get_node_from_slot(
                slot, self.read_from_replicas and command in READ_COMMANDS
            )
            log.debug(f"Target for {args}: slot {slot}")
            return [node]

    def _should_reinitialized(self):
        # To reinitialize the cluster on every MOVED error,
        # set reinitialize_steps to 1.
        # To avoid reinitializing the cluster on moved errors, set
        # reinitialize_steps to 0.
        if self.reinitialize_steps == 0:
            return False
        else:
            return self.reinitialize_counter % self.reinitialize_steps == 0

    def keyslot(self, key):
        """
        Calculate keyslot for a given key.
        See Keys distribution model in https://redis.io/topics/cluster-spec
        """
        k = self.encoder.encode(key)
        return key_slot(k)

    async def _get_command_keys(self, *args):
        """
        Get the keys in the command. If the command has no keys in in, None is
        returned.

        NOTE: Due to a bug in redis<7.0, this function does not work properly
        for EVAL or EVALSHA when the `numkeys` arg is 0.
         - issue: https://github.com/redis/redis/issues/9493
         - fix: https://github.com/redis/redis/pull/9733

        So, don't use this function with EVAL or EVALSHA.
        """
        redis_conn = self.get_default_node().redis_connection
        return await self.commands_parser.get_keys(redis_conn, *args)

    async def determine_slot(self, *args):
        """
        Figure out what slot to use based on args.

        Raises a RedisClusterException if there's a missing key and we can't
            determine what slots to map the command to; or, if the keys don't
            all map to the same key slot.
        """
        command = args[0]
        if self.command_flags.get(command) == SLOT_ID:
            # The command contains the slot ID
            return args[1]

        # Get the keys in the command

        # EVAL and EVALSHA are common enough that it's wasteful to go to the
        # redis server to parse the keys. Besides, there is a bug in redis<7.0
        # where `self._get_command_keys()` fails anyway. So, we special case
        # EVAL/EVALSHA.
        if command in ("EVAL", "EVALSHA"):
            # command syntax: EVAL "script body" num_keys ...
            if len(args) <= 2:
                raise RedisClusterException(f"Invalid args in command: {args}")
            num_actual_keys = args[2]
            eval_keys = args[3 : 3 + num_actual_keys]
            # if there are 0 keys, that means the script can be run on any node
            # so we can just return a random slot
            if len(eval_keys) == 0:
                return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
            keys = eval_keys
        else:
            keys = await self._get_command_keys(*args)
            if keys is None or len(keys) == 0:
                # FCALL can call a function with 0 keys, that means the function
                #  can be run on any node so we can just return a random slot
                if command in ("FCALL", "FCALL_RO"):
                    return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
                raise RedisClusterException(
                    "No way to dispatch this command to Redis Cluster. "
                    "Missing key.\nYou can execute the command by specifying "
                    f"target nodes.\nCommand: {args}"
                )

        # single key command
        if len(keys) == 1:
            return self.keyslot(keys[0])

        # multi-key command; we need to make sure all keys are mapped to
        # the same slot
        slots = {self.keyslot(key) for key in keys}
        if len(slots) != 1:
            raise RedisClusterException(
                f"{command} - all keys must map to the same key slot"
            )

        return slots.pop()

    def get_encoder(self):
        """
        Get the connections' encoder
        """
        return self.encoder

    def get_connection_kwargs(self):
        """
        Get the connections' key-word arguments
        """
        return self.nodes_manager.connection_kwargs

    def _is_nodes_flag(self, target_nodes):
        return isinstance(target_nodes, str) and target_nodes in self.node_flags

    def _parse_target_nodes(self, target_nodes):
        if isinstance(target_nodes, list):
            nodes = target_nodes
        elif isinstance(target_nodes, ClusterNode):
            # Supports passing a single ClusterNode as a variable
            nodes = [target_nodes]
        elif isinstance(target_nodes, dict):
            # Supports dictionaries of the format {node_name: node}.
            # It enables to execute commands with multi nodes as follows:
            # rc.cluster_save_config(rc.get_primaries())
            nodes = target_nodes.values()
        else:
            raise TypeError(
                "target_nodes type can be one of the following: "
                "node_flag (PRIMARIES, REPLICAS, RANDOM, ALL_NODES),"
                "ClusterNode, list<ClusterNode>, or dict<any, ClusterNode>. "
                f"The passed type is {type(target_nodes)}"
            )
        return nodes

    async def execute_command(self, *args, **kwargs):
        """
        Wrapper for ERRORS_ALLOW_RETRY error handling.

        It will try the number of times specified by the config option
        "self.cluster_error_retry_attempts" which defaults to 3 unless manually
        configured.

        If it reaches the number of times, the command will raise the exception

        Key argument :target_nodes: can be passed with the following types:
            nodes_flag: PRIMARIES, REPLICAS, ALL_NODES, RANDOM
            ClusterNode
            list<ClusterNode>
            dict<Any, ClusterNode>
        """
        await self.initialize()
        target_nodes_specified = False
        target_nodes = None
        passed_targets = kwargs.pop("target_nodes", None)
        if passed_targets is not None and not self._is_nodes_flag(passed_targets):
            target_nodes = self._parse_target_nodes(passed_targets)
            target_nodes_specified = True
        # If an error that allows retrying was thrown, the nodes and slots
        # cache were reinitialized. We will retry executing the command with
        # the updated cluster setup only when the target nodes can be
        # determined again with the new cache tables. Therefore, when target
        # nodes were passed to this function, we cannot retry the command
        # execution since the nodes may not be valid anymore after the tables
        # were reinitialized. So in case of passed target nodes,
        # retry_attempts will be set to 1.
        retry_attempts = (
            1 if target_nodes_specified else self.cluster_error_retry_attempts
        )
        exception = None
        for _ in range(0, retry_attempts):
            try:
                res = {}
                if not target_nodes_specified:
                    # Determine the nodes to execute the command on
                    target_nodes = await self._determine_nodes(
                        *args, **kwargs, nodes_flag=passed_targets
                    )
                    if not target_nodes:
                        raise RedisClusterException(
                            f"No targets were found to execute {args} command on"
                        )
                for node in target_nodes:
                    res[node.name] = await self._execute_command(node, *args, **kwargs)
                # Return the processed result
                return self._process_result(args[0], res, **kwargs)
            except BaseException as e:
                if type(e) in AbstractRedisCluster.ERRORS_ALLOW_RETRY:
                    # The nodes and slots cache were reinitialized.
                    # Try again with the new cluster setup.
                    exception = e
                else:
                    # All other errors should be raised.
                    raise e

        # If it fails the configured number of times then raise exception back
        # to caller of this method
        raise exception

    async def _execute_command(self, target_node, *args, **kwargs):
        """
        Send a command to a node in the cluster
        """
        command = args[0]
        redis_node = None
        connection = None
        redirect_addr = None
        asking = False
        moved = False
        ttl = int(self.RedisClusterRequestTTL)
        connection_error_retry_counter = 0

        while ttl > 0:
            ttl -= 1
            try:
                if asking:
                    target_node = self.get_node(node_name=redirect_addr)
                elif moved:
                    # MOVED occurred and the slots cache was updated,
                    # refresh the target node
                    slot = await self.determine_slot(*args)
                    target_node = await self.nodes_manager.get_node_from_slot(
                        slot, self.read_from_replicas and command in READ_COMMANDS
                    )
                    moved = False

                log.debug(
                    f"Executing command {command} on target node: "
                    f"{target_node.server_type} {target_node.name}"
                )
                redis_node = await self.get_redis_connection(target_node)
                connection = await get_connection(redis_node, *args, **kwargs)
                if asking:
                    await connection.send_command("ASKING")
                    await redis_node.parse_response(connection, "ASKING", **kwargs)
                    asking = False

                await connection.send_command(*args)
                response = await redis_node.parse_response(
                    connection, command, **kwargs
                )
                if command in self.cluster_response_callbacks:
                    response = self.cluster_response_callbacks[command](
                        response, **kwargs
                    )
                return response

            except (RedisClusterException, BusyLoadingError) as e:
                log.exception(type(e))
                raise
            except (ConnectionError, TimeoutError) as e:
                log.exception(type(e))
                # ConnectionError can also be raised if we couldn't get a
                # connection from the pool before timing out, so check that
                # this is an actual connection before attempting to disconnect.
                if connection is not None:
                    await connection.disconnect()
                connection_error_retry_counter += 1

                # Give the node 0.25 seconds to get back up and retry again
                # with same node and configuration. After 5 attempts then try
                # to reinitialize the cluster and see if the nodes
                # configuration has changed or not
                if connection_error_retry_counter < 5:
                    await asyncio.sleep(0.25)
                else:
                    # Hard force of reinitialize of the node/slots setup
                    # and try again with the new setup
                    await self.initialize(force=True)
                    raise
            except MovedError as e:
                # First, we will try to patch the slots/nodes cache with the
                # redirected node output and try again. If MovedError exceeds
                # 'reinitialize_steps' number of times, we will force
                # reinitializing the tables, and then try again.
                # 'reinitialize_steps' counter will increase faster when
                # the same client object is shared between multiple threads. To
                # reduce the frequency you can set this variable in the
                # RedisCluster constructor.
                log.exception("MovedError")
                self.reinitialize_counter += 1
                if self._should_reinitialized():
                    await self.initialize(force=True)
                    # Reset the counter
                    self.reinitialize_counter = 0
                else:
                    self.nodes_manager.update_moved_exception(e)
                moved = True
            except TryAgainError:
                log.exception("TryAgainError")

                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.05)
            except AskError as e:
                log.exception("AskError")

                redirect_addr = get_node_name(host=e.host, port=e.port)
                asking = True
            except ClusterDownError as e:
                log.exception("ClusterDownError")
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command
                await asyncio.sleep(0.25)
                await self.initialize(force=True)
                raise e
            except ResponseError as e:
                message = e.__str__()
                log.exception(f"ResponseError: {message}")
                raise e
            except BaseException as e:
                log.exception("BaseException")
                if connection:
                    await connection.disconnect()
                raise e
            finally:
                if connection is not None:
                    await redis_node.connection_pool.release(connection)

        raise ClusterError("TTL exhausted.")

    async def close(self):
        try:
            async with self._lock:
                if self.nodes_manager:
                    await self.nodes_manager.close()
        except AttributeError:
            # RedisCluster's __init__ can fail before nodes_manager is set
            pass

    def _process_result(self, command, res, **kwargs):
        """
        Process the result of the executed command.
        The function would return a dict or a single value.

        :type command: str
        :type res: dict

        `res` should be in the following format:
            Dict<node_name, command_result>
        """
        if command in self.result_callbacks:
            return self.result_callbacks[command](command, res, **kwargs)
        elif len(res) == 1:
            # When we execute the command on a single node, we can
            # remove the dictionary and return a single response
            return list(res.values())[0]
        else:
            return res


class ClusterNode:
    def __init__(self, host, port, server_type=None, redis_connection=None):
        if host == "localhost":
            host = socket.gethostbyname(host)

        self.host = host
        self.port = port
        self.name = get_node_name(host, port)
        self.server_type = server_type
        self.redis_connection = redis_connection

    def __repr__(self):
        return (
            f"[host={self.host},"
            f"port={self.port},"
            f"name={self.name},"
            f"server_type={self.server_type},"
            f"redis_connection={self.redis_connection}]"
        )

    def __eq__(self, obj):
        return isinstance(obj, ClusterNode) and obj.name == self.name

    def __del__(self):
        try:
            if self.redis_connection is not None:
                loop = asyncio.get_event_loop()
                coro = self.redis_connection.close(True)
                if loop.is_running():
                    loop.create_task(coro)
                else:
                    loop.run_until_complete(coro)
        except Exception:
            pass


class NodesManager:
    def __init__(
        self,
        startup_nodes,
        from_url=False,
        require_full_coverage=False,
        lock=None,
        **kwargs,
    ):
        self.nodes_cache = {}
        self.slots_cache = {}
        self.startup_nodes = {}
        self.default_node = None
        self.populate_startup_nodes(startup_nodes)
        self.from_url = from_url
        self._require_full_coverage = require_full_coverage
        self._moved_exception = None
        self.connection_kwargs = kwargs
        self.read_load_balancer = LoadBalancer()
        if lock is None:
            lock = asyncio.Lock()
        self._lock = lock

    def get_node(self, host=None, port=None, node_name=None):
        """
        Get the requested node from the cluster's nodes.
        nodes.
        :return: ClusterNode if the node exists, else None
        """
        if host and port:
            # the user passed host and port
            if host == "localhost":
                host = socket.gethostbyname(host)
            return self.nodes_cache.get(get_node_name(host=host, port=port))
        elif node_name:
            return self.nodes_cache.get(node_name)
        else:
            log.error(
                "get_node requires one of the following: "
                "1. node name "
                "2. host and port"
            )
            return None

    def update_moved_exception(self, exception):
        self._moved_exception = exception

    def _update_moved_slots(self):
        """
        Update the slot's node with the redirected one
        """
        e = self._moved_exception
        redirected_node = self.get_node(host=e.host, port=e.port)
        if redirected_node is not None:
            # The node already exists
            if redirected_node.server_type is not PRIMARY:
                # Update the node's server type
                redirected_node.server_type = PRIMARY
        else:
            # This is a new node, we will add it to the nodes cache
            redirected_node = ClusterNode(e.host, e.port, PRIMARY)
            self.nodes_cache[redirected_node.name] = redirected_node
        if redirected_node in self.slots_cache[e.slot_id]:
            # The MOVED error resulted from a failover, and the new slot owner
            # had previously been a replica.
            old_primary = self.slots_cache[e.slot_id][0]
            # Update the old primary to be a replica and add it to the end of
            # the slot's node list
            old_primary.server_type = REPLICA
            self.slots_cache[e.slot_id].append(old_primary)
            # Remove the old replica, which is now a primary, from the slot's
            # node list
            self.slots_cache[e.slot_id].remove(redirected_node)
            # Override the old primary with the new one
            self.slots_cache[e.slot_id][0] = redirected_node
            if self.default_node == old_primary:
                # Update the default node with the new primary
                self.default_node = redirected_node
        else:
            # The new slot owner is a new server, or a server from a different
            # shard. We need to remove all current nodes from the slot's list
            # (including replications) and add just the new node.
            self.slots_cache[e.slot_id] = [redirected_node]
        # Reset moved_exception
        self._moved_exception = None

    async def get_node_from_slot(
        self, slot, read_from_replicas=False, server_type=None
    ):
        """
        Gets a node that servers this hash slot
        """
        if self._moved_exception:
            async with self._lock:
                if self._moved_exception:
                    self._update_moved_slots()

        if self.slots_cache.get(slot) is None or len(self.slots_cache[slot]) == 0:
            raise SlotNotCoveredError(
                f'Slot "{slot}" not covered by the cluster. '
                f'"require_full_coverage={self._require_full_coverage}"'
            )

        if read_from_replicas is True:
            # get the server index in a Round-Robin manner
            primary_name = self.slots_cache[slot][0].name
            node_idx = self.read_load_balancer.get_server_index(
                primary_name, len(self.slots_cache[slot])
            )
        elif (
            server_type is None
            or server_type == PRIMARY
            or len(self.slots_cache[slot]) == 1
        ):
            # return a primary
            node_idx = 0
        else:
            # return a replica
            # randomly choose one of the replicas
            node_idx = random.randint(1, len(self.slots_cache[slot]) - 1)

        return self.slots_cache[slot][node_idx]

    def get_nodes_by_server_type(self, server_type):
        """
        Get all nodes with the specified server type
        :param server_type: 'primary' or 'replica'
        :return: list of ClusterNode
        """
        return [
            node
            for node in self.nodes_cache.values()
            if node.server_type == server_type
        ]

    def populate_startup_nodes(self, nodes):
        """
        Populate all startup nodes and filters out any duplicates
        """
        for n in nodes:
            self.startup_nodes[n.name] = n

    def check_slots_coverage(self, slots_cache):
        # Validate if all slots are covered or if we should try next
        # startup node
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            if i not in slots_cache:
                return False
        return True

    async def create_redis_connections(self, nodes):
        """
        This function will create a redis connection to all nodes in :nodes:
        """
        for node in nodes:
            if node.redis_connection is None:
                node.redis_connection = await self.create_redis_node(
                    host=node.host, port=node.port, **self.connection_kwargs
                )

    async def create_redis_node(self, host, port, **kwargs):
        if self.from_url:
            # Create a redis node with a costumed connection pool
            kwargs.update({"host": host})
            kwargs.update({"port": port})
            r = Redis(connection_pool=ConnectionPool(**kwargs))
        else:
            r = Redis(host=host, port=port, **kwargs)
        await r.initialize()
        return r

    async def initialize(self):
        """
        Initializes the nodes cache, slots cache and redis connections.
        :startup_nodes:
            Responsible for discovering other nodes in the cluster
        """
        log.debug("Initializing the nodes' topology of the cluster")
        self.reset()
        tmp_nodes_cache = {}
        tmp_slots = {}
        disagreements = []
        startup_nodes_reachable = False
        fully_covered = False
        kwargs = self.connection_kwargs
        for startup_node in self.startup_nodes.values():
            try:
                if startup_node.redis_connection:
                    r = startup_node.redis_connection
                else:
                    # Create a new Redis connection and let Redis decode the
                    # responses so we won't need to handle that
                    # TODO: redis_connect_func shouldn't need to be removed & readded
                    redis_connect_func = kwargs.pop("redis_connect_func")
                    copy_kwargs = copy.deepcopy(kwargs)
                    kwargs.setdefault("redis_connect_func", redis_connect_func)
                    copy_kwargs.update(
                        {
                            "decode_responses": True,
                            "encoding": "utf-8",
                            "redis_connect_func": redis_connect_func,
                        }
                    )
                    r = await self.create_redis_node(
                        startup_node.host, startup_node.port, **copy_kwargs
                    )
                    self.startup_nodes[startup_node.name].redis_connection = r
                # Make sure cluster mode is enabled on this node
                if bool((await r.info()).get("cluster_enabled")) is False:
                    raise RedisClusterException(
                        "Cluster mode is not enabled on this node"
                    )
                cluster_slots = str_if_bytes(await r.execute_command("CLUSTER SLOTS"))
                startup_nodes_reachable = True
            except (ConnectionError, TimeoutError) as e:
                msg = e.__str__
                log.exception(
                    "An exception occurred while trying to"
                    " initialize the cluster using the seed node"
                    f" {startup_node.name}:\n{msg}"
                )
                continue
            except ResponseError as e:
                log.exception('ReseponseError sending "cluster slots" to redis server')

                # Isn't a cluster connection, so it won't parse these
                # exceptions automatically
                message = e.__str__()
                if "CLUSTERDOWN" in message or "MASTERDOWN" in message:
                    continue
                else:
                    raise RedisClusterException(
                        'ERROR sending "cluster slots" command to redis '
                        f"server: {startup_node}. error: {message}"
                    )
            except Exception as e:
                message = e.__str__()
                raise RedisClusterException(
                    'ERROR sending "cluster slots" command to redis '
                    f"server {startup_node.name}. error: {message}"
                )

            # CLUSTER SLOTS command results in the following output:
            # [[slot_section[from_slot,to_slot,master,replica1,...,replicaN]]]
            # where each node contains the following list: [IP, port, node_id]
            # Therefore, cluster_slots[0][2][0] will be the IP address of the
            # primary node of the first slot section.
            # If there's only one server in the cluster, its ``host`` is ''
            # Fix it to the host in startup_nodes
            if (
                len(cluster_slots) == 1
                and len(cluster_slots[0][2][0]) == 0
                and len(self.startup_nodes) == 1
            ):
                cluster_slots[0][2][0] = startup_node.host

            for slot in cluster_slots:
                primary_node = slot[2]
                host = primary_node[0]
                if host == "":
                    host = startup_node.host
                port = int(primary_node[1])

                target_node = tmp_nodes_cache.get(get_node_name(host, port))
                if target_node is None:
                    target_node = ClusterNode(host, port, PRIMARY)
                # add this node to the nodes cache
                tmp_nodes_cache[target_node.name] = target_node

                for i in range(int(slot[0]), int(slot[1]) + 1):
                    if i not in tmp_slots:
                        tmp_slots[i] = []
                        tmp_slots[i].append(target_node)
                        replica_nodes = [slot[j] for j in range(3, len(slot))]

                        for replica_node in replica_nodes:
                            host = replica_node[0]
                            port = replica_node[1]

                            target_replica_node = tmp_nodes_cache.get(
                                get_node_name(host, port)
                            )
                            if target_replica_node is None:
                                target_replica_node = ClusterNode(host, port, REPLICA)
                            tmp_slots[i].append(target_replica_node)
                            # add this node to the nodes cache
                            tmp_nodes_cache[
                                target_replica_node.name
                            ] = target_replica_node
                    else:
                        # Validate that 2 nodes want to use the same slot cache
                        # setup
                        tmp_slot = tmp_slots[i][0]
                        if tmp_slot.name != target_node.name:
                            disagreements.append(
                                f"{tmp_slot.name} vs {target_node.name} on slot: {i}"
                            )

                            if len(disagreements) > 5:
                                raise RedisClusterException(
                                    f"startup_nodes could not agree on a valid "
                                    f'slots cache: {", ".join(disagreements)}'
                                )

            fully_covered = self.check_slots_coverage(tmp_slots)
            if fully_covered:
                # Don't need to continue to the next startup node if all
                # slots are covered
                break

        if not startup_nodes_reachable:
            raise RedisClusterException(
                "Redis Cluster cannot be connected. Please provide at least "
                "one reachable node. "
            )

        # Create Redis connections to all nodes
        await self.create_redis_connections(list(tmp_nodes_cache.values()))

        # Check if the slots are not fully covered
        if not fully_covered and self._require_full_coverage:
            # Despite the requirement that the slots be covered, there
            # isn't a full coverage
            raise RedisClusterException(
                f"All slots are not covered after query all startup_nodes. "
                f"{len(tmp_slots)} of {REDIS_CLUSTER_HASH_SLOTS} "
                f"covered..."
            )

        # Set the tmp variables to the real variables
        self.nodes_cache = tmp_nodes_cache
        self.slots_cache = tmp_slots
        # Set the default node
        self.default_node = self.get_nodes_by_server_type(PRIMARY)[0]
        # Populate the startup nodes with all discovered nodes
        self.populate_startup_nodes(self.nodes_cache.values())
        # If initialize was called after a MovedError, clear it
        self._moved_exception = None

    async def close(self):
        self.default_node = None
        for node in self.nodes_cache.values():
            if node.redis_connection:
                await node.redis_connection.close(True)

    def reset(self):
        try:
            self.read_load_balancer.reset()
        except TypeError:
            # The read_load_balancer is None, do nothing
            pass
