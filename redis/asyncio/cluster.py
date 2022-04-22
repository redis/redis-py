import asyncio
import collections
import random
import socket
import threading
import warnings
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from redis.asyncio.client import EMPTY_RESPONSE, NEVER_DECODE, AbstractRedis
from redis.asyncio.connection import Connection, DefaultParser, Encoder, parse_url
from redis.asyncio.parser import CommandsParser
from redis.cluster import (
    PRIMARY,
    READ_COMMANDS,
    REPLICA,
    SLOT_ID,
    AbstractRedisCluster,
    LoadBalancer,
    cleanup_kwargs,
    get_node_name,
    parse_cluster_slots,
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
    DataError,
    MasterDownError,
    MovedError,
    RedisClusterException,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.typing import EncodableT, KeyT
from redis.utils import dict_merge, str_if_bytes

TargetNodesT = TypeVar(
    "TargetNodesT", "ClusterNode", List["ClusterNode"], Dict[Any, "ClusterNode"]
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


class RedisCluster(AbstractRedis, AbstractRedisCluster, AsyncRedisClusterCommands):
    """
    Create a new RedisCluster client.

    Pass one of parameters:

      - `url`
      - `host`
      - `startup_nodes`

    | Use :meth:`initialize` to find cluster nodes & create connections.
    | Use :meth:`close` to disconnect connections & close client.

    Many commands support the target_nodes kwarg. It can be one of the
    :attr:`NODE_FLAGS`:

      - :attr:`PRIMARIES`
      - :attr:`REPLICAS`
      - :attr:`ALL_NODES`
      - :attr:`RANDOM`
      - :attr:`DEFAULT_NODE`

    :param host:
        | Can be used to point to a startup node
    :param port:
        | Port used if **host** or **url** is provided
    :param startup_nodes:
        | :class:`~.ClusterNode` to used as a startup node
    :param cluster_error_retry_attempts:
        | Retry command execution attempts when encountering :class:`~.ClusterDownError`
          or :class:`~.ConnectionError`
    :param require_full_coverage:
        | When set to ``False``: the client will not require a full coverage of the
          slots. However, if not all slots are covered, and at least one node has
          ``cluster-require-full-coverage`` set to ``yes``, the server will throw a
          :class:`~.ClusterDownError` for some key-based commands.
        | When set to ``True``: all slots must be covered to construct the cluster
          client. If not all slots are covered, :class:`~.RedisClusterException` will be
          thrown.
        | See:
          https://redis.io/docs/manual/scaling/#redis-cluster-configuration-parameters
    :param reinitialize_steps:
        | Specifies the number of MOVED errors that need to occur before reinitializing
          the whole cluster topology. If a MOVED error occurs and the cluster does not
          need to be reinitialized on this current error handling, only the MOVED slot
          will be patched with the redirected node.
          To reinitialize the cluster on every MOVED error, set reinitialize_steps to 1.
          To avoid reinitializing the cluster on moved errors, set reinitialize_steps to
          0.
    :param read_from_replicas:
        | Enable read from replicas in READONLY mode. You can read possibly stale data.
          When set to true, read commands will be assigned between the primary and
          its replications in a Round-Robin manner.
    :param url:
        | See :meth:`.from_url`
    :param kwargs:
        | Extra arguments that will be passed to the
          :class:`~redis.asyncio.connection.Connection` instance when created

    :raises RedisClusterException:
        if any arguments are invalid. Eg:

        - db kwarg
        - db != 0 in url
        - unix socket connection
        - none of host & url & startup_nodes were provided

    """

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "RedisCluster":
        """
        Return a Redis client object configured from the given URL.

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

    __slots__ = (
        "_initialize",
        "_lock",
        "cluster_error_retry_attempts",
        "command_flags",
        "commands_parser",
        "connection_kwargs",
        "encoder",
        "node_flags",
        "nodes_manager",
        "read_from_replicas",
        "reinitialize_counter",
        "reinitialize_steps",
        "response_callbacks",
        "result_callbacks",
    )

    def __init__(
        self,
        host: Optional[str] = None,
        port: int = 6379,
        startup_nodes: Optional[List["ClusterNode"]] = None,
        require_full_coverage: bool = False,
        read_from_replicas: bool = False,
        cluster_error_retry_attempts: int = 3,
        reinitialize_steps: int = 10,
        url: Optional[str] = None,
        **kwargs,
    ) -> None:
        if not startup_nodes:
            startup_nodes = []

        if "db" in kwargs:
            # Argument 'db' is not possible to use in cluster mode
            raise RedisClusterException(
                "Argument 'db' is not possible to use in cluster mode"
            )

        # Get the startup node/s
        if url:
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
        elif (not host or not port) and not startup_nodes:
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

        # Update the connection arguments
        # Whenever a new connection is established, RedisCluster's on_connect
        # method should be run
        kwargs["redis_connect_func"] = self.on_connect
        self.connection_kwargs = kwargs = cleanup_kwargs(**kwargs)
        self.response_callbacks = kwargs[
            "response_callbacks"
        ] = self.__class__.RESPONSE_CALLBACKS
        if host and port:
            startup_nodes.append(ClusterNode(host, port, **self.connection_kwargs))

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
        self.nodes_manager = NodesManager(
            startup_nodes=startup_nodes,
            require_full_coverage=require_full_coverage,
            **self.connection_kwargs,
        )

        self.result_callbacks = self.__class__.RESULT_CALLBACKS
        self.result_callbacks[
            "CLUSTER SLOTS"
        ] = lambda cmd, res, **kwargs: parse_cluster_slots(
            list(res.values())[0], **kwargs
        )
        self.commands_parser = CommandsParser()
        self._initialize = True
        self._lock = asyncio.Lock()

    async def initialize(self) -> "RedisCluster":
        """Get all nodes from startup nodes & creates connections if not initialized."""
        if self._initialize:
            async with self._lock:
                if self._initialize:
                    self._initialize = False
                    try:
                        await self.nodes_manager.initialize()
                        await self.commands_parser.initialize(
                            self.nodes_manager.default_node
                        )
                    except BaseException:
                        self._initialize = True
                        await self.nodes_manager.close()
                        await self.nodes_manager.close("startup_nodes")
                        raise
        return self

    async def close(self) -> None:
        """Close all connections & client if initialized."""
        if not self._initialize:
            async with self._lock:
                if not self._initialize:
                    self._initialize = True
                    await self.nodes_manager.close()

    async def __aenter__(self) -> "RedisCluster":
        return await self.initialize()

    async def __aexit__(self, exc_type: None, exc_value: None, traceback: None) -> None:
        await self.close()

    def __await__(self):
        return self.initialize().__await__()

    _DEL_MESSAGE = "Unclosed RedisCluster client"

    def __del__(self, _warnings=warnings):
        if hasattr(self, "_initialize") and not self._initialize:
            _warnings.warn(
                f"{self._DEL_MESSAGE} {self!r}", ResourceWarning, source=self
            )
            try:
                context = {"client": self, "message": self._DEL_MESSAGE}
                # TODO: Change to get_running_loop() when dropping support for py3.6
                asyncio.get_event_loop().call_exception_handler(context)
            except RuntimeError:
                ...

    async def on_connect(self, connection: Connection) -> None:
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

    def get_node(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> Optional["ClusterNode"]:
        """Get node by (host, port) or node_name."""
        return self.nodes_manager.get_node(host, port, node_name)

    def get_primaries(self) -> List["ClusterNode"]:
        """Get the primary nodes of the cluster."""
        return self.nodes_manager.get_nodes_by_server_type(PRIMARY)

    def get_replicas(self) -> List["ClusterNode"]:
        """Get the replica nodes of the cluster."""
        return self.nodes_manager.get_nodes_by_server_type(REPLICA)

    def get_random_node(self) -> "ClusterNode":
        """Get a random node of the cluster."""
        return random.choice(list(self.nodes_manager.nodes_cache.values()))

    def get_nodes(self) -> List["ClusterNode"]:
        """Get all nodes of the cluster."""
        return list(self.nodes_manager.nodes_cache.values())

    def get_node_from_key(
        self, key: str, replica: bool = False
    ) -> Optional["ClusterNode"]:
        """
        Get the cluster node corresponding to the provided key.

        :param key:
        :param replica:
            | Indicates if a replica should be returned
              None will returned if no replica holds this key

        :raises SlotNotCoveredError: if the key is not covered by any slot.
        """
        slot = self.keyslot(key)
        slot_cache = self.nodes_manager.slots_cache.get(slot)
        if not slot_cache:
            raise SlotNotCoveredError(f'Slot "{slot}" is not covered by the cluster.')
        if replica and len(self.nodes_manager.slots_cache[slot]) < 2:
            return None
        elif replica:
            node_idx = 1
        else:
            # primary
            node_idx = 0

        return slot_cache[node_idx]

    def get_default_node(self) -> "ClusterNode":
        """Get the default node of the client."""
        return self.nodes_manager.default_node

    def set_default_node(self, node: "ClusterNode") -> None:
        """
        Set the default node of the client.

        :raises DataError: if None is passed or node does not exist in cluster.
        """
        if not node or not self.get_node(node_name=node.name):
            raise DataError("The requested node does not exist in the cluster.")

        self.nodes_manager.default_node = node

    def set_response_callback(self, command: KeyT, callback: Callable) -> None:
        """Set a custom response callback."""
        self.response_callbacks[command] = callback

    def get_encoder(self) -> Encoder:
        """Get the encoder object of the client."""
        return self.encoder

    def get_connection_kwargs(self) -> Dict[str, Optional[Any]]:
        """Get the kwargs passed to :class:`~redis.asyncio.connection.Connection`."""
        return self.connection_kwargs

    def keyslot(self, key: EncodableT) -> int:
        """
        Find the keyslot for a given key.

        See: https://redis.io/docs/manual/scaling/#redis-cluster-data-sharding
        """
        k = self.encoder.encode(key)
        return key_slot(k)

    async def _determine_nodes(
        self, *args, node_flag: Optional[str] = None
    ) -> List["ClusterNode"]:
        command = args[0]
        if not node_flag:
            # get the nodes group for this command if it was predefined
            node_flag = self.command_flags.get(command)

        if node_flag in self.node_flags:
            if node_flag == self.__class__.DEFAULT_NODE:
                # return the cluster's default node
                return [self.nodes_manager.default_node]
            if node_flag == self.__class__.PRIMARIES:
                # return all primaries
                return self.nodes_manager.get_nodes_by_server_type(PRIMARY)
            if node_flag == self.__class__.REPLICAS:
                # return all replicas
                return self.nodes_manager.get_nodes_by_server_type(REPLICA)
            if node_flag == self.__class__.ALL_NODES:
                # return all nodes
                return list(self.nodes_manager.nodes_cache.values())
            if node_flag == self.__class__.RANDOM:
                # return a random node
                return [random.choice(list(self.nodes_manager.nodes_cache.values()))]

        # get the node that holds the key's slot
        slot = await self._determine_slot(*args)
        node = self.nodes_manager.get_node_from_slot(
            slot, self.read_from_replicas and command in READ_COMMANDS
        )
        return [node]

    async def _determine_slot(self, *args) -> int:
        command = args[0]
        if self.command_flags.get(command) == SLOT_ID:
            # The command contains the slot ID
            return args[1]

        # Get the keys in the command

        # EVAL and EVALSHA are common enough that it's wasteful to go to the
        # redis server to parse the keys. Besides, there is a bug in redis<7.0
        # where `self._get_command_keys()` fails anyway. So, we special case
        # EVAL/EVALSHA.
        # - issue: https://github.com/redis/redis/issues/9493
        # - fix: https://github.com/redis/redis/pull/9733
        if command in ("EVAL", "EVALSHA"):
            # command syntax: EVAL "script body" num_keys ...
            if len(args) <= 2:
                raise RedisClusterException(f"Invalid args in command: {args}")
            num_actual_keys = args[2]
            eval_keys = args[3 : 3 + num_actual_keys]
            # if there are 0 keys, that means the script can be run on any node
            # so we can just return a random slot
            if not eval_keys:
                return random.randrange(0, REDIS_CLUSTER_HASH_SLOTS)
            keys = eval_keys
        else:
            keys = await self.commands_parser.get_keys(
                self.nodes_manager.default_node, *args
            )
            if not keys:
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

    def _is_node_flag(
        self, target_nodes: Union[List["ClusterNode"], "ClusterNode", str]
    ) -> bool:
        return isinstance(target_nodes, str) and target_nodes in self.node_flags

    def _parse_target_nodes(
        self, target_nodes: Union[List["ClusterNode"], "ClusterNode"]
    ) -> List["ClusterNode"]:
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

    async def execute_command(self, *args: Union[KeyT, EncodableT], **kwargs) -> Any:
        """
        Execute a raw command on the appropriate cluster node or target_nodes.

        It will retry the command as specified by :attr:`cluster_error_retry_attempts` &
        then raise an exception.

        :param args:
            | Raw command args
        :param kwargs:

            - target_nodes: :attr:`NODE_FLAGS` or :class:`~.ClusterNode`
              or List[:class:`~.ClusterNode`] or Dict[Any, :class:`~.ClusterNode`]
            - Rest of the kwargs are passed to the Redis connection

        :raises RedisClusterException: if target_nodes is not provided & the command
            can't be mapped to a slot
        """
        command = args[0]
        target_nodes_specified = target_nodes = exception = None
        retry_attempts = self.cluster_error_retry_attempts

        passed_targets = kwargs.pop("target_nodes", None)
        if passed_targets and not self._is_node_flag(passed_targets):
            target_nodes = self._parse_target_nodes(passed_targets)
            target_nodes_specified = True
            retry_attempts = 1

        for _ in range(0, retry_attempts):
            if self._initialize:
                await self.initialize()
            try:
                if not target_nodes_specified:
                    # Determine the nodes to execute the command on
                    target_nodes = await self._determine_nodes(
                        *args, node_flag=passed_targets
                    )
                    if not target_nodes:
                        raise RedisClusterException(
                            f"No targets were found to execute {args} command on"
                        )

                if len(target_nodes) == 1:
                    # Return the processed result
                    ret = await self._execute_command(target_nodes[0], *args, **kwargs)
                    if command in self.result_callbacks:
                        return self.result_callbacks[command](
                            command, {target_nodes[0].name: ret}, **kwargs
                        )
                    return ret
                else:
                    keys = [node.name for node in target_nodes]
                    values = await asyncio.gather(
                        *(
                            asyncio.ensure_future(
                                self._execute_command(node, *args, **kwargs)
                            )
                            for node in target_nodes
                        )
                    )
                    if command in self.result_callbacks:
                        return self.result_callbacks[command](
                            command, dict(zip(keys, values)), **kwargs
                        )
                    return dict(zip(keys, values))
            except BaseException as e:
                if type(e) in self.__class__.ERRORS_ALLOW_RETRY:
                    # The nodes and slots cache were reinitialized.
                    # Try again with the new cluster setup.
                    exception = e
                else:
                    # All other errors should be raised.
                    raise e

        # If it fails the configured number of times then raise exception back
        # to caller of this method
        raise exception

    async def _execute_command(
        self, target_node: "ClusterNode", *args: Union[KeyT, EncodableT], **kwargs
    ) -> Any:
        redirect_addr = asking = moved = None
        ttl = self.RedisClusterRequestTTL
        connection_error_retry_counter = 0

        while ttl > 0:
            ttl -= 1
            try:
                if asking:
                    target_node = self.get_node(node_name=redirect_addr)
                    await target_node.execute_command("ASKING")
                    asking = False
                elif moved:
                    # MOVED occurred and the slots cache was updated,
                    # refresh the target node
                    slot = await self._determine_slot(*args)
                    target_node = self.nodes_manager.get_node_from_slot(
                        slot, self.read_from_replicas and args[0] in READ_COMMANDS
                    )
                    moved = False

                return await target_node.execute_command(*args, **kwargs)
            except BusyLoadingError:
                raise
            except (ConnectionError, TimeoutError):
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
                    await self.close()
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
                self.reinitialize_counter += 1
                if (
                    self.reinitialize_steps
                    and self.reinitialize_counter % self.reinitialize_steps == 0
                ):
                    await self.close()
                    # Reset the counter
                    self.reinitialize_counter = 0
                else:
                    self.nodes_manager._moved_exception = e
                moved = True
            except TryAgainError:
                if ttl < self.RedisClusterRequestTTL / 2:
                    await asyncio.sleep(0.05)
            except AskError as e:
                redirect_addr = get_node_name(host=e.host, port=e.port)
                asking = True
            except ClusterDownError:
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command
                await asyncio.sleep(0.25)
                await self.close()
                raise

        raise ClusterError("TTL exhausted.")


class ClusterNode:
    """
    Create a new ClusterNode.

    Each ClusterNode manages multiple :class:`~redis.asyncio.connection.Connection`
    objects for the (host, port).
    """

    __slots__ = (
        "_connections",
        "_free",
        "connection_class",
        "connection_kwargs",
        "host",
        "max_connections",
        "name",
        "port",
        "response_callbacks",
        "server_type",
    )

    def __init__(
        self,
        host: str,
        port: int,
        server_type: Optional[str] = None,
        max_connections: int = 2 ** 31,
        connection_class: Type[Connection] = Connection,
        response_callbacks: Dict = None,
        **connection_kwargs,
    ) -> None:
        if host == "localhost":
            host = socket.gethostbyname(host)

        connection_kwargs["host"] = host
        connection_kwargs["port"] = port
        self.host = host
        self.port = port
        self.name = get_node_name(host, port)
        self.server_type = server_type

        self.max_connections = max_connections
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.response_callbacks = response_callbacks

        self._connections = []
        self._free = collections.deque(maxlen=self.max_connections)

    def __repr__(self) -> str:
        return (
            f"[host={self.host}, port={self.port}, "
            f"name={self.name}, server_type={self.server_type}]"
        )

    def __eq__(self, obj: "ClusterNode") -> bool:
        return isinstance(obj, ClusterNode) and obj.name == self.name

    _DEL_MESSAGE = "Unclosed ClusterNode object"

    def __del__(self, _warnings=warnings):
        for connection in self._connections:
            if connection.is_connected:
                _warnings.warn(
                    f"{self._DEL_MESSAGE} {self!r}", ResourceWarning, source=self
                )
                try:
                    context = {"client": self, "message": self._DEL_MESSAGE}
                    # TODO: Change to get_running_loop() when dropping support for py3.6
                    asyncio.get_event_loop().call_exception_handler(context)
                except RuntimeError:
                    ...
                break

    async def disconnect(self) -> None:
        ret = await asyncio.gather(
            *(
                asyncio.ensure_future(connection.disconnect())
                for connection in self._connections
            ),
            return_exceptions=True,
        )
        exc = next((res for res in ret if isinstance(res, Exception)), None)
        if exc:
            raise exc

    async def execute_command(self, *args, **kwargs) -> Any:
        # Acquire connection
        connection = None
        if self._free:
            for _ in range(len(self._free)):
                if self._free[0].is_connected:
                    connection = self._free.popleft()
                    break
                self._free.rotate(-1)
            else:
                connection = self._free.popleft()
        else:
            if len(self._connections) < self.max_connections:
                connection = self.connection_class(**self.connection_kwargs)
                self._connections.append(connection)
            else:
                raise ConnectionError("Too many connections")

        # Execute command
        command = connection.pack_command(*args)
        await connection.send_packed_command(command, False)
        try:
            if NEVER_DECODE in kwargs:
                response = await connection.read_response(disable_decoding=True)
            else:
                response = await connection.read_response()
        except ResponseError:
            if EMPTY_RESPONSE in kwargs:
                return kwargs[EMPTY_RESPONSE]
            raise
        finally:
            # Release connection
            self._free.append(connection)

        # Return response
        try:
            return self.response_callbacks[args[0]](response, **kwargs)
        except KeyError:
            return response


class NodesManager:
    __slots__ = (
        "_lock",
        "_moved_exception",
        "_require_full_coverage",
        "connection_kwargs",
        "default_node",
        "nodes_cache",
        "read_load_balancer",
        "slots_cache",
        "startup_nodes",
    )

    def __init__(
        self,
        startup_nodes: List["ClusterNode"],
        require_full_coverage: bool = False,
        **kwargs,
    ) -> None:
        self.nodes_cache = {}
        self.slots_cache = {}
        self.startup_nodes = {node.name: node for node in startup_nodes}
        self.default_node = None
        self._require_full_coverage = require_full_coverage
        self._moved_exception = None
        self.connection_kwargs = kwargs
        self.read_load_balancer = LoadBalancer()
        self._lock = threading.Lock()

    def get_node(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        node_name: Optional[str] = None,
    ) -> "ClusterNode":
        if host and port:
            # the user passed host and port
            if host == "localhost":
                host = socket.gethostbyname(host)
            return self.nodes_cache.get(get_node_name(host=host, port=port))
        elif node_name:
            return self.nodes_cache.get(node_name)
        else:
            raise DataError(
                "get_node requires one of the following: "
                "1. node name "
                "2. host and port"
            )

    def set_nodes(
        self,
        old: Dict[str, "ClusterNode"],
        new: Dict[str, "ClusterNode"],
        remove_old=False,
    ) -> None:
        tasks = []
        if remove_old:
            tasks = [
                asyncio.ensure_future(node.disconnect())
                for name, node in old.items()
                if name not in new
            ]
        for name, node in new.items():
            if name in old:
                if old[name] is node:
                    continue
                tasks.append(asyncio.ensure_future(old[name].disconnect()))
            old[name] = node

    def _update_moved_slots(self) -> None:
        e = self._moved_exception
        redirected_node = self.get_node(host=e.host, port=e.port)
        if redirected_node:
            # The node already exists
            if redirected_node.server_type != PRIMARY:
                # Update the node's server type
                redirected_node.server_type = PRIMARY
        else:
            # This is a new node, we will add it to the nodes cache
            redirected_node = ClusterNode(
                e.host, e.port, PRIMARY, **self.connection_kwargs
            )
            self.set_nodes(self.nodes_cache, {redirected_node.name: redirected_node})
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

    def get_node_from_slot(
        self, slot: int, read_from_replicas: bool = False
    ) -> "ClusterNode":
        if self._moved_exception:
            with self._lock:
                if self._moved_exception:
                    self._update_moved_slots()

        try:
            if read_from_replicas:
                # get the server index in a Round-Robin manner
                primary_name = self.slots_cache[slot][0].name
                node_idx = self.read_load_balancer.get_server_index(
                    primary_name, len(self.slots_cache[slot])
                )
                return self.slots_cache[slot][node_idx]
            return self.slots_cache[slot][0]
        except (IndexError, TypeError):
            raise SlotNotCoveredError(
                f'Slot "{slot}" not covered by the cluster. '
                f'"require_full_coverage={self._require_full_coverage}"'
            )

    def get_nodes_by_server_type(self, server_type: str) -> List["ClusterNode"]:
        return [
            node
            for node in self.nodes_cache.values()
            if node.server_type == server_type
        ]

    def check_slots_coverage(self, slots_cache: Dict[int, List["ClusterNode"]]) -> bool:
        # Validate if all slots are covered or if we should try next
        # startup node
        for i in range(0, REDIS_CLUSTER_HASH_SLOTS):
            if i not in slots_cache:
                return False
        return True

    async def initialize(self) -> None:
        self.read_load_balancer.reset()
        tmp_nodes_cache = {}
        tmp_slots = {}
        disagreements = []
        startup_nodes_reachable = False
        fully_covered = False
        for startup_node in self.startup_nodes.values():
            try:
                # Make sure cluster mode is enabled on this node
                if not (await startup_node.execute_command("INFO")).get(
                    "cluster_enabled"
                ):
                    raise RedisClusterException(
                        "Cluster mode is not enabled on this node"
                    )
                cluster_slots = str_if_bytes(
                    await startup_node.execute_command("CLUSTER SLOTS")
                )
                startup_nodes_reachable = True
            except (ConnectionError, TimeoutError):
                continue
            except ResponseError as e:
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
                and not cluster_slots[0][2][0]
                and len(self.startup_nodes) == 1
            ):
                cluster_slots[0][2][0] = startup_node.host

            for slot in cluster_slots:
                for i in range(2, len(slot)):
                    slot[i] = [str_if_bytes(val) for val in slot[i]]
                primary_node = slot[2]
                host = primary_node[0]
                if host == "":
                    host = startup_node.host
                port = int(primary_node[1])

                target_node = tmp_nodes_cache.get(get_node_name(host, port))
                if not target_node:
                    target_node = ClusterNode(
                        host, port, PRIMARY, **self.connection_kwargs
                    )
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
                            if not target_replica_node:
                                target_replica_node = ClusterNode(
                                    host, port, REPLICA, **self.connection_kwargs
                                )
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
        self.slots_cache = tmp_slots
        self.set_nodes(self.nodes_cache, tmp_nodes_cache, remove_old=True)
        # Populate the startup nodes with all discovered nodes
        self.set_nodes(self.startup_nodes, self.nodes_cache, remove_old=True)

        # Set the default node
        self.default_node = self.get_nodes_by_server_type(PRIMARY)[0]
        # If initialize was called after a MovedError, clear it
        self._moved_exception = None

    async def close(self, attr: str = "nodes_cache") -> None:
        self.default_node = None
        await asyncio.gather(
            *(
                asyncio.ensure_future(node.disconnect())
                for node in getattr(self, attr).values()
            )
        )
