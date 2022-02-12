import copy
import logging
import random
import socket
import sys
import threading
import time
from collections import OrderedDict

from redis.client import CaseInsensitiveDict, PubSub, Redis
from redis.commands import CommandsParser, RedisClusterCommands
from redis.connection import ConnectionPool, DefaultParser, Encoder, parse_url
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
    RedisError,
    ResponseError,
    SlotNotCoveredError,
    TimeoutError,
    TryAgainError,
)
from redis.utils import (
    dict_merge,
    list_keys_to_dict,
    merge_result,
    safe_str,
    str_if_bytes,
)

log = logging.getLogger(__name__)


def get_node_name(host, port):
    return f"{host}:{port}"


def get_connection(redis_node, *args, **options):
    return redis_node.connection or redis_node.connection_pool.get_connection(
        args[0], **options
    )


def parse_scan_result(command, res, **options):
    keys_list = []
    for primary_res in res.values():
        keys_list += primary_res[1]
    return 0, keys_list


def parse_pubsub_numsub(command, res, **options):
    numsub_d = OrderedDict()
    for numsub_tups in res.values():
        for channel, numsubbed in numsub_tups:
            try:
                numsub_d[channel] += numsubbed
            except KeyError:
                numsub_d[channel] = numsubbed

    ret_numsub = [(channel, numsub) for channel, numsub in numsub_d.items()]
    return ret_numsub


def parse_cluster_slots(resp, **options):
    current_host = options.get("current_host", "")

    def fix_server(*args):
        return str_if_bytes(args[0]) or current_host, args[1]

    slots = {}
    for slot in resp:
        start, end, primary = slot[:3]
        replicas = slot[3:]
        slots[start, end] = {
            "primary": fix_server(*primary),
            "replicas": [fix_server(*replica) for replica in replicas],
        }

    return slots


PRIMARY = "primary"
REPLICA = "replica"
SLOT_ID = "slot-id"

REDIS_ALLOWED_KEYS = (
    "charset",
    "connection_class",
    "connection_pool",
    "client_name",
    "db",
    "decode_responses",
    "encoding",
    "encoding_errors",
    "errors",
    "host",
    "max_connections",
    "nodes_flag",
    "redis_connect_func",
    "password",
    "port",
    "retry",
    "retry_on_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "socket_timeout",
    "ssl",
    "ssl_ca_certs",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_keyfile",
    "ssl_password",
    "unix_socket_path",
    "username",
)
KWARGS_DISABLED_KEYS = (
    "host",
    "port",
)

# Not complete, but covers the major ones
# https://redis.io/commands
READ_COMMANDS = frozenset(
    [
        "BITCOUNT",
        "BITPOS",
        "EXISTS",
        "GEODIST",
        "GEOHASH",
        "GEOPOS",
        "GEORADIUS",
        "GEORADIUSBYMEMBER",
        "GET",
        "GETBIT",
        "GETRANGE",
        "HEXISTS",
        "HGET",
        "HGETALL",
        "HKEYS",
        "HLEN",
        "HMGET",
        "HSTRLEN",
        "HVALS",
        "KEYS",
        "LINDEX",
        "LLEN",
        "LRANGE",
        "MGET",
        "PTTL",
        "RANDOMKEY",
        "SCARD",
        "SDIFF",
        "SINTER",
        "SISMEMBER",
        "SMEMBERS",
        "SRANDMEMBER",
        "STRLEN",
        "SUNION",
        "TTL",
        "ZCARD",
        "ZCOUNT",
        "ZRANGE",
        "ZSCORE",
    ]
)


def cleanup_kwargs(**kwargs):
    """
    Remove unsupported or disabled keys from kwargs
    """
    connection_kwargs = {
        k: v
        for k, v in kwargs.items()
        if k in REDIS_ALLOWED_KEYS and k not in KWARGS_DISABLED_KEYS
    }

    return connection_kwargs


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


class RedisCluster(RedisClusterCommands):
    RedisClusterRequestTTL = 16

    PRIMARIES = "primaries"
    REPLICAS = "replicas"
    ALL_NODES = "all"
    RANDOM = "random"
    DEFAULT_NODE = "default-node"

    NODE_FLAGS = {PRIMARIES, REPLICAS, ALL_NODES, RANDOM, DEFAULT_NODE}

    COMMAND_FLAGS = dict_merge(
        list_keys_to_dict(
            [
                "ACL CAT",
                "ACL DELUSER",
                "ACL GENPASS",
                "ACL GETUSER",
                "ACL HELP",
                "ACL LIST",
                "ACL LOG",
                "ACL LOAD",
                "ACL SAVE",
                "ACL SETUSER",
                "ACL USERS",
                "ACL WHOAMI",
                "CLIENT LIST",
                "CLIENT SETNAME",
                "CLIENT GETNAME",
                "CONFIG SET",
                "CONFIG REWRITE",
                "CONFIG RESETSTAT",
                "TIME",
                "PUBSUB CHANNELS",
                "PUBSUB NUMPAT",
                "PUBSUB NUMSUB",
                "PING",
                "INFO",
                "SHUTDOWN",
                "KEYS",
                "SCAN",
                "DBSIZE",
                "BGSAVE",
                "SLOWLOG GET",
                "SLOWLOG LEN",
                "SLOWLOG RESET",
                "WAIT",
                "SAVE",
                "MEMORY PURGE",
                "MEMORY MALLOC-STATS",
                "MEMORY STATS",
                "LASTSAVE",
                "CLIENT TRACKINGINFO",
                "CLIENT PAUSE",
                "CLIENT UNPAUSE",
                "CLIENT UNBLOCK",
                "CLIENT ID",
                "CLIENT REPLY",
                "CLIENT GETREDIR",
                "CLIENT INFO",
                "CLIENT KILL",
                "READONLY",
                "READWRITE",
                "CLUSTER INFO",
                "CLUSTER MEET",
                "CLUSTER NODES",
                "CLUSTER REPLICAS",
                "CLUSTER RESET",
                "CLUSTER SET-CONFIG-EPOCH",
                "CLUSTER SLOTS",
                "CLUSTER COUNT-FAILURE-REPORTS",
                "CLUSTER KEYSLOT",
                "COMMAND",
                "COMMAND COUNT",
                "COMMAND GETKEYS",
                "CONFIG GET",
                "DEBUG",
                "RANDOMKEY",
                "READONLY",
                "READWRITE",
                "TIME",
            ],
            DEFAULT_NODE,
        ),
        list_keys_to_dict(
            [
                "FLUSHALL",
                "FLUSHDB",
            ],
            PRIMARIES,
        ),
        list_keys_to_dict(
            [
                "CLUSTER COUNTKEYSINSLOT",
                "CLUSTER DELSLOTS",
                "CLUSTER GETKEYSINSLOT",
                "CLUSTER SETSLOT",
            ],
            SLOT_ID,
        ),
    )

    CLUSTER_COMMANDS_RESPONSE_CALLBACKS = {
        "CLUSTER ADDSLOTS": bool,
        "CLUSTER COUNT-FAILURE-REPORTS": int,
        "CLUSTER COUNTKEYSINSLOT": int,
        "CLUSTER DELSLOTS": bool,
        "CLUSTER FAILOVER": bool,
        "CLUSTER FORGET": bool,
        "CLUSTER GETKEYSINSLOT": list,
        "CLUSTER KEYSLOT": int,
        "CLUSTER MEET": bool,
        "CLUSTER REPLICATE": bool,
        "CLUSTER RESET": bool,
        "CLUSTER SAVECONFIG": bool,
        "CLUSTER SET-CONFIG-EPOCH": bool,
        "CLUSTER SETSLOT": bool,
        "CLUSTER SLOTS": parse_cluster_slots,
        "ASKING": bool,
        "READONLY": bool,
        "READWRITE": bool,
    }

    RESULT_CALLBACKS = dict_merge(
        list_keys_to_dict(
            [
                "PUBSUB NUMSUB",
            ],
            parse_pubsub_numsub,
        ),
        list_keys_to_dict(
            [
                "PUBSUB NUMPAT",
            ],
            lambda command, res: sum(list(res.values())),
        ),
        list_keys_to_dict(
            [
                "KEYS",
                "PUBSUB CHANNELS",
            ],
            merge_result,
        ),
        list_keys_to_dict(
            [
                "PING",
                "CONFIG SET",
                "CONFIG REWRITE",
                "CONFIG RESETSTAT",
                "CLIENT SETNAME",
                "BGSAVE",
                "SLOWLOG RESET",
                "SAVE",
                "MEMORY PURGE",
                "CLIENT PAUSE",
                "CLIENT UNPAUSE",
            ],
            lambda command, res: all(res.values()) if isinstance(res, dict) else res,
        ),
        list_keys_to_dict(
            [
                "DBSIZE",
                "WAIT",
            ],
            lambda command, res: sum(res.values()) if isinstance(res, dict) else res,
        ),
        list_keys_to_dict(
            [
                "CLIENT UNBLOCK",
            ],
            lambda command, res: 1 if sum(res.values()) > 0 else 0,
        ),
        list_keys_to_dict(
            [
                "SCAN",
            ],
            parse_scan_result,
        ),
    )

    ERRORS_ALLOW_RETRY = (
        ConnectionError,
        TimeoutError,
        ClusterDownError,
    )

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
        log.info("Creating a new instance of RedisCluster client")

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
        # If the user passed on_connect function we'll save it and run it
        # inside the RedisCluster.on_connect() function
        self.user_on_connect_func = kwargs.pop("redis_connect_func", None)
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
        self.commands_parser = CommandsParser(self)
        self._lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def disconnect_connection_pools(self):
        for node in self.get_nodes():
            if node.redis_connection:
                try:
                    node.redis_connection.connection_pool.disconnect()
                except OSError:
                    # Client was already disconnected. do nothing
                    pass

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

    def on_connect(self, connection):
        """
        Initialize the connection, authenticate and select a database and send
         READONLY if it is set during object initialization.
        """
        connection.set_parser(ClusterParser)
        connection.on_connect()

        if self.read_from_replicas:
            # Sending READONLY command to server to configure connection as
            # readonly. Since each cluster node may change its server type due
            # to a failover, we should establish a READONLY connection
            # regardless of the server type. If this is a primary connection,
            # READONLY would not affect executing write commands.
            connection.send_command("READONLY")
            if str_if_bytes(connection.read_response()) != "OK":
                raise ConnectionError("READONLY command failed")

        if self.user_on_connect_func is not None:
            self.user_on_connect_func(connection)

    def get_redis_connection(self, node):
        if not node.redis_connection:
            with self._lock:
                if not node.redis_connection:
                    self.nodes_manager.create_redis_connections([node])
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

    def monitor(self, target_node=None):
        """
        Returns a Monitor object for the specified target node.
        The default cluster node will be selected if no target node was
        specified.
        Monitor is useful for handling the MONITOR command to the redis server.
        next_command() method returns one command from monitor
        listen() method yields commands from monitor.
        """
        if target_node is None:
            target_node = self.get_default_node()
        if target_node.redis_connection is None:
            raise RedisClusterException(
                f"Cluster Node {target_node.name} has no redis_connection"
            )
        return target_node.redis_connection.monitor()

    def pubsub(self, node=None, host=None, port=None, **kwargs):
        """
        Allows passing a ClusterNode, or host&port, to get a pubsub instance
        connected to the specified node
        """
        return ClusterPubSub(self, node=node, host=host, port=port, **kwargs)

    def pipeline(self, transaction=None, shard_hint=None):
        """
        Cluster impl:
            Pipelines do not work in cluster mode the same way they
            do in normal mode. Create a clone of this object so
            that simulating pipelines will work correctly. Each
            command will be called directly when used and
            when calling execute() will only return the result stack.
        """
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")

        if transaction:
            raise RedisClusterException("transaction is deprecated in cluster mode")

        return ClusterPipeline(
            nodes_manager=self.nodes_manager,
            startup_nodes=self.nodes_manager.startup_nodes,
            result_callbacks=self.result_callbacks,
            cluster_response_callbacks=self.cluster_response_callbacks,
            cluster_error_retry_attempts=self.cluster_error_retry_attempts,
            read_from_replicas=self.read_from_replicas,
            reinitialize_steps=self.reinitialize_steps,
        )

    def _determine_nodes(self, *args, **kwargs):
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
        else:
            # get the node that holds the key's slot
            slot = self.determine_slot(*args)
            node = self.nodes_manager.get_node_from_slot(
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

    def _get_command_keys(self, *args):
        """
        Get the keys in the command. If the command has no keys in in, None is
        returned.
        """
        redis_conn = self.get_default_node().redis_connection
        return self.commands_parser.get_keys(redis_conn, *args)

    def determine_slot(self, *args):
        """
        Figure out what slot based on command and args
        """
        if self.command_flags.get(args[0]) == SLOT_ID:
            # The command contains the slot ID
            return args[1]

        # Get the keys in the command
        keys = self._get_command_keys(*args)
        if keys is None or len(keys) == 0:
            raise RedisClusterException(
                "No way to dispatch this command to Redis Cluster. "
                "Missing key.\nYou can execute the command by specifying "
                f"target nodes.\nCommand: {args}"
            )

        if len(keys) > 1:
            # multi-key command, we need to make sure all keys are mapped to
            # the same slot
            slots = {self.keyslot(key) for key in keys}
            if len(slots) != 1:
                raise RedisClusterException(
                    f"{args[0]} - all keys must map to the same key slot"
                )
            return slots.pop()
        else:
            # single key command
            return self.keyslot(keys[0])

    def reinitialize_caches(self):
        self.nodes_manager.initialize()

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

    def execute_command(self, *args, **kwargs):
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
                    target_nodes = self._determine_nodes(
                        *args, **kwargs, nodes_flag=passed_targets
                    )
                    if not target_nodes:
                        raise RedisClusterException(
                            f"No targets were found to execute {args} command on"
                        )
                for node in target_nodes:
                    res[node.name] = self._execute_command(node, *args, **kwargs)
                # Return the processed result
                return self._process_result(args[0], res, **kwargs)
            except BaseException as e:
                if type(e) in RedisCluster.ERRORS_ALLOW_RETRY:
                    # The nodes and slots cache were reinitialized.
                    # Try again with the new cluster setup.
                    exception = e
                else:
                    # All other errors should be raised.
                    raise e

        # If it fails the configured number of times then raise exception back
        # to caller of this method
        raise exception

    def _execute_command(self, target_node, *args, **kwargs):
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
                    slot = self.determine_slot(*args)
                    target_node = self.nodes_manager.get_node_from_slot(
                        slot, self.read_from_replicas and command in READ_COMMANDS
                    )
                    moved = False

                log.debug(
                    f"Executing command {command} on target node: "
                    f"{target_node.server_type} {target_node.name}"
                )
                redis_node = self.get_redis_connection(target_node)
                connection = get_connection(redis_node, *args, **kwargs)
                if asking:
                    connection.send_command("ASKING")
                    redis_node.parse_response(connection, "ASKING", **kwargs)
                    asking = False

                connection.send_command(*args)
                response = redis_node.parse_response(connection, command, **kwargs)
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
                    connection.disconnect()
                connection_error_retry_counter += 1

                # Give the node 0.25 seconds to get back up and retry again
                # with same node and configuration. After 5 attempts then try
                # to reinitialize the cluster and see if the nodes
                # configuration has changed or not
                if connection_error_retry_counter < 5:
                    time.sleep(0.25)
                else:
                    # Hard force of reinitialize of the node/slots setup
                    # and try again with the new setup
                    self.nodes_manager.initialize()
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
                    self.nodes_manager.initialize()
                    # Reset the counter
                    self.reinitialize_counter = 0
                else:
                    self.nodes_manager.update_moved_exception(e)
                moved = True
            except TryAgainError:
                log.exception("TryAgainError")

                if ttl < self.RedisClusterRequestTTL / 2:
                    time.sleep(0.05)
            except AskError as e:
                log.exception("AskError")

                redirect_addr = get_node_name(host=e.host, port=e.port)
                asking = True
            except ClusterDownError as e:
                log.exception("ClusterDownError")
                # ClusterDownError can occur during a failover and to get
                # self-healed, we will try to reinitialize the cluster layout
                # and retry executing the command
                time.sleep(0.25)
                self.nodes_manager.initialize()
                raise e
            except ResponseError as e:
                message = e.__str__()
                log.exception(f"ResponseError: {message}")
                raise e
            except BaseException as e:
                log.exception("BaseException")
                if connection:
                    connection.disconnect()
                raise e
            finally:
                if connection is not None:
                    redis_node.connection_pool.release(connection)

        raise ClusterError("TTL exhausted.")

    def close(self):
        try:
            with self._lock:
                if self.nodes_manager:
                    self.nodes_manager.close()
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
        if self.redis_connection is not None:
            self.redis_connection.close()


class LoadBalancer:
    """
    Round-Robin Load Balancing
    """

    def __init__(self, start_index=0):
        self.primary_to_idx = {}
        self.start_index = start_index

    def get_server_index(self, primary, list_size):
        server_index = self.primary_to_idx.setdefault(primary, self.start_index)
        # Update the index
        self.primary_to_idx[primary] = (server_index + 1) % list_size
        return server_index

    def reset(self):
        self.primary_to_idx.clear()


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
            lock = threading.Lock()
        self._lock = lock
        self.initialize()

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

    def get_node_from_slot(self, slot, read_from_replicas=False, server_type=None):
        """
        Gets a node that servers this hash slot
        """
        if self._moved_exception:
            with self._lock:
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

    def create_redis_connections(self, nodes):
        """
        This function will create a redis connection to all nodes in :nodes:
        """
        for node in nodes:
            if node.redis_connection is None:
                node.redis_connection = self.create_redis_node(
                    host=node.host,
                    port=node.port,
                    **self.connection_kwargs,
                )

    def create_redis_node(self, host, port, **kwargs):
        if self.from_url:
            # Create a redis node with a costumed connection pool
            kwargs.update({"host": host})
            kwargs.update({"port": port})
            r = Redis(connection_pool=ConnectionPool(**kwargs))
        else:
            r = Redis(host=host, port=port, **kwargs)
        return r

    def initialize(self):
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
                    copy_kwargs = copy.deepcopy(kwargs)
                    copy_kwargs.update({"decode_responses": True, "encoding": "utf-8"})
                    r = self.create_redis_node(
                        startup_node.host, startup_node.port, **copy_kwargs
                    )
                    self.startup_nodes[startup_node.name].redis_connection = r
                # Make sure cluster mode is enabled on this node
                if bool(r.info().get("cluster_enabled")) is False:
                    raise RedisClusterException(
                        "Cluster mode is not enabled on this node"
                    )
                cluster_slots = str_if_bytes(r.execute_command("CLUSTER SLOTS"))
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
        self.create_redis_connections(list(tmp_nodes_cache.values()))

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

    def close(self):
        self.default_node = None
        for node in self.nodes_cache.values():
            if node.redis_connection:
                node.redis_connection.close()

    def reset(self):
        try:
            self.read_load_balancer.reset()
        except TypeError:
            # The read_load_balancer is None, do nothing
            pass


class ClusterPubSub(PubSub):
    """
    Wrapper for PubSub class.

    IMPORTANT: before using ClusterPubSub, read about the known limitations
    with pubsub in Cluster mode and learn how to workaround them:
    https://redis-py-cluster.readthedocs.io/en/stable/pubsub.html
    """

    def __init__(self, redis_cluster, node=None, host=None, port=None, **kwargs):
        """
        When a pubsub instance is created without specifying a node, a single
        node will be transparently chosen for the pubsub connection on the
        first command execution. The node will be determined by:
         1. Hashing the channel name in the request to find its keyslot
         2. Selecting a node that handles the keyslot: If read_from_replicas is
            set to true, a replica can be selected.

        :type redis_cluster: RedisCluster
        :type node: ClusterNode
        :type host: str
        :type port: int
        """
        log.info("Creating new instance of ClusterPubSub")
        self.node = None
        self.set_pubsub_node(redis_cluster, node, host, port)
        connection_pool = (
            None
            if self.node is None
            else redis_cluster.get_redis_connection(self.node).connection_pool
        )
        self.cluster = redis_cluster
        super().__init__(
            **kwargs, connection_pool=connection_pool, encoder=redis_cluster.encoder
        )

    def set_pubsub_node(self, cluster, node=None, host=None, port=None):
        """
        The pubsub node will be set according to the passed node, host and port
        When none of the node, host, or port are specified - the node is set
        to None and will be determined by the keyslot of the channel in the
        first command to be executed.
        RedisClusterException will be thrown if the passed node does not exist
        in the cluster.
        If host is passed without port, or vice versa, a DataError will be
        thrown.
        :type cluster: RedisCluster
        :type node: ClusterNode
        :type host: str
        :type port: int
        """
        if node is not None:
            # node is passed by the user
            self._raise_on_invalid_node(cluster, node, node.host, node.port)
            pubsub_node = node
        elif host is not None and port is not None:
            # host and port passed by the user
            node = cluster.get_node(host=host, port=port)
            self._raise_on_invalid_node(cluster, node, host, port)
            pubsub_node = node
        elif any([host, port]) is True:
            # only 'host' or 'port' passed
            raise DataError("Passing a host requires passing a port, " "and vice versa")
        else:
            # nothing passed by the user. set node to None
            pubsub_node = None

        self.node = pubsub_node

    def get_pubsub_node(self):
        """
        Get the node that is being used as the pubsub connection
        """
        return self.node

    def _raise_on_invalid_node(self, redis_cluster, node, host, port):
        """
        Raise a RedisClusterException if the node is None or doesn't exist in
        the cluster.
        """
        if node is None or redis_cluster.get_node(node_name=node.name) is None:
            raise RedisClusterException(
                f"Node {host}:{port} doesn't exist in the cluster"
            )

    def execute_command(self, *args, **kwargs):
        """
        Execute a publish/subscribe command.

        Taken code from redis-py and tweak to make it work within a cluster.
        """
        # NOTE: don't parse the response in this function -- it could pull a
        # legitimate message off the stack if the connection is already
        # subscribed to one or more channels

        if self.connection is None:
            if self.connection_pool is None:
                if len(args) > 1:
                    # Hash the first channel and get one of the nodes holding
                    # this slot
                    channel = args[1]
                    slot = self.cluster.keyslot(channel)
                    node = self.cluster.nodes_manager.get_node_from_slot(
                        slot, self.cluster.read_from_replicas
                    )
                else:
                    # Get a random node
                    node = self.cluster.get_random_node()
                self.node = node
                redis_connection = self.cluster.get_redis_connection(node)
                self.connection_pool = redis_connection.connection_pool
            self.connection = self.connection_pool.get_connection(
                "pubsub", self.shard_hint
            )
            # register a callback that re-subscribes to any channels we
            # were listening to when we were disconnected
            self.connection.register_connect_callback(self.on_connect)
        connection = self.connection
        self._execute(connection, connection.send_command, *args)

    def get_redis_connection(self):
        """
        Get the Redis connection of the pubsub connected node.
        """
        if self.node is not None:
            return self.node.redis_connection


class ClusterPipeline(RedisCluster):
    """
    Support for Redis pipeline
    in cluster mode
    """

    ERRORS_ALLOW_RETRY = (
        ConnectionError,
        TimeoutError,
        MovedError,
        AskError,
        TryAgainError,
    )

    def __init__(
        self,
        nodes_manager,
        result_callbacks=None,
        cluster_response_callbacks=None,
        startup_nodes=None,
        read_from_replicas=False,
        cluster_error_retry_attempts=5,
        reinitialize_steps=10,
        **kwargs,
    ):
        """ """
        log.info("Creating new instance of ClusterPipeline")
        self.command_stack = []
        self.nodes_manager = nodes_manager
        self.refresh_table_asap = False
        self.result_callbacks = (
            result_callbacks or self.__class__.RESULT_CALLBACKS.copy()
        )
        self.startup_nodes = startup_nodes if startup_nodes else []
        self.read_from_replicas = read_from_replicas
        self.command_flags = self.__class__.COMMAND_FLAGS.copy()
        self.cluster_response_callbacks = cluster_response_callbacks
        self.cluster_error_retry_attempts = cluster_error_retry_attempts
        self.reinitialize_counter = 0
        self.reinitialize_steps = reinitialize_steps
        self.encoder = Encoder(
            kwargs.get("encoding", "utf-8"),
            kwargs.get("encoding_errors", "strict"),
            kwargs.get("decode_responses", False),
        )

        # The commands parser refers to the parent
        # so that we don't push the COMMAND command
        # onto the stack
        self.commands_parser = CommandsParser(super())

    def __repr__(self):
        """ """
        return f"{type(self).__name__}"

    def __enter__(self):
        """ """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ """
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        """ """
        return len(self.command_stack)

    def __nonzero__(self):
        "Pipeline instances should  always evaluate to True on Python 2.7"
        return True

    def __bool__(self):
        "Pipeline instances should  always evaluate to True on Python 3+"
        return True

    def execute_command(self, *args, **kwargs):
        """
        Wrapper function for pipeline_execute_command
        """
        return self.pipeline_execute_command(*args, **kwargs)

    def pipeline_execute_command(self, *args, **options):
        """
        Appends the executed command to the pipeline's command stack
        """
        self.command_stack.append(
            PipelineCommand(args, options, len(self.command_stack))
        )
        return self

    def raise_first_error(self, stack):
        """
        Raise the first exception on the stack
        """
        for c in stack:
            r = c.result
            if isinstance(r, Exception):
                self.annotate_exception(r, c.position + 1, c.args)
                raise r

    def annotate_exception(self, exception, number, command):
        """
        Provides extra context to the exception prior to it being handled
        """
        cmd = " ".join(map(safe_str, command))
        msg = (
            f"Command # {number} ({cmd}) of pipeline "
            f"caused error: {exception.args[0]}"
        )
        exception.args = (msg,) + exception.args[1:]

    def execute(self, raise_on_error=True):
        """
        Execute all the commands in the current pipeline
        """
        stack = self.command_stack
        try:
            return self.send_cluster_commands(stack, raise_on_error)
        finally:
            self.reset()

    def reset(self):
        """
        Reset back to empty pipeline.
        """
        self.command_stack = []

        self.scripts = set()

        # TODO: Implement
        # make sure to reset the connection state in the event that we were
        # watching something
        # if self.watching and self.connection:
        #     try:
        #         # call this manually since our unwatch or
        #         # immediate_execute_command methods can call reset()
        #         self.connection.send_command('UNWATCH')
        #         self.connection.read_response()
        #     except ConnectionError:
        #         # disconnect will also remove any previous WATCHes
        #         self.connection.disconnect()

        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False

        # TODO: Implement
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        # if self.connection:
        #     self.connection_pool.release(self.connection)
        #     self.connection = None

    def send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        """
        Wrapper for CLUSTERDOWN error handling.

        If the cluster reports it is down it is assumed that:
         - connection_pool was disconnected
         - connection_pool was reseted
         - refereh_table_asap set to True

        It will try the number of times specified by
        the config option "self.cluster_error_retry_attempts"
        which defaults to 3 unless manually configured.

        If it reaches the number of times, the command will
        raises ClusterDownException.
        """
        if not stack:
            return []

        for _ in range(0, self.cluster_error_retry_attempts):
            try:
                return self._send_cluster_commands(
                    stack,
                    raise_on_error=raise_on_error,
                    allow_redirections=allow_redirections,
                )
            except ClusterDownError:
                # Try again with the new cluster setup. All other errors
                # should be raised.
                pass

        # If it fails the configured number of times then raise
        # exception back to caller of this method
        raise ClusterDownError("CLUSTERDOWN error. Unable to rebuild the cluster")

    def _send_cluster_commands(
        self, stack, raise_on_error=True, allow_redirections=True
    ):
        """
        Send a bunch of cluster commands to the redis cluster.

        `allow_redirections` If the pipeline should follow
        `ASK` & `MOVED` responses automatically. If set
        to false it will raise RedisClusterException.
        """
        # the first time sending the commands we send all of
        # the commands that were queued up.
        # if we have to run through it again, we only retry
        # the commands that failed.
        attempt = sorted(stack, key=lambda x: x.position)

        # build a list of node objects based on node names we need to
        nodes = {}

        # as we move through each command that still needs to be processed,
        # we figure out the slot number that command maps to, then from
        # the slot determine the node.
        for c in attempt:
            # refer to our internal node -> slot table that
            # tells us where a given
            # command should route to.
            slot = self.determine_slot(*c.args)
            node = self.nodes_manager.get_node_from_slot(
                slot, self.read_from_replicas and c.args[0] in READ_COMMANDS
            )

            # now that we know the name of the node
            # ( it's just a string in the form of host:port )
            # we can build a list of commands for each node.
            node_name = node.name
            if node_name not in nodes:
                redis_node = self.get_redis_connection(node)
                connection = get_connection(redis_node, c.args)
                nodes[node_name] = NodeCommands(
                    redis_node.parse_response, redis_node.connection_pool, connection
                )

            nodes[node_name].append(c)

        # send the commands in sequence.
        # we  write to all the open sockets for each node first,
        # before reading anything
        # this allows us to flush all the requests out across the
        # network essentially in parallel
        # so that we can read them all in parallel as they come back.
        # we dont' multiplex on the sockets as they come available,
        # but that shouldn't make too much difference.
        node_commands = nodes.values()
        for n in node_commands:
            n.write()

        for n in node_commands:
            n.read()

        # release all of the redis connections we allocated earlier
        # back into the connection pool.
        # we used to do this step as part of a try/finally block,
        # but it is really dangerous to
        # release connections back into the pool if for some
        # reason the socket has data still left in it
        # from a previous operation. The write and
        # read operations already have try/catch around them for
        # all known types of errors including connection
        # and socket level errors.
        # So if we hit an exception, something really bad
        # happened and putting any oF
        # these connections back into the pool is a very bad idea.
        # the socket might have unread buffer still sitting in it,
        # and then the next time we read from it we pass the
        # buffered result back from a previous command and
        # every single request after to that connection will always get
        # a mismatched result.
        for n in nodes.values():
            n.connection_pool.release(n.connection)

        # if the response isn't an exception it is a
        # valid response from the node
        # we're all done with that command, YAY!
        # if we have more commands to attempt, we've run into problems.
        # collect all the commands we are allowed to retry.
        # (MOVED, ASK, or connection errors or timeout errors)
        attempt = sorted(
            (
                c
                for c in attempt
                if isinstance(c.result, ClusterPipeline.ERRORS_ALLOW_RETRY)
            ),
            key=lambda x: x.position,
        )
        if attempt and allow_redirections:
            # RETRY MAGIC HAPPENS HERE!
            # send these remaing comamnds one at a time using `execute_command`
            # in the main client. This keeps our retry logic
            # in one place mostly,
            # and allows us to be more confident in correctness of behavior.
            # at this point any speed gains from pipelining have been lost
            # anyway, so we might as well make the best
            # attempt to get the correct behavior.
            #
            # The client command will handle retries for each
            # individual command sequentially as we pass each
            # one into `execute_command`. Any exceptions
            # that bubble out should only appear once all
            # retries have been exhausted.
            #
            # If a lot of commands have failed, we'll be setting the
            # flag to rebuild the slots table from scratch.
            # So MOVED errors should correct themselves fairly quickly.
            log.exception(
                f"An exception occurred during pipeline execution. "
                f"args: {attempt[-1].args}, "
                f"error: {type(attempt[-1].result).__name__} "
                f"{str(attempt[-1].result)}"
            )
            self.reinitialize_counter += 1
            if self._should_reinitialized():
                self.nodes_manager.initialize()
            for c in attempt:
                try:
                    # send each command individually like we
                    # do in the main client.
                    c.result = super().execute_command(*c.args, **c.options)
                except RedisError as e:
                    c.result = e

        # turn the response back into a simple flat array that corresponds
        # to the sequence of commands issued in the stack in pipeline.execute()
        response = [c.result for c in sorted(stack, key=lambda x: x.position)]

        if raise_on_error:
            self.raise_first_error(stack)

        return response

    def _fail_on_redirect(self, allow_redirections):
        """ """
        if not allow_redirections:
            raise RedisClusterException(
                "ASK & MOVED redirection not allowed in this pipeline"
            )

    def eval(self):
        """ """
        raise RedisClusterException("method eval() is not implemented")

    def multi(self):
        """ """
        raise RedisClusterException("method multi() is not implemented")

    def immediate_execute_command(self, *args, **options):
        """ """
        raise RedisClusterException(
            "method immediate_execute_command() is not implemented"
        )

    def _execute_transaction(self, *args, **kwargs):
        """ """
        raise RedisClusterException("method _execute_transaction() is not implemented")

    def load_scripts(self):
        """ """
        raise RedisClusterException("method load_scripts() is not implemented")

    def watch(self, *names):
        """ """
        raise RedisClusterException("method watch() is not implemented")

    def unwatch(self):
        """ """
        raise RedisClusterException("method unwatch() is not implemented")

    def script_load_for_pipeline(self, *args, **kwargs):
        """ """
        raise RedisClusterException(
            "method script_load_for_pipeline() is not implemented"
        )

    def delete(self, *names):
        """
        "Delete a key specified by ``names``"
        """
        if len(names) != 1:
            raise RedisClusterException(
                "deleting multiple keys is not " "implemented in pipeline command"
            )

        return self.execute_command("DEL", names[0])


def block_pipeline_command(func):
    """
    Prints error because some pipelined commands should
    be blocked when running in cluster-mode
    """

    def inner(*args, **kwargs):
        raise RedisClusterException(
            f"ERROR: Calling pipelined function {func.__name__} is blocked "
            f"when running redis in cluster mode..."
        )

    return inner


# Blocked pipeline commands
ClusterPipeline.bitop = block_pipeline_command(RedisCluster.bitop)
ClusterPipeline.brpoplpush = block_pipeline_command(RedisCluster.brpoplpush)
ClusterPipeline.client_getname = block_pipeline_command(RedisCluster.client_getname)
ClusterPipeline.client_list = block_pipeline_command(RedisCluster.client_list)
ClusterPipeline.client_setname = block_pipeline_command(RedisCluster.client_setname)
ClusterPipeline.config_set = block_pipeline_command(RedisCluster.config_set)
ClusterPipeline.dbsize = block_pipeline_command(RedisCluster.dbsize)
ClusterPipeline.flushall = block_pipeline_command(RedisCluster.flushall)
ClusterPipeline.flushdb = block_pipeline_command(RedisCluster.flushdb)
ClusterPipeline.keys = block_pipeline_command(RedisCluster.keys)
ClusterPipeline.mget = block_pipeline_command(RedisCluster.mget)
ClusterPipeline.move = block_pipeline_command(RedisCluster.move)
ClusterPipeline.mset = block_pipeline_command(RedisCluster.mset)
ClusterPipeline.msetnx = block_pipeline_command(RedisCluster.msetnx)
ClusterPipeline.pfmerge = block_pipeline_command(RedisCluster.pfmerge)
ClusterPipeline.pfcount = block_pipeline_command(RedisCluster.pfcount)
ClusterPipeline.ping = block_pipeline_command(RedisCluster.ping)
ClusterPipeline.publish = block_pipeline_command(RedisCluster.publish)
ClusterPipeline.randomkey = block_pipeline_command(RedisCluster.randomkey)
ClusterPipeline.rename = block_pipeline_command(RedisCluster.rename)
ClusterPipeline.renamenx = block_pipeline_command(RedisCluster.renamenx)
ClusterPipeline.rpoplpush = block_pipeline_command(RedisCluster.rpoplpush)
ClusterPipeline.scan = block_pipeline_command(RedisCluster.scan)
ClusterPipeline.sdiff = block_pipeline_command(RedisCluster.sdiff)
ClusterPipeline.sdiffstore = block_pipeline_command(RedisCluster.sdiffstore)
ClusterPipeline.sinter = block_pipeline_command(RedisCluster.sinter)
ClusterPipeline.sinterstore = block_pipeline_command(RedisCluster.sinterstore)
ClusterPipeline.smove = block_pipeline_command(RedisCluster.smove)
ClusterPipeline.sort = block_pipeline_command(RedisCluster.sort)
ClusterPipeline.sunion = block_pipeline_command(RedisCluster.sunion)
ClusterPipeline.sunionstore = block_pipeline_command(RedisCluster.sunionstore)
ClusterPipeline.readwrite = block_pipeline_command(RedisCluster.readwrite)
ClusterPipeline.readonly = block_pipeline_command(RedisCluster.readonly)


class PipelineCommand:
    """ """

    def __init__(self, args, options=None, position=None):
        self.args = args
        if options is None:
            options = {}
        self.options = options
        self.position = position
        self.result = None
        self.node = None
        self.asking = False


class NodeCommands:
    """ """

    def __init__(self, parse_response, connection_pool, connection):
        """ """
        self.parse_response = parse_response
        self.connection_pool = connection_pool
        self.connection = connection
        self.commands = []

    def append(self, c):
        """ """
        self.commands.append(c)

    def write(self):
        """
        Code borrowed from Redis so it can be fixed
        """
        connection = self.connection
        commands = self.commands

        # We are going to clobber the commands with the write, so go ahead
        # and ensure that nothing is sitting there from a previous run.
        for c in commands:
            c.result = None

        # build up all commands into a single request to increase network perf
        # send all the commands and catch connection and timeout errors.
        try:
            connection.send_packed_command(
                connection.pack_commands([c.args for c in commands])
            )
        except (ConnectionError, TimeoutError) as e:
            for c in commands:
                c.result = e

    def read(self):
        """ """
        connection = self.connection
        for c in self.commands:

            # if there is a result on this command,
            # it means we ran into an exception
            # like a connection error. Trying to parse
            # a response on a connection that
            # is no longer open will result in a
            # connection error raised by redis-py.
            # but redis-py doesn't check in parse_response
            # that the sock object is
            # still set and if you try to
            # read from a closed connection, it will
            # result in an AttributeError because
            # it will do a readline() call on None.
            # This can have all kinds of nasty side-effects.
            # Treating this case as a connection error
            # is fine because it will dump
            # the connection object back into the
            # pool and on the next write, it will
            # explicitly open the connection and all will be well.
            if c.result is None:
                try:
                    c.result = self.parse_response(connection, c.args[0], **c.options)
                except (ConnectionError, TimeoutError) as e:
                    for c in self.commands:
                        c.result = e
                    return
                except RedisError:
                    c.result = sys.exc_info()[1]
