"""cluster support ported from https://github.com/Grokzen/redis-py-cluster
"""
import time
import logging

from redis.crc import crc16
from redis._compat import iteritems, nativestr
from redis.client import StrictRedis, dict_merge
from redis.connection import Connection, ConnectionPool, DefaultParser
from redis.exceptions import (
    ConnectionError, ClusterPartitionError, ClusterError,
    TimeoutError, ResponseError, BusyLoadingError, ClusterCrossSlotError,
    ClusterSlotNotServedError, ClusterDownError,
)

# TODO: loose redis interface
# TODO: collect results
# TODO: advanced balancer
# TODO: pipeline
# TODO: script
# TODO: pubsub
LOGGER = logging #.getLogger(__name__)


class ClusterBalancer(object):
    def get_node_for_key(self, key_name, readonly):
        raise NotImplementedError()

    def get_node_for_slot(self, slot_id, readonly):
        raise NotImplementedError()

    def get_random_node(self, readonly):
        raise NotImplementedError()


class RoundRobinClusterNodeBalancer(ClusterBalancer):
    RR_COUNTER = 1

    def __init__(self, manager):
        self.manager = manager

    def get_node_for_slot(self, slot_id, readonly):
        if not readonly:
            return self.manager.get_master_node(slot_id)
        else:
            counter = self.__class__.RR_COUNTER = (self.__class__.RR_COUNTER + 1) % Cluster.KEY_SLOTS
            nodes = self.manager.get_slave_nodes(slot_id, slave_only=False)
            return list(nodes)[counter % len(nodes)]

    def get_node_for_key(self, key_name, readonly):
        return self.get_node_for_slot(Cluster.keyslot(key_name), readonly=readonly)

    def get_random_node(self, readonly):
        counter = self.__class__.RR_COUNTER = (self.__class__.RR_COUNTER + 1) % Cluster.KEY_SLOTS

        if readonly:
            nodes = self.manager.nodes
        else:
            nodes = self.manager.master_nodes

        return list(nodes)[counter % len(nodes)]


class ClusterParser(DefaultParser):
    class AskError(ResponseError):
        def __init__(self, resp):
            print resp

    class MovedError(ResponseError):
        def __init__(self, resp):
            """redis only redirect to master node"""
            slot_id, new_node = resp.split(' ')
            host, port = new_node.rsplit(':', 1)
            self.slot_id = int(slot_id)
            self.node = self.host, self.port = host, int(port)

    EXCEPTION_CLASSES = dict_merge(
        DefaultParser.EXCEPTION_CLASSES, {
            'ASK': AskError,
            'MOVED': MovedError,
            'CLUSTERDOWN': ClusterDownError,
            'CROSSSLOT': ClusterCrossSlotError,
        })


class ClusterConnection(Connection):
    description_format = "ClusterConnection<host=%(host)s,port=%(port)s>"

    def __init__(self, *args, **kwargs):
        self.use_readonly = kwargs.pop('use_readonly', False)
        kwargs['parser_class'] = ClusterParser
        super(ClusterConnection, self).__init__(*args, **kwargs)

    def on_connect(self):
        """Initialize the connection, set readonly is required"""
        super(ClusterConnection, self).on_connect()
        if self.use_readonly:
            self.send_command('READONLY')
            if nativestr(self.read_response()) != 'OK':
                raise ResponseError('Cannot set READONLY flag')


class Cluster(object):
    """keep knowledge of cluster"""
    KEY_SLOTS = 16384

    def __init__(self, startup_nodes=None, allow_partition=False, **cluster_kwargs):
        """allow_partition: raise Exception when partition appears or not."""

        if not startup_nodes:
            raise ValueError('No startup nodes provided')

        self.cluster_kwargs = dict([
            (k, v) for k, v in iteritems(cluster_kwargs)
            if k.startswith('socket_')
        ])

        self.cluster_kwargs['decode_responses'] = True
        self.cluster_kwargs['password'] = cluster_kwargs.get('password')
        self.allow_partition = allow_partition

        self.master_nodes = set()
        self.nodes = set(startup_nodes)
        self.slots = {}
        self.pubsub_node = None
        self.state = None

    @classmethod
    def keyslot(cls, key):
        """Calculate keyslot for a given key.

        This also works for binary keys that is used in python 3.
        """
        k = unicode(key)
        start = k.find('{')
        if start > -1:
            end = k.find('}', start + 1)
            if end > -1 and end != start + 1:
                k = k[start + 1:end]
        return crc16(k) % cls.KEY_SLOTS

    def discover_cluster(self, force=False):
        if len(self.slots) == self.KEY_SLOTS and not force:
            return

        slots_node = {}
        startup_nodes, self.nodes, self.master_nodes = self.nodes, set(), set()
        for node in startup_nodes:
            host, port = node
            node_conn = StrictRedis(host, port, **self.cluster_kwargs)
            try:
                node_conn.ping()
            except ConnectionError:
                continue

            if self.state is None:
                # TODO: upgrade to use CLUSTER NODES
                self.state = node_conn.cluster_info()['cluster_state']

            for (start, end), slot in node_conn.cluster_slots().items():
                for slot_id in range(start, end + 1):
                    if not self.allow_partition and slot_id in self.slots:
                        old_master = self.slots[slot_id]['master']
                        if old_master != slot['master']:
                            raise ClusterPartitionError(
                                'Cluster partition appears: slot #%s, node: [%s]:[%s] and [%s]:[%s]' % (
                                    slot_id, slots_node[slot_id], old_master, node, slot['master']))

                    self.master_nodes.add(slot['master'])
                    self.nodes.update([slot['master']] + slot['slaves'])
                    self.slots[slot_id] = {
                        'master': slot['master'],
                        'slaves': set(slot['slaves']),
                    }
                    slots_node[slot_id] = node

            if len(self.slots) == self.KEY_SLOTS:
                self.pubsub_node = self.determine_pubsub_node()
                break

        if not self.nodes:
            self.nodes = startup_nodes
            raise ClusterDownError('no startup node can be reached.')

    def get_master_node(self, slot_id):
        self.discover_cluster()
        try:
            node = self.slots[slot_id]
        except IndexError:
            raise ClusterSlotNotServedError(slot_id)
        else:
            return node['master']

    def get_slave_nodes(self, slot_id, slave_only=True):
        self.discover_cluster()
        try:
            node = self.slots[slot_id]
        except IndexError:
            raise ClusterSlotNotServedError(slot_id)
        else:
            if slave_only:
                return list(node['slaves'])
            else:
                return list(node['slaves']) + [node['master']]

    def determine_pubsub_node(self):
        """
        Determine what node object should be used for pubsub commands.

        All clients in the cluster will talk to the same pubsub node to ensure
        all code stay compatible. See pubsub doc for more details why.

        Allways use the server with highest port number
        """
        highest = -1
        node = None, None
        for host, port in self.nodes:
            if port > highest:
                highest = port
                node = host, port

        return node

    def slot_moved(self, slot_id, node):
        """signal from response"""
        slot = self.slots.setdefault(slot_id, {'master': None, 'slaves': set()})
        slot['master'] = node
        self.nodes.add(node)
        self.master_nodes.add(node)


class ClusterConnectionPool(object):
    """connection pool for redis cluster
    collection of pools
    """
    DEFAULT_TIMEOUT = .05
    DEFAULT_MAX_CONN = 32

    def __init__(self, manager, connection_class=ClusterConnection,
                 max_connections=None, **connection_kwargs):

        max_connections = max_connections or self.DEFAULT_MAX_CONN
        if not isinstance(max_connections, (int, long)) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.manager = manager
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.connection_kwargs.setdefault('socket_timeout', self.DEFAULT_TIMEOUT)
        self.max_connections = max_connections

        # (host, port) -> pool
        self.pools = {}
        self.reset()

    def reset(self, force=False):
        self.manager.discover_cluster(force=force)
        self.pools = dict([
            (node, self.make_connection_pool(node))
            for node in self.manager.nodes
        ])

    def get_connection(self, node):
        """Get a connection from the pool"""
        return self.pools[node].get_connection(None)

    def make_connection_pool(self, node):
        """Create a new connection"""
        host, port = node
        use_readonly = node not in self.manager.master_nodes
        return ConnectionPool(host=host, port=port,
                              connection_class=self.connection_class,
                              max_connections=self.max_connections,
                              use_readonly=use_readonly,
                              **self.connection_kwargs)

    def release(self, connection):
        """Releases the connection back to the pool"""
        self.pools[connection.host, connection.port].release(connection)

    def disconnect(self):
        """Disconnects all connections in the pool"""
        for pool in self.pools.values():
            pool.disconnect()


class StrictClusterRedis(StrictRedis):
    """
    If a command is implemented over the one in StrictRedis then it requires some changes compared to
    the regular implementation of the method.
    """
    COMMAND_TTL = 16
    COMMAND_FLAGS = dict_merge(
        dict.fromkeys([
            'RANDOMKEY', 'CLUSTER KEYSLOT', 'ECHO',
        ], 'random'),
        dict.fromkeys([
            'CLUSTER COUNTKEYSINSLOT',
        ], 'slot-id'),
        dict.fromkeys([
            'DBSIZE',
        ], 'collect'),
        dict.fromkeys([
            # impossible in cluster mode
            'SELECT', 'MOVE', 'SLAVEOF',

            # sentinels
            'SENTINEL GET-MASTER-ADDR-BY-NAME', 'SENTINEL MASTER', 'SENTINEL MASTERS',
            'SENTINEL MONITOR', 'SENTINEL REMOVE', 'SENTINEL SENTINELS', 'SENTINEL SET',
            'SENTINEL SLAVES',

            # admin commands
            'BGREWRITEAOF', 'BGSAVE', 'SAVE', 'INFO', 'KEYS', 'LASTSAVE',
            'CONFIG GET', 'CONFIG SET', 'CONFIG RESETSTAT', 'CONFIG REWRITE',
            'CLIENT KILL', 'CLIENT LIST', 'CLIENT GETNAME', 'CLIENT SETNAME',
            'SLOWLOG GET', 'SLOWLOG RESET', 'SLOWLOG LEN', 'SHUTDOWN',

            # lua script
            'EVALSHA', 'SCRIPT EXISTS', 'SCRIPT KILL', 'SCRIPT LOAD', 'SCRIPT FLUSH',

            # unknown behaviors in cluster
            'DBSIZE', 'PING', 'MONITOR', 'TIME', 'PFSELFTEST', 'READONLY', 'READWRITE',
            'OBJECT REFCOUNT', 'OBJECT ENCODING', 'OBJECT IDLETIME',

            # test/doc command
            'PFSELFTEST', 'COMMAND', 'COMMAND COUNT', 'COMMAND GETKEYS', 'COMMAND INFO',

            # pipeline related
            'DISCARD', 'MULTI', 'WATCH', 'UNWATCH',

            # latency monitor
            'LATENCY LATEST', 'LATENCY HISTORY', 'LATENCY RESET', 'LATENCY GRAPH', 'LATENCY DOCTOR',

            # unknown commands
            'REPLCONF', 'SYNC', 'PSYNC',

            # all_master for loose
            'FLUSHALL', 'FLUSHDB', 'SCAN',

            # all_shard for loose
            'KEYS',

            # pubsub_node for loose
            'PUBLISH', 'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE', 'PUNSUBSCRIBE',
            'PUBSUB CHANNELS', 'PUBSUB NUMSUB', 'PUBSUB NUMPAT',
        ], 'blocked'),  # block for strict client
    )
    COMMAND_PARSE_KEYS = dict_merge(
        dict.fromkeys([
            'BRPOPLPUSH', 'RPOPLPUSH', 'RENAME', 'SMOVE',
        ], lambda args: [args[1], args[2]]),
        dict.fromkeys([
            'BLPOP', 'BRPOP',
        ], lambda args: args[2:-1]),
        dict.fromkeys([
            'ZINTERSTORE', 'ZUNIONSTORE',
        ], lambda args: [args[1]] + args[3:3+args[2]]),
        dict.fromkeys([
            'MSET', 'MSETNX',
        ], lambda args: args[1::2]),
        dict.fromkeys([
            'DEL', 'RPOPLPUSH', 'RENAME', 'RENAMENX', 'SMOVE', 'SDIFF', 'SDIFFSTORE',
            'SINTER', 'SINTERSTORE', 'SUNION', 'SUNIONSTORE', 'PFMERGE'
        ], lambda args: args[1:]),
        {
            'BITOP': lambda args: args[2:],
            '': lambda args: args[2:-1],
        },
    ),
    READONLY_COMMANDS = {
        # single key ops
        # - bits
        'BITCOUNT', 'BITPOS', 'GETBIT',

        # - string
        'GET', 'MGET', 'TTL', 'PTTL', 'EXISTS', 'GETRANGE', 'SUBSTR', 'STRLEN',
        'DUMP', 'TYPE', 'RANDOMKEY',

        # - hash
        'HEXISTS', 'HGET', 'HGETALL', 'HKEYS', 'HLEN', 'HMGET', 'HSCAN', 'HVALS',

        # - list
        'LINDEX', 'LLEN', 'LRANGE',

        # - SET
        'SISMEMBER', 'SMEMBERS', 'SRANDMEMBER', 'SCARD', 'SSCAN', 'SDIFF', 'SINTER', 'SUNION',

        # - ZSET
        'ZCARD', 'ZCOUNT', 'ZLEXCOUNT', 'ZRANGE', 'ZRANGEBYLEX', 'ZRANGEBYSCORE', 'ZSCAN',
        'ZRANK', 'ZREVRANGE', 'ZREVRANGEBYLEX', 'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCORE',

        # - hyper loglog
        'PFCOUNT',
    }

    def __init__(self, startup_nodes, max_connections=32, discover_cluster=True,
                 pipeline_use_threads=True, node_balancer=None, packer_kwargs=None, **kwargs):
        """
        startup_nodes    --> List of nodes that initial bootstrapping can be done from
        max_connections  --> Maximum number of connections that should be kept open at one time
        pipeline_use_threads  ->  By default, use threads in pipeline if this flag is set to True
        **kwargs         --> Extra arguments that will be sent into StrictRedis instance when created
                             (See Official redis-py doc for supported kwargs
                             [https://github.com/andymccurdy/redis-py/blob/master/redis/client.py])
                             Some kwargs is not supported and will raise RedisClusterException
                              - db (Redis do not support database SELECT in cluster mode)
        """
        super(StrictClusterRedis, self).__init__(**kwargs)

        if 'db' in kwargs:
            raise ClusterError("Argument 'db' is not possible to use in cluster mode")

        self.manager = Cluster(startup_nodes=startup_nodes, **kwargs)
        self.connection_pool = ClusterConnectionPool(manager=self.manager, max_connections=max_connections, **kwargs)
        self.node_balancer = node_balancer or RoundRobinClusterNodeBalancer(self.manager)

        if discover_cluster:
            self.manager.discover_cluster()

        self.packer_conn = Connection(**packer_kwargs or {})
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()
        self.pipeline_use_threads = pipeline_use_threads

    def _get_random_node(self):
        """for read"""
        slot_id = self.__class__.RANDOM_RR_COUNT = (self.__class__.RANDOM_RR_COUNT + 1) % Cluster.KEY_SLOTS
        nodes = self.manager.get_slave_nodes(slot_id, slave_only=False)
        return nodes[slot_id % len(nodes)]

    def prepare_command(self, command_args):
        command = command_args[0]
        readonly = command in self.READONLY_COMMANDS

        node_flag = self.COMMAND_FLAGS.get(command)
        if node_flag == 'blocked':
            raise ClusterError('Blocked command: %s' % command)
        elif node_flag == 'random':
            node = self.node_balancer.get_random_node(readonly=readonly)
        elif command in self.COMMAND_PARSE_KEYS:
            slot_ids = set()
            for key_name in self.COMMAND_PARSE_KEYS[command](command_args):
                slot_ids.add(Cluster.keyslot(key_name))

            if len(slot_ids) != 1:
                raise ClusterCrossSlotError()

            node = self.node_balancer.get_node_for_slot(slot_id=slot_ids[0], readonly=readonly)
        else:
            key_name = command_args[1]
            node = self.node_balancer.get_node_for_key(key_name=key_name, readonly=readonly)

        connection = self.connection_pool.get_connection(node)
        return connection

    @classmethod
    def _desc_node(cls, node_or_conn):
        if isinstance(node_or_conn, Connection):
            node_or_conn = node_or_conn.host, node_or_conn.port

        return '%s:%s' % node_or_conn

    def execute_command(self, *command_args, **parser_args):
        """Send a command to a node in the cluster
        SINGLE & SIMPLE MODE
        1. single slot command  [v]
        2. random node command  [v]
        3. multiple slot command [loose]
        3.1. *cross slot command [loose]

        1. single key  [v]
        2. no key [v]
        3. multiple key in one slot [v]
        """
        command = command_args[0]
        packed_command = self.packer_conn.pack_command(*command_args)

        ttl = self.COMMAND_TTL
        while ttl > 0:
            ttl -= 1

            connection = self.prepare_command(command_args)
            try:
                connection.send_packed_command(packed_command)
                return self.parse_response(connection, command, **parser_args)
            except BusyLoadingError:
                raise
            except (ConnectionError, TimeoutError) as e:
                if ttl < self.COMMAND_TTL / 2:
                    time.sleep(0.01)
                LOGGER.warning('Node %s: %s' % (e.__class__.__name__, self._desc_node(connection)))
            except ClusterParser.MovedError as e:
                self.manager.slot_moved(e.slot_id, e.node)
                LOGGER.warning('slot moved: %s [%s] -> [%s]' % (
                    e.slot_id, self._desc_node(connection), self._desc_node(e.node)))
            finally:
                self.connection_pool.release(connection)

        raise ClusterError('TTL exhausted.')


class ClusterRedis(StrictClusterRedis):
    """
        KEYS pattern
        Find all keys matching the given pattern
        MIGRATE host port key destination-db timeout [COPY] [REPLACE]
        Atomically transfer a key from a Redis instance to another one.

        implements cross-slot/all-nodes operations
    """

    COMMAND_FLAGS = dict_merge(
        StrictClusterRedis.COMMAND_FLAGS,
        dict.fromkeys([
            'FLUSHALL', 'FLUSHDB',
        ], 'all-master'),
    )
