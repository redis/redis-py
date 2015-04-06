"""cluster support ported from https://github.com/Grokzen/redis-py-cluster

MY GOALS:

1. expose abilities which redis cluster provided
  - high availability, endure partially slot coverage
  - scaling up read operations using slave nodes
  - interactive load balance interface via python generator
2. Strict interface provides
  - commands without ambiguity
  - multiple-key operations MUST located in single slot
3. Loose interface provides
  - cross slot helper methods
4. ability to adapt tornado's Future, via some external component(not-included)
5. let exception just raise to user if it's not in redis protocol.
"""
import time
import logging

from redis.crc import crc16
from redis._compat import iteritems, nativestr
from redis.client import StrictRedis, dict_merge, list_or_args
from redis.connection import Connection, ConnectionPool, DefaultParser
from redis.exceptions import (
    ConnectionError, ClusterPartitionError, ClusterError,
    TimeoutError, ResponseError, BusyLoadingError, ClusterCrossSlotError,
    ClusterSlotNotServedError, ClusterDownError,
)

# TODO: handle TryAgain
# TODO: pipeline
# TODO: generator as interactive load balancer
# TODO: master slave changed
# TODO: master timed out
# TODO: slave timed out
# TODO: READWRITE/READONLY switching
# TODO: connection_pool (partially) rebuild
# TODO: every possible situation in cluster

# TODO: read from slave, but slave changed to master
# TODO: pubsub
# TODO: lock
# TODO: advanced balancer
# TODO: loose redis interface(cross slot ops)
# TODO: migrate tests
# TODO: script
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(logging.StreamHandler())


class ClusterBalancer(object):
    def get_node_for_key(self, key_name, readonly):
        return self.get_node_for_slot(Cluster.keyslot(key_name), readonly=readonly)

    def get_node_for_slot(self, slot_id, readonly):
        raise NotImplementedError()

    def get_random_node(self, readonly):
        raise NotImplementedError()

    def get_shards_nodes(self, readonly):
        """one each shard"""
        raise NotImplementedError()


class RoundRobinClusterNodeBalancer(ClusterBalancer):
    RR_COUNTER = 1

    def __init__(self, manager):
        self.manager = manager

    def _incr(self, by=1):
        counter = self.__class__.RR_COUNTER = (self.__class__.RR_COUNTER + by) % Cluster.KEY_SLOTS
        return counter

    def get_node_for_slot(self, slot_id, readonly):
        self.manager.discover_cluster()

        if not readonly:
            return self.manager.get_master_node(slot_id)
        else:
            nodes = self.manager.get_slave_nodes(slot_id, slave_only=False)
            return list(nodes)[self._incr() % len(nodes)]

    def get_random_node(self, readonly):
        self.manager.discover_cluster()

        if readonly:
            nodes = self.manager.nodes.keys()
        else:
            nodes = self.manager.shards.keys()

        return nodes[self._incr() % len(nodes)]

    def get_shards_nodes(self, readonly):
        self.manager.discover_cluster()

        for shard in self.manager.shards.values():
            if readonly:
                nodes = list(shard['slaves']) + [shard['master']]
                yield list(nodes)[self._incr() % len(nodes)]
            else:
                yield shard['master']


class ClusterParser(DefaultParser):
    class AskError(ResponseError):
        """
        src node: MIGRATING to dst node
            get > ASK error
            ask dst node > ASKING command
        dst node: IMPORTING from src node
            asking command only affects next command
            any op will be allowed after asking command
        """
        def __init__(self, resp):
            """should only redirect to master node"""
            slot_id, new_node = resp.split(' ')
            host, port = new_node.rsplit(':', 1)
            self.slot_id = int(slot_id)
            self.node_addr = self.host, self.port = host, int(port)

    class TryAgainError(ResponseError):
        def __init__(self, resp):
            pass

    class MovedError(AskError):
        pass

    EXCEPTION_CLASSES = dict_merge(
        DefaultParser.EXCEPTION_CLASSES, {
            'ASK': AskError,
            'TRYAGAIN': TryAgainError,
            'MOVED': MovedError,
            'CLUSTERDOWN': ClusterDownError,
            'CROSSSLOT': ClusterCrossSlotError,
        })


class ClusterConnection(Connection):
    description_format = "ClusterConnection<host=%(host)s,port=%(port)s>"

    def __init__(self, *args, **kwargs):
        self.readonly = kwargs.pop('readonly', False)
        kwargs['parser_class'] = ClusterParser
        super(ClusterConnection, self).__init__(*args, **kwargs)

    def on_connect(self):
        """Initialize the connection, set readonly is required"""
        super(ClusterConnection, self).on_connect()
        if self.readonly:
            self.send_command('READONLY')
            if nativestr(self.read_response()) != 'OK':
                raise ResponseError('Cannot set READONLY flag')


class Cluster(object):
    """keep knowledge of cluster"""

    KEY_SLOTS = 16384
    # timeout for collecting from startup nodes
    DEFAULT_TIMEOUT = 0.5

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
        self.cluster_kwargs.setdefault('socket_timeout', self.DEFAULT_TIMEOUT)
        self.allow_partition = allow_partition

        self.shards = {}  # master_id -> shard_info
        self.nodes = {}
        self.startup_nodes = set(startup_nodes)  # [(host, port), ...]
        self.slots = {}
        self.pubsub_node = None

        # version for keep generators work
        self.slots_epoch = 0

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

    def discover_cluster(self, force=False, check_all=False):
        """
        check_all: read from each startup nodes for detect cluster partition
        force: do the discover action even cluster slot info is complete
        """
        if not force and len(self.slots) == self.KEY_SLOTS:
            return

        self.nodes, self.shards = {}, {}
        # TODO: discover more node dynamically
        for startup_node in self.startup_nodes:
            try:
                nodes = StrictRedis(*startup_node, **self.cluster_kwargs).cluster_nodes()
            except ConnectionError:
                LOGGER.warning('Startup node: %s:%s not responding in time.' % startup_node)
                continue

            # build shards
            for node in nodes:
                node_id = node['id']
                self.nodes[node_id] = {
                    'connected': node['link-state'] == 'connected',
                    'id': node['id'],
                    'host': node['host'],
                    'port': node['port'],
                    'addr': (node['host'], node['port']),
                    'flags': node['flags'],
                    'master': node['master'],
                    'is_master': not node['master'],
                    'slots': node['slots'],
                }

                if 'master' not in node['flags']:
                    continue

                self.shards[node_id] = {
                    'master': node_id,
                    'slaves': set(),
                    'slots': node['slots'],
                }

            # fill slaves & slots
            for node in nodes:
                shard_id = node['master'] or node['id']

                if 'slave' in node['flags']:
                    self.shards[shard_id]['slaves'].add(node['id'])

                for slot_id in node['slots']:
                    old_master = self.slots.setdefault(slot_id, shard_id)
                    if old_master != shard_id:
                        raise ClusterPartitionError(
                            'Cluster partition appears: slot #%s, node: %s:[%s] and %s:[%s]' % (
                                slot_id, old_master, self.node_addr(old_master),
                                shard_id, self.node_addr(shard_id)))
                    else:
                        self.slots[slot_id] = shard_id

            if not check_all and len(self.slots) == self.KEY_SLOTS:
                break

        if not self.nodes:
            raise ClusterDownError('No startup node can be reached. [\n%s\n]' % self.startup_nodes)

        self.startup_nodes = set([node['addr'] for node in self.nodes.values()])
        self.pubsub_node = self.determine_pubsub_node()
        self.slots_epoch += 1

    def get_master_node(self, slot_id):
        self.discover_cluster()
        try:
            shard_id = self.slots[slot_id]
        except KeyError:
            raise ClusterSlotNotServedError(slot_id)
        else:
            return self.shards[shard_id]['master']

    def get_slave_nodes(self, slot_id, slave_only=True):
        self.discover_cluster()
        try:
            shard_id = self.slots[slot_id]
        except KeyError:
            raise ClusterSlotNotServedError(slot_id)
        else:
            shard = self.shards[shard_id]
            if slave_only:
                return list(shard['slaves'])
            else:
                return list(shard['slaves']) + [shard['master']]

    def determine_pubsub_node(self):
        """
        Determine what node object should be used for pubsub commands.

        All clients in the cluster will talk to the same pubsub node to ensure
        all code stay compatible. See pubsub doc for more details why.

        Always use the server with highest port number
        """
        highest = -1
        node = None
        for node in self.nodes.values():
            host, port = node['addr']
            if port > highest:
                highest = port

        return node

    def all_nodes(self):
        return self.nodes.values()

    def node_addr(self, node_id):
        return self.nodes[node_id]['addr']

    def slot_moved(self, slot_id, addr):
        """signal from response, target node should be master"""
        # XXX: maybe no rebuild cluster? only current slot?
        self.startup_nodes.add(addr)
        self.discover_cluster(force=True)

    def ask_node(self, slot_id, addr):
        """signal from response, target node should be master"""
        self.startup_nodes.add(addr)


class ClusterConnectionPool(object):
    """connection pool for redis cluster
    collection of pools
    """
    DEFAULT_TIMEOUT = 0.1
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
            (node['addr'], self.make_connection_pool(node['addr'], not node['is_master']))
            for node in self.manager.all_nodes()
        ])

    def get_connection(self, addr, command_name=None, *keys, **options):
        """Get a connection from the pool"""
        return self.pools[addr].get_connection(command_name, *keys, **options)

    def make_connection_pool(self, (host, port), readonly):
        """Create a new connection pool"""
        return ConnectionPool(host=host, port=port,
                              connection_class=self.connection_class,
                              max_connections=self.max_connections,
                              readonly=readonly,
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
            # impossible in cluster mode
            'SELECT', 'MOVE', 'SLAVEOF',

            # sentinels
            'SENTINEL GET-MASTER-ADDR-BY-NAME', 'SENTINEL MASTER', 'SENTINEL MASTERS',
            'SENTINEL MONITOR', 'SENTINEL REMOVE', 'SENTINEL SENTINELS', 'SENTINEL SET',
            'SENTINEL SLAVES',

            # admin commands
            'BGREWRITEAOF', 'BGSAVE', 'SAVE', 'INFO', 'LASTSAVE',
            'CONFIG GET', 'CONFIG SET', 'CONFIG RESETSTAT', 'CONFIG REWRITE',
            'CLIENT KILL', 'CLIENT LIST', 'CLIENT GETNAME', 'CLIENT SETNAME',
            'SLOWLOG GET', 'SLOWLOG RESET', 'SLOWLOG LEN', 'SHUTDOWN',

            # lua script
            'EVALSHA', 'SCRIPT EXISTS', 'SCRIPT KILL', 'SCRIPT LOAD', 'SCRIPT FLUSH',

            # unknown behaviors in cluster
            'PING', 'MONITOR', 'TIME', 'READONLY', 'READWRITE',
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
            'SINTER', 'SINTERSTORE', 'SUNION', 'SUNIONSTORE', 'PFMERGE', 'MGET',
        ], lambda args: args[1:]),
        {
            'BITOP': lambda args: args[2:],
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
                 node_balancer=None, packer_kwargs=None, **kwargs):
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

    def mget(self, keys, *args):
        """collects from slots
        """
        slot_keys = {}
        origin_keys = list_or_args(keys, args)
        for key in origin_keys:
            slot_keys.setdefault(Cluster.keyslot(key), set()).add(key)

        results = {}
        for slot_id, keys in slot_keys.iteritems():
            keys = list(keys)
            values = super(StrictClusterRedis, self).mget(keys)
            results.update(dict(zip(keys, values)))

        return [results[key] for key in origin_keys]

    def delete(self, *names):
        """collects from slots
        """
        slot_keys = {}
        for key in names:
            slot_keys.setdefault(Cluster.keyslot(key), set()).add(key)

        result = 0
        for slot_id, keys in slot_keys.iteritems():
            result += super(StrictClusterRedis, self).delete(*keys)

        return result

    def dbsize(self):
        """collects from all shards
        """
        result = 0
        for node in self.node_balancer.get_shards_nodes(readonly=True):
            connection = self.connection_pool.get_connection(node)
            try:
                result += self.execute_connection_command(connection, ('DBSIZE', ))
            finally:
                self.connection_pool.release(connection)

        return result

    def keys(self, pattern='*'):
        """collects from all shards
        """
        result = []
        for node in self.node_balancer.get_shards_nodes(readonly=True):
            connection = self.connection_pool.get_connection(node)
            try:
                result += self.execute_connection_command(connection, ('KEYS', pattern))
            finally:
                self.connection_pool.release(connection)

        return result

    def determine_node(self, command_args):
        command = command_args[0]
        readonly = command in self.READONLY_COMMANDS

        node_flag = self.COMMAND_FLAGS.get(command)
        if node_flag == 'blocked':
            raise ClusterError('Blocked command: %s' % command)
        elif node_flag == 'random':
            node_id = self.node_balancer.get_random_node(readonly=readonly)
        elif node_flag == 'slot-id':
            node_id = self.node_balancer.get_node_for_slot(slot_id=int(command_args[1]), readonly=readonly)
        elif command in self.COMMAND_PARSE_KEYS:
            slot_ids = set()
            for key_name in self.COMMAND_PARSE_KEYS[command](command_args):
                slot_ids.add(Cluster.keyslot(key_name))

            if len(slot_ids) != 1:
                raise ClusterCrossSlotError()

            node_id = self.node_balancer.get_node_for_slot(slot_id=slot_ids.pop(), readonly=readonly)
        else:
            key_name = command_args[1]
            node_id = self.node_balancer.get_node_for_key(key_name=key_name, readonly=readonly)

        return node_id

    @classmethod
    def _desc_node(cls, node_or_conn):
        if isinstance(node_or_conn, Connection):
            node_or_conn = node_or_conn.host, node_or_conn.port

        return '%s:%s' % node_or_conn

    def execute_connection_command(self, connection, command_args, parser_args=None):
        command = command_args[0]
        connection.send_command(*command_args)
        return self.parse_response(connection, command, **parser_args or {})

    def execute_command(self, *command_args, **parser_args):
        """Send a command to a node in the cluster
        SINGLE & SIMPLE MODE
        1. single slot command  [v]
        2. random node command  [v]
        3. multiple slot command [loose]
        3.1. *cross slot command [loose]

        1. single key  [v]
        2. no key [v]
        3. multiple key in single slot [v]
        """
        command = command_args[0]
        packed_command = self.packer_conn.pack_command(*command_args)

        ttl = self.COMMAND_TTL
        redirect_addr = None
        asking = False
        while ttl > 0:
            ttl -= 1

            if not redirect_addr:
                node_id = self.determine_node(command_args)
                node_addr = self.manager.node_addr(node_id)
            else:
                node_addr, redirect_addr = redirect_addr, None

            connection = self.connection_pool.get_connection(node_addr)
            try:
                if asking:
                    asking = False
                    connection.send_command('ASKING')
                    resp = self.parse_response(connection, 'ASKING')
                    if resp != 'OK':
                        raise ResponseError('ASKING %s is %s' % (self._desc_node(connection), resp))

                connection.send_packed_command(packed_command)
                return self.parse_response(connection, command, **parser_args)
            except BusyLoadingError:
                raise
            except (ConnectionError, TimeoutError) as e:
                LOGGER.warning('Node %s: %s' % (e.__class__.__name__, self._desc_node(connection)))
                if ttl < self.COMMAND_TTL / 2:
                    time.sleep(0.01)
            except ClusterParser.MovedError as e:
                LOGGER.warning('MOVED: %s [%s] -> [%s]' % (
                    e.slot_id, self._desc_node(connection), self._desc_node(e.node_addr)))
                self.manager.slot_moved(e.slot_id, e.node_addr)
                redirect_addr = e.node_addr
            except ClusterParser.AskError as e:
                LOGGER.warning('ASK redirect: %s [%s] -> [%s]' % (
                    e.slot_id, self._desc_node(connection), self._desc_node(e.node_addr)))
                self.manager.ask_node(e.slot_id, e.node_addr)
                redirect_addr, asking = e.node_addr, True
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
