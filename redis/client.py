import datetime
import errno
import socket
import threading
import time
import warnings
from itertools import chain
from redis.exceptions import ConnectionError, ResponseError, InvalidResponse
from redis.exceptions import RedisError, AuthenticationError


class ConnectionPool(threading.local):
    "Manages a list of connections on the local thread"
    def __init__(self):
        self.connections = {}

    def make_connection_key(self, host, port, db):
        "Create a unique key for the specified host, port and db"
        return '%s:%s:%s' % (host, port, db)

    def get_connection(self, host, port, db, password, socket_timeout):
        "Return a specific connection for the specified host, port and db"
        key = self.make_connection_key(host, port, db)
        if key not in self.connections:
            self.connections[key] = Connection(
                host, port, db, password, socket_timeout)
        return self.connections[key]

    def get_all_connections(self):
        "Return a list of all connection objects the manager knows about"
        return self.connections.values()


class Connection(object):
    "Manages TCP communication to and from a Redis server"
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self._sock = None
        self._fp = None

    def connect(self, redis_instance):
        "Connects to the Redis server if not already connected"
        if self._sock:
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            # args for socket.error can either be (errno, "message")
            # or just "message"
            if len(e.args) == 1:
                error_message = "Error connecting to %s:%s. %s." % \
                    (self.host, self.port, e.args[0])
            else:
                error_message = "Error %s connecting %s:%s. %s." % \
                    (e.args[0], self.host, self.port, e.args[1])
            raise ConnectionError(error_message)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self.socket_timeout)
        self._sock = sock
        self._fp = sock.makefile('r')
        redis_instance._setup_connection()

    def disconnect(self):
        "Disconnects from the Redis server"
        if self._sock is None:
            return
        try:
            self._sock.close()
        except socket.error:
            pass
        self._sock = None
        self._fp = None

    def send(self, command, redis_instance):
        "Send ``command`` to the Redis server. Return the result."
        self.connect(redis_instance)
        try:
            self._sock.sendall(command)
        except socket.error, e:
            if e.args[0] == errno.EPIPE:
                self.disconnect()
            raise ConnectionError("Error %s while writing to socket. %s." % \
                e.args)

    def read(self, length=None):
        """
        Read a line from the socket is length is None,
        otherwise read ``length`` bytes
        """
        try:
            if length is not None:
                return self._fp.read(length)
            return self._fp.readline()
        except socket.error, e:
            self.disconnect()
            if e.args and e.args[0] == errno.EAGAIN:
                raise ConnectionError("Error while reading from socket: %s" % \
                    e.args[1])
        return ''

def list_or_args(command, keys, args):
    # returns a single list combining keys and args
    # if keys is not a list or args has items, issue a
    # deprecation warning
    oldapi = bool(args)
    try:
        i = iter(keys)
        # a string can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, basestring):
            oldapi = True
    except TypeError:
        oldapi = True
        keys = [keys]
    if oldapi:
        warnings.warn(DeprecationWarning(
            "Passing *args to Redis.%s has been deprecated. "
            "Pass an iterable to ``keys`` instead" % command
        ))
        keys.extend(args)
    return keys

def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)

def string_keys_to_dict(key_string, callback):
    return dict([(key, callback) for key in key_string.split()])

def dict_merge(*dicts):
    merged = {}
    [merged.update(d) for d in dicts]
    return merged

def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    def get_value(value):
        if ',' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    for line in response.splitlines():
        key, value = line.split(':')
        try:
            info[key] = int(value)
        except ValueError:
            info[key] = get_value(value)
    return info

def pairs_to_dict(response):
    "Create a dict given a list of key/value pairs"
    return dict(zip(response[::2], response[1::2]))

def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options['withscores']:
        return response
    return zip(response[::2], map(float, response[1::2]))

def int_or_none(response):
    if response is None:
        return None
    return int(response)

def float_or_none(response):
    if response is None:
        return None
    return float(response)


class Redis(threading.local):
    """
    Implementation of the Redis protocol.

    This abstract class provides a Python interface to all Redis commands
    and an implementation of the Redis protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the Redis server
    """
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'AUTH DEL EXISTS EXPIRE EXPIREAT HDEL HEXISTS HMSET MOVE MSETNX '
            'RENAMENX SADD SISMEMBER SMOVE SETEX SETNX SREM ZADD ZREM',
            bool
            ),
        string_keys_to_dict(
            'DECRBY HLEN INCRBY LLEN SCARD SDIFFSTORE SINTERSTORE '
            'SUNIONSTORE ZCARD ZREMRANGEBYRANK ZREMRANGEBYSCORE ZREVRANK',
            int
            ),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: isinstance(r, int) and r or r == 'OK'
            ),
        string_keys_to_dict('ZSCORE ZINCRBY', float_or_none),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET RENAME '
            'SAVE SELECT SET SHUTDOWN',
            lambda r: r == 'OK'
            ),
        string_keys_to_dict('BLPOP BRPOP', lambda r: r and tuple(r) or None),
        string_keys_to_dict('SDIFF SINTER SMEMBERS SUNION',
            lambda r: set(r)
            ),
        string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE', zset_score_pairs),
        {
            'BGREWRITEAOF': lambda r: \
                r == 'Background rewriting of AOF file started',
            'BGSAVE': lambda r: r == 'Background saving started',
            'HGETALL': lambda r: r and pairs_to_dict(r) or {},
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'PING': lambda r: r == 'PONG',
            'RANDOMKEY': lambda r: r and r or None,
            'TTL': lambda r: r != -1 and r or None,
            'ZRANK': int_or_none,
        }
        )

    # commands that should NOT pull data off the network buffer when executed
    SUBSCRIPTION_COMMANDS = set([
        'SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE'
        ])

    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 connection_pool=None,
                 charset='utf-8', errors='strict'):
        self.encoding = charset
        self.errors = errors
        self.connection = None
        self.subscribed = False
        self.connection_pool = connection_pool and connection_pool or ConnectionPool()
        self.select(db, host, port, password, socket_timeout)

    #### Legacty accessors of connection information ####
    def _get_host(self):
        return self.connection.host
    host = property(_get_host)

    def _get_port(self):
        return self.connection.port
    port = property(_get_port)

    def _get_db(self):
        return self.connection.db
    db = property(_get_db)

    def pipeline(self, transaction=True):
        """
        Return a new pipeline object that can queue multiple commands for
        later execution. ``transaction`` indicates whether all commands
        should be executed atomically. Apart from multiple atomic operations,
        pipelines are useful for batch loading of data as they reduce the
        number of back and forth network operations between client and server.
        """
        return Pipeline(
            self.connection,
            transaction,
            self.encoding,
            self.errors
            )

    def lock(self, name, timeout=None, sleep=0.1):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.
        """
        return Lock(self, name, timeout=timeout, sleep=sleep)

    #### COMMAND EXECUTION AND PROTOCOL PARSING ####
    def _execute_command(self, command_name, command, **options):
        subscription_command = command_name in self.SUBSCRIPTION_COMMANDS
        if self.subscribed and not subscription_command:
            raise RedisError("Cannot issue commands other than SUBSCRIBE and "
                "UNSUBSCRIBE while channels are open")
        try:
            self.connection.send(command, self)
            if subscription_command:
                return None
            return self.parse_response(command_name, **options)
        except ConnectionError:
            self.connection.disconnect()
            self.connection.send(command, self)
            if subscription_command:
                return None
            return self.parse_response(command_name, **options)

    def execute_command(self, *args, **options):
        "Sends the command to the redis server and returns it's response"
        cmd_count = len(args)
        cmds = []
        for i in args:
            enc_value = self.encode(i)
            cmds.append('$%s\r\n%s\r\n' % (len(enc_value), enc_value))
        return self._execute_command(
            args[0],
            '*%s\r\n%s' % (cmd_count, ''.join(cmds)),
            **options
            )

    def _parse_response(self, command_name, catch_errors):
        conn = self.connection
        response = conn.read()[:-2] # strip last two characters (\r\n)
        if not response:
            self.connection.disconnect()
            raise ConnectionError("Socket closed on remote end")

        # server returned a null value
        if response in ('$-1', '*-1'):
            return None
        byte, response = response[0], response[1:]

        # server returned an error
        if byte == '-':
            if response.startswith('ERR '):
                response = response[4:]
            raise ResponseError(response)
        # single value
        elif byte == '+':
            return response
        # int value
        elif byte == ':':
            return int(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = length and conn.read(length) or ''
            conn.read(2) # read the \r\n delimiter
            return response
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            if not catch_errors:
                return [self._parse_response(command_name, catch_errors)
                    for i in range(length)]
            else:
                # for pipelines, we need to read everything,
                # including response errors. otherwise we'd
                # completely mess up the receive buffer
                data = []
                for i in range(length):
                    try:
                        data.append(
                            self._parse_response(command_name, catch_errors)
                            )
                    except Exception, e:
                        data.append(e)
                return data

        raise InvalidResponse("Unknown response type for: %s" % command_name)

    def parse_response(self, command_name, catch_errors=False, **options):
        "Parses a response from the Redis server"
        response = self._parse_response(command_name, catch_errors)
        if command_name in self.RESPONSE_CALLBACKS:
            return self.RESPONSE_CALLBACKS[command_name](response, **options)
        return response

    def encode(self, value):
        "Encode ``value`` using the instance's charset"
        if isinstance(value, str):
            return value
        if isinstance(value, unicode):
            return value.encode(self.encoding, self.errors)
        # not a string or unicode, attempt to convert to a string
        return str(value)

    #### CONNECTION HANDLING ####
    def get_connection(self, host, port, db, password, socket_timeout):
        "Returns a connection object"
        conn = self.connection_pool.get_connection(
            host, port, db, password, socket_timeout)
        # if for whatever reason the connection gets a bad password, make
        # sure a subsequent attempt with the right password makes its way
        # to the connection
        conn.password = password
        return conn

    def _setup_connection(self):
        """
        After successfully opening a socket to the Redis server, the
        connection object calls this method to authenticate and select
        the appropriate database.
        """
        if self.connection.password:
            if not self.execute_command('AUTH', self.connection.password):
                raise AuthenticationError("Invalid Password")
        self.execute_command('SELECT', self.connection.db)

    def select(self, db, host=None, port=None, password=None,
            socket_timeout=None):
        """
        Switch to a different Redis connection.

        If the host and port aren't provided and there's an existing
        connection, use the existing connection's host and port instead.

        Note this method actually replaces the underlying connection object
        prior to issuing the SELECT command.  This makes sure we protect
        the thread-safe connections
        """
        if host is None:
            if self.connection is None:
                raise RedisError("A valid hostname or IP address "
                    "must be specified")
            host = self.connection.host
        if port is None:
            if self.connection is None:
                raise RedisError("A valid port must be specified")
            port = self.connection.port

        self.connection = self.get_connection(
            host, port, db, password, socket_timeout)


    #### SERVER INFORMATION ####
    def bgrewriteaof(self):
        "Tell the Redis server to rewrite the AOF file from data in memory."
        return self.execute_command('BGREWRITEAOF')

    def bgsave(self):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return self.execute_command('BGSAVE')

    def dbsize(self):
        "Returns the number of keys in the current database"
        return self.execute_command('DBSIZE')

    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('DEL', *names)
    __delitem__ = delete

    def flush(self, all_dbs=False):
        warnings.warn(DeprecationWarning(
            "'flush' has been deprecated. "
            "Use Redis.flushdb() or Redis.flushall() instead"))
        if all_dbs:
            return self.flushall()
        return self.flushdb()

    def flushall(self):
        "Delete all keys in all databases on the current host"
        return self.execute_command('FLUSHALL')

    def flushdb(self):
        "Delete all keys in the current database"
        return self.execute_command('FLUSHDB')

    def info(self):
        "Returns a dictionary containing information about the Redis server"
        return self.execute_command('INFO')

    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.execute_command('LASTSAVE')

    def ping(self):
        "Ping the Redis server"
        return self.execute_command('PING')

    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.execute_command('SAVE')

    #### BASIC KEY COMMANDS ####
    def append(self, key, value):
        """
        Appends the string ``value`` to the value at ``key``. If ``key``
        doesn't already exist, create it with a value of ``value``.
        Returns the new length of the value at ``key``.
        """
        return self.execute_command('APPEND', key, value)

    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.execute_command('DECRBY', name, amount)

    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('EXISTS', name)
    __contains__ = exists

    def expire(self, name, time):
        "Set an expire flag on key ``name`` for ``time`` seconds"
        return self.execute_command('EXPIRE', name, time)

    def expireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer indicating unix time or a Python datetime object.
        """
        if isinstance(when, datetime.datetime):
            when = int(time.mktime(when.timetuple()))
        return self.execute_command('EXPIREAT', name, when)

    def get(self, name):
        """
        Return the value at key ``name``, or None of the key doesn't exist
        """
        return self.execute_command('GET', name)
    __getitem__ = get

    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        return self.execute_command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.execute_command('INCRBY', name, amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.execute_command('KEYS', pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``

        * Passing *args to this method has been deprecated *
        """
        keys = list_or_args('mget', keys, args)
        return self.execute_command('MGET', *keys)

    def mset(self, mapping):
        "Sets each key in the ``mapping`` dict to its corresponding value"
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        return self.execute_command('MSET', *items)

    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        return self.execute_command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.execute_command('MOVE', name, db)

    def randomkey(self):
        "Returns the name of a random key"
        return self.execute_command('RANDOMKEY')

    def rename(self, src, dst, **kwargs):
        """
        Rename key ``src`` to ``dst``

        * The following flags have been deprecated *
        If ``preserve`` is True, rename the key only if the destination name
            doesn't already exist
        """
        if kwargs:
            if 'preserve' in kwargs:
                warnings.warn(DeprecationWarning(
                    "preserve option to 'rename' is deprecated, "
                    "use Redis.renamenx instead"))
                if kwargs['preserve']:
                    return self.renamenx(src, dst)
        return self.execute_command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.execute_command('RENAMENX', src, dst)


    def set(self, name, value, **kwargs):
        """
        Set the value at key ``name`` to ``value``

        * The following flags have been deprecated *
        If ``preserve`` is True, set the value only if key doesn't already
        exist
        If ``getset`` is True, set the value only if key doesn't already exist
        and return the resulting value of key
        """
        if kwargs:
            if 'getset' in kwargs:
                warnings.warn(DeprecationWarning(
                    "getset option to 'set' is deprecated, "
                    "use Redis.getset() instead"))
                if kwargs['getset']:
                    return self.getset(name, value)
            if 'preserve' in kwargs:
                warnings.warn(DeprecationWarning(
                    "preserve option to 'set' is deprecated, "
                    "use Redis.setnx() instead"))
                if kwargs['preserve']:
                    return self.setnx(name, value)
        return self.execute_command('SET', name, value)
    __setitem__ = set

    def setex(self, name, value, time):
        """
        Set the value of key ``name`` to ``value``
        that expires in ``time`` seconds
        """
        return self.execute_command('SETEX', name, time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('SETNX', name, value)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.execute_command('SUBSTR', name, start, end)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.execute_command('TYPE', name)


    #### LIST COMMANDS ####
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.

        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.

        If timeout is 0, then block indefinitely.
        """
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.execute_command('BRPOP', *keys)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``

        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.execute_command('LINDEX', name, index)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.execute_command('LLEN', name)

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.execute_command('LPOP', name)

    def lpush(self, name, value):
        "Push ``value`` onto the head of the list ``name``"
        return self.execute_command('LPUSH', name, value)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LRANGE', name, start, end)

    def lrem(self, name, value, num=0):
        """
        Remove the first ``num`` occurrences of ``value`` from list ``name``

        If ``num`` is 0, then all occurrences will be removed
        """
        return self.execute_command('LREM', name, num, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.execute_command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``

        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.execute_command('LTRIM', name, start, end)

    def pop(self, name, tail=False):
        """
        Pop and return the first or last element of list ``name``

        * This method has been deprecated,
          use Redis.lpop or Redis.rpop instead *
        """
        warnings.warn(DeprecationWarning(
            "Redis.pop has been deprecated, "
            "use Redis.lpop or Redis.rpop instead"))
        if tail:
            return self.rpop(name)
        return self.lpop(name)

    def push(self, name, value, head=False):
        """
        Push ``value`` onto list ``name``.

        * This method has been deprecated,
          use Redis.lpush or Redis.rpush instead *
        """
        warnings.warn(DeprecationWarning(
            "Redis.push has been deprecated, "
            "use Redis.lpush or Redis.rpush instead"))
        if head:
            return self.lpush(name, value)
        return self.rpush(name, value)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.execute_command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.execute_command('RPOPLPUSH', src, dst)

    def rpush(self, name, value):
        "Push ``value`` onto the tail of the list ``name``"
        return self.execute_command('RPUSH', name, value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None):
        """
        Sort and return the list, set or sorted set at ``name``.

        ``start`` and ``num`` allow for paging through the sorted data

        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located

        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located

        ``desc`` allows for reversing the sort

        ``alpha`` allows for sorting lexicographically rather than numerically

        ``store`` allows for storing the result of the sort into
            the key ``store``
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append('BY')
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append('LIMIT')
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            pieces.append('GET')
            pieces.append(get)
        if desc:
            pieces.append('DESC')
        if alpha:
            pieces.append('ALPHA')
        if store is not None:
            pieces.append('STORE')
            pieces.append(store)
        return self.execute_command('SORT', *pieces)


    #### SET COMMANDS ####
    def sadd(self, name, value):
        "Add ``value`` to set ``name``"
        return self.execute_command('SADD', name, value)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.execute_command('SCARD', name)

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        keys = list_or_args('sdiff', keys, args)
        return self.execute_command('SDIFF', *keys)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sdiffstore', keys, args)
        return self.execute_command('SDIFFSTORE', dest, *keys)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        keys = list_or_args('sinter', keys, args)
        return self.execute_command('SINTER', *keys)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sinterstore', keys, args)
        return self.execute_command('SINTERSTORE', dest, *keys)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.execute_command('SISMEMBER', name, value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.execute_command('SMEMBERS', name)

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.execute_command('SMOVE', src, dst, value)

    def spop(self, name):
        "Remove and return a random member of set ``name``"
        return self.execute_command('SPOP', name)

    def srandmember(self, name):
        "Return a random member of set ``name``"
        return self.execute_command('SRANDMEMBER', name)

    def srem(self, name, value):
        "Remove ``value`` from set ``name``"
        return self.execute_command('SREM', name, value)

    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        keys = list_or_args('sunion', keys, args)
        return self.execute_command('SUNION', *keys)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sunionstore', keys, args)
        return self.execute_command('SUNIONSTORE', dest, *keys)


    #### SORTED SET COMMANDS ####
    def zadd(self, name, value, score):
        "Add member ``value`` with score ``score`` to sorted set ``name``"
        return self.execute_command('ZADD', name, score, value)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.execute_command('ZCARD', name)

    def zincr(self, key, member, value=1):
        "This has been deprecated, use zincrby instead"
        warnings.warn(DeprecationWarning(
            "Redis.zincr has been deprecated, use Redis.zincrby instead"
            ))
        return self.zincrby(key, member, value)

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.execute_command('ZINCRBY', name, amount, value)

    def zinter(self, dest, keys, aggregate=None):
        warnings.warn(DeprecationWarning(
            "Redis.zinter has been deprecated, use Redis.zinterstore instead"
            ))
        return self.zinterstore(dest, keys, aggregate)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zrange(self, name, start, end, desc=False, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.

        ``start`` and ``end`` can be negative, indicating the end of the range.

        ``desc`` indicates to sort in descending order.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        if desc:
            return self.zrevrange(name, start, end, withscores)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrangebyscore(self, name, min, max,
            start=None, num=None, withscores=False):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.

        If ``start`` and ``num`` are specified, then return a slice of the range.

        ``withscores`` indicates to return the scores along with the values.
            The return type is a list of (value, score) pairs
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise RedisError("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend(['LIMIT', start, num])
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.execute_command('ZRANK', name, value)

    def zrem(self, name, value):
        "Remove member ``value`` from sorted set ``name``"
        return self.execute_command('ZREM', name, value)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.execute_command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.execute_command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, num, withscores=False):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``num`` sorted in descending order.

        ``start`` and ``num`` can be negative, indicating the end of the range.

        ``withscores`` indicates to return the scores along with the values
            as a dictionary of value => score
        """
        pieces = ['ZREVRANGE', name, start, num]
        if withscores:
            pieces.append('withscores')
        return self.execute_command(*pieces, **{'withscores': withscores})

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.execute_command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.execute_command('ZSCORE', name, value)

    def zunion(self, dest, keys, aggregate=None):
        warnings.warn(DeprecationWarning(
            "Redis.zunion has been deprecated, use Redis.zunionstore instead"
            ))
        return self.zunionstore(dest, keys, aggregate)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def _zaggregate(self, command, dest, keys, aggregate=None):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            items = keys.items()
            keys = [i[0] for i in items]
            weights = [i[1] for i in items]
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append('WEIGHTS')
            pieces.extend(weights)
        if aggregate:
            pieces.append('AGGREGATE')
            pieces.append(aggregate)
        return self.execute_command(*pieces)

    #### HASH COMMANDS ####
    def hdel(self, name, key):
        "Delete ``key`` from hash ``name``"
        return self.execute_command('HDEL', name, key)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.execute_command('HEXISTS', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.execute_command('HGET', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.execute_command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.execute_command('HINCRBY', name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.execute_command('HKEYS', name)

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.execute_command('HLEN', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.execute_command('HSET', name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.execute_command("HSETNX", name, key, value)

    def hmset(self, name, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value
        in the hash ``name``
        """
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        return self.execute_command('HMSET', name, *items)

    def hmget(self, name, keys):
        "Returns a list of values ordered identically to ``keys``"
        return self.execute_command('HMGET', name, *keys)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.execute_command('HVALS', name)


    # channels
    def psubscribe(self, patterns):
        "Subscribe to all channels matching any pattern in ``patterns``"
        if isinstance(patterns, basestring):
            patterns = [patterns]
        response = self.execute_command('PSUBSCRIBE', *patterns)
        # this is *after* the SUBSCRIBE in order to allow for lazy and broken
        # connections that need to issue AUTH and SELECT commands
        self.subscribed = True
        return response

    def punsubscribe(self, patterns=[]):
        """
        Unsubscribe from any channel matching any pattern in ``patterns``.
        If empty, unsubscribe from all channels.
        """
        if isinstance(patterns, basestring):
            patterns = [patterns]
        return self.execute_command('PUNSUBSCRIBE', *patterns)

    def subscribe(self, channels):
        "Subscribe to ``channels``, waiting for messages to be published"
        if isinstance(channels, basestring):
            channels = [channels]
        response = self.execute_command('SUBSCRIBE', *channels)
        # this is *after* the SUBSCRIBE in order to allow for lazy and broken
        # connections that need to issue AUTH and SELECT commands
        self.subscribed = True
        return response

    def unsubscribe(self, channels=[]):
        """
        Unsubscribe from ``channels``. If empty, unsubscribe
        from all channels
        """
        if isinstance(channels, basestring):
            channels = [channels]
        return self.execute_command('UNSUBSCRIBE', *channels)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.execute_command('PUBLISH', channel, message)

    def listen(self):
        "Listen for messages on channels this client has been subscribed to"
        while self.subscribed:
            r = self.parse_response('LISTEN')
            if r[0] == 'pmessage':
                msg = {
                'type': r[0],
                'pattern': r[1],
                'channel': r[2],
                'data': r[3]
                }
            else:
                msg = {
                'type': r[0],
                'pattern': None,
                'channel': r[1],
                'data': r[2]
                }
            if r[0] == 'unsubscribe' and r[2] == 0:
                self.subscribed = False
            yield msg


class Pipeline(Redis):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """
    def __init__(self, connection, transaction, charset, errors):
        self.connection = connection
        self.transaction = transaction
        self.encoding = charset
        self.errors = errors
        self.subscribed = False # NOTE not in use, but necessary
        self.reset()

    def reset(self):
        self.command_stack = []

    def _execute_command(self, command_name, command, **options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        # if the command_name is 'AUTH' or 'SELECT', then this command
        # must have originated after a socket connection and a call to
        # _setup_connection(). run these commands immediately without
        # buffering them.
        if command_name in ('AUTH', 'SELECT'):
            return super(Pipeline, self)._execute_command(
                command_name, command, **options)
        else:
            self.command_stack.append((command_name, command, options))
        return self

    def _execute_transaction(self, commands):
        # wrap the commands in MULTI ... EXEC statements to indicate an
        # atomic operation
        all_cmds = ''.join([c for _1, c, _2 in chain(
            (('', 'MULTI\r\n', ''),),
            commands,
            (('', 'EXEC\r\n', ''),)
            )])
        self.connection.send(all_cmds, self)
        # parse off the response for MULTI and all commands prior to EXEC
        for i in range(len(commands)+1):
            _ = self.parse_response('_')
        # parse the EXEC. we want errors returned as items in the response
        response = self.parse_response('_', catch_errors=True)
        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from "
                "pipeline execution")
        # Run any callbacks for the commands run in the pipeline
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                if cmd[0] in self.RESPONSE_CALLBACKS:
                    r = self.RESPONSE_CALLBACKS[cmd[0]](r, **cmd[2])
            data.append(r)
        return data

    def _execute_pipeline(self, commands):
        # build up all commands into a single request to increase network perf
        all_cmds = ''.join([c for _1, c, _2 in commands])
        self.connection.send(all_cmds, self)
        data = []
        for command_name, _, options in commands:
            data.append(
                self.parse_response(command_name, catch_errors=True, **options)
                )
        return data

    def execute(self):
        "Execute all the commands in the current pipeline"
        stack = self.command_stack
        self.reset()
        if self.transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline
        try:
            return execute(stack)
        except ConnectionError:
            self.connection.disconnect()
            return execute(stack)

    def select(self, *args, **kwargs):
        raise RedisError("Cannot select a different database from a pipeline")


class Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """

    LOCK_FOREVER = 2**31+1 # 1 past max unix time

    def __init__(self, redis, name, timeout=None, sleep=0.1):
        """
        Create a new Lock instnace named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        Note: If using ``timeout``, you should make sure all the hosts
        that are running clients are within the same timezone and are using
        a network time service like ntp.
        """
        self.redis = redis
        self.name = name
        self.acquired_until = None
        self.timeout = timeout
        self.sleep = sleep

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=True):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.
        """
        sleep = self.sleep
        timeout = self.timeout
        while 1:
            unixtime = int(time.time())
            if timeout:
                timeout_at = unixtime + timeout
            else:
                timeout_at = Lock.LOCK_FOREVER
            if self.redis.setnx(self.name, timeout_at):
                self.acquired_until = timeout_at
                return True
            # We want blocking, but didn't acquire the lock
            # check to see if the current lock is expired
            existing = long(self.redis.get(self.name) or 1)
            if existing < unixtime:
                # the previous lock is expired, attempt to overwrite it
                existing = long(self.redis.getset(self.name, timeout_at) or 1)
                if existing < unixtime:
                    # we successfully acquired the lock
                    self.acquired_until = timeout_at
                    return True
            if not blocking:
                return False
            time.sleep(sleep)

    def release(self):
        "Releases the already acquired lock"
        if self.acquired_until is None:
            raise ValueError("Cannot release an unlocked lock")
        existing = long(self.redis.get(self.name) or 1)
        # if the lock time is in the future, delete the lock
        if existing >= self.acquired_until:
            self.redis.delete(self.name)
        self.acquired_until = None
