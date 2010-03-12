import datetime
import errno
import socket
import threading
import warnings
from redis.exceptions import ConnectionError, ResponseError, InvalidResponse
from redis.exceptions import RedisError, AuthenticationError

class ConnectionManager(threading.local):
    "Manages a list of connections on the local thread"
    def __init__(self):
        self.connections = {}
        
    def make_connection_key(self, host, port, db):
        "Create a unique key for the specified host, port and db"
        return '%s:%s:%s' % (host, port, db)
        
    def get_connection(self, host, port, db, password):
        "Return a specific connection for the specified host, port and db"
        key = self.make_connection_key(host, port, db)
        if key not in self.connections:
            self.connections[key] = Connection(host, port, db, password)
        return self.connections[key]
        
    def get_all_connections(self):
        "Return a list of all connection objects the manager knows about"
        return self.connections.values()
        
connection_manager = ConnectionManager()

class Connection(object):
    "Manages TCP communication to and from a Redis server"
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self._sock = None
        self._fp = None
        
    def connect(self, redis_instance):
        "Connects to the Redis server is not already connected"
        if self._sock:
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, e:
            raise ConnectionError("Error %s connecting to %s:%s. %s." % \
                (e.args[0], self.host, self.port, e.args[1]))
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
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
    
def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options['withscores']:
        return response
    return zip(response[::2], map(float, response[1::2]))
    
    
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
            'AUTH DEL EXISTS EXPIRE MOVE MSETNX RENAMENX SADD SISMEMBER SMOVE '
            'SETNX SREM ZADD ZREM',
            bool
            ),
        string_keys_to_dict(
            'DECRBY INCRBY LLEN SCARD SDIFFSTORE SINTERSTORE SUNIONSTORE '
            'ZCARD ZREMRANGEBYSCORE',
            int
            ),
        string_keys_to_dict(
            # these return OK, or int if redis-server is >=1.3.4
            'LPUSH RPUSH',
            lambda r: isinstance(r, int) and r or r == 'OK'
            ),
        string_keys_to_dict('ZSCORE ZINCRBY',
            lambda r: r is not None and float(r) or r),
        string_keys_to_dict(
            'FLUSHALL FLUSHDB LSET LTRIM MSET RENAME '
            'SAVE SELECT SET SHUTDOWN',
            lambda r: r == 'OK'
            ),
        string_keys_to_dict('SDIFF SINTER SMEMBERS SUNION',
            lambda r: r and set(r) or r
            ),
        string_keys_to_dict('ZRANGE ZRANGEBYSCORE ZREVRANGE', zset_score_pairs),
        {
            'BGSAVE': lambda r: r == 'Background saving started',
            'INFO': parse_info,
            'LASTSAVE': timestamp_to_datetime,
            'PING': lambda r: r == 'PONG',
            'RANDOMKEY': lambda r: r and r or None,
            'TTL': lambda r: r != -1 and r or None,
        }
        )
    
    def __init__(self, host='localhost', port=6379,
                db=0, password=None,
                charset='utf-8', errors='strict'):
        self.encoding = charset
        self.errors = errors
        self.select(host, port, db, password)
        
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
    
    def pipeline(self):
        return Pipeline(self.connection, self.encoding, self.errors)
        
    #### COMMAND EXECUTION AND PROTOCOL PARSING ####
    def _execute_command(self, command_name, command, **options):
        self.connection.send(command, self)
        return self.parse_response(command_name, **options)

    def execute_command(self, command_name, command, **options):
        "Sends the command to the Redis server and returns it's response"
        try:
            return self._execute_command(command_name, command, **options)
        except ConnectionError:
            self.connection.disconnect()
            return self._execute_command(command_name, command, **options)
        
    def _parse_response(self, command_name, catch_errors):
        conn = self.connection
        response = conn.read().strip()
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
                # for pipelines, we need to read everything, including response errors.
                # otherwise we'd completely mess up the receive buffer
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
        
    def format_inline(self, *args, **options):
        "Formats a request with the inline protocol"
        cmd = '%s\r\n' % ' '.join([self.encode(a) for a in args])
        return self.execute_command(args[0], cmd, **options)
        
    def format_bulk(self, *args, **options):
        "Formats a request with the bulk protocol"
        bulk_value = self.encode(args[-1])
        cmd = '%s %s\r\n%s\r\n' % (
            ' '.join([self.encode(a) for a in args[:-1]]),
            len(bulk_value),
            bulk_value,
            )
        return self.execute_command(args[0], cmd, **options)
        
    def format_multi_bulk(self, *args, **options):
        "Formats the request with the multi-bulk protocol"
        cmd_count = len(args)
        cmds = []
        for i in args:
            enc_value = self.encode(i)
            cmds.append('$%s\r\n%s\r\n' % (len(enc_value), enc_value))
        return self.execute_command(
            args[0],
            '*%s\r\n%s' % (cmd_count, ''.join(cmds)),
            **options
            )
        
    #### CONNECTION HANDLING ####
    def get_connection(self, host, port, db, password):
        "Returns a connection object"
        conn = connection_manager.get_connection(host, port, db, password)
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
            if not self.format_inline('AUTH', self.connection.password):
                raise AuthenticationError("Invalid Password")
        self.format_inline('SELECT', self.connection.db)
        
    def select(self, host, port, db, password=None):
        """
        Switch to a different database on the current host/port
        
        Note this method actually replaces the underlying connection object
        prior to issuing the SELECT command.  This makes sure we protect
        the thread-safe connections
        """
        self.connection = self.get_connection(host, port, db, password)
        
    #### SERVER INFORMATION ####
    def bgsave(self):
        """
        Tell the Redis server to save its data to disk.  Unlike save(),
        this method is asynchronous and returns immediately.
        """
        return self.format_inline('BGSAVE')
        
    def dbsize(self):
        "Returns the number of keys in the current database"
        return self.format_inline('DBSIZE')
        
    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.format_inline('DEL', *names)
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
        return self.format_inline('FLUSHALL')
        
    def flushdb(self):
        "Delete all keys in the current database"
        return self.format_inline('FLUSHDB')
        
    def info(self):
        "Returns a dictionary containing information about the Redis server"
        return self.format_inline('INFO')
        
    def lastsave(self):
        """
        Return a Python datetime object representing the last time the
        Redis database was saved to disk
        """
        return self.format_inline('LASTSAVE')
        
    def ping(self):
        "Ping the Redis server"
        return self.format_inline('PING')
        
    def save(self):
        """
        Tell the Redis server to save its data to disk,
        blocking until the save is complete
        """
        return self.format_inline('SAVE')
        
    #### BASIC KEY COMMANDS ####
    def decr(self, name, amount=1):
        """
        Decrements the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as 0 - ``amount``
        """
        return self.format_inline('DECRBY', name, amount)
        
    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.format_inline('EXISTS', name)
    __contains__ = exists
        
    def expire(self, name, time):
        "Set an expire on key ``name`` for ``time`` seconds"
        return self.format_inline('EXPIRE', name, time)
        
    def get(self, name):
        """
        Return the value at key ``name``, or None of the key doesn't exist
        """
        return self.format_inline('GET', name)
    __getitem__ = get
    
    def getset(self, name, value):
        """
        Set the value at key ``name`` to ``value`` if key doesn't exist
        Return the value at key ``name`` atomically
        """
        return self.format_bulk('GETSET', name, value)
        
    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.format_inline('INCRBY', name, amount)
        
    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.format_inline('KEYS', pattern)
        
    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        
        * Passing *args to this method has been deprecated *
        """
        keys = list_or_args('mget', keys, args)
        return self.format_inline('MGET', *keys)
        
    def mset(self, mapping):
        "Sets each key in the ``mapping`` dict to its corresponding value"
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        return self.format_multi_bulk('MSET', *items)
        
    def msetnx(self, mapping):
        """
        Sets each key in the ``mapping`` dict to its corresponding value if
        none of the keys are already set
        """
        items = []
        [items.extend(pair) for pair in mapping.iteritems()]
        return self.format_multi_bulk('MSETNX', *items)
        
    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.format_inline('MOVE', name, db)
        
    def randomkey(self):
        "Returns the name of a random key"
        return self.format_inline('RANDOMKEY')

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
        return self.format_inline('RENAME', src, dst)
        
    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.format_inline('RENAMENX', src, dst)
        
        
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
        return self.format_bulk('SET', name, value)
    __setitem__ = set
    
    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.format_bulk('SETNX', name, value)
        
    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.format_inline('TTL', name)
        
    def type(self, name):
        "Returns the type of key ``name``"
        return self.format_inline('TYPE', name)
        
        
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
        keys = list(keys)
        keys.append(timeout)
        return self.format_inline('BLPOP', *keys)
        
    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.
        
        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.
        
        If timeout is 0, then block indefinitely.
        """
        keys = list(keys)
        keys.append(timeout)
        return self.format_inline('BRPOP', *keys)
        
    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``
        
        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.format_inline('LINDEX', name, index)
        
    def llen(self, name):
        "Return the length of the list ``name``"
        return self.format_inline('LLEN', name)
        
    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.format_inline('LPOP', name)
    
    def lpush(self, name, value):
        "Push ``value`` onto the head of the list ``name``"
        return self.format_bulk('LPUSH', name, value)
        
    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``
        
        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.format_inline('LRANGE', name, start, end)
        
    def lrem(self, name, value, num=0):
        """
        Remove the first ``num`` occurrences of ``value`` from list ``name``
        
        If ``num`` is 0, then all occurrences will be removed
        """
        return self.format_bulk('LREM', name, num, value)
        
    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.format_bulk('LSET', name, index, value)
        
    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``
        
        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.format_inline('LTRIM', name, start, end)
        
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
        return self.format_inline('RPOP', name)
        
    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.format_inline('RPOPLPUSH', src, dst)
        
    def rpush(self, name, value):
        "Push ``value`` onto the tail of the list ``name``"
        return self.format_bulk('RPUSH', name, value)
        
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
            pieces.append('BY %s' % by)
        if start is not None and num is not None:
            pieces.append('LIMIT %s %s' % (start, num))
        if get is not None:
            pieces.append('GET %s' % get)
        if desc:
            pieces.append('DESC')
        if alpha:
            pieces.append('ALPHA')
        if store is not None:
            pieces.append('STORE %s' % store)
        return self.format_inline('SORT', *pieces)
        
    
    #### SET COMMANDS ####
    def sadd(self, name, value):
        "Add ``value`` to set ``name``"
        return self.format_bulk('SADD', name, value)
        
    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.format_inline('SCARD', name)
        
    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        keys = list_or_args('sdiff', keys, args)
        return self.format_inline('SDIFF', *keys)
        
    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sdiffstore', keys, args)
        return self.format_inline('SDIFFSTORE', dest, *keys)
        
    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        keys = list_or_args('sinter', keys, args)
        return self.format_inline('SINTER', *keys)
        
    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sinterstore', keys, args)
        return self.format_inline('SINTERSTORE', dest, *keys)
        
    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.format_bulk('SISMEMBER', name, value)
        
    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.format_inline('SMEMBERS', name)
        
    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.format_bulk('SMOVE', src, dst, value)
        
    def spop(self, name):
        "Remove and return a random member of set ``name``"
        return self.format_inline('SPOP', name)
        
    def srandmember(self, name):
        "Return a random member of set ``name``"
        return self.format_inline('SRANDMEMBER', name)
        
    def srem(self, name, value):
        "Remove ``value`` from set ``name``"
        return self.format_bulk('SREM', name, value)
        
    def sunion(self, keys, *args):
        "Return the union of sets specifiued by ``keys``"
        keys = list_or_args('sunion', keys, args)
        return self.format_inline('SUNION', *keys)
        
    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        keys = list_or_args('sunionstore', keys, args)
        return self.format_inline('SUNIONSTORE', dest, *keys)
        
    
    #### SORTED SET COMMANDS ####
    def zadd(self, name, value, score):
        "Add member ``value`` with score ``score`` to sorted set ``name``"
        return self.format_bulk('ZADD', name, score, value)
        
    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.format_inline('ZCARD', name)
        
    def zincr(self, key, member, value=1):
        "This has been deprecated, use zincrby instead"
        warnings.warn(DeprecationWarning(
            "Redis.zincr has been deprecated, use Redis.zincrby instead"
            ))
        return self.zincrby(key, member, value)
        
    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.format_bulk('ZINCRBY', name, amount, value)
        
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
        return self.format_inline(*pieces, **{'withscores': withscores})
        
    def zrangebyscore(self, name, min, max, start=None, num=None, withscores=False):
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
        return self.format_inline(*pieces, **{'withscores': withscores})
        
    def zrem(self, name, value):
        "Remove member ``value`` from sorted set ``name``"
        return self.format_bulk('ZREM', name, value)
        
    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``
        """
        return self.format_inline('ZREMRANGEBYSCORE', name, min, max)
        
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
        return self.format_inline(*pieces, **{'withscores': withscores})
        
    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.format_bulk('ZSCORE', name, value)
        
    
    #### HASH COMMANDS ####
    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.format_bulk('HGET', name, key)
        
    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.format_multi_bulk('HSET', name, key, value)
        
    
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
    def __init__(self, connection, charset, errors):
        self.connection = connection
        self.encoding = charset
        self.errors = errors
        self.reset()
        
    def reset(self):
        self.command_stack = []
        self.format_inline('MULTI')
        
    def execute_command(self, command_name, command, **options):
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
            return super(Pipeline, self).execute_command(
                command_name, command, **options)
        else:
            self.command_stack.append((command_name, command, options))
        return self
        
    def _execute(self, commands):
        # build up all commands into a single request to increase network perf
        all_cmds = ''.join([c for _1, c, _2 in commands])
        self.connection.send(all_cmds, self)
        # we only care about the last item in the response, which should be
        # the EXEC command
        for i in range(len(commands)-1):
            _ = self.parse_response('_')
        # tell the response parse to catch errors and return them as
        # part of the response
        response = self.parse_response('_', catch_errors=True)
        # don't return the results of the MULTI or EXEC command
        commands = [(c[0], c[2]) for c in commands[1:-1]]
        if len(response) != len(commands):
            raise ResponseError("Wrong number of response items from "
                "pipline execution")
        # Run any callbacks for the commands run in the pipeline
        data = []
        for r, cmd in zip(response, commands):
            if not isinstance(r, Exception):
                if cmd[0] in self.RESPONSE_CALLBACKS:
                    r = self.RESPONSE_CALLBACKS[cmd[0]](r, **cmd[1])
            data.append(r)
        return data
        
    def execute(self):
        "Execute all the commands in the current pipeline"
        self.format_inline('EXEC')
        stack = self.command_stack
        self.reset()
        try:
            return self._execute(stack)
        except ConnectionError:
            self.connection.disconnect()
            return self._execute(stack)
            
    def select(self, host, port, db):
        raise RedisError("Cannot select a different database from a pipeline")
        
