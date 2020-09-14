import os
import socket
import threading
from contextlib import contextmanager
from itertools import chain
from time import time
from queue import LifoQueue, Empty, Full
from urllib.parse import parse_qs, unquote, urlparse

from .exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from .features import SSL_AVAILABLE
from .parser import DefaultParser
from .utils import DEFAULT, str_if_bytes

if SSL_AVAILABLE:
    # import the ssl module if it's available
    import ssl


class Encoder:
    "Encode strings to bytes-like and decode bytes-like to strings"

    def __init__(self, encoding, encoding_errors, decode_responses):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    @contextmanager
    def update(self, yielded_object, encoding=DEFAULT,
               encoding_errors=DEFAULT, decode_responses=DEFAULT):
        """
        Update the encoding settings temporarily as a context manager

        This is generally called from a method on an object that holds an
        Encoder instance, e.g. the Connection or Client object.
        """
        prev_encoding = self.encoding
        prev_encoding_errors = self.encoding_errors
        prev_decode_responses = self.decode_responses

        if encoding is not DEFAULT:
            self.encoding = encoding
        if encoding_errors is not DEFAULT:
            self.encoding_errors = encoding_errors
        if decode_responses is not DEFAULT:
            self.decode_responses = decode_responses

        yield yielded_object

        self.encoding = prev_encoding
        self.encoding_errors = prev_encoding_errors
        self.decode_responses = prev_decode_responses

    def encode(self, value):
        "Return a bytestring or bytes-like representation of the value"
        if isinstance(value, (bytes, memoryview)):
            return value
        elif isinstance(value, bool):
            # special case bool since it is a subclass of int
            raise DataError("Invalid input of type: 'bool'. Convert to a "
                            "bytes, string, int or float first.")
        elif isinstance(value, (int, float)):
            value = repr(value).encode()
        elif not isinstance(value, str):
            # a value we don't know how to deal with. throw an error
            typename = type(value).__name__
            raise DataError("Invalid input of type: '%s'. Convert to a "
                            "bytes, string, int or float first." % typename)
        if isinstance(value, str):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def decode(self, value, force=False):
        "Return a unicode string from the bytes-like representation"
        if self.decode_responses or force:
            if isinstance(value, memoryview):
                value = value.tobytes()
            if isinstance(value, bytes):
                value = value.decode(self.encoding, self.encoding_errors)
        return value


class Connection:
    "Manages TCP communication to and from a Redis server"

    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, socket_connect_timeout=None,
                 socket_keepalive=False, socket_keepalive_options=None,
                 socket_type=0, retry_on_timeout=False, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser, socket_read_size=65536,
                 health_check_interval=0, client_name=None, username=None):
        self.pid = os.getpid()
        self.host = host
        self.port = int(port)
        self.db = db
        self.username = username
        self.client_name = client_name
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout or socket_timeout
        self.socket_keepalive = socket_keepalive
        self.socket_keepalive_options = socket_keepalive_options or {}
        self.socket_type = socket_type
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.next_health_check = 0
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self._sock = None
        self._parser = parser_class(encoder=self.encoder,
                                    socket_read_size=socket_read_size)
        self._connect_callbacks = []
        self._buffer_cutoff = 6000

    def __repr__(self):
        repr_args = ','.join(['%s=%s' % (k, v) for k, v in self.repr_pieces()])
        return '%s<%s>' % (self.__class__.__name__, repr_args)

    def repr_pieces(self):
        pieces = [
            ('host', self.host),
            ('port', self.port),
            ('db', self.db)
        ]
        if self.client_name:
            pieces.append(('client_name', self.client_name))
        return pieces

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

    def register_connect_callback(self, callback):
        self._connect_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._connect_callbacks = []

    def connect(self):
        "Connects to the Redis server if not already connected"
        if self._sock:
            return
        try:
            sock = self._connect()
        except socket.timeout:
            raise TimeoutError("Timeout connecting to server")
        except socket.error as e:
            raise ConnectionError(self._error_message(e))

        self._sock = sock
        try:
            self.on_connect()
        except RedisError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

        # run any user callbacks. right now the only internal callback
        # is for pubsub channel/pattern resubscription
        for callback in self._connect_callbacks:
            callback(self)

    def _connect(self):
        "Create a TCP socket connection"
        # we want to mimic what socket.create_connection does to support
        # ipv4/ipv6, but we want to set options prior to calling
        # socket.connect()
        err = None
        for res in socket.getaddrinfo(self.host, self.port, self.socket_type,
                                      socket.SOCK_STREAM):
            family, socktype, proto, canonname, socket_address = res
            sock = None
            try:
                sock = socket.socket(family, socktype, proto)
                # TCP_NODELAY
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

                # TCP_KEEPALIVE
                if self.socket_keepalive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    for k, v in self.socket_keepalive_options.items():
                        sock.setsockopt(socket.IPPROTO_TCP, k, v)

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)

                # connect
                sock.connect(socket_address)

                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)
                return sock

            except OSError as _:
                err = _
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        raise OSError("socket.getaddrinfo returned an empty list")

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting to %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if username and/or password are set, authenticate
        if self.username or self.password:
            if self.username:
                auth_args = (self.username, self.password or '')
            else:
                auth_args = (self.password,)
            # avoid checking health here -- PING will fail if we try
            # to check the health prior to the AUTH
            self.send_command('AUTH', *auth_args, check_health=False)

            try:
                auth_response = self.read_response()
            except AuthenticationWrongNumberOfArgsError:
                # a username and password were specified but the Redis
                # server seems to be < 6.0.0 which expects a single password
                # arg. retry auth with just the password.
                # https://github.com/andymccurdy/redis-py/issues/1274
                self.send_command('AUTH', self.password, check_health=False)
                auth_response = self.read_response()

            if str_if_bytes(auth_response) != 'OK':
                raise AuthenticationError('Invalid Username or Password')

        # if a client_name is given, set it
        if self.client_name:
            self.send_command('CLIENT', 'SETNAME', self.client_name)
            if str_if_bytes(self.read_response()) != 'OK':
                raise ConnectionError('Error setting client name')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if str_if_bytes(self.read_response()) != 'OK':
                raise ConnectionError('Invalid Database')

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            if os.getpid() == self.pid:
                self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except OSError:
            pass
        self._sock = None

    def check_health(self):
        "Check the health of the connection with a PING/PONG"
        if self.health_check_interval and time() > self.next_health_check:
            try:
                self.send_command('PING', check_health=False)
                if str_if_bytes(self.read_response()) != 'PONG':
                    raise ConnectionError(
                        'Bad response from PING health check')
            except (ConnectionError, TimeoutError):
                self.disconnect()
                self.send_command('PING', check_health=False)
                if str_if_bytes(self.read_response()) != 'PONG':
                    raise ConnectionError(
                        'Bad response from PING health check')

    def send_packed_command(self, command, check_health=True):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        # guard against health check recursion
        if check_health:
            self.check_health()
        try:
            if isinstance(command, str):
                command = [command]
            for item in command:
                self._sock.sendall(item)
        except socket.timeout:
            self.disconnect()
            raise TimeoutError("Timeout writing to socket")
        except socket.error as e:
            self.disconnect()
            if len(e.args) == 1:
                errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                errno = e.args[0]
                errmsg = e.args[1]
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (errno, errmsg))
        except BaseException:
            self.disconnect()
            raise

    def send_command(self, *args, **kwargs):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self._parser.pack_command(*args),
                                 check_health=kwargs.get('check_health', True))

    def can_read(self, timeout=0):
        "Poll the socket to see if there's data that can be read."
        if not self._sock:
            self.connect()
        return self._parser.can_read(timeout)

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
        except socket.timeout:
            self.disconnect()
            raise TimeoutError("Timeout reading from %s:%s" %
                               (self.host, self.port))
        except socket.error as e:
            self.odisconnect()
            raise ConnectionError("Error while reading from %s:%s : %s" %
                                  (self.host, self.port, e.args))
        except BaseException:
            self.disconnect()
            raise

        if self.health_check_interval:
            self.next_health_check = time() + self.health_check_interval

        if isinstance(response, ResponseError):
            raise response
        return response

    def execute(self, *args, return_response=True):
        self.send_command(*args)
        if return_response:
            return self.read_response()

    def encoding_context(self, encoding=DEFAULT,
                         encoding_errors=DEFAULT, decode_responses=DEFAULT):
        return self.encoder.update(self,
                                   encoding=encoding,
                                   encoding_errors=encoding_errors,
                                   decode_responses=decode_responses)

    # TODO: remove these two once refactor is complete
    def pack_commands(self, *args, **kwargs):
        return self._parser.pack_commands(*args, **kwargs)

    def pack_command(self, *args, **kwargs):
        return self._parser.pack_command(*args, **kwargs)


class SSLConnection(Connection):

    def __init__(self, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs='required', ssl_ca_certs=None,
                 ssl_check_hostname=False, **kwargs):
        if not SSL_AVAILABLE:
            raise RedisError("Python wasn't built with SSL support")

        super().__init__(**kwargs)

        self.keyfile = ssl_keyfile
        self.certfile = ssl_certfile
        if ssl_cert_reqs is None:
            ssl_cert_reqs = ssl.CERT_NONE
        elif isinstance(ssl_cert_reqs, str):
            CERT_REQS = {
                'none': ssl.CERT_NONE,
                'optional': ssl.CERT_OPTIONAL,
                'required': ssl.CERT_REQUIRED
            }
            if ssl_cert_reqs not in CERT_REQS:
                raise RedisError(
                    "Invalid SSL Certificate Requirements Flag: %s" %
                    ssl_cert_reqs)
            ssl_cert_reqs = CERT_REQS[ssl_cert_reqs]
        self.cert_reqs = ssl_cert_reqs
        self.ca_certs = ssl_ca_certs
        self.check_hostname = ssl_check_hostname

    def _connect(self):
        "Wrap the socket with SSL support"
        sock = super()._connect()
        context = ssl.create_default_context()
        context.check_hostname = self.check_hostname
        context.verify_mode = self.cert_reqs
        if self.certfile and self.keyfile:
            context.load_cert_chain(certfile=self.certfile,
                                    keyfile=self.keyfile)
        if self.ca_certs:
            context.load_verify_locations(self.ca_certs)
        return context.wrap_socket(sock, server_hostname=self.host)


class UnixDomainSocketConnection(Connection):

    def __init__(self, path='', db=0, username=None, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 retry_on_timeout=False,
                 parser_class=DefaultParser, socket_read_size=65536,
                 health_check_interval=0, client_name=None):
        self.pid = os.getpid()
        self.path = path
        self.db = db
        self.username = username
        self.client_name = client_name
        self.password = password
        self.socket_timeout = socket_timeout
        self.retry_on_timeout = retry_on_timeout
        self.health_check_interval = health_check_interval
        self.next_health_check = 0
        self.encoder = Encoder(encoding, encoding_errors, decode_responses)
        self._sock = None
        self._parser = parser_class(encoder=self.encoder,
                                    socket_read_size=socket_read_size)
        self._connect_callbacks = []
        self._buffer_cutoff = 6000

    def repr_pieces(self):
        pieces = [
            ('path', self.path),
            ('db', self.db),
        ]
        if self.client_name:
            pieces.append(('client_name', self.client_name))
        return pieces

    def _connect(self):
        "Create a Unix domain socket connection"
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect(self.path)
        return sock

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to unix socket: %s. %s." % \
                (self.path, exception.args[0])
        else:
            return "Error %s connecting to unix socket: %s. %s." % \
                (exception.args[0], self.path, exception.args[1])


FALSE_STRINGS = ('0', 'F', 'FALSE', 'N', 'NO')


def to_bool(value):
    if value is None or value == '':
        return None
    if isinstance(value, str) and value.upper() in FALSE_STRINGS:
        return False
    return bool(value)


URL_QUERY_ARGUMENT_PARSERS = {
    'db': int,
    'socket_timeout': float,
    'socket_connect_timeout': float,
    'socket_keepalive': to_bool,
    'retry_on_timeout': to_bool,
    'max_connections': int,
    'health_check_interval': int,
    'ssl_check_hostname': to_bool,
}


def parse_url(url):
    url = urlparse(url)
    kwargs = {}

    for name, value in parse_qs(url.query).items():
        if value and len(value) > 0:
            value = unquote(value[0])
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    kwargs[name] = parser(value)
                except (TypeError, ValueError):
                    raise ValueError(
                        "Invalid value for `%s` in connection URL." % name
                    )
            else:
                kwargs[name] = value

    if url.username:
        kwargs['username'] = unquote(url.username)
    if url.password:
        kwargs['password'] = unquote(url.password)

    # We only support redis://, rediss:// and unix:// schemes.
    if url.scheme == 'unix':
        if url.path:
            kwargs['path'] = unquote(url.path)
        kwargs['connection_class'] = UnixDomainSocketConnection

    elif url.scheme in ('redis', 'rediss'):
        if url.hostname:
            kwargs['host'] = unquote(url.hostname)
        if url.port:
            kwargs['port'] = int(url.port)

        # If there's a path argument, use it as the db argument if a
        # querystring value wasn't specified
        if url.path and 'db' not in kwargs:
            try:
                kwargs['db'] = int(unquote(url.path).replace('/', ''))
            except (AttributeError, ValueError):
                pass

        if url.scheme == 'rediss':
            kwargs['connection_class'] = SSLConnection
    else:
        valid_schemes = 'redis://, rediss://, unix://'
        raise ValueError('Redis URL must specify one of the following '
                         'schemes (%s)' % valid_schemes)

    return kwargs


class ConnectionPool:
    """
    Create a connection pool. ``If max_connections`` is set, then this
    object raises :py:class:`~redis.ConnectionError` when the pool's
    limit is reached.

    By default, TCP connections are created unless ``connection_class``
    is specified. Use :py:class:`~redis.UnixDomainSocketConnection` for
    unix sockets.

    Any additional keyword arguments are passed to the constructor of
    ``connection_class``.
    """
    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a connection pool configured from the given URL.

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
        url_options = parse_url(url)
        kwargs.update(url_options)
        return cls(**kwargs)

    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections

        # a lock to protect the critical section in _checkpid().
        # this lock is acquired when the process id changes, such as
        # after a fork. during this time, multiple threads in the child
        # process could attempt to acquire this lock. the first thread
        # to acquire the lock will reset the data structures and lock
        # object of this pool. subsequent threads acquiring this lock
        # will notice the first thread already did the work and simply
        # release the lock.
        self._fork_lock = threading.Lock()
        self.reset()

    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            repr(self.connection_class(**self.connection_kwargs)),
        )

    def reset(self):
        self._lock = threading.Lock()
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def _checkpid(self):
        # _checkpid() attempts to keep ConnectionPool fork-safe on modern
        # systems. this is called by all ConnectionPool methods that
        # manipulate the pool's state such as get_connection() and release().
        #
        # _checkpid() determines whether the process has forked by comparing
        # the current process id to the process id saved on the ConnectionPool
        # instance. if these values are the same, _checkpid() simply returns.
        #
        # when the process ids differ, _checkpid() assumes that the process
        # has forked and that we're now running in the child process. the child
        # process cannot use the parent's file descriptors (e.g., sockets).
        # therefore, when _checkpid() sees the process id change, it calls
        # reset() in order to reinitialize the child's ConnectionPool. this
        # will cause the child to make all new connection objects.
        #
        # _checkpid() is protected by self._fork_lock to ensure that multiple
        # threads in the child process do not call reset() multiple times.
        #
        # there is an extremely small chance this could fail in the following
        # scenario:
        #   1. process A calls _checkpid() for the first time and acquires
        #      self._fork_lock.
        #   2. while holding self._fork_lock, process A forks (the fork()
        #      could happen in a different thread owned by process A)
        #   3. process B (the forked child process) inherits the
        #      ConnectionPool's state from the parent. that state includes
        #      a locked _fork_lock. process B will not be notified when
        #      process A releases the _fork_lock and will thus never be
        #      able to acquire the _fork_lock.
        #
        # to mitigate this possible deadlock, _checkpid() will only wait 5
        # seconds to acquire _fork_lock. if _fork_lock cannot be acquired in
        # that time it is assumed that the child is deadlocked and a
        # redis.ChildDeadlockedError error is raised.
        if self.pid != os.getpid():
            acquired = self._fork_lock.acquire(timeout=5)
            if not acquired:
                raise ChildDeadlockedError
            # reset() the instance for the new process if another thread
            # hasn't already done so
            try:
                if self.pid != os.getpid():
                    self.reset()
            finally:
                self._fork_lock.release()

    def get_connection(self, command_name, *keys, **options):
        "Get a connection from the pool"
        self._checkpid()
        with self._lock:
            try:
                connection = self._available_connections.pop()
            except IndexError:
                connection = self.make_connection()
            self._in_use_connections.add(connection)

        try:
            # ensure this connection is connected to Redis
            connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if connection.can_read():
                    raise ConnectionError('Connection has data')
            except ConnectionError:
                connection.disconnect()
                connection.connect()
                if connection.can_read():
                    raise ConnectionError('Connection not ready')
        except BaseException:
            # release the connection back to the pool so that we don't
            # leak it
            self.release(connection)
            raise

        return connection

    def get_encoder(self):
        "Return an encoder based on encoding settings"
        kwargs = self.connection_kwargs
        return Encoder(
            encoding=kwargs.get('encoding', 'utf-8'),
            encoding_errors=kwargs.get('encoding_errors', 'strict'),
            decode_responses=kwargs.get('decode_responses', False)
        )

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._checkpid()
        with self._lock:
            try:
                self._in_use_connections.remove(connection)
            except KeyError:
                # Gracefully fail when a connection is returned to this pool
                # that the pool doesn't actually own
                pass

            if self.owns_connection(connection):
                self._available_connections.append(connection)
            else:
                # pool doesn't own this connection. do not add it back
                # to the pool and decrement the count so that another
                # connection can take its place if needed
                self._created_connections -= 1
                connection.disconnect()
                return

    def owns_connection(self, connection):
        return connection.pid == self.pid

    def disconnect(self, inuse_connections=True):
        """
        Disconnects connections in the pool

        If ``inuse_connections`` is True, disconnect connections that are
        current in use, potentially by other threads. Otherwise only disconnect
        connections that are idle in the pool.
        """
        self._checkpid()
        with self._lock:
            if inuse_connections:
                connections = chain(self._available_connections,
                                    self._in_use_connections)
            else:
                connections = self._available_connections

            for connection in connections:
                connection.disconnect()


class BlockingConnectionPool(ConnectionPool):
    """
    Thread-safe blocking connection pool::

        >>> from redis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    :py:class:`~redis.ConnectionPool` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a :py:class:`~redis.ConnectionError` (as the default
    :py:class:`~redis.ConnectionPool` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        >>> # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        >>> # Raise a ``ConnectionError`` after five seconds if a connection is
        >>> # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """
    def __init__(self, max_connections=50, timeout=20,
                 connection_class=Connection, queue_class=LifoQueue,
                 **connection_kwargs):

        self.queue_class = queue_class
        self.timeout = timeout
        super().__init__(
            connection_class=connection_class,
            max_connections=max_connections,
            **connection_kwargs)

    def reset(self):
        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(self.max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

        # this must be the last operation in this method. while reset() is
        # called when holding _fork_lock, other threads in this process
        # can call _checkpid() which compares self.pid and os.getpid() without
        # holding any lock (for performance reasons). keeping this assignment
        # as the last operation ensures that those other threads will also
        # notice a pid difference and block waiting for the first thread to
        # release _fork_lock. when each of these threads eventually acquire
        # _fork_lock, they will notice that another thread already called
        # reset() and they will immediately release _fork_lock and continue on.
        self.pid = os.getpid()

    def make_connection(self):
        "Make a fresh connection."
        connection = self.connection_class(**self.connection_kwargs)
        self._connections.append(connection)
        return connection

    def get_connection(self, command_name, *keys, **options):
        """
        Get a connection, blocking for ``self.timeout`` until a connection
        is available from the pool.

        If the connection returned is ``None`` then creates a new connection.
        Because we use a last-in first-out queue, the existing connections
        (having been returned to the pool after the initial ``None`` values
        were added) will be returned before ``None`` values. This means we only
        create new connections when we need to, i.e.: the actual number of
        connections will only increase in response to demand.
        """
        # Make sure we haven't changed process.
        self._checkpid()

        # Try and get a connection from the pool. If one isn't available within
        # self.timeout then raise a ``ConnectionError``.
        connection = None
        try:
            connection = self.pool.get(block=True, timeout=self.timeout)
        except Empty:
            # Note that this is not caught by the redis client and will be
            # raised unless handled by application code. If you want never to
            raise ConnectionError("No connection available.")

        # If the ``connection`` is actually ``None`` then that's a cue to make
        # a new connection to add to the pool.
        if connection is None:
            connection = self.make_connection()

        try:
            # ensure this connection is connected to Redis
            connection.connect()
            # connections that the pool provides should be ready to send
            # a command. if not, the connection was either returned to the
            # pool before all data has been read or the socket has been
            # closed. either way, reconnect and verify everything is good.
            try:
                if connection.can_read():
                    raise ConnectionError('Connection has data')
            except ConnectionError:
                connection.disconnect()
                connection.connect()
                if connection.can_read():
                    raise ConnectionError('Connection not ready')
        except BaseException:
            # release the connection back to the pool so that we don't leak it
            self.release(connection)
            raise

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()
        if not self.owns_connection(connection):
            # pool doesn't own this connection. do not add it back
            # to the pool. instead add a None value which is a placeholder
            # that will cause the pool to recreate the connection if
            # its needed.
            connection.disconnect()
            self.pool.put_nowait(None)
            return

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # perhaps the pool has been reset() after a fork? regardless,
            # we don't want this connection
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        self._checkpid()
        for connection in self._connections:
            connection.disconnect()
