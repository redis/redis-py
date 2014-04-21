from __future__ import with_statement
from distutils.version import StrictVersion
from itertools import chain
from select import select
import os
import socket
import sys
import threading
import warnings

from redis._compat import (b, xrange, imap, byte_to_chr, unicode, bytes, long,
                           BytesIO, nativestr, basestring,
                           LifoQueue, Empty, Full)
from redis.exceptions import (
    RedisError,
    ConnectionError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
)
from redis.utils import HIREDIS_AVAILABLE
if HIREDIS_AVAILABLE:
    import hiredis

    hiredis_version = StrictVersion(hiredis.__version__)
    HIREDIS_SUPPORTS_CALLABLE_ERRORS = \
        hiredis_version >= StrictVersion('0.1.3')

    if not HIREDIS_SUPPORTS_CALLABLE_ERRORS:
        msg = ('redis-py works best with hiredis >= 0.1.3. Your hiredis '
               'is %s. Please consider upgrading.' % hiredis.__version__)
        warnings.warn(msg)

SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_EMPTY = b('')


class BaseParser(object):
    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            return self.EXCEPTION_CLASSES[error_code](response)
        return ResponseError(response)


class SocketBuffer(object):
    def __init__(self, socket):
        self._sock = socket
        self._buffer = BytesIO()
        # number of bytes written to the buffer from the socket
        self.bytes_written = 0
        # number of bytes read from the buffer
        self.bytes_read = 0

    @property
    def length(self):
        return self.bytes_written - self.bytes_read

    def _read_from_socket(self, length=None):
        chunksize = 8192
        if length is None:
            length = chunksize

        buf = self._buffer
        buf.seek(self.bytes_written)
        marker = 0

        try:
            while length > marker:
                data = self._sock.recv(chunksize)
                buf.write(data)
                self.bytes_written += len(data)
                marker += chunksize
        except (socket.error, socket.timeout):
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

    def read(self, length):
        length += 2  # make sure to read the \r\n terminator
        # make sure we've read enough data from the socket
        if length > self.length:
            self._read_from_socket(length - self.length)

        self._buffer.seek(self.bytes_read)
        data = self._buffer.read(length)
        self.bytes_read += len(data)

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    def readline(self):
        buf = self._buffer
        buf.seek(self.bytes_read)
        data = buf.readline()
        while not data.endswith(SYM_CRLF):
            # there's more data in the socket that we need
            self._read_from_socket()
            buf.seek(self.bytes_read)
            data = buf.readline()

        self.bytes_read += len(data)
        return data[:-2]

    def purge(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0

    def close(self):
        self.purge()
        self._buffer.close()
        self._buffer = None
        self._sock = None


class PythonParser(BaseParser):
    "Plain Python parsing class"
    encoding = None

    def __init__(self):
        self._sock = None
        self._buffer = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._sock = connection._sock
        self._buffer = SocketBuffer(self._sock)
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"
        if self._sock is not None:
            self._sock.close()
            self._sock = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoding = None

    def can_read(self):
        return self._buffer and bool(self._buffer.length)

    def read_response(self):
        response = self._buffer.readline()
        if not response:
            raise ConnectionError("Socket closed on remote end")

        byte, response = byte_to_chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error: %s, %s" %
                                  (str(byte), str(response)))

        # server returned an error
        if byte == '-':
            response = nativestr(response)
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == '+':
            pass
        # int value
        elif byte == ':':
            response = long(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = self._buffer.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response


class HiredisParser(BaseParser):
    "Parser class for connections using Hiredis"
    def __init__(self):
        self._next_response = False
        self._reader = None
        self._sock = None
        self._next_response = False
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        self._sock = connection._sock
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': self.parse_error,
        }

        if not HIREDIS_SUPPORTS_CALLABLE_ERRORS:
            kwargs['replyError'] = ResponseError

        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        self._next_response = False

    def can_read(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")

        if self._next_response is False:
            self._next_response = self._reader.gets()
        return self._next_response is not False

    def read_response(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")

        # _next_response might be cached from a can_read() call
        if self._next_response is not False:
            response = self._next_response
            return response

        response = self._reader.gets()
        while response is False:
            try:
                buffer = self._sock.recv(4096)
            except (socket.error, socket.timeout):
                e = sys.exc_info()[1]
                raise ConnectionError("Error while reading from socket: %s" %
                                      (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # proactively, but not conclusively, check if more data is in the
            # buffer. if the data received doesn't end with \r\n, there's more.
            if not buffer.endswith(SYM_CRLF):
                continue
            response = self._reader.gets()
        # if an older version of hiredis is installed, we need to attempt
        # to convert ResponseErrors to their appropriate types.
        if not HIREDIS_SUPPORTS_CALLABLE_ERRORS:
            if isinstance(response, ResponseError):
                response = self.parse_error(response.args[0])
            elif isinstance(response, list) and response and \
                    isinstance(response[0], ResponseError):
                response[0] = self.parse_error(response[0].args[0])
        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif isinstance(response, list) and response and \
                isinstance(response[0], ConnectionError):
            raise response[0]
        return response

if HIREDIS_AVAILABLE:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser


class Connection(object):
    "Manages TCP communication to and from a Redis server"
    description_format = "Connection<host=%(host)s,port=%(port)s,db=%(db)s>"

    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser):
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = parser_class()
        self._description_args = {
            'host': self.host,
            'port': self.port,
            'db': self.db,
        }
        self._connect_callbacks = []

    def __repr__(self):
        return self.description_format % self._description_args

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
        except socket.error:
            e = sys.exc_info()[1]
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
        # in 2.6+ try to use IPv6/4 compatibility, else just original code
        if hasattr(socket, 'create_connection'):
            sock = socket.create_connection((self.host, self.port),
                                            self.socket_timeout)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.socket_timeout)
            sock.connect((self.host, self.port))
        return sock

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)

        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            if nativestr(self.read_response()) != 'OK':
                raise AuthenticationError('Invalid Password')

        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            if nativestr(self.read_response()) != 'OK':
                raise ConnectionError('Invalid Database')

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._sock is None:
            return
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._sock:
            self.connect()
        try:
            self._sock.sendall(command)
        except socket.error:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (_errno, errmsg))
        except:
            self.disconnect()
            raise

    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

    def can_read(self):
        "Poll the socket to see if there's data that can be read."
        sock = self._sock
        if not sock:
            self.connect()
            sock = self._sock
        return bool(select([sock], [], [], 0)[0]) or self._parser.can_read()

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response

    def encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, bytes):
            return value
        if isinstance(value, float):
            value = repr(value)
        if not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        args_output = SYM_EMPTY.join([
            SYM_EMPTY.join((SYM_DOLLAR, b(str(len(k))), SYM_CRLF, k, SYM_CRLF))
            for k in imap(self.encode, args)])
        output = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF, args_output))
        return output


class UnixDomainSocketConnection(Connection):
    description_format = "UnixDomainSocketConnection<path=%(path)s,db=%(db)s>"

    def __init__(self, path='', db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser):
        super(UnixDomainSocketConnection, self).__init__()
        self.pid = os.getpid()
        self.path = path
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self._sock = None
        self._parser = parser_class()
        self._description_args = {
            'path': self.path,
            'db': self.db,
        }
        self._connect_callbacks = []

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


class ConnectionPool(object):
    "Generic connection pool"
    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        """
        Create a connection pool. If max_connections is set, then this
        object raises redis.ConnectionError when the pool's limit is reached.

        By default, TCP connections are created connection_class is specified.
        Use redis.UnixDomainSocketConnection for unix sockets.

        Any additional keyword arguments are passed to the constructor of
        connection_class.
        """
        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()
        self._check_lock = threading.Lock()

    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def _checkpid(self):
        if self.pid != os.getpid():
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.__init__(self.connection_class, self.max_connections,
                              **self.connection_kwargs)

    def get_connection(self, command_name, *keys, **options):
        "Get a connection from the pool"
        self._checkpid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        "Create a new connection"
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        return self.connection_class(**self.connection_kwargs)

    def release(self, connection):
        "Releases the connection back to the pool"
        self._checkpid()
        if connection.pid == self.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        all_conns = chain(self._available_connections,
                          self._in_use_connections)
        for connection in all_conns:
            connection.disconnect()


class BlockingConnectionPool(object):
    """
    Thread-safe blocking connection pool::

        >>> from redis.client import Redis
        >>> client = Redis(connection_pool=BlockingConnectionPool())

    It performs the same function as the default
    ``:py:class: ~redis.connection.ConnectionPool`` implementation, in that,
    it maintains a pool of reusable connections that can be shared by
    multiple redis clients (safely across threads if required).

    The difference is that, in the event that a client tries to get a
    connection from the pool when all of connections are in use, rather than
    raising a ``:py:class: ~redis.exceptions.ConnectionError`` (as the default
    ``:py:class: ~redis.connection.ConnectionPool`` implementation does), it
    makes the client wait ("blocks") for a specified number of seconds until
    a connection becomes available.

    Use ``max_connections`` to increase / decrease the pool size::

        >>> pool = BlockingConnectionPool(max_connections=10)

    Use ``timeout`` to tell it either how many seconds to wait for a connection
    to become available, or to block forever:

        # Block forever.
        >>> pool = BlockingConnectionPool(timeout=None)

        # Raise a ``ConnectionError`` after five seconds if a connection is
        # not available.
        >>> pool = BlockingConnectionPool(timeout=5)
    """
    def __init__(self, max_connections=50, timeout=20, connection_class=None,
                 queue_class=None, **connection_kwargs):
        "Compose and assign values."
        # Compose.
        if connection_class is None:
            connection_class = Connection
        if queue_class is None:
            queue_class = LifoQueue

        # Assign.
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.queue_class = queue_class
        self.max_connections = max_connections
        self.timeout = timeout

        # Validate the ``max_connections``.  With the "fill up the queue"
        # algorithm we use, it must be a positive integer.
        is_valid = isinstance(max_connections, int) and max_connections > 0
        if not is_valid:
            raise ValueError('``max_connections`` must be a positive integer')

        # Get the current process id, so we can disconnect and reinstantiate if
        # it changes.
        self.pid = os.getpid()
        self._check_lock = threading.Lock()

        # Create and fill up a thread safe queue with ``None`` values.
        self.pool = self.queue_class(max_connections)
        while True:
            try:
                self.pool.put_nowait(None)
            except Full:
                break

        # Keep a list of actual connection instances so that we can
        # disconnect them later.
        self._connections = []

    def __repr__(self):
        return "%s<%s>" % (
            type(self).__name__,
            self.connection_class.description_format % self.connection_kwargs,
        )

    def _checkpid(self):
        """
        Check the current process id.  If it has changed, disconnect and
        re-instantiate this connection pool instance.
        """
        pid = os.getpid()
        if self.pid != pid:
            with self._check_lock:
                if self.pid == os.getpid():
                    # another thread already did the work while we waited
                    # on the lock.
                    return
                self.disconnect()
                self.reinstantiate()

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

        return connection

    def release(self, connection):
        "Releases the connection back to the pool."
        # Make sure we haven't changed process.
        self._checkpid()

        # Put the connection back into the pool.
        try:
            self.pool.put_nowait(connection)
        except Full:
            # This shouldn't normally happen but might perhaps happen after a
            # reinstantiation. So, we can handle the exception by not putting
            # the connection back on the pool, because we definitely do not
            # want to reuse it.
            pass

    def disconnect(self):
        "Disconnects all connections in the pool."
        for connection in self._connections:
            connection.disconnect()

    def reinstantiate(self):
        """
        Reinstatiate this instance within a new process with a new connection
        pool set.
        """
        self.__init__(max_connections=self.max_connections,
                      timeout=self.timeout,
                      connection_class=self.connection_class,
                      queue_class=self.queue_class, **self.connection_kwargs)
