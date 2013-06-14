from itertools import chain
import os
import socket
import sys

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


SYM_STAR = b('*')
SYM_DOLLAR = b('$')
SYM_CRLF = b('\r\n')
SYM_LF = b('\n')


class PythonParser(object):
    "Plain Python parsing class"
    MAX_READ_LENGTH = 1000000
    encoding = None

    EXCEPTION_CLASSES = {
        'ERR': ResponseError,
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
    }

    def __init__(self):
        self._fp = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._fp = connection._sock.makefile('rb')
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    def read(self, length=None):
        """
        Read a line from the socket if no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        try:
            if length is not None:
                bytes_left = length + 2  # read the line ending
                if length > self.MAX_READ_LENGTH:
                    # apparently reading more than 1MB or so from a windows
                    # socket can cause MemoryErrors. See:
                    # https://github.com/andymccurdy/redis-py/issues/205
                    # read smaller chunks at a time to work around this
                    try:
                        buf = BytesIO()
                        while bytes_left > 0:
                            read_len = min(bytes_left, self.MAX_READ_LENGTH)
                            buf.write(self._fp.read(read_len))
                            bytes_left -= read_len
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()
                return self._fp.read(bytes_left)[:-2]

            # no length, read a full line
            return self._fp.readline()[:-2]
        except (socket.error, socket.timeout):
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            return self.EXCEPTION_CLASSES[error_code](response)
        return ResponseError(response)

    def read_response(self):
        response = self.read()
        if not response:
            raise ConnectionError("Socket closed on remote end")

        byte, response = byte_to_chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error")

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
            response = self.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response


class HiredisParser(object):
    "Parser class for connections using Hiredis"
    def __init__(self):
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
            'replyError': ResponseError,
        }
        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def read_response(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")
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
            # buffer. if the data received doesn't end with \n, there's more.
            if not buffer.endswith(SYM_LF):
                continue
            response = self._reader.gets()
        return response

if HIREDIS_AVAILABLE:
    DefaultParser = HiredisParser
else:
    DefaultParser = PythonParser


class Connection(object):
    "Manages TCP communication to and from a Redis server"
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

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

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
        self.on_connect()

    def _connect(self):
        "Create a TCP socket connection"
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
        except Exception:
            self.disconnect()
            raise

    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
        except Exception:
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
        output = SYM_STAR + b(str(len(args))) + SYM_CRLF
        for enc_value in imap(self.encode, args):
            output += SYM_DOLLAR
            output += b(str(len(enc_value)))
            output += SYM_CRLF
            output += enc_value
            output += SYM_CRLF
        return output


class UnixDomainSocketConnection(Connection):
    def __init__(self, path='', db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=DefaultParser):
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


# TODO: add ability to block waiting on a connection to be released
class ConnectionPool(object):
    "Generic connection pool"
    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        self.pid = os.getpid()
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.max_connections = max_connections or 2 ** 31
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def _checkpid(self):
        if self.pid != os.getpid():
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

    def _checkpid(self):
        """
        Check the current process id.  If it has changed, disconnect and
        re-instantiate this connection pool instance.
        """
        # Get the current process id.
        pid = os.getpid()

        # If it hasn't changed since we were instantiated, then we're fine, so
        # just exit, remaining connected.
        if self.pid == pid:
            return

        # If it has changed, then disconnect and re-instantiate.
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
