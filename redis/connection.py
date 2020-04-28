from __future__ import unicode_literals
from distutils.version import StrictVersion
from itertools import chain
from time import time
import errno
import io
import os
import socket
import threading
import warnings

from redis._compat import (xrange, imap, unicode, long,
                           nativestr, basestring, iteritems,
                           LifoQueue, Empty, Full, urlparse, parse_qs,
                           recv, recv_into, unquote, BlockingIOError,
                           sendall, shutdown, ssl_wrap_socket)
from redis.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ChildDeadlockedError,
    ConnectionError,
    DataError,
    ExecAbortError,
    InvalidResponse,
    NoPermissionError,
    NoScriptError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    TimeoutError,
)
from redis.utils import HIREDIS_AVAILABLE

try:
    import ssl
    ssl_available = True
except ImportError:
    ssl_available = False

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {
    BlockingIOError: errno.EWOULDBLOCK,
}

if ssl_available:
    if hasattr(ssl, 'SSLWantReadError'):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

# In Python 2.7 a socket.error is raised for a nonblocking read.
# The _compat module aliases BlockingIOError to socket.error to be
# Python 2/3 compatible.
# However this means that all socket.error exceptions need to be handled
# properly within these exception handlers.
# We need to make sure socket.error is included in these handlers and
# provide a dummy error number that will never match a real exception.
if socket.error not in NONBLOCKING_EXCEPTION_ERROR_NUMBERS:
    NONBLOCKING_EXCEPTION_ERROR_NUMBERS[socket.error] = -999999

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())

if HIREDIS_AVAILABLE:
    import hiredis

    hiredis_version = StrictVersion(hiredis.__version__)
    HIREDIS_SUPPORTS_CALLABLE_ERRORS = \
        hiredis_version >= StrictVersion('0.1.3')
    HIREDIS_SUPPORTS_BYTE_BUFFER = \
        hiredis_version >= StrictVersion('0.1.4')
    HIREDIS_SUPPORTS_ENCODING_ERRORS = \
        hiredis_version >= StrictVersion('1.0.0')

    if not HIREDIS_SUPPORTS_BYTE_BUFFER:
        msg = ("redis-py works best with hiredis >= 0.1.4. You're running "
               "hiredis %s. Please consider upgrading." % hiredis.__version__)
        warnings.warn(msg)

    HIREDIS_USE_BYTE_BUFFER = True
    # only use byte buffer if hiredis supports it
    if not HIREDIS_SUPPORTS_BYTE_BUFFER:
        HIREDIS_USE_BYTE_BUFFER = False

SYM_STAR = b'*'
SYM_DOLLAR = b'$'
SYM_CRLF = b'\r\n'
SYM_EMPTY = b''

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

SENTINEL = object()


class Encoder(object):
    "Encode strings to bytes-like and decode bytes-like to strings"

    def __init__(self, encoding, encoding_errors, decode_responses):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses

    def encode(self, value):
        "Return a bytestring or bytes-like representation of the value"
        if isinstance(value, (bytes, memoryview)):
            return value
        elif isinstance(value, bool):
            # special case bool since it is a subclass of int
            raise DataError("Invalid input of type: 'bool'. Convert to a "
                            "bytes, string, int or float first.")
        elif isinstance(value, float):
            value = repr(value).encode()
        elif isinstance(value, (int, long)):
            # python 2 repr() on longs is '123L', so use str() instead
            value = str(value).encode()
        elif not isinstance(value, basestring):
            # a value we don't know how to deal with. throw an error
            typename = type(value).__name__
            raise DataError("Invalid input of type: '%s'. Convert to a "
                            "bytes, string, int or float first." % typename)
        if isinstance(value, unicode):
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


class BaseParser(object):
    EXCEPTION_CLASSES = {
        'ERR': {
            'max number of clients reached': ConnectionError,
            'Client sent AUTH, but no password is set': AuthenticationError,
            'invalid password': AuthenticationError,
            # some Redis server versions report invalid command syntax
            # in lowercase
            'wrong number of arguments for \'auth\' command':
                AuthenticationWrongNumberOfArgsError,
            # some Redis server versions report invalid command syntax
            # in uppercase
            'wrong number of arguments for \'AUTH\' command':
                AuthenticationWrongNumberOfArgsError,
        },
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
        'READONLY': ReadOnlyError,
        'NOAUTH': AuthenticationError,
        'NOPERM': NoPermissionError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(' ')[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1:]
            exception_class = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response)
        return ResponseError(response)


class SocketBuffer(object):
    def __init__(self, socket, socket_read_size, socket_timeout):
        self._sock = socket
        self.socket_read_size = socket_read_size
        self.socket_timeout = socket_timeout
        self._buffer = io.BytesIO()
        # number of bytes written to the buffer from the socket
        self.bytes_written = 0
        # number of bytes read from the buffer
        self.bytes_read = 0

    @property
    def length(self):
        return self.bytes_written - self.bytes_read

    def _read_from_socket(self, length=None, timeout=SENTINEL,
                          raise_on_timeout=True):
        sock = self._sock
        socket_read_size = self.socket_read_size
        buf = self._buffer
        buf.seek(self.bytes_written)
        marker = 0
        custom_timeout = timeout is not SENTINEL

        try:
            if custom_timeout:
                sock.settimeout(timeout)
            while True:
                data = recv(self._sock, socket_read_size)
                # an empty string indicates the server shutdown the socket
                if isinstance(data, bytes) and len(data) == 0:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
                buf.write(data)
                data_length = len(data)
                self.bytes_written += data_length
                marker += data_length

                if length is not None and length > marker:
                    continue
                return True
        except socket.timeout:
            if raise_on_timeout:
                raise TimeoutError("Timeout reading from socket")
            return False
        except NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(ex.__class__, -1)
            if not raise_on_timeout and ex.errno == allowed:
                return False
            raise ConnectionError("Error while reading from socket: %s" %
                                  (ex.args,))
        finally:
            if custom_timeout:
                sock.settimeout(self.socket_timeout)

    def can_read(self, timeout):
        return bool(self.length) or \
            self._read_from_socket(timeout=timeout,
                                   raise_on_timeout=False)

    def read(self, length):
        length = length + 2  # make sure to read the \r\n terminator
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

        # purge the buffer when we've consumed it all so it doesn't
        # grow forever
        if self.bytes_read == self.bytes_written:
            self.purge()

        return data[:-2]

    def purge(self):
        self._buffer.seek(0)
        self._buffer.truncate()
        self.bytes_written = 0
        self.bytes_read = 0

    def close(self):
        try:
            self.purge()
            self._buffer.close()
        except Exception:
            # issue #633 suggests the purge/close somehow raised a
            # BadFileDescriptor error. Perhaps the client ran out of
            # memory or something else? It's probably OK to ignore
            # any error being raised from purge/close since we're
            # removing the reference to the instance below.
            pass
        self._buffer = None
        self._sock = None


class PythonParser(BaseParser):
    "Plain Python parsing class"
    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size
        self.encoder = None
        self._sock = None
        self._buffer = None

    def __del__(self):
        self.on_disconnect()

    def on_connect(self, connection):
        "Called when the socket connects"
        self._sock = connection._sock
        self._buffer = SocketBuffer(self._sock,
                                    self.socket_read_size,
                                    connection.socket_timeout)
        self.encoder = connection.encoder

    def on_disconnect(self):
        "Called when the socket disconnects"
        self._sock = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None
        self.encoder = None

    def can_read(self, timeout):
        return self._buffer and self._buffer.can_read(timeout)

    def read_response(self):
        raw = self._buffer.readline()
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        if byte not in (b'-', b'+', b':', b'$', b'*'):
            raise InvalidResponse("Protocol Error: %r" % raw)

        # server returned an error
        if byte == b'-':
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
        elif byte == b'+':
            pass
        # int value
        elif byte == b':':
            response = long(response)
        # bulk response
        elif byte == b'$':
            length = int(response)
            if length == -1:
                return None
            response = self._buffer.read(length)
        # multi-bulk response
        elif byte == b'*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, bytes):
            response = self.encoder.decode(response)
        return response


class HiredisParser(BaseParser):
    "Parser class for connections using Hiredis"
    def __init__(self, socket_read_size):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")
        self.socket_read_size = socket_read_size

        if HIREDIS_USE_BYTE_BUFFER:
            self._buffer = bytearray(socket_read_size)

    def __del__(self):
        self.on_disconnect()

    def on_connect(self, connection):
        self._sock = connection._sock
        self._socket_timeout = connection.socket_timeout
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': self.parse_error,
        }

        # hiredis < 0.1.3 doesn't support functions that create exceptions
        if not HIREDIS_SUPPORTS_CALLABLE_ERRORS:
            kwargs['replyError'] = ResponseError

        if connection.encoder.decode_responses:
            kwargs['encoding'] = connection.encoder.encoding
        if HIREDIS_SUPPORTS_ENCODING_ERRORS:
            kwargs['errors'] = connection.encoder.encoding_errors
        self._reader = hiredis.Reader(**kwargs)
        self._next_response = False

    def on_disconnect(self):
        self._sock = None
        self._reader = None
        self._next_response = False

    def can_read(self, timeout):
        if not self._reader:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        if self._next_response is False:
            self._next_response = self._reader.gets()
            if self._next_response is False:
                return self.read_from_socket(timeout=timeout,
                                             raise_on_timeout=False)
        return True

    def read_from_socket(self, timeout=SENTINEL, raise_on_timeout=True):
        sock = self._sock
        custom_timeout = timeout is not SENTINEL
        try:
            if custom_timeout:
                sock.settimeout(timeout)
            if HIREDIS_USE_BYTE_BUFFER:
                bufflen = recv_into(self._sock, self._buffer)
                if bufflen == 0:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
                self._reader.feed(self._buffer, 0, bufflen)
            else:
                buffer = recv(self._sock, self.socket_read_size)
                # an empty string indicates the server shutdown the socket
                if not isinstance(buffer, bytes) or len(buffer) == 0:
                    raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
                self._reader.feed(buffer)
            # data was read from the socket and added to the buffer.
            # return True to indicate that data was read.
            return True
        except socket.timeout:
            if raise_on_timeout:
                raise TimeoutError("Timeout reading from socket")
            return False
        except NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(ex.__class__, -1)
            if not raise_on_timeout and ex.errno == allowed:
                return False
            raise ConnectionError("Error while reading from socket: %s" %
                                  (ex.args,))
        finally:
            if custom_timeout:
                sock.settimeout(self._socket_timeout)

    def read_response(self):
        if not self._reader:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        # _next_response might be cached from a can_read() call
        if self._next_response is not False:
            response = self._next_response
            self._next_response = False
            return response

        response = self._reader.gets()
        while response is False:
            self.read_from_socket()
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
        self._parser = parser_class(socket_read_size=socket_read_size)
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
        self.disconnect()

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
                    for k, v in iteritems(self.socket_keepalive_options):
                        sock.setsockopt(socket.IPPROTO_TCP, k, v)

                # set the socket_connect_timeout before we connect
                sock.settimeout(self.socket_connect_timeout)

                # connect
                sock.connect(socket_address)

                # set the socket_timeout now that we're connected
                sock.settimeout(self.socket_timeout)
                return sock

            except socket.error as _:
                err = _
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        raise socket.error("socket.getaddrinfo returned an empty list")

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

            if nativestr(auth_response) != 'OK':
                raise AuthenticationError('Invalid Username or Password')

        # if a client_name is given, set it
        if self.client_name:
            self.send_command('CLIENT', 'SETNAME', self.client_name)
            if nativestr(self.read_response()) != 'OK':
                raise ConnectionError('Error setting client name')

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
            if os.getpid() == self.pid:
                shutdown(self._sock, socket.SHUT_RDWR)
            self._sock.close()
        except socket.error:
            pass
        self._sock = None

    def check_health(self):
        "Check the health of the connection with a PING/PONG"
        if self.health_check_interval and time() > self.next_health_check:
            try:
                self.send_command('PING', check_health=False)
                if nativestr(self.read_response()) != 'PONG':
                    raise ConnectionError(
                        'Bad response from PING health check')
            except (ConnectionError, TimeoutError):
                self.disconnect()
                self.send_command('PING', check_health=False)
                if nativestr(self.read_response()) != 'PONG':
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
                sendall(self._sock, item)
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
        self.send_packed_command(self.pack_command(*args),
                                 check_health=kwargs.get('check_health', True))

    def can_read(self, timeout=0):
        "Poll the socket to see if there's data that can be read."
        sock = self._sock
        if not sock:
            self.connect()
            sock = self._sock
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
            self.disconnect()
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

    def pack_command(self, *args):
        "Pack a series of arguments into the Redis protocol"
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        if isinstance(args[0], unicode):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b' ' in args[0]:
            args = tuple(args[0].split()) + args[1:]

        buff = SYM_EMPTY.join((SYM_STAR, str(len(args)).encode(), SYM_CRLF))

        buffer_cutoff = self._buffer_cutoff
        for arg in imap(self.encoder.encode, args):
            # to avoid large string mallocs, chunk the command into the
            # output list if we're sending large values or memoryviews
            arg_length = len(arg)
            if (len(buff) > buffer_cutoff or arg_length > buffer_cutoff
                    or isinstance(arg, memoryview)):
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(), SYM_CRLF))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join(
                    (buff, SYM_DOLLAR, str(arg_length).encode(),
                     SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def pack_commands(self, commands):
        "Pack multiple commands into the Redis protocol"
        output = []
        pieces = []
        buffer_length = 0
        buffer_cutoff = self._buffer_cutoff

        for cmd in commands:
            for chunk in self.pack_command(*cmd):
                chunklen = len(chunk)
                if (buffer_length > buffer_cutoff or chunklen > buffer_cutoff
                        or isinstance(chunk, memoryview)):
                    output.append(SYM_EMPTY.join(pieces))
                    buffer_length = 0
                    pieces = []

                if chunklen > buffer_cutoff or isinstance(chunk, memoryview):
                    output.append(chunk)
                else:
                    pieces.append(chunk)
                    buffer_length += chunklen

        if pieces:
            output.append(SYM_EMPTY.join(pieces))
        return output


class SSLConnection(Connection):

    def __init__(self, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs='required', ssl_ca_certs=None,
                 ssl_check_hostname=False, **kwargs):
        if not ssl_available:
            raise RedisError("Python wasn't built with SSL support")

        super(SSLConnection, self).__init__(**kwargs)

        self.keyfile = ssl_keyfile
        self.certfile = ssl_certfile
        if ssl_cert_reqs is None:
            ssl_cert_reqs = ssl.CERT_NONE
        elif isinstance(ssl_cert_reqs, basestring):
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
        sock = super(SSLConnection, self)._connect()
        if hasattr(ssl, "create_default_context"):
            context = ssl.create_default_context()
            context.check_hostname = self.check_hostname
            context.verify_mode = self.cert_reqs
            if self.certfile and self.keyfile:
                context.load_cert_chain(certfile=self.certfile,
                                        keyfile=self.keyfile)
            if self.ca_certs:
                context.load_verify_locations(self.ca_certs)
            sock = ssl_wrap_socket(context, sock, server_hostname=self.host)
        else:
            # In case this code runs in a version which is older than 2.7.9,
            # we want to fall back to old code
            sock = ssl_wrap_socket(ssl,
                                   sock,
                                   cert_reqs=self.cert_reqs,
                                   keyfile=self.keyfile,
                                   certfile=self.certfile,
                                   ca_certs=self.ca_certs)
        return sock


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
        self._parser = parser_class(socket_read_size=socket_read_size)
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
    if isinstance(value, basestring) and value.upper() in FALSE_STRINGS:
        return False
    return bool(value)


URL_QUERY_ARGUMENT_PARSERS = {
    'socket_timeout': float,
    'socket_connect_timeout': float,
    'socket_keepalive': to_bool,
    'retry_on_timeout': to_bool,
    'max_connections': int,
    'health_check_interval': int,
    'ssl_check_hostname': to_bool,
}


class ConnectionPool(object):
    "Generic connection pool"
    @classmethod
    def from_url(cls, url, db=None, decode_components=False, **kwargs):
        """
        Return a connection pool configured from the given URL.

        For example::

            redis://[[username]:[password]]@localhost:6379/0
            rediss://[[username]:[password]]@localhost:6379/0
            unix://[[username]:[password]]@/path/to/socket.sock?db=0

        Three URL schemes are supported:

        - ```redis://``
          <https://www.iana.org/assignments/uri-schemes/prov/redis>`_ creates a
          normal TCP socket connection
        - ```rediss://``
          <https://www.iana.org/assignments/uri-schemes/prov/rediss>`_ creates
          a SSL wrapped TCP socket connection
        - ``unix://`` creates a Unix Domain Socket connection

        There are several ways to specify a database number. The parse function
        will return the first specified option:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// scheme, the path argument of the url, e.g.
               redis://localhost/0
            3. The ``db`` argument to this function.

        If none of these options are specified, db=0 is used.

        The ``decode_components`` argument allows this function to work with
        percent-encoded URLs. If this argument is set to ``True`` all ``%xx``
        escapes will be replaced by their single-character equivalents after
        the URL has been parsed. This only applies to the ``hostname``,
        ``path``, ``username`` and ``password`` components.

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. The querystring
        arguments ``socket_connect_timeout`` and ``socket_timeout`` if supplied
        are parsed as float values. The arguments ``socket_keepalive`` and
        ``retry_on_timeout`` are parsed to boolean values that accept
        True/False, Yes/No values to indicate state. Invalid types cause a
        ``UserWarning`` to be raised. In the case of conflicting arguments,
        querystring arguments always win.

        """
        url = urlparse(url)
        url_options = {}

        for name, value in iteritems(parse_qs(url.query)):
            if value and len(value) > 0:
                parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
                if parser:
                    try:
                        url_options[name] = parser(value[0])
                    except (TypeError, ValueError):
                        warnings.warn(UserWarning(
                            "Invalid value for `%s` in connection URL." % name
                        ))
                else:
                    url_options[name] = value[0]

        if decode_components:
            username = unquote(url.username) if url.username else None
            password = unquote(url.password) if url.password else None
            path = unquote(url.path) if url.path else None
            hostname = unquote(url.hostname) if url.hostname else None
        else:
            username = url.username or None
            password = url.password or None
            path = url.path
            hostname = url.hostname

        # We only support redis://, rediss:// and unix:// schemes.
        if url.scheme == 'unix':
            url_options.update({
                'username': username,
                'password': password,
                'path': path,
                'connection_class': UnixDomainSocketConnection,
            })

        elif url.scheme in ('redis', 'rediss'):
            url_options.update({
                'host': hostname,
                'port': int(url.port or 6379),
                'username': username,
                'password': password,
            })

            # If there's a path argument, use it as the db argument if a
            # querystring value wasn't specified
            if 'db' not in url_options and path:
                try:
                    url_options['db'] = int(path.replace('/', ''))
                except (AttributeError, ValueError):
                    pass

            if url.scheme == 'rediss':
                url_options['connection_class'] = SSLConnection
        else:
            valid_schemes = ', '.join(('redis://', 'rediss://', 'unix://'))
            raise ValueError('Redis URL must specify one of the following '
                             'schemes (%s)' % valid_schemes)

        # last shot at the db value
        url_options['db'] = int(url_options.get('db', db or 0))

        # update the arguments from the URL values
        kwargs.update(url_options)

        # backwards compatability
        if 'charset' in kwargs:
            warnings.warn(DeprecationWarning(
                '"charset" is deprecated. Use "encoding" instead'))
            kwargs['encoding'] = kwargs.pop('charset')
        if 'errors' in kwargs:
            warnings.warn(DeprecationWarning(
                '"errors" is deprecated. Use "encoding_errors" instead'))
            kwargs['encoding_errors'] = kwargs.pop('errors')

        return cls(**kwargs)

    def __init__(self, connection_class=Connection, max_connections=None,
                 **connection_kwargs):
        """
        Create a connection pool. If max_connections is set, then this
        object raises redis.ConnectionError when the pool's limit is reached.

        By default, TCP connections are created unless connection_class is
        specified. Use redis.UnixDomainSocketConnection for unix sockets.

        Any additional keyword arguments are passed to the constructor of
        connection_class.
        """
        max_connections = max_connections or 2 ** 31
        if not isinstance(max_connections, (int, long)) or max_connections < 0:
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
        self._lock = threading.RLock()
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
            # python 2.7 doesn't support a timeout option to lock.acquire()
            # we have to mimic lock timeouts ourselves.
            timeout_at = time() + 5
            acquired = False
            while time() < timeout_at:
                acquired = self._fork_lock.acquire(False)
                if acquired:
                    break
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
            if connection.pid != self.pid:
                return
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)

    def disconnect(self):
        "Disconnects all connections in the pool"
        self._checkpid()
        with self._lock:
            all_conns = chain(self._available_connections,
                              self._in_use_connections)
            for connection in all_conns:
                connection.disconnect()


class BlockingConnectionPool(ConnectionPool):
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
    def __init__(self, max_connections=50, timeout=20,
                 connection_class=Connection, queue_class=LifoQueue,
                 **connection_kwargs):

        self.queue_class = queue_class
        self.timeout = timeout
        super(BlockingConnectionPool, self).__init__(
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
        if connection.pid != self.pid:
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
