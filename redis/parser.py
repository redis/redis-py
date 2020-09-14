import errno
import io
import socket

from .exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ConnectionError,
    ExecAbortError,
    InvalidResponse,
    NoPermissionError,
    NoScriptError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    ModuleError,
)
from .features import HIREDIS_AVAILABLE, SSL_AVAILABLE
from .utils import DEFAULT

if HIREDIS_AVAILABLE:
    import hiredis


# constants used in redis protocol packing and parsing
SYM_STAR = b'*'
SYM_DOLLAR = b'$'
SYM_CRLF = b'\r\n'
SYM_EMPTY = b''

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."

# sockets in nonblocking mode raise an exception when the socket would
# otherwise block (e.g., trying to read from a socket that has no data
# to be read). plain sockets raise BlockingIOError while sockets enabled with
# ssl support raise various exceptions defined in the ssl module.
# define a dict of exception class => error number that includes all
# exceptions and error numbers raised when the socket would block but is
# otherwise healthy.
NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {
    BlockingIOError: errno.EWOULDBLOCK,
}

if SSL_AVAILABLE:
    import ssl
    if hasattr(ssl, 'SSLWantReadError'):
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantReadError] = 2
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLWantWriteError] = 2
    else:
        NONBLOCKING_EXCEPTION_ERROR_NUMBERS[ssl.SSLError] = 2

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())


class BaseParser:
    max_buffer_size = 6000

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
            'Error loading the extension. Please check the server logs.':
                ModuleError,
            ('Error unloading module: the module exports one or more '
             'module-side data types, can\'t unload'):
                ModuleError,
            'Error unloading module: no such module with that name':
                ModuleError,
            'Error unloading module: operation not possible.': ModuleError,
        },
        'EXECABORT': ExecAbortError,
        'LOADING': BusyLoadingError,
        'NOSCRIPT': NoScriptError,
        'READONLY': ReadOnlyError,
        'NOAUTH': AuthenticationError,
        'NOPERM': NoPermissionError,
    }

    def __init__(self, encoder, socket_read_size):
        self.encoder = encoder
        self.socket_read_size = socket_read_size

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

    def on_connect(self, connection):
        pass

    def on_disconnect(self):
        pass

    def can_read(self, timeout):
        raise NotImplementedError

    def parse_response(self):
        raise NotImplementedError

    def pack_command(self, *args, output=None):
        "Pack a series of arguments into the Redis protocol"
        if output is None:
            output = []

        # if we already have output and the last element is a bytes object
        # we can start appending directly to it
        if output and isinstance(output[-1], bytes):
            buff = output.pop()
        else:
            buff = SYM_EMPTY

        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. These arguments should be bytestrings so that they are
        # not encoded.
        if isinstance(args[0], str):
            args = tuple(args[0].encode().split()) + args[1:]
        elif b' ' in args[0]:
            args = tuple(args[0].split()) + args[1:]

        max_buffer_size = self.max_buffer_size
        num_args = str(len(args)).encode()
        buff = SYM_EMPTY.join((buff, SYM_STAR, num_args, SYM_CRLF))

        for arg in map(self.encoder.encode, args):
            # creating small-ish (< 6kb or so) byte strings via bytes.join()
            # is faster on most platforms than appending all the pieces to a
            # list and calling join later. grow the buffer until it hits the
            # max_buffer_size threshold, then append it to the output. upon
            # encountering a large value or memoryview, append it directly to
            # avoid unnecessary mallocs
            arg_length = len(arg)
            if (len(buff) > max_buffer_size
                    or arg_length > max_buffer_size
                    or isinstance(arg, memoryview)):
                buff = SYM_EMPTY.join((
                    buff,
                    SYM_DOLLAR,
                    str(arg_length).encode(),
                    SYM_CRLF
                ))
                output.append(buff)
                output.append(arg)
                buff = SYM_CRLF
            else:
                buff = SYM_EMPTY.join((
                    buff,
                    SYM_DOLLAR,
                    str(arg_length).encode(),
                    SYM_CRLF,
                    arg,
                    SYM_CRLF
                ))
        output.append(buff)
        return output

    def pack_commands(self, commands):
        "Pack multiple commands into the Redis protocol"
        output = []

        for command in commands:
            output = self.pack_command(*command, output=output)
        return output


class SocketBuffer:
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

    def _read_from_socket(self, length=None, timeout=DEFAULT,
                          raise_on_timeout=True):
        sock = self._sock
        socket_read_size = self.socket_read_size
        buf = self._buffer
        buf.seek(self.bytes_written)
        marker = 0
        custom_timeout = timeout is not DEFAULT

        try:
            if custom_timeout:
                sock.settimeout(timeout)
            while True:
                data = self._sock.recv(socket_read_size)
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
    def __init__(self, encoder, socket_read_size):
        super().__init__(encoder, socket_read_size)
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
        self._buffer = SocketBuffer(self._sock,
                                    self.socket_read_size,
                                    connection.socket_timeout)

    def on_disconnect(self):
        "Called when the socket disconnects"
        self._sock = None
        if self._buffer is not None:
            self._buffer.close()
            self._buffer = None

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
            response = response.decode('utf-8', errors='replace')
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
            response = int(response)
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
            response = [self.read_response() for i in range(length)]
        if isinstance(response, bytes):
            response = self.encoder.decode(response)
        return response


class HiredisParser(BaseParser):
    "Parser class for connections using Hiredis"
    def __init__(self, encoder, socket_read_size):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")
        super().__init__(encoder, socket_read_size)
        self._buffer = bytearray(socket_read_size)

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        self._sock = connection._sock
        self._socket_timeout = connection.socket_timeout
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': self.parse_error,
            'errors': self.encoder.encoding_errors,
        }

        if connection.encoder.decode_responses:
            kwargs['encoding'] = self.encoder.encoding

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

    def read_from_socket(self, timeout=DEFAULT, raise_on_timeout=True):
        sock = self._sock
        custom_timeout = timeout is not DEFAULT
        try:
            if custom_timeout:
                sock.settimeout(timeout)
            bufflen = self._sock.recv_into(self._buffer)
            if bufflen == 0:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
            self._reader.feed(self._buffer, 0, bufflen)
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
