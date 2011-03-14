import errno
import socket
import threading
from redis.exceptions import ConnectionError, ResponseError, InvalidResponse

class BaseConnection(object):
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
            sock.settimeout(self.socket_timeout)
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
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." % \
                (_errno, errmsg))

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

class PythonConnection(BaseConnection):
    def read_response(self, command_name, catch_errors):
        response = self.read()[:-2] # strip last two characters (\r\n)
        if not response:
            self.disconnect()
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
            return long(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = length and self.read(length) or ''
            self.read(2) # read the \r\n delimiter
            return response
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            if not catch_errors:
                return [self.read_response(command_name, catch_errors)
                    for i in range(length)]
            else:
                # for pipelines, we need to read everything,
                # including response errors. otherwise we'd
                # completely mess up the receive buffer
                data = []
                for i in range(length):
                    try:
                        data.append(
                            self.read_response(command_name, catch_errors)
                            )
                    except Exception, e:
                        data.append(e)
                return data

        raise InvalidResponse("Unknown response type for: %s" % command_name)

class HiredisConnection(BaseConnection):
    def connect(self, redis_instance):
        if self._sock == None:
            self._reader = hiredis.Reader(
                    protocolError=InvalidResponse,
                    replyError=ResponseError)
        super(HiredisConnection, self).connect(redis_instance)

    def disconnect(self):
        if self._sock:
            self._reader = None
        super(HiredisConnection, self).disconnect()

    def read_response(self, command_name, catch_errors):
        response = self._reader.gets()
        while response is False:
            buffer = self._sock.recv(4096)
            if not buffer:
                self.disconnect()
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            response = self._reader.gets()

        if isinstance(response, ResponseError):
            raise response

        return response

try:
    import hiredis
    Connection = HiredisConnection
except ImportError:
    Connection = PythonConnection

class ConnectionPool(threading.local):
    "Manages a list of connections on the local thread"
    def __init__(self, connection_class=None):
        self.connections = {}
        self.connection_class = connection_class or Connection

    def make_connection_key(self, host, port, db):
        "Create a unique key for the specified host, port and db"
        return '%s:%s:%s' % (host, port, db)

    def get_connection(self, host, port, db, password, socket_timeout):
        "Return a specific connection for the specified host, port and db"
        key = self.make_connection_key(host, port, db)
        if key not in self.connections:
            self.connections[key] = self.connection_class(
                host, port, db, password, socket_timeout)
        return self.connections[key]

    def get_all_connections(self):
        "Return a list of all connection objects the manager knows about"
        return self.connections.values()
