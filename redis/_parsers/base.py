import logging
import sys
from abc import ABC
from asyncio import IncompleteReadError, StreamReader, TimeoutError
from typing import Awaitable, Callable, List, Optional, Protocol, Union

from redis.maintenance_events import (
    MaintenanceEvent,
    NodeFailedOverEvent,
    NodeFailingOverEvent,
    NodeMigratedEvent,
    NodeMigratingEvent,
    NodeMovingEvent,
)

if sys.version_info.major >= 3 and sys.version_info.minor >= 11:
    from asyncio import timeout as async_timeout
else:
    from async_timeout import timeout as async_timeout

from ..exceptions import (
    AskError,
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ClusterCrossSlotError,
    ClusterDownError,
    ConnectionError,
    ExecAbortError,
    MasterDownError,
    ModuleError,
    MovedError,
    NoPermissionError,
    NoScriptError,
    OutOfMemoryError,
    ReadOnlyError,
    RedisError,
    ResponseError,
    TryAgainError,
)
from ..typing import EncodableT
from .encoders import Encoder
from .socket import SERVER_CLOSED_CONNECTION_ERROR, SocketBuffer

MODULE_LOAD_ERROR = "Error loading the extension. Please check the server logs."
NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not possible."
MODULE_EXPORTS_DATA_TYPES_ERROR = (
    "Error unloading module: the module "
    "exports one or more module-side data "
    "types, can't unload"
)
# user send an AUTH cmd to a server without authorization configured
NO_AUTH_SET_ERROR = {
    # Redis >= 6.0
    "AUTH <password> called without any password "
    "configured for the default user. Are you sure "
    "your configuration is correct?": AuthenticationError,
    # Redis < 6.0
    "Client sent AUTH, but no password is set": AuthenticationError,
}

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    EXCEPTION_CLASSES = {
        "ERR": {
            "max number of clients reached": ConnectionError,
            "invalid password": AuthenticationError,
            # some Redis server versions report invalid command syntax
            # in lowercase
            "wrong number of arguments "
            "for 'auth' command": AuthenticationWrongNumberOfArgsError,
            # some Redis server versions report invalid command syntax
            # in uppercase
            "wrong number of arguments "
            "for 'AUTH' command": AuthenticationWrongNumberOfArgsError,
            MODULE_LOAD_ERROR: ModuleError,
            MODULE_EXPORTS_DATA_TYPES_ERROR: ModuleError,
            NO_SUCH_MODULE_ERROR: ModuleError,
            MODULE_UNLOAD_NOT_POSSIBLE_ERROR: ModuleError,
            **NO_AUTH_SET_ERROR,
        },
        "OOM": OutOfMemoryError,
        "WRONGPASS": AuthenticationError,
        "EXECABORT": ExecAbortError,
        "LOADING": BusyLoadingError,
        "NOSCRIPT": NoScriptError,
        "READONLY": ReadOnlyError,
        "NOAUTH": AuthenticationError,
        "NOPERM": NoPermissionError,
        "ASK": AskError,
        "TRYAGAIN": TryAgainError,
        "MOVED": MovedError,
        "CLUSTERDOWN": ClusterDownError,
        "CROSSSLOT": ClusterCrossSlotError,
        "MASTERDOWN": MasterDownError,
    }

    @classmethod
    def parse_error(cls, response):
        "Parse an error response"
        error_code = response.split(" ")[0]
        if error_code in cls.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1 :]
            exception_class = cls.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response)
        return ResponseError(response)

    def on_disconnect(self):
        raise NotImplementedError()

    def on_connect(self, connection):
        raise NotImplementedError()


class _RESPBase(BaseParser):
    """Base class for sync-based resp parsing"""

    def __init__(self, socket_read_size):
        self.socket_read_size = socket_read_size
        self.encoder = None
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
        self._buffer = SocketBuffer(
            self._sock, self.socket_read_size, connection.socket_timeout
        )
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


class AsyncBaseParser(BaseParser):
    """Base parsing class for the python-backed async parser"""

    __slots__ = "_stream", "_read_size"

    def __init__(self, socket_read_size: int):
        self._stream: Optional[StreamReader] = None
        self._read_size = socket_read_size

    async def can_read_destructive(self) -> bool:
        raise NotImplementedError()

    async def read_response(
        self, disable_decoding: bool = False
    ) -> Union[EncodableT, ResponseError, None, List[EncodableT]]:
        raise NotImplementedError()


class MaintenanceNotificationsParser:
    """Protocol defining maintenance push notification parsing functionality"""

    @staticmethod
    def parse_maintenance_start_msg(response, notification_type):
        # Expected message format is: <event_type> <seq_number> <time>
        id = response[1]
        ttl = response[2]
        return notification_type(id, ttl)

    @staticmethod
    def parse_maintenance_completed_msg(response, notification_type):
        # Expected message format is: <event_type> <seq_number>
        id = response[1]
        return notification_type(id)

    @staticmethod
    def parse_moving_msg(response):
        # Expected message format is: MOVING <seq_number> <time> <endpoint>
        id = response[1]
        ttl = response[2]
        if response[3] is None:
            host, port = None, None
        else:
            value = response[3]
            if isinstance(value, bytes):
                value = value.decode()
            host, port = value.split(":")
            port = int(port) if port is not None else None

        return NodeMovingEvent(id, host, port, ttl)


_INVALIDATION_MESSAGE = "invalidate"
_MOVING_MESSAGE = "MOVING"
_MIGRATING_MESSAGE = "MIGRATING"
_MIGRATED_MESSAGE = "MIGRATED"
_FAILING_OVER_MESSAGE = "FAILING_OVER"
_FAILED_OVER_MESSAGE = "FAILED_OVER"

_MAINTENANCE_MESSAGES = (
    _MIGRATING_MESSAGE,
    _MIGRATED_MESSAGE,
    _FAILING_OVER_MESSAGE,
    _FAILED_OVER_MESSAGE,
)

MSG_TYPE_TO_EVENT_PARSER_MAPPING: dict[str, tuple[type[MaintenanceEvent], Callable]] = {
    _MIGRATING_MESSAGE: (
        NodeMigratingEvent,
        MaintenanceNotificationsParser.parse_maintenance_start_msg,
    ),
    _MIGRATED_MESSAGE: (
        NodeMigratedEvent,
        MaintenanceNotificationsParser.parse_maintenance_completed_msg,
    ),
    _FAILING_OVER_MESSAGE: (
        NodeFailingOverEvent,
        MaintenanceNotificationsParser.parse_maintenance_start_msg,
    ),
    _FAILED_OVER_MESSAGE: (
        NodeFailedOverEvent,
        MaintenanceNotificationsParser.parse_maintenance_completed_msg,
    ),
    _MOVING_MESSAGE: (
        NodeMovingEvent,
        MaintenanceNotificationsParser.parse_moving_msg,
    ),
}


class PushNotificationsParser(Protocol):
    """Protocol defining RESP3-specific parsing functionality"""

    pubsub_push_handler_func: Callable
    invalidation_push_handler_func: Optional[Callable] = None
    node_moving_push_handler_func: Optional[Callable] = None
    maintenance_push_handler_func: Optional[Callable] = None

    def handle_pubsub_push_response(self, response):
        """Handle pubsub push responses"""
        raise NotImplementedError()

    def handle_push_response(self, response, **kwargs):
        msg_type = response[0]
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode()

        if msg_type not in (
            _INVALIDATION_MESSAGE,
            *_MAINTENANCE_MESSAGES,
            _MOVING_MESSAGE,
        ):
            return self.pubsub_push_handler_func(response)

        try:
            if (
                msg_type == _INVALIDATION_MESSAGE
                and self.invalidation_push_handler_func
            ):
                return self.invalidation_push_handler_func(response)

            if msg_type == _MOVING_MESSAGE and self.node_moving_push_handler_func:
                parser_function = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][1]

                notification = parser_function(response)
                return self.node_moving_push_handler_func(notification)

            if msg_type in _MAINTENANCE_MESSAGES and self.maintenance_push_handler_func:
                parser_function = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][1]
                notification_type = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][0]
                notification = parser_function(response, notification_type)

                if notification is not None:
                    return self.maintenance_push_handler_func(notification)
        except Exception as e:
            logger.error(
                "Error handling {} message ({}): {}".format(msg_type, response, e)
            )

        return None

    def set_pubsub_push_handler(self, pubsub_push_handler_func):
        self.pubsub_push_handler_func = pubsub_push_handler_func

    def set_invalidation_push_handler(self, invalidation_push_handler_func):
        self.invalidation_push_handler_func = invalidation_push_handler_func

    def set_node_moving_push_handler(self, node_moving_push_handler_func):
        self.node_moving_push_handler_func = node_moving_push_handler_func

    def set_maintenance_push_handler(self, maintenance_push_handler_func):
        self.maintenance_push_handler_func = maintenance_push_handler_func


class AsyncPushNotificationsParser(Protocol):
    """Protocol defining async RESP3-specific parsing functionality"""

    pubsub_push_handler_func: Callable
    invalidation_push_handler_func: Optional[Callable] = None
    node_moving_push_handler_func: Optional[Callable[..., Awaitable[None]]] = None
    maintenance_push_handler_func: Optional[Callable[..., Awaitable[None]]] = None

    async def handle_pubsub_push_response(self, response):
        """Handle pubsub push responses asynchronously"""
        raise NotImplementedError()

    async def handle_push_response(self, response, **kwargs):
        """Handle push responses asynchronously"""

        msg_type = response[0]
        if isinstance(msg_type, bytes):
            msg_type = msg_type.decode()

        if msg_type not in (
            _INVALIDATION_MESSAGE,
            *_MAINTENANCE_MESSAGES,
            _MOVING_MESSAGE,
        ):
            return await self.pubsub_push_handler_func(response)

        try:
            if (
                msg_type == _INVALIDATION_MESSAGE
                and self.invalidation_push_handler_func
            ):
                return await self.invalidation_push_handler_func(response)

            if isinstance(msg_type, bytes):
                msg_type = msg_type.decode()

            if msg_type == _MOVING_MESSAGE and self.node_moving_push_handler_func:
                parser_function = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][1]
                notification = parser_function(response)
                return await self.node_moving_push_handler_func(notification)

            if msg_type in _MAINTENANCE_MESSAGES and self.maintenance_push_handler_func:
                parser_function = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][1]
                notification_type = MSG_TYPE_TO_EVENT_PARSER_MAPPING[msg_type][0]
                notification = parser_function(response, notification_type)

                if notification is not None:
                    return await self.maintenance_push_handler_func(notification)
        except Exception as e:
            logger.error(
                "Error handling {} message ({}): {}".format(msg_type, response, e)
            )

        return None

    def set_pubsub_push_handler(self, pubsub_push_handler_func):
        """Set the pubsub push handler function"""
        self.pubsub_push_handler_func = pubsub_push_handler_func

    def set_invalidation_push_handler(self, invalidation_push_handler_func):
        """Set the invalidation push handler function"""
        self.invalidation_push_handler_func = invalidation_push_handler_func

    def set_node_moving_push_handler(self, node_moving_push_handler_func):
        self.node_moving_push_handler_func = node_moving_push_handler_func

    def set_maintenance_push_handler(self, maintenance_push_handler_func):
        self.maintenance_push_handler_func = maintenance_push_handler_func


class _AsyncRESPBase(AsyncBaseParser):
    """Base class for async resp parsing"""

    __slots__ = AsyncBaseParser.__slots__ + ("encoder", "_buffer", "_pos", "_chunks")

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size)
        self.encoder: Optional[Encoder] = None
        self._buffer = b""
        self._chunks = []
        self._pos = 0

    def _clear(self):
        self._buffer = b""
        self._chunks.clear()

    def on_connect(self, connection):
        """Called when the stream connects"""
        self._stream = connection._reader
        if self._stream is None:
            raise RedisError("Buffer is closed.")
        self.encoder = connection.encoder
        self._clear()
        self._connected = True

    def on_disconnect(self):
        """Called when the stream disconnects"""
        self._connected = False

    async def can_read_destructive(self) -> bool:
        if not self._connected:
            raise RedisError("Buffer is closed.")
        if self._buffer:
            return True
        try:
            async with async_timeout(0):
                return self._stream.at_eof()
        except TimeoutError:
            return False

    async def _read(self, length: int) -> bytes:
        """
        Read `length` bytes of data.  These are assumed to be followed
        by a '\r\n' terminator which is subsequently discarded.
        """
        want = length + 2
        end = self._pos + want
        if len(self._buffer) >= end:
            result = self._buffer[self._pos : end - 2]
        else:
            tail = self._buffer[self._pos :]
            try:
                data = await self._stream.readexactly(want - len(tail))
            except IncompleteReadError as error:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from error
            result = (tail + data)[:-2]
            self._chunks.append(data)
        self._pos += want
        return result

    async def _readline(self) -> bytes:
        """
        read an unknown number of bytes up to the next '\r\n'
        line separator, which is discarded.
        """
        found = self._buffer.find(b"\r\n", self._pos)
        if found >= 0:
            result = self._buffer[self._pos : found]
        else:
            tail = self._buffer[self._pos :]
            data = await self._stream.readline()
            if not data.endswith(b"\r\n"):
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
            result = (tail + data)[:-2]
            self._chunks.append(data)
        self._pos += len(result) + 2
        return result
