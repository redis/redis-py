import select
import selectors
import socket
from logging import getLogger
from typing import Callable, List, Optional, TypedDict, Union

from ..exceptions import ConnectionError, InvalidResponse, RedisError, TimeoutError
from ..typing import EncodableT
from ..utils import HIREDIS_AVAILABLE, SENTINEL, deprecated_function
from .base import (
    AsyncBaseParser,
    AsyncPushNotificationsParser,
    BaseParser,
    PushNotificationsParser,
)
from .socket import (
    NONBLOCKING_EXCEPTION_ERROR_NUMBERS,
    NONBLOCKING_EXCEPTIONS,
    SERVER_CLOSED_CONNECTION_ERROR,
)

# Used to signal that hiredis-py does not have enough data to parse.
# Using `False` or `None` is not reliable, given that the parser can
# return `False` or `None` for legitimate reasons from RESP payloads.
NOT_ENOUGH_DATA = object()

# select.poll() is unavailable on Windows; fall back to selectors there.
_HAS_POLL = hasattr(select, "poll")


def _socket_can_read(sock, timeout: float) -> bool:
    # SSL sockets can have decrypted bytes buffered above the OS socket layer.
    if hasattr(sock, "pending") and sock.pending():
        return True
    # timeout=0 must be a non-blocking readiness check only; both branches
    # below are non-destructive and have no FD_SETSIZE limit (select.select
    # raises ValueError for fds >= 1024).
    if _HAS_POLL:
        # Prefer poll() over selectors.DefaultSelector: epoll/kqueue selectors
        # allocate a file descriptor per check and so fail with EMFILE under
        # fd exhaustion - the very condition that pushes sockets onto high
        # fds. poll() allocates nothing.
        poller = select.poll()
        poller.register(sock, select.POLLIN)
        # poll() takes milliseconds (None blocks forever). POLLHUP/POLLERR/
        # POLLNVAL are always reported regardless of the registered mask, so
        # closed or errored sockets still count as readable, like select().
        poll_timeout = None if timeout is None else timeout * 1000
        return bool(poller.poll(poll_timeout))
    with selectors.DefaultSelector() as selector:
        selector.register(sock, selectors.EVENT_READ)
        return bool(selector.select(timeout))


class _HiredisReaderArgs(TypedDict, total=False):
    protocolError: Callable[[str], Exception]
    replyError: Callable[[str], Exception]
    encoding: Optional[str]
    errors: Optional[str]


class _HiredisParser(BaseParser, PushNotificationsParser):
    "Parser class for connections using Hiredis"

    def __init__(self, socket_read_size):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")
        self.socket_read_size = socket_read_size
        self._buffer = bytearray(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.node_moving_push_handler_func = None
        self.maintenance_push_handler_func = None
        self.oss_cluster_maint_push_handler_func = None
        self.invalidation_push_handler_func = None
        self._hiredis_PushNotificationType = None

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: " + str(response))
        return response

    def on_connect(self, connection, **kwargs):
        import hiredis

        self._sock = connection._sock
        self._socket_timeout = connection.socket_timeout
        kwargs = {
            "protocolError": InvalidResponse,
            "replyError": self.parse_error,
            "errors": connection.encoder.encoding_errors,
            "notEnoughData": NOT_ENOUGH_DATA,
        }

        if connection.encoder.decode_responses:
            kwargs["encoding"] = connection.encoder.encoding
        self._reader = hiredis.Reader(**kwargs)

        try:
            self._hiredis_PushNotificationType = hiredis.PushNotification
        except AttributeError:
            # hiredis < 3.2
            self._hiredis_PushNotificationType = None

    def on_disconnect(self):
        self._sock = None
        self._reader = None

    def can_read(self, timeout: float = 0) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        if not self._reader:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        if self._reader.has_data():
            return True
        return _socket_can_read(self._sock, timeout)

    def read_from_socket(self, timeout=SENTINEL, raise_on_timeout=True):
        sock = self._sock
        custom_timeout = timeout is not SENTINEL
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
            if ex.errno == allowed:
                if not raise_on_timeout:
                    return False
                if timeout == 0:
                    raise TimeoutError("Timeout reading from socket")
            raise ConnectionError(f"Error while reading from socket: {ex.args}")
        finally:
            if custom_timeout:
                sock.settimeout(self._socket_timeout)

    def read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        if not self._reader:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        if disable_decoding:
            response = self._reader.gets(False)
        else:
            response = self._reader.gets()

        while response is NOT_ENOUGH_DATA:
            self.read_from_socket(timeout=timeout)
            if disable_decoding:
                response = self._reader.gets(False)
            else:
                response = self._reader.gets()
        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif self._hiredis_PushNotificationType is not None and isinstance(
            response, self._hiredis_PushNotificationType
        ):
            response = self.handle_push_response(response)
            if push_request:
                return response
            return self.read_response(
                disable_decoding=disable_decoding,
                push_request=push_request,
                timeout=timeout,
            )

        elif (
            isinstance(response, list)
            and response
            and isinstance(response[0], ConnectionError)
        ):
            raise response[0]
        return response


class _AsyncHiredisParser(AsyncBaseParser, AsyncPushNotificationsParser):
    """Async implementation of parser class for connections using Hiredis"""

    __slots__ = ("_reader",)

    def __init__(self, socket_read_size: int):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not available.")
        super().__init__(socket_read_size=socket_read_size)
        self._reader = None
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.invalidation_push_handler_func = None
        self._hiredis_PushNotificationType = None

    async def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: " + str(response))
        return response

    def on_connect(self, connection):
        import hiredis

        self._stream = connection._reader
        kwargs: _HiredisReaderArgs = {
            "protocolError": InvalidResponse,
            "replyError": self.parse_error,
            "notEnoughData": NOT_ENOUGH_DATA,
        }
        if connection.encoder.decode_responses:
            kwargs["encoding"] = connection.encoder.encoding
            kwargs["errors"] = connection.encoder.encoding_errors

        self._reader = hiredis.Reader(**kwargs)
        self._connected = True

        try:
            self._hiredis_PushNotificationType = getattr(
                hiredis, "PushNotification", None
            )
        except AttributeError:
            # hiredis < 3.2
            self._hiredis_PushNotificationType = None

    def on_disconnect(self):
        self._connected = False

    @deprecated_function(
        version="8.0.0", reason="Use can_read() instead", name="can_read_destructive"
    )
    async def can_read_destructive(self) -> bool:
        return await self.can_read()

    async def can_read(self) -> bool:
        # TODO: Rename this API; it detects pending data or dirty/closed
        # connection state, not only whether application data can be read.
        if not self._connected:
            raise OSError("Buffer is closed.")
        # EOF means the connection is closed and not safe to reuse.
        if self._reader.has_data() or self._stream.at_eof():
            return True
        # asyncio.StreamReader has no public non-destructive API for checking
        # buffered bytes. Preserve dirty-connection detection for hiredis; tests
        # with a real StreamReader guard this private buffer API in CI.
        return bool(self._stream._buffer)

    async def read_from_socket(self):
        buffer = await self._stream.read(self._read_size)
        if not buffer or not isinstance(buffer, bytes):
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None
        self._reader.feed(buffer)
        # data was read from the socket and added to the buffer.
        # return True to indicate that data was read.
        return True

    async def read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ) -> Union[EncodableT, List[EncodableT]]:
        # If `on_disconnect()` has been called, prohibit any more reads
        # even if they could happen because data might be present.
        # We still allow reads in progress to finish
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        if disable_decoding:
            response = self._reader.gets(False)
        else:
            response = self._reader.gets()

        while response is NOT_ENOUGH_DATA:
            await self.read_from_socket()
            if disable_decoding:
                response = self._reader.gets(False)
            else:
                response = self._reader.gets()

        # if the response is a ConnectionError or the response is a list and
        # the first item is a ConnectionError, raise it as something bad
        # happened
        if isinstance(response, ConnectionError):
            raise response
        elif self._hiredis_PushNotificationType is not None and isinstance(
            response, self._hiredis_PushNotificationType
        ):
            response = await self.handle_push_response(response)
            if not push_request:
                return await self.read_response(
                    disable_decoding=disable_decoding, push_request=push_request
                )
            else:
                return response
        elif (
            isinstance(response, list)
            and response
            and isinstance(response[0], ConnectionError)
        ):
            raise response[0]
        return response
