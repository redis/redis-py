from asyncio import IncompleteReadError, TimeoutError
from typing import Any, Optional, Union

import async_timeout

from redis.typing import EncodableT

from ..exceptions import ConnectionError, InvalidResponse, RedisError, ResponseError
from .encoders import Encoder
from .protocol import AsyncBaseParser, BaseParser
from .socket import SERVER_CLOSED_CONNECTION_ERROR, SocketBuffer


class RESP2Parser(BaseParser):
    "Plain Python parsing class"

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

    def read_response(self, disable_decoding=False):
        raw = self._buffer.readline()
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        if byte not in (b"-", b"+", b":", b"$", b"*"):
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        # server returned an error
        if byte == b"-":
            response = response.decode("utf-8", errors="replace")
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
        elif byte == b"+":
            pass
        # int value
        elif byte == b":":
            response = int(response)
        # bulk response
        elif byte == b"$":
            length = int(response)
            if length == -1:
                return None
            response = self._buffer.read(length)
        # multi-bulk response
        elif byte == b"*":
            length = int(response)
            if length == -1:
                return None
            response = [
                self.read_response(disable_decoding=disable_decoding)
                for i in range(length)
            ]
        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)
        return response


class AsyncRESP2Parser(AsyncBaseParser):
    """Parsing class for the RESP2 protocol"""

    __slots__ = AsyncBaseParser.__slots__ + ("encoder",)

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size)
        self.encoder: Optional[Encoder] = None

    def on_connect(self, connection: "Connection"):
        """Called when the stream connects"""
        self._stream = connection._reader
        if self._stream is None:
            raise RedisError("Buffer is closed.")

        self.encoder = connection.encoder

    def on_disconnect(self):
        """Called when the stream disconnects"""
        if self._stream is not None:
            self._stream = None
        self.encoder = None

    async def can_read_destructive(self) -> bool:
        if self._stream is None:
            raise RedisError("Buffer is closed.")
        try:
            async with async_timeout.timeout(0):
                return await self._stream.read(1)
        except TimeoutError:
            return False

    async def read_response(
        self, disable_decoding: bool = False
    ) -> Union[EncodableT, ResponseError, None]:
        if not self._stream or not self.encoder:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        raw = await self._readline()
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        response: Any
        byte, response = raw[:1], raw[1:]

        if byte not in (b"-", b"+", b":", b"$", b"*"):
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        # server returned an error
        if byte == b"-":
            response = response.decode("utf-8", errors="replace")
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
        elif byte == b"+":
            pass
        # int value
        elif byte == b":":
            response = int(response)
        # bulk response
        elif byte == b"$":
            length = int(response)
            if length == -1:
                return None
            response = await self._read(length)
        # multi-bulk response
        elif byte == b"*":
            length = int(response)
            if length == -1:
                return None
            response = [
                (await self.read_response(disable_decoding)) for _ in range(length)
            ]
        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)
        return response

    async def _read(self, length: int) -> bytes:
        """
        Read `length` bytes of data.  These are assumed to be followed
        by a '\r\n' terminator which is subsequently discarded.
        """
        if self._stream is None:
            raise RedisError("Buffer is closed.")
        try:
            data = await self._stream.readexactly(length + 2)
        except IncompleteReadError as error:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from error
        return data[:-2]

    async def _readline(self) -> bytes:
        """
        read an unknown number of bytes up to the next '\r\n'
        line separator, which is discarded.
        """
        if self._stream is None:
            raise RedisError("Buffer is closed.")
        data = await self._stream.readline()
        if not data.endswith(b"\r\n"):
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        return data[:-2]
