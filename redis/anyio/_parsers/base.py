from __future__ import annotations

import sys
from abc import abstractmethod

from anyio import IncompleteRead, move_on_after
from anyio.abc import ByteStream
from anyio.streams.buffered import BufferedByteReceiveStream

from redis._parsers import BaseParser, Encoder
from redis.exceptions import RedisError, ResponseError
from redis.typing import EncodableT

from ..._parsers.socket import SERVER_CLOSED_CONNECTION_ERROR


class AnyIOBaseParser(BaseParser):
    """Base parsing class for the python-backed async parser"""

    def __init__(self, socket_read_size: int):
        self._socket_read_size = socket_read_size
        self._stream: ByteStream | None = None
        self._connected = False

    async def can_read_destructive(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    async def read_response(
        self, disable_decoding: bool = False
    ) -> EncodableT | ResponseError | list[EncodableT] | None:
        pass


class _AnyIORESPBase(AnyIOBaseParser):
    """Base class for async resp parsing"""

    def __init__(self, socket_read_size: int):
        super().__init__(socket_read_size)
        self.encoder: Encoder | None = None
        self._buffer = b""
        self._chunks = []
        self._pos = 0

    def _clear(self):
        self._buffer = b""
        self._chunks.clear()
        self._pos = 0

    def on_connect(self, connection):
        """Called when the stream connects"""
        if connection._stream is None:
            raise RedisError("Buffer is closed.")

        self._stream = BufferedByteReceiveStream(connection._stream)
        self.encoder = connection.encoder
        self._clear()
        self._connected = True

    def on_disconnect(self):
        """Called when the stream disconnects"""
        self._connected = False

    async def can_read_destructive(self) -> bool:
        if not self._connected:
            raise RedisError("Buffer is closed.")

        if self._stream.buffer:
            return True

        with move_on_after(0):
            await self._read(1)
            return True

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
                data = await self._stream.receive_exactly(want - len(tail))
            except IncompleteRead as error:
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
            try:
                data = await self._stream.receive_until(b"\r\n", sys.maxsize)
            except IncompleteRead as error:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from error

            result = tail + data
            self._chunks.append(data + b"\r\n")
        self._pos += len(result) + 2
        return result
