from __future__ import annotations

from collections.abc import Callable
from typing import TypedDict

from anyio import BrokenResourceError, EndOfStream, move_on_after

from ..._parsers.socket import (
    SERVER_CLOSED_CONNECTION_ERROR,
)
from ...exceptions import ConnectionError, InvalidResponse, RedisError
from ...typing import EncodableT
from ...utils import HIREDIS_AVAILABLE
from .base import AnyIOBaseParser

# Used to signal that hiredis-py does not have enough data to parse.
# Using `False` or `None` is not reliable, given that the parser can
# return `False` or `None` for legitimate reasons from RESP payloads.
NOT_ENOUGH_DATA = object()


class _HiredisReaderArgs(TypedDict, total=False):
    protocolError: Callable[[str], Exception]
    replyError: Callable[[str], Exception]
    encoding: str | None
    errors: str | None
    notEnoughData: object


class _AnyIOHiredisParser(AnyIOBaseParser):
    """AnyIO implementation of parser class for connections using Hiredis"""

    def __init__(self, socket_read_size: int):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not available.")

        super().__init__(socket_read_size=socket_read_size)
        self._reader = None

    def on_connect(self, connection):
        import hiredis

        self._stream = connection._stream
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

    def on_disconnect(self):
        self._connected = False

    async def can_read_destructive(self) -> bool:
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        if self._reader.gets() is not NOT_ENOUGH_DATA:
            return True

        with move_on_after(0):
            await self.read_from_socket()
            return True

        return False

    async def read_from_socket(self) -> None:
        try:
            buffer = await self._stream.receive(self._socket_read_size)
        except (EndOfStream, BrokenResourceError):
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR) from None

        self._reader.feed(buffer)

    async def read_response(
        self, disable_decoding: bool = False
    ) -> EncodableT | list[EncodableT]:
        # If `on_disconnect()` has been called, prohibit any more reads
        # even if they could happen because data might be present.
        # We still allow reads in progress to finish
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

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
        elif (
            isinstance(response, list)
            and response
            and isinstance(response[0], ConnectionError)
        ):
            raise response[0]

        return response
