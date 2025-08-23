from __future__ import annotations

from typing import Any

from ..._parsers.socket import SERVER_CLOSED_CONNECTION_ERROR
from ...exceptions import ConnectionError, InvalidResponse, ResponseError
from ...typing import EncodableT
from .base import _AnyIORESPBase


class _AnyIORESP2Parser(_AnyIORESPBase):
    """Async class for the RESP2 protocol"""

    async def read_response(self, disable_decoding: bool = False):
        if not self._connected:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)
        if self._chunks:
            # augment parsing buffer with previously read data
            self._buffer += b"".join(self._chunks)
            self._chunks.clear()
        self._pos = 0
        response = await self._read_response(disable_decoding=disable_decoding)
        # Successfully parsing a response allows us to clear our parsing buffer
        self._clear()
        return response

    async def _read_response(
        self, disable_decoding: bool = False
    ) -> EncodableT | ResponseError | None:
        raw = await self._readline()
        response: Any
        byte, response = raw[:1], raw[1:]

        # server returned an error
        if byte == b"-":
            response = response.decode("utf-8", errors="replace")
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                self._clear()  # Successful parse
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
            return int(response)
        # bulk response
        elif byte == b"$" and response == b"-1":
            return None
        elif byte == b"$":
            response = await self._read(int(response))
        # multi-bulk response
        elif byte == b"*" and response == b"-1":
            return None
        elif byte == b"*":
            response = [
                (await self._read_response(disable_decoding))
                for _ in range(int(response))  # noqa
            ]
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response
