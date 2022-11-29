from typing import Any, Union

from ..exceptions import ConnectionError, InvalidResponse, ResponseError
from ..typing import EncodableT
from .base import AsyncBaseParser, _AsyncRESPBase, _RESPBase
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP2Parser(_RESPBase):
    """RESP2 protocol implementation"""

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


class _AsyncRESP2Parser(_AsyncRESPBase):
    """Async class for the RESP2 protocol"""

    __slots__ = AsyncBaseParser.__slots__ + ("encoder",)

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
