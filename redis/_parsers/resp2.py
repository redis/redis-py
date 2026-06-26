from typing import Any, Union

from ..exceptions import ConnectionError, InvalidResponse, ResponseError
from ..typing import EncodableT
from ..utils import SENTINEL
from .base import _AsyncRESPBase, _RESPBase
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP2Parser(_RESPBase):
    """RESP2 protocol implementation"""

    def read_response(
        self, disable_decoding=False, timeout: Union[float, object] = SENTINEL
    ):
        pos = self._buffer.get_pos() if self._buffer else None
        try:
            result = self._read_response(
                disable_decoding=disable_decoding, timeout=timeout
            )
        except BaseException:
            if self._buffer:
                self._buffer.rewind(pos)
            raise
        else:
            self._buffer.purge()
            return result

    def _read_response(
        self, disable_decoding=False, timeout: Union[float, object] = SENTINEL
    ):
        # Iterative stack-based parsing to avoid RecursionError on deeply nested arrays.
        # Each stack entry: (remaining_count, results_list)
        stack = []
        response = None

        while True:
            raw = self._buffer.readline(timeout=timeout)
            if not raw:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

            byte, response = raw[:1], raw[1:]

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
                response = error
            # single value
            elif byte == b"+":
                pass
            # int value
            elif byte == b":":
                response = int(response)
            # bulk response
            elif byte == b"$" and response == b"-1":
                response = None
            elif byte == b"$":
                response = self._buffer.read(int(response), timeout=timeout)
            # multi-bulk response
            elif byte == b"*" and response == b"-1":
                response = None
            elif byte == b"*":
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append((count, []))
                    continue
            else:
                raise InvalidResponse(f"Protocol Error: {raw!r}")

            while stack:
                remaining, results = stack[-1]
                results.append(response)
                remaining -= 1
                if remaining > 0:
                    stack[-1] = (remaining, results)
                    break
                else:
                    stack.pop()
                    response = results
            else:
                break

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response


class _AsyncRESP2Parser(_AsyncRESPBase):
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
    ) -> Union[EncodableT, ResponseError, None]:
        # Iterative stack-based parsing to avoid RecursionError on deeply nested arrays.
        # Each stack entry: (remaining_count, results_list)
        stack = []
        response: Any = None

        while True:
            raw = await self._readline()
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
                response = error
            # single value
            elif byte == b"+":
                pass
            # int value
            elif byte == b":":
                response = int(response)
            # bulk response
            elif byte == b"$" and response == b"-1":
                response = None
            elif byte == b"$":
                response = await self._read(int(response))
            # multi-bulk response
            elif byte == b"*" and response == b"-1":
                response = None
            elif byte == b"*":
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append((count, []))
                    continue
            else:
                raise InvalidResponse(f"Protocol Error: {raw!r}")

            while stack:
                remaining, results = stack[-1]
                results.append(response)
                remaining -= 1
                if remaining > 0:
                    stack[-1] = (remaining, results)
                    break
                else:
                    stack.pop()
                    response = results
            else:
                break

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response
