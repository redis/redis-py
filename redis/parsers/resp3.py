from ..exceptions import ConnectionError, InvalidResponse
from .base import _RESPBase
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP3Parser(_RESPBase):
    """RESP3 protocol implementation"""

    def read_response(self, disable_decoding=False):
        pos = self._buffer.get_pos()
        try:
            result = self._read_response(disable_decoding=disable_decoding)
        except BaseException:
            self._buffer.rewind(pos)
            raise
        else:
            self._buffer.purge()
            return result

    def _read_response(self, disable_decoding=False):
        raw = self._buffer.readline()
        if not raw:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        byte, response = raw[:1], raw[1:]

        # server returned an error
        if byte in (b"-", b"!"):
            if byte == b"!":
                response = self._buffer.read(int(response))
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
        # null value
        elif byte == b"_":
            return None
        # int and big int values
        elif byte in (b":", b"("):
            return int(response)
        # double value
        elif byte == b",":
            return float(response)
        # bool value
        elif byte == b"#":
            return response == b"t"
        # bulk response and verbatim strings
        elif byte in (b"$", b"="):
            response = self._buffer.read(int(response))
        # array response
        elif byte == b"*":
            response = [
                self._read_response(disable_decoding=disable_decoding)
                for i in range(int(response))
            ]
        # set response
        elif byte == b"~":
            response = {
                self._read_response(disable_decoding=disable_decoding)
                for i in range(int(response))
            }
        # map response
        elif byte == b"%":
            response = {
                self._read_response(
                    disable_decoding=disable_decoding
                ): self._read_response(disable_decoding=disable_decoding)
                for i in range(int(response))
            }
        else:
            raise InvalidResponse(f"Protocol Error: {raw!r}")

        if disable_decoding is False:
            response = self.encoder.decode(response)
        return response
