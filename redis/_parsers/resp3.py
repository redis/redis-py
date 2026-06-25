from logging import getLogger
from typing import Any, Union

from ..exceptions import ConnectionError, InvalidResponse, ResponseError
from ..typing import EncodableT
from ..utils import SENTINEL
from .base import (
    AsyncPushNotificationsParser,
    PushNotificationsParser,
    _AsyncRESPBase,
    _RESPBase,
)
from .socket import SERVER_CLOSED_CONNECTION_ERROR


class _RESP3Parser(_RESPBase, PushNotificationsParser):
    """RESP3 protocol implementation"""

    def __init__(self, socket_read_size):
        super().__init__(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.node_moving_push_handler_func = None
        self.maintenance_push_handler_func = None
        self.oss_cluster_maint_push_handler_func = None
        self.invalidation_push_handler_func = None

    def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: " + str(response))
        return response

    def read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        pos = self._buffer.get_pos() if self._buffer is not None else None
        try:
            result = self._read_response(
                disable_decoding=disable_decoding,
                push_request=push_request,
                timeout=timeout,
            )
        except BaseException:
            if self._buffer is not None:
                self._buffer.rewind(pos)
            raise
        else:
            if self._buffer is not None:
                try:
                    self._buffer.purge()
                except AttributeError:
                    # Buffer may have been set to None by another thread after
                    # the check above; result is still valid so we don't raise
                    pass
            return result

    def _read_response(
        self,
        disable_decoding=False,
        push_request=False,
        timeout: Union[float, object] = SENTINEL,
    ):
        # Iterative stack-based parsing to avoid RecursionError on deeply nested aggregates.
        # Each stack entry: (type, remaining_count, container, pending_key)
        # type is one of: 'array', 'set', 'map', 'push'
        # pending_key is used only for maps to store the key until we read the value
        stack = []
        response = None

        while True:
            raw = self._buffer.readline(timeout=timeout)
            if not raw:
                raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

            byte, response = raw[:1], raw[1:]

            # server returned an error
            if byte in (b"-", b"!"):
                if byte == b"!":
                    response = self._buffer.read(int(response), timeout=timeout)
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
            # single value
            elif byte == b"+":
                pass
            # null value
            elif byte == b"_":
                response = None
            # int and big int values
            elif byte in (b":", b"("):
                response = int(response)
            # double value
            elif byte == b",":
                response = float(response)
            # bool value
            elif byte == b"#":
                response = response == b"t"
            # bulk response
            elif byte == b"$":
                response = self._buffer.read(int(response), timeout=timeout)
            # verbatim string response
            elif byte == b"=":
                response = self._buffer.read(int(response), timeout=timeout)[4:]
            # array response
            elif byte == b"*":
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append(('array', count, [], None))
                    continue
            # set response
            elif byte == b"~":
                # redis can return unhashable types (like dict) in a set,
                # so we return sets as list, all the time, for predictability
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append(('set', count, [], None))
                    continue
            # map response
            elif byte == b"%":
                count = int(response)
                if count == 0:
                    response = {}
                else:
                    # We cannot use a dict-comprehension to parse stream.
                    # Evaluation order of key:val expression in dict comprehension only
                    # became defined to be left-right in version 3.8
                    stack.append(('map', count, {}, None))
                    continue
            # push response
            elif byte == b">":
                count = int(response)
                if count == 0:
                    response = self.handle_push_response([])
                else:
                    stack.append(('push', count, [], None))
                    continue
            else:
                raise InvalidResponse(f"Protocol Error: {raw!r}")

            while stack:
                frame_type, remaining, container, pending_key = stack[-1]
                if frame_type == 'map':
                    if pending_key is None:
                        # This is a key - store it and continue reading value
                        stack[-1] = (frame_type, remaining, container, response)
                        break
                    else:
                        # This is a value - store key-value pair
                        container[pending_key] = response
                        remaining -= 1
                        if remaining > 0:
                            stack[-1] = (frame_type, remaining, container, None)
                            break
                        else:
                            stack.pop()
                            response = container
                            continue
                else:
                    container.append(response)
                    remaining -= 1
                    if remaining > 0:
                        stack[-1] = (frame_type, remaining, container, None)
                        break
                    else:
                        stack.pop()
                        if frame_type == 'push':
                            response = self.handle_push_response(container)
                            if push_request:
                                return response
                            continue
                        response = container
                        continue
            else:
                break

        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)

        return response


class _AsyncRESP3Parser(_AsyncRESPBase, AsyncPushNotificationsParser):
    def __init__(self, socket_read_size):
        super().__init__(socket_read_size)
        self.pubsub_push_handler_func = self.handle_pubsub_push_response
        self.invalidation_push_handler_func = None

    async def handle_pubsub_push_response(self, response):
        logger = getLogger("push_response")
        logger.debug("Push response: " + str(response))
        return response

    async def read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ):
        if self._chunks:
            # augment parsing buffer with previously read data
            self._buffer += b"".join(self._chunks)
            self._chunks.clear()
        self._pos = 0
        response = await self._read_response(
            disable_decoding=disable_decoding, push_request=push_request
        )
        # Successfully parsing a response allows us to clear our parsing buffer
        self._clear()
        return response

    async def _read_response(
        self, disable_decoding: bool = False, push_request: bool = False
    ) -> Union[EncodableT, ResponseError, None]:
        if not self._stream or not self.encoder:
            raise ConnectionError(SERVER_CLOSED_CONNECTION_ERROR)

        # Iterative stack-based parsing to avoid RecursionError on deeply nested aggregates.
        # Each stack entry: (type, remaining_count, container, pending_key)
        # type is one of: 'array', 'set', 'map', 'push'
        # pending_key is used only for maps to store the key until we read the value
        stack = []
        response: Any = None

        while True:
            raw = await self._readline()
            byte, response = raw[:1], raw[1:]

            # server returned an error
            if byte in (b"-", b"!"):
                if byte == b"!":
                    response = await self._read(int(response))
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
            # single value
            elif byte == b"+":
                pass
            # null value
            elif byte == b"_":
                response = None
            # int and big int values
            elif byte in (b":", b"("):
                response = int(response)
            # double value
            elif byte == b",":
                response = float(response)
            # bool value
            elif byte == b"#":
                response = response == b"t"
            # bulk response
            elif byte == b"$":
                response = await self._read(int(response))
            # verbatim string response
            elif byte == b"=":
                response = (await self._read(int(response)))[4:]
            # array response
            elif byte == b"*":
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append(('array', count, [], None))
                    continue
            # set response
            elif byte == b"~":
                # redis can return unhashable types (like dict) in a set,
                # so we always convert to a list, to have predictable return types
                count = int(response)
                if count == 0:
                    response = []
                else:
                    stack.append(('set', count, [], None))
                    continue
            # map response
            elif byte == b"%":
                count = int(response)
                if count == 0:
                    response = {}
                else:
                    # We cannot use a dict-comprehension to parse stream.
                    # Evaluation order of key:val expression in dict comprehension only
                    # became defined to be left-right in version 3.8
                    stack.append(('map', count, {}, None))
                    continue
            # push response
            elif byte == b">":
                count = int(response)
                if count == 0:
                    response = await self.handle_push_response([])
                else:
                    stack.append(('push', count, [], None))
                    continue
            else:
                raise InvalidResponse(f"Protocol Error: {raw!r}")

            while stack:
                frame_type, remaining, container, pending_key = stack[-1]
                if frame_type == 'map':
                    if pending_key is None:
                        # This is a key - store it and continue reading value
                        stack[-1] = (frame_type, remaining, container, response)
                        break
                    else:
                        # This is a value - store key-value pair
                        container[pending_key] = response
                        remaining -= 1
                        if remaining > 0:
                            stack[-1] = (frame_type, remaining, container, None)
                            break
                        else:
                            stack.pop()
                            response = container
                            continue
                else:
                    container.append(response)
                    remaining -= 1
                    if remaining > 0:
                        stack[-1] = (frame_type, remaining, container, None)
                        break
                    else:
                        stack.pop()
                        if frame_type == 'push':
                            response = await self.handle_push_response(container)
                            if push_request:
                                return response
                            continue
                        response = container
                        continue
            else:
                break

        if isinstance(response, bytes) and disable_decoding is False:
            response = self.encoder.decode(response)
        return response
