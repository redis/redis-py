from abc import ABC
from asyncio import StreamReader
from typing import List, Optional, Union

from redis.typing import EncodableT

from ..exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ConnectionError,
    ExecAbortError,
    ModuleError,
    NoPermissionError,
    NoScriptError,
    ReadOnlyError,
    ResponseError,
)

MODULE_LOAD_ERROR = "Error loading the extension. " "Please check the server logs."
NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not " "possible."
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
        "WRONGPASS": AuthenticationError,
        "EXECABORT": ExecAbortError,
        "LOADING": BusyLoadingError,
        "NOSCRIPT": NoScriptError,
        "READONLY": ReadOnlyError,
        "NOAUTH": AuthenticationError,
        "NOPERM": NoPermissionError,
    }

    def parse_error(self, response):
        "Parse an error response"
        error_code = response.split(" ")[0]
        if error_code in self.EXCEPTION_CLASSES:
            response = response[len(error_code) + 1 :]
            exception_class = self.EXCEPTION_CLASSES[error_code]
            if isinstance(exception_class, dict):
                exception_class = exception_class.get(response, ResponseError)
            return exception_class(response)
        return ResponseError(response)

    def on_disconnect(self):
        raise NotImplementedError()

    def on_connect(self, connection):
        raise NotImplementedError()


class AsyncBaseParser(BaseParser):
    """Base parsing class for the python-backed async parser"""

    __slots__ = "_stream", "_read_size"

    def __init__(self, socket_read_size: int):
        self._stream: Optional[StreamReader] = None
        self._read_size = socket_read_size

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    async def can_read_destructive(self) -> bool:
        raise NotImplementedError()

    async def read_response(
        self, disable_decoding: bool = False
    ) -> Union[EncodableT, ResponseError, None, List[EncodableT]]:
        raise NotImplementedError()
