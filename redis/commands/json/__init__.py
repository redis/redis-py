from json import JSONDecoder, JSONEncoder

from .decoders import (
    decode_list_or_int,
    decode_toggle,
    int_or_none,
)
from .helpers import bulk_of_jsons
from ..helpers import nativestr, delist
from .commands import JSONCommands


class JSON(JSONCommands):
    """
    Create a client for talking to json.

    :param decoder:
    :type json.JSONDecoder: An instance of json.JSONDecoder

    :param encoder:
    :type json.JSONEncoder: An instance of json.JSONEncoder
    """

    def __init__(
        self,
        client,
        version=None,
        decoder=JSONDecoder(),
        encoder=JSONEncoder(),
    ):
        """
        Create a client for talking to json.

        :param decoder:
        :type json.JSONDecoder: An instance of json.JSONDecoder

        :param encoder:
        :type json.JSONEncoder: An instance of json.JSONEncoder
        """
        # Set the module commands' callbacks
        self.MODULE_CALLBACKS = {
            "JSON.CLEAR": int,
            "JSON.DEL": int,
            "JSON.FORGET": int,
            "JSON.GET": self._decode,
            "JSON.MGET": bulk_of_jsons(self._decode),
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.NUMINCRBY": self._decode,
            "JSON.NUMMULTBY": self._decode,
            "JSON.TOGGLE": decode_toggle,
            "JSON.STRAPPEND": decode_list_or_int,
            "JSON.STRLEN": decode_list_or_int,
            "JSON.ARRAPPEND": decode_list_or_int,
            "JSON.ARRINDEX": decode_list_or_int,
            "JSON.ARRINSERT": decode_list_or_int,
            "JSON.ARRLEN": int_or_none,
            "JSON.ARRPOP": self._decode,
            "JSON.ARRTRIM": decode_list_or_int,
            "JSON.OBJLEN": decode_list_or_int,
            "JSON.OBJKEYS": delist,
            # "JSON.RESP": delist,
            "JSON.DEBUG": decode_list_or_int,
        }

        self.client = client
        self.execute_command = client.execute_command
        self.MODULE_VERSION = version

        for key, value in self.MODULE_CALLBACKS.items():
            self.client.set_response_callback(key, value)

        self.__encoder__ = encoder
        self.__decoder__ = decoder

    def _decode(self, obj):
        """Get the decoder."""
        if obj is None:
            return obj

        try:
            x = self.__decoder__.decode(obj)
            if x is None:
                raise TypeError
        except TypeError:
            return self.__decoder__.decode(obj.decode())
        finally:
            import json
            return json.loads(obj.decode())

    def _encode(self, obj):
        """Get the encoder."""
        return self.__encoder__.encode(obj)
