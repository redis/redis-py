import asyncio
import os
from json import JSONDecodeError, JSONDecoder, JSONEncoder, loads
from typing import Literal

import redis

from ..helpers import get_protocol_version, nativestr
from .commands import JSONCommands
from .decoders import bulk_of_jsons, decode_list


class _JSONBase(JSONCommands):
    """
    Create a client for talking to json.

    :param decoder:
    :type json.JSONDecoder: An instance of json.JSONDecoder

    :param encoder:
    :type json.JSONEncoder: An instance of json.JSONEncoder
    """

    def __init__(
        self, client, version=None, decoder=JSONDecoder(), encoder=JSONEncoder()
    ):
        """
        Create a client for talking to json.

        :param decoder:
        :type json.JSONDecoder: An instance of json.JSONDecoder

        :param encoder:
        :type json.JSONEncoder: An instance of json.JSONEncoder
        """
        # Set the module commands' callbacks
        self._MODULE_CALLBACKS = {
            "JSON.ARRPOP": self._decode,
            "JSON.DEBUG": self._decode,
            "JSON.GET": self._decode,
            "JSON.MERGE": lambda r: r and nativestr(r) == "OK",
            "JSON.MGET": bulk_of_jsons(self._decode),
            "JSON.MSET": lambda r: r and nativestr(r) == "OK",
            "JSON.RESP": self._decode,
            "JSON.SET": lambda r: r and nativestr(r) == "OK",
            "JSON.TOGGLE": self._decode,
        }

        _RESP2_MODULE_CALLBACKS = {
            "JSON.ARRAPPEND": self._decode,
            "JSON.ARRINDEX": self._decode,
            "JSON.ARRINSERT": self._decode,
            "JSON.ARRLEN": self._decode,
            "JSON.ARRTRIM": self._decode,
            "JSON.CLEAR": int,
            "JSON.DEL": int,
            "JSON.FORGET": int,
            "JSON.GET": self._decode,
            "JSON.NUMINCRBY": self._decode,
            "JSON.NUMMULTBY": self._decode,
            "JSON.OBJKEYS": self._decode,
            "JSON.STRAPPEND": self._decode,
            "JSON.OBJLEN": self._decode,
            "JSON.STRLEN": self._decode,
            "JSON.TOGGLE": self._decode,
        }

        _RESP3_MODULE_CALLBACKS = {}

        self.client = client
        self.execute_command = client.execute_command
        self.MODULE_VERSION = version

        if get_protocol_version(self.client) in ["3", 3]:
            self._MODULE_CALLBACKS.update(_RESP3_MODULE_CALLBACKS)
        else:
            self._MODULE_CALLBACKS.update(_RESP2_MODULE_CALLBACKS)

        for key, value in self._MODULE_CALLBACKS.items():
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
            return x
        except TypeError:
            try:
                return self.__decoder__.decode(obj.decode())
            except AttributeError:
                return decode_list(obj)
        except (AttributeError, JSONDecodeError):
            return decode_list(obj)

    def _encode(self, obj):
        """Get the encoder."""
        return self.__encoder__.encode(obj)

    def pipeline(self, transaction=True, shard_hint=None):
        """Creates a pipeline for the JSON module, that can be used for executing
        JSON commands, as well as classic core commands.

        Usage example:

        r = redis.Redis()
        pipe = r.json().pipeline()
        pipe.jsonset('foo', '.', {'hello!': 'world'})
        pipe.jsonget('foo')
        pipe.jsonget('notakey')
        """
        if isinstance(self.client, redis.RedisCluster):
            p = ClusterPipeline(
                nodes_manager=self.client.nodes_manager,
                commands_parser=self.client.commands_parser,
                startup_nodes=self.client.nodes_manager.startup_nodes,
                result_callbacks=self.client.result_callbacks,
                cluster_response_callbacks=self.client.cluster_response_callbacks,
                cluster_error_retry_attempts=self.client.retry.get_retries(),
                read_from_replicas=self.client.read_from_replicas,
                reinitialize_steps=self.client.reinitialize_steps,
                lock=self.client._lock,
            )

        else:
            p = Pipeline(
                connection_pool=self.client.connection_pool,
                response_callbacks=self._MODULE_CALLBACKS,
                transaction=transaction,
                shard_hint=shard_hint,
            )

        p._encode = self._encode
        p._decode = self._decode
        return p


class ClusterPipeline(JSONCommands, redis.cluster.ClusterPipeline):
    """Cluster pipeline for the module."""


class Pipeline(JSONCommands, redis.client.Pipeline):
    """Pipeline for the module."""


class JSON(_JSONBase):
    _is_async_client: Literal[False] = False


class AsyncJSON(_JSONBase):
    _is_async_client: Literal[True] = True

    async def set_file(
        self,
        name: str,
        path: str,
        file_name: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
    ) -> bool | None:
        """
        Set the JSON value at key ``name`` under the ``path`` to the content
        of the json file ``file_name``.

        This runs the blocking file read in a thread pool to avoid blocking
        the event loop.
        """

        def _read_file(fp: str) -> dict:
            with open(fp) as f:
                return loads(f.read())

        file_content = await asyncio.to_thread(_read_file, file_name)
        return await self.set(
            name, path, file_content, nx=nx, xx=xx, decode_keys=decode_keys
        )

    async def set_path(
        self,
        json_path: str,
        root_folder: str,
        nx: bool | None = False,
        xx: bool | None = False,
        decode_keys: bool | None = False,
    ) -> dict[str, bool]:
        """
        Iterate over ``root_folder`` and set each JSON file to a value
        under ``json_path`` with the file name as the key.

        This method runs blocking filesystem operations (os.walk and file reads)
        in a thread pool to avoid blocking the event loop.
        """

        def _walk_directory(folder: str) -> list[str]:
            """Walk directory and return list of file paths (runs in thread pool)."""
            file_paths = []
            for root, dirs, files in os.walk(folder):
                for file in files:
                    file_paths.append(os.path.join(root, file))
            return file_paths

        set_files_result = {}

        # Run blocking os.walk in thread pool
        file_paths = await asyncio.to_thread(_walk_directory, root_folder)

        for file_path in file_paths:
            try:
                # TODO: rsplit(".") splits on all dots, mishandling paths
                # with dots in directories (e.g. /data/v1.2/file.json).
                # Should be rsplit(".", 1) — fix in a separate PR.
                file_name = file_path.rsplit(".")[0]
                await self.set_file(
                    file_name,
                    json_path,
                    file_path,
                    nx=nx,
                    xx=xx,
                    decode_keys=decode_keys,
                )
                set_files_result[file_path] = True
            except JSONDecodeError:
                set_files_result[file_path] = False

        return set_files_result
