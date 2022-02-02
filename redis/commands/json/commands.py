import os
from json import JSONDecodeError, loads
from typing import Dict, List, Optional, Union

from deprecated import deprecated

from redis.exceptions import DataError

from ._util import JsonType
from .decoders import decode_dict_keys
from .path import Path


class JSONCommands:
    """json commands."""

    def arrappend(
        self, name: str, path: Optional[str] = Path.rootPath(), *args: List[JsonType]
    ) -> List[Union[int, None]]:
        """Append the objects ``args`` to the array under the
        ``path` in key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrappend
        """  # noqa
        pieces = [name, str(path)]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRAPPEND", *pieces)

    def arrindex(
        self,
        name: str,
        path: str,
        scalar: int,
        start: Optional[int] = 0,
        stop: Optional[int] = -1,
    ) -> List[Union[int, None]]:
        """
        Return the index of ``scalar`` in the JSON array under ``path`` at key
        ``name``.

        The search can be limited using the optional inclusive ``start``
        and exclusive ``stop`` indices.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrindex
        """  # noqa
        return self.execute_command(
            "JSON.ARRINDEX", name, str(path), self._encode(scalar), start, stop
        )

    def arrinsert(
        self, name: str, path: str, index: int, *args: List[JsonType]
    ) -> List[Union[int, None]]:
        """Insert the objects ``args`` to the array at index ``index``
        under the ``path` in key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrinsert
        """  # noqa
        pieces = [name, str(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRINSERT", *pieces)

    def arrlen(
        self, name: str, path: Optional[str] = Path.rootPath()
    ) -> List[Union[int, None]]:
        """Return the length of the array JSON value under ``path``
        at key``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrlen
        """  # noqa
        return self.execute_command("JSON.ARRLEN", name, str(path))

    def arrpop(
        self,
        name: str,
        path: Optional[str] = Path.rootPath(),
        index: Optional[int] = -1,
    ) -> List[Union[str, None]]:
        """Pop the element at ``index`` in the array JSON value under
        ``path`` at key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrpop
        """  # noqa
        return self.execute_command("JSON.ARRPOP", name, str(path), index)

    def arrtrim(
        self, name: str, path: str, start: int, stop: int
    ) -> List[Union[int, None]]:
        """Trim the array JSON value under ``path`` at key ``name`` to the
        inclusive range given by ``start`` and ``stop``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonarrtrim
        """  # noqa
        return self.execute_command("JSON.ARRTRIM", name, str(path), start, stop)

    def type(self, name: str, path: Optional[str] = Path.rootPath()) -> List[str]:
        """Get the type of the JSON value under ``path`` from key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsontype
        """  # noqa
        return self.execute_command("JSON.TYPE", name, str(path))

    def resp(self, name: str, path: Optional[str] = Path.rootPath()) -> List:
        """Return the JSON value under ``path`` at key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonresp
        """  # noqa
        return self.execute_command("JSON.RESP", name, str(path))

    def objkeys(
        self, name: str, path: Optional[str] = Path.rootPath()
    ) -> List[Union[List[str], None]]:
        """Return the key names in the dictionary JSON value under ``path`` at
        key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonobjkeys
        """  # noqa
        return self.execute_command("JSON.OBJKEYS", name, str(path))

    def objlen(self, name: str, path: Optional[str] = Path.rootPath()) -> int:
        """Return the length of the dictionary JSON value under ``path`` at key
        ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonobjlen
        """  # noqa
        return self.execute_command("JSON.OBJLEN", name, str(path))

    def numincrby(self, name: str, path: str, number: int) -> str:
        """Increment the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` by the provided ``number``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonnumincrby
        """  # noqa
        return self.execute_command(
            "JSON.NUMINCRBY", name, str(path), self._encode(number)
        )

    @deprecated(version="4.0.0", reason="deprecated since redisjson 1.0.0")
    def nummultby(self, name: str, path: str, number: int) -> str:
        """Multiply the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` with the provided ``number``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonnummultby
        """  # noqa
        return self.execute_command(
            "JSON.NUMMULTBY", name, str(path), self._encode(number)
        )

    def clear(self, name: str, path: Optional[str] = Path.rootPath()) -> int:
        """
        Empty arrays and objects (to have zero slots/keys without deleting the
        array/object).

        Return the count of cleared paths (ignoring non-array and non-objects
        paths).

        For more information: https://oss.redis.com/redisjson/commands/#jsonclear
        """  # noqa
        return self.execute_command("JSON.CLEAR", name, str(path))

    def delete(self, key: str, path: Optional[str] = Path.rootPath()) -> int:
        """Delete the JSON value stored at key ``key`` under ``path``.

        For more information: https://oss.redis.com/redisjson/commands/#jsondel
        """
        return self.execute_command("JSON.DEL", key, str(path))

    # forget is an alias for delete
    forget = delete

    def get(
        self, name: str, *args, no_escape: Optional[bool] = False
    ) -> List[JsonType]:
        """
        Get the object stored as a JSON value at key ``name``.

        ``args`` is zero or more paths, and defaults to root path
        ```no_escape`` is a boolean flag to add no_escape option to get
        non-ascii characters

        For more information: https://oss.redis.com/redisjson/commands/#jsonget
        """  # noqa
        pieces = [name]
        if no_escape:
            pieces.append("noescape")

        if len(args) == 0:
            pieces.append(Path.rootPath())

        else:
            for p in args:
                pieces.append(str(p))

        # Handle case where key doesn't exist. The JSONDecoder would raise a
        # TypeError exception since it can't decode None
        try:
            return self.execute_command("JSON.GET", *pieces)
        except TypeError:
            return None

    def mget(self, keys: List[str], path: str) -> List[JsonType]:
        """
        Get the objects stored as a JSON values under ``path``. ``keys``
        is a list of one or more keys.

        For more information: https://oss.redis.com/redisjson/commands/#jsonmget
        """  # noqa
        pieces = []
        pieces += keys
        pieces.append(str(path))
        return self.execute_command("JSON.MGET", *pieces)

    def set(
        self,
        name: str,
        path: str,
        obj: JsonType,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        decode_keys: Optional[bool] = False,
    ) -> Optional[str]:
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.

        For the purpose of using this within a pipeline, this command is also
        aliased to jsonset.

        For more information: https://oss.redis.com/redisjson/commands/#jsonset
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str(path), self._encode(obj)]

        # Handle existential modifiers
        if nx and xx:
            raise Exception(
                "nx and xx are mutually exclusive: use one, the "
                "other or neither - but not both"
            )
        elif nx:
            pieces.append("NX")
        elif xx:
            pieces.append("XX")
        return self.execute_command("JSON.SET", *pieces)

    def set_file(
        self,
        name: str,
        path: str,
        file_name: str,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        decode_keys: Optional[bool] = False,
    ) -> Optional[str]:
        """
        Set the JSON value at key ``name`` under the ``path`` to the content
        of the json file ``file_name``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.

        """

        with open(file_name, "r") as fp:
            file_content = loads(fp.read())

        return self.set(name, path, file_content, nx=nx, xx=xx, decode_keys=decode_keys)

    def set_path(
        self,
        json_path: str,
        root_folder: str,
        nx: Optional[bool] = False,
        xx: Optional[bool] = False,
        decode_keys: Optional[bool] = False,
    ) -> List[Dict[str, bool]]:
        """
        Iterate over ``root_folder`` and set each JSON file to a value
        under ``json_path`` with the file name as the key.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.

        """
        set_files_result = {}
        for root, dirs, files in os.walk(root_folder):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_name = file_path.rsplit(".")[0]
                    self.set_file(
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

    def strlen(self, name: str, path: Optional[str] = None) -> List[Union[int, None]]:
        """Return the length of the string JSON value under ``path`` at key
        ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsonstrlen
        """  # noqa
        pieces = [name]
        if path is not None:
            pieces.append(str(path))
        return self.execute_command("JSON.STRLEN", *pieces)

    def toggle(
        self, name: str, path: Optional[str] = Path.rootPath()
    ) -> Union[bool, List[Optional[int]]]:
        """Toggle boolean value under ``path`` at key ``name``.
        returning the new value.

        For more information: https://oss.redis.com/redisjson/commands/#jsontoggle
        """  # noqa
        return self.execute_command("JSON.TOGGLE", name, str(path))

    def strappend(
        self, name: str, value: str, path: Optional[int] = Path.rootPath()
    ) -> Union[int, List[Optional[int]]]:
        """Append to the string JSON value. If two options are specified after
        the key name, the path is determined to be the first. If a single
        option is passed, then the rootpath (i.e Path.rootPath()) is used.

        For more information: https://oss.redis.com/redisjson/commands/#jsonstrappend
        """  # noqa
        pieces = [name, str(path), self._encode(value)]
        return self.execute_command("JSON.STRAPPEND", *pieces)

    def debug(
        self,
        subcommand: str,
        key: Optional[str] = None,
        path: Optional[str] = Path.rootPath(),
    ) -> Union[int, List[str]]:
        """Return the memory usage in bytes of a value under ``path`` from
        key ``name``.

        For more information: https://oss.redis.com/redisjson/commands/#jsondebug
        """  # noqa
        valid_subcommands = ["MEMORY", "HELP"]
        if subcommand not in valid_subcommands:
            raise DataError("The only valid subcommands are ", str(valid_subcommands))
        pieces = [subcommand]
        if subcommand == "MEMORY":
            if key is None:
                raise DataError("No key specified")
            pieces.append(key)
            pieces.append(str(path))
        return self.execute_command("JSON.DEBUG", *pieces)

    @deprecated(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonget(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    @deprecated(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonmget(self, *args, **kwargs):
        return self.mget(*args, **kwargs)

    @deprecated(
        version="4.0.0", reason="redisjson-py supported this, call get directly."
    )
    def jsonset(self, *args, **kwargs):
        return self.set(*args, **kwargs)
