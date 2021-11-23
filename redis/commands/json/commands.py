from .path import Path
from .decoders import decode_dict_keys
from deprecated import deprecated
from redis.exceptions import DataError


class JSONCommands:
    """json commands."""

    def arrappend(self, name, path=Path.rootPath(), *args):
        """Append the objects ``args`` to the array under the
        ``path` in key ``name``.
        """
        pieces = [name, str(path)]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRAPPEND", *pieces)

    def arrindex(self, name, path, scalar, start=0, stop=-1):
        """
        Return the index of ``scalar`` in the JSON array under ``path`` at key
        ``name``.

        The search can be limited using the optional inclusive ``start``
        and exclusive ``stop`` indices.
        """
        return self.execute_command(
            "JSON.ARRINDEX", name, str(path), self._encode(scalar),
            start, stop
        )

    def arrinsert(self, name, path, index, *args):
        """Insert the objects ``args`` to the array at index ``index``
        under the ``path` in key ``name``.
        """
        pieces = [name, str(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRINSERT", *pieces)

    def arrlen(self, name, path=Path.rootPath()):
        """Return the length of the array JSON value under ``path``
        at key``name``.
        """
        return self.execute_command("JSON.ARRLEN", name, str(path))

    def arrpop(self, name, path=Path.rootPath(), index=-1):
        """Pop the element at ``index`` in the array JSON value under
        ``path`` at key ``name``.
        """
        return self.execute_command("JSON.ARRPOP", name, str(path), index)

    def arrtrim(self, name, path, start, stop):
        """Trim the array JSON value under ``path`` at key ``name`` to the
        inclusive range given by ``start`` and ``stop``.
        """
        return self.execute_command("JSON.ARRTRIM", name, str(path),
                                    start, stop)

    def type(self, name, path=Path.rootPath()):
        """Get the type of the JSON value under ``path`` from key ``name``."""
        return self.execute_command("JSON.TYPE", name, str(path))

    def resp(self, name, path=Path.rootPath()):
        """Return the JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.RESP", name, str(path))

    def objkeys(self, name, path=Path.rootPath()):
        """Return the key names in the dictionary JSON value under ``path`` at
        key ``name``."""
        return self.execute_command("JSON.OBJKEYS", name, str(path))

    def objlen(self, name, path=Path.rootPath()):
        """Return the length of the dictionary JSON value under ``path`` at key
        ``name``.
        """
        return self.execute_command("JSON.OBJLEN", name, str(path))

    def numincrby(self, name, path, number):
        """Increment the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` by the provided ``number``.
        """
        return self.execute_command(
            "JSON.NUMINCRBY", name, str(path), self._encode(number)
        )

    @deprecated(version='4.0.0', reason='deprecated since redisjson 1.0.0')
    def nummultby(self, name, path, number):
        """Multiply the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` with the provided ``number``.
        """
        return self.execute_command(
            "JSON.NUMMULTBY", name, str(path), self._encode(number)
        )

    def clear(self, name, path=Path.rootPath()):
        """
        Empty arrays and objects (to have zero slots/keys without deleting the
        array/object).

        Return the count of cleared paths (ignoring non-array and non-objects
        paths).
        """
        return self.execute_command("JSON.CLEAR", name, str(path))

    def delete(self, key, path=Path.rootPath()):
        """Delete the JSON value stored at key ``key`` under ``path``."""
        return self.execute_command("JSON.DEL", key, str(path))

    # forget is an alias for delete
    forget = delete

    def get(self, name, *args, no_escape=False):
        """
        Get the object stored as a JSON value at key ``name``.

        ``args`` is zero or more paths, and defaults to root path
        ```no_escape`` is a boolean flag to add no_escape option to get
        non-ascii characters
        """
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

    def mget(self, keys, path):
        """
        Get the objects stored as a JSON values under ``path``. ``keys``
        is a list of one or more keys.
        """
        pieces = []
        pieces += keys
        pieces.append(str(path))
        return self.execute_command("JSON.MGET", *pieces)

    def set(self, name, path, obj, nx=False, xx=False, decode_keys=False):
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.

        For the purpose of using this within a pipeline, this command is also
        aliased to jsonset.
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

    def strlen(self, name, path=None):
        """Return the length of the string JSON value under ``path`` at key
        ``name``.
        """
        pieces = [name]
        if path is not None:
            pieces.append(str(path))
        return self.execute_command("JSON.STRLEN", *pieces)

    def toggle(self, name, path=Path.rootPath()):
        """Toggle boolean value under ``path`` at key ``name``.
        returning the new value.
        """
        return self.execute_command("JSON.TOGGLE", name, str(path))

    def strappend(self, name, value, path=Path.rootPath()):
        """Append to the string JSON value. If two options are specified after
        the key name, the path is determined to be the first. If a single
        option is passed, then the rootpath (i.e Path.rootPath()) is used.
        """
        pieces = [name, str(path), self._encode(value)]
        return self.execute_command(
            "JSON.STRAPPEND", *pieces
        )

    def debug(self, subcommand, key=None, path=Path.rootPath()):
        """Return the memory usage in bytes of a value under ``path`` from
        key ``name``.
        """
        valid_subcommands = ["MEMORY", "HELP"]
        if subcommand not in valid_subcommands:
            raise DataError("The only valid subcommands are ",
                            str(valid_subcommands))
        pieces = [subcommand]
        if subcommand == "MEMORY":
            if key is None:
                raise DataError("No key specified")
            pieces.append(key)
            pieces.append(str(path))
        return self.execute_command("JSON.DEBUG", *pieces)

    @deprecated(version='4.0.0',
                reason='redisjson-py supported this, call get directly.')
    def jsonget(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    @deprecated(version='4.0.0',
                reason='redisjson-py supported this, call get directly.')
    def jsonmget(self, *args, **kwargs):
        return self.mget(*args, **kwargs)

    @deprecated(version='4.0.0',
                reason='redisjson-py supported this, call get directly.')
    def jsonset(self, *args, **kwargs):
        return self.set(*args, **kwargs)
