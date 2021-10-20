from .path import Path, str_path
from .helpers import decode_dict_keys


class JSONCommands:
    """json commands."""

    def arrappend(self, name, path=Path.rootPath(), *args):
        """Append the objects ``args`` to the array under the
        ``path` in key ``name``.
        """
        pieces = [name, str_path(path)]
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
            "JSON.ARRINDEX", name, str_path(path), self._encode(scalar),
            start, stop
        )

    def arrinsert(self, name, path, index, *args):
        """Insert the objects ``args`` to the array at index ``index``
        under the ``path` in key ``name``.
        """
        pieces = [name, str_path(path), index]
        for o in args:
            pieces.append(self._encode(o))
        return self.execute_command("JSON.ARRINSERT", *pieces)

    def forget(self, name, path=Path.rootPath()):
        """Alias for jsondel (delete the JSON value)."""
        return self.execute_command("JSON.FORGET", name, str_path(path))

    def arrlen(self, name, path=Path.rootPath()):
        """Return the length of the array JSON value under ``path``
        at key``name``.
        """
        return self.execute_command("JSON.ARRLEN", name, str_path(path))

    def arrpop(self, name, path=Path.rootPath(), index=-1):
        """Pop the element at ``index`` in the array JSON value under
        ``path`` at key ``name``.
        """
        return self.execute_command("JSON.ARRPOP", name, str_path(path), index)

    def arrtrim(self, name, path, start, stop):
        """Trim the array JSON value under ``path`` at key ``name`` to the
        inclusive range given by ``start`` and ``stop``.
        """
        return self.execute_command("JSON.ARRTRIM", name, str_path(path),
                                    start, stop)

    def type(self, name, path=Path.rootPath()):
        """Get the type of the JSON value under ``path`` from key ``name``."""
        return self.execute_command("JSON.TYPE", name, str_path(path))

    def resp(self, name, path=Path.rootPath()):
        """Return the JSON value under ``path`` at key ``name``."""
        return self.execute_command("JSON.RESP", name, str_path(path))

    def objkeys(self, name, path=Path.rootPath()):
        """Return the key names in the dictionary JSON value under ``path`` at
        key ``name``."""
        return self.execute_command("JSON.OBJKEYS", name, str_path(path))

    def objlen(self, name, path=Path.rootPath()):
        """Return the length of the dictionary JSON value under ``path`` at key
        ``name``.
        """
        return self.execute_command("JSON.OBJLEN", name, str_path(path))

    def numincrby(self, name, path, number):
        """Increment the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` by the provided ``number``.
        """
        return self.execute_command(
            "JSON.NUMINCRBY", name, str_path(path), self._encode(number)
        )

    def nummultby(self, name, path, number):
        """Multiply the numeric (integer or floating point) JSON value under
        ``path`` at key ``name`` with the provided ``number``.
        """
        return self.execute_command(
            "JSON.NUMMULTBY", name, str_path(path), self._encode(number)
        )

    def clear(self, name, path=Path.rootPath()):
        """
        Empty arrays and objects (to have zero slots/keys without deleting the
        array/object).

        Return the count of cleared paths (ignoring non-array and non-objects
        paths).
        """
        return self.execute_command("JSON.CLEAR", name, str_path(path))

    def delete(self, name, path=Path.rootPath()):
        """Delete the JSON value stored at key ``name`` under ``path``."""
        return self.execute_command("JSON.DEL", name, str_path(path))

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
                pieces.append(str_path(p))

        # Handle case where key doesn't exist. The JSONDecoder would raise a
        # TypeError exception since it can't decode None
        try:
            return self.execute_command("JSON.GET", *pieces)
        except TypeError:
            return None

    def mget(self, path, *args):
        """Get the objects stored as a JSON values under ``path`` from keys
        ``args``.
        """
        pieces = []
        pieces.extend(args)
        pieces.append(str_path(path))
        return self.execute_command("JSON.MGET", *pieces)

    def set(self, name, path, obj, nx=False, xx=False, decode_keys=False):
        """
        Set the JSON value at key ``name`` under the ``path`` to ``obj``.

        ``nx`` if set to True, set ``value`` only if it does not exist.
        ``xx`` if set to True, set ``value`` only if it exists.
        ``decode_keys`` If set to True, the keys of ``obj`` will be decoded
        with utf-8.
        """
        if decode_keys:
            obj = decode_dict_keys(obj)

        pieces = [name, str_path(path), self._encode(obj)]

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

    def strlen(self, name, path=Path.rootPath()):
        """Return the length of the string JSON value under ``path`` at key
        ``name``.
        """
        return self.execute_command("JSON.STRLEN", name, str_path(path))

    def toggle(self, name, path=Path.rootPath()):
        """Toggle boolean value under ``path`` at key ``name``.
        returning the new value.
        """
        return self.execute_command("JSON.TOGGLE", name, str_path(path))

    def strappend(self, name, string, path=Path.rootPath()):
        """Append to the string JSON value under ``path`` at key ``name``
        the provided ``string``.
        """
        return self.execute_command(
            "JSON.STRAPPEND", name, str_path(path), self._encode(string)
        )

    def debug(self, name, path=Path.rootPath()):
        """Return the memory usage in bytes of a value under ``path`` from
        key ``name``.
        """
        return self.execute_command("JSON.DEBUG", "MEMORY",
                                    name, str_path(path))
