import copy
import random
import string


def list_or_args(keys, args):
    # returns a single new list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (bytes, str)):
            keys = [keys]
        else:
            keys = list(keys)
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type."""
    r = x.decode("utf-8", "replace") if isinstance(x, bytes) else x
    if r == 'null':
        return
    return r


def delist(x):
    """Given a list of binaries, return the stringified version."""
    if x is None:
        return x
    return [nativestr(obj) for obj in x]


def parse_to_list(response):
    """Optimistally parse the response to a list.
    """
    res = []
    for item in response:
        try:
            res.append(int(item))
        except ValueError:
            try:
                res.append(float(item))
            except ValueError:
                res.append(nativestr(item))
        except TypeError:
            res.append(None)
    return res


def bulk_of_jsons(d):
    """Replace serialized JSON values with objects in a bulk array response (list)."""

    def _f(b):
        for index, item in enumerate(b):
            if item is not None:
                b[index] = d(item)
        return b

    return _f


def nativestr(x):
    """Return the decoded binary string, or a string, depending on type."""
    return x.decode("utf-8", "replace") if isinstance(x, bytes) else x


def delist(x):
    """Given a list of binaries, return the stringified version."""
    return [nativestr(obj) for obj in x]


def spaceHolder(response):
    """Return the response without parsing."""
    return response


def parseToList(response):
    """Parse the response to a list."""
    res = []
    for item in response:
        try:
            res.append(int(item))
        except ValueError:
            res.append(nativestr(item))
        except TypeError:
            res.append(None)
    return res


def random_string(length=10):
    """
    Returns a random N character long string.
    """
    return "".join(  # nosec
        random.choice(string.ascii_lowercase) for x in range(length)
    )


def quote_string(v):
    """
    RedisGraph strings must be quoted,
    quote_string wraps given v with quotes incase
    v is a string.
    """

    if isinstance(v, bytes):
        v = v.decode()
    elif not isinstance(v, str):
        return v
    if len(v) == 0:
        return '""'

    v = v.replace('"', '\\"')

    return '"{}"'.format(v)


def decodeDictKeys(obj):
    """Decode the keys of the given dictionary with utf-8."""
    newobj = copy.copy(obj)
    for k in obj.keys():
        if isinstance(k, bytes):
            newobj[k.decode("utf-8")] = newobj[k]
            newobj.pop(k)
    return newobj


def stringify_param_value(value):
    """
    Turn a parameter value into a string suitable for the params header of
    a Cypher command.
    You may pass any value that would be accepted by `json.dumps()`.

    Ways in which output differs from that of `str()`:
        * Strings are quoted.
        * None --> "null".
        * In dictionaries, keys are _not_ quoted.

    :param value: The parameter value to be turned into a string.
    :return: string
    """

    if isinstance(value, str):
        return quote_string(value)
    elif value is None:
        return "null"
    elif isinstance(value, (list, tuple)):
        return f'[{",".join(map(stringify_param_value, value))}]'
    elif isinstance(value, dict):
        return f'{{{",".join(f"{k}:{stringify_param_value(v)}" for k, v in value.items())}}}'
    else:
        return str(value)
