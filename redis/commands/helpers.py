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
    """Optimistically parse the response to a list."""
    res = []

    if response is None:
        return res

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


def parse_list_to_dict(response):
    res = {}
    for i in range(0, len(response), 2):
        if isinstance(response[i], list):
            res['Child iterators'].append(parse_list_to_dict(response[i]))
        elif isinstance(response[i+1], list):
            res['Child iterators'] = [parse_list_to_dict(response[i+1])]
        else:
            try:
                res[response[i]] = float(response[i+1])
            except (TypeError, ValueError):
                res[response[i]] = response[i+1]
    return res


def parse_to_dict(response):
    if response is None:
        return {}

    res = {}
    for det in response:
        if isinstance(det[1], list):
            res[det[0]] = parse_list_to_dict(det[1])
        else:
            try:  # try to set the attribute. may be provided without value
                try:  # try to convert the value to float
                    res[det[0]] = float(det[1])
                except (TypeError, ValueError):
                    res[det[0]] = det[1]
            except IndexError:
                pass
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
