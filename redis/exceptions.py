"Core exceptions raised by the Redis client"
from redis._compat import unicode


class RedisError(Exception):
    pass


# python 2.5 doesn't implement Exception.__unicode__. Add it here to all
# our exception types
if not hasattr(RedisError, '__unicode__'):
    def __unicode__(self):
        if isinstance(self.args[0], unicode):
            return self.args[0]
        return unicode(self.args[0])
    RedisError.__unicode__ = __unicode__


class AuthenticationError(RedisError):
    pass


class ServerError(RedisError):
    pass


class ConnectionError(ServerError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(ServerError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass
