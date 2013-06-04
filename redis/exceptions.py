"Core exceptions raised by the Redis client"


class RedisError(Exception):
    pass


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
