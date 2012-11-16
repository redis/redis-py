"Core exceptions raised by the Redis client"


class RedisError(Exception):
    pass


class AuthenticationError(RedisError):
    pass


class ConnectionError(RedisError):
    pass


class ResponseError(RedisError):
    pass


class InvalidResponse(RedisError):
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
