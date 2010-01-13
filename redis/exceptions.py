class RedisError(Exception): pass
class ConnectionError(RedisError): pass
class ResponseError(RedisError): pass
class InvalidResponse(RedisError): pass
class InvalidData(RedisError): pass
