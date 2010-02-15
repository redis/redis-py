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
    
class InvalidData(RedisError):
    pass
    