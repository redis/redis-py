"Core exceptions raised by the Redis client"


class RedisError(Exception):
    pass


class ConnectionError(RedisError):
    pass


class TimeoutError(RedisError):
    pass


class AuthenticationError(ConnectionError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
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


class ReadOnlyError(ResponseError):
    pass


class NoPermissionError(ResponseError):
    pass


class ModuleError(ResponseError):
    pass


class LockError(RedisError, ValueError):
    "Errors acquiring or releasing a lock"
    # NOTE: For backwards compatibility, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass


class LockNotOwnedError(LockError):
    "Error trying to extend or release a lock that is (no longer) owned"
    pass


class ChildDeadlockedError(Exception):
    "Error indicating that a child process is deadlocked after a fork()"
    pass


class AuthenticationWrongNumberOfArgsError(ResponseError):
    """
    An error to indicate that the wrong number of args
    were sent to the AUTH command
    """
    pass


class RedisClusterException(Exception):
    pass


class ClusterError(RedisError):
    pass


class ClusterDownError(ClusterError, ResponseError):

    def __init__(self, resp):
        self.args = (resp,)
        self.message = resp


class AskError(ResponseError):
    """
    src node: MIGRATING to dst node
        get > ASK error
        ask dst node > ASKING command
    dst node: IMPORTING from src node
        asking command only affects next command
        any op will be allowed after asking command
    """

    def __init__(self, resp):
        """should only redirect to master node"""
        self.args = (resp,)
        self.message = resp
        slot_id, new_node = resp.split(' ')
        host, port = new_node.rsplit(':', 1)
        self.slot_id = int(slot_id)
        self.node_addr = self.host, self.port = host, int(port)


class TryAgainError(ResponseError):

    def __init__(self, *args, **kwargs):
        pass


class ClusterCrossSlotError(ResponseError):
    message = "Keys in request don't hash to the same slot"


class MovedError(AskError):
    pass


class MasterDownError(ClusterDownError):
    pass


class SlotNotCoveredError(RedisClusterException):
    """
    This error only happens in the case where the connection pool will try to
    fetch what node that is covered by a given slot.

    If this error is raised the client should drop the current node layout and
    attempt to reconnect and refresh the node layout again
    """
    pass
