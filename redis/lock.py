import time as mod_time
import uuid
from redis.exceptions import LockError, WatchError
from redis._compat import b


class Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """
    def __init__(self, redis, name, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None):
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.token = None
        if self.timeout and self.sleep > self.timeout:
            raise LockError("'sleep' must be less than 'timeout'")

    def __enter__(self):
        # force blocking, as otherwise the user would have to check whether
        # the lock was actually acquired or not.
        self.acquire(blocking=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=None, blocking_timeout=None):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.

        ``blocking_timeout`` specifies the maximum number of seconds to
        wait trying to acquire the lock.
        """
        sleep = self.sleep
        token = b(uuid.uuid1().hex)
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if self.blocking_timeout is not None:
            stop_trying_at = mod_time.time() + self.blocking_timeout
        while 1:
            if self.do_acquire(token):
                self.token = token
                return True
            if not blocking:
                return False
            if stop_trying_at is not None and mod_time.time() > stop_trying_at:
                return False
            mod_time.sleep(sleep)

    def do_acquire(self, token):
        if self.redis.setnx(self.name, token):
            if self.timeout:
                # convert to milliseconds
                timeout = int(self.timeout * 1000)
                self.redis.pexpire(self.name, timeout)
            return True
        return False

    def release(self):
        "Releases the already acquired lock"
        if self.token is None:
            raise LockError("Cannot release an unlocked lock")
        try:
            self.do_release()
        finally:
            self.token = None

    def do_release(self):
        name = self.name
        token = self.token

        def execute_release(pipe):
            lock_value = pipe.get(name)
            if lock_value != token:
                raise LockError("Cannot release a lock that's no longer owned")
            pipe.delete(name)

        self.redis.transaction(execute_release, name)

    def extend(self, additional_time):
        """
        Adds more time to an already acquired lock.

        ``additional_time`` can be specified as an integer or a float, both
        representing the number of seconds to add.
        """
        if self.token is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_extend(additional_time)

    def do_extend(self, additional_time):
        pipe = self.redis.pipeline()
        pipe.watch(self.name)
        lock_value = pipe.get(self.name)
        if lock_value != self.token:
            raise LockError("Cannot extend a lock that's no longer owned")
        expiration = pipe.pttl(self.name)
        if expiration is None or expiration < 0:
            # Redis evicted the lock key between the previous get() and now
            # we'll handle this when we call pexpire()
            expiration = 0
        pipe.multi()
        pipe.pexpire(self.name, expiration + int(additional_time * 1000))

        try:
            response = pipe.execute()
        except WatchError:
            # someone else acquired the lock
            raise LockError("Cannot extend a lock that's no longer owned")
        if not response[0]:
            # pexpire returns False if the key doesn't exist
            raise LockError("Cannot extend a lock that's no longer owned")
        return True


class LuaLock(Lock):
    """
    A lock implementation that uses Lua scripts rather than pipelines
    and watches.
    """
    lua_acquire = None
    lua_release = None
    lua_extend = None

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - timeout in milliseconds
    # return 1 if lock was acquired, otherwise 0
    LUA_ACQUIRE_SCRIPT = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then
            if ARGV[2] ~= '' then
                redis.call('pexpire', KEYS[1], ARGV[2])
            end
            return 1
        end
        return 0
    """

    # KEYS[1] - lock name
    # ARGS[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """

    # KEYS[1] - lock name
    # ARGS[1] - token
    # ARGS[2] - additional milliseconds
    # return 1 if the locks time was extended, otherwise 0
    LUA_EXTEND_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end
        redis.call('pexpire', KEYS[1], expiration + ARGV[2])
        return 1
    """

    def __init__(self, *args, **kwargs):
        super(LuaLock, self).__init__(*args, **kwargs)
        LuaLock.register_scripts(self.redis)

    @classmethod
    def register_scripts(cls, redis):
        if cls.lua_acquire is None:
            cls.lua_acquire = redis.register_script(cls.LUA_ACQUIRE_SCRIPT)
        if cls.lua_release is None:
            cls.lua_release = redis.register_script(cls.LUA_RELEASE_SCRIPT)
        if cls.lua_extend is None:
            cls.lua_extend = redis.register_script(cls.LUA_EXTEND_SCRIPT)

    def do_acquire(self, token):
        timeout = self.timeout and int(self.timeout * 1000) or ''
        return bool(self.lua_acquire(keys=[self.name],
                                     args=[token, timeout],
                                     client=self.redis))

    def do_release(self):
        if not bool(self.lua_release(keys=[self.name],
                                     args=[self.token],
                                     client=self.redis)):
            raise LockError("Cannot release a lock that's no longer owned")

    def do_extend(self, additional_time):
        additional_time = int(additional_time * 1000)
        if not bool(self.lua_extend(keys=[self.name],
                                    args=[self.token, additional_time],
                                    client=self.redis)):
            raise LockError("Cannot extend a lock that's no longer owned")
        return True
