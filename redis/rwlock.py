from __future__ import annotations

import abc
import time as mod_time
from types import TracebackType
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import Self
from typing import Type
from uuid import uuid4

from typing_extensions import override

from redis.exceptions import LockError
from redis.exceptions import LockMaxWritersError
from redis.exceptions import LockNotOwnedError
from redis.typing import Number

if TYPE_CHECKING:
    from redis import Redis


# RwLock implementation
# =====================
#
# The lock owns three keys:
# - `<prefix>:write`: If this exists, holds the token of the user that
#   owns the exclusive write lock.
# - `<prefix>:write_semaphore`: Semaphore tracking writers that are
#   waiting to acquire the lock. If any writers are waiting, readers
#   block.
# - `<prefix>:read`: Another semaphore, tracks readers.
#
# Semaphores are implemented as ordered sets, where score is the
# expiration time of the semaphore lease (Redis instance time). Expired
# leases are pruned before attempting to acquire the lock.
#
# We can't use built-in key expiration because individual set members
# cannot have an expiration. We can't create keys dynamically because
# this may break multi-node compatibility.
#
# The write-acquire script is careful to ensure that the writer waiting
# semaphore is only held if the caller is actually blocking; otherwise
# the caller adds contention for no reason.


class RwLock:
    """A shared reader-writer lock.

    Unlike a standard mutex, a reader-writer lock allows multiple
    readers to concurrently access the locked resource. However, writers
    will wait for exclusive access to the locked resource. Writers get
    priority when waiting on the lock so that readers do not starve
    waiting writers. Writers are allowed to starve readers, however.

    This type of unfair lock is effective for scenarios where reads are
    frequent and writes are infrequent. Because this lock relies on busy
    waiting, it can be wasteful to use if your critical sections are
    long and frequent.

    This lock is not fault-tolerant in a multi-node Redis setup. When a
    master fails and data is lost, writer exclusivity may be violated.
    In a single-node setup, the lock is sound.

    This lock is not re-entrant; attempting to acquire it twice in the
    same thread may cause a deadlock until the blocking timeout ends.
    """

    lua_acquire_reader = None
    lua_acquire_writer = None
    lua_release_writer = None
    lua_reacquire_reader = None
    lua_reacquire_writer = None

    # KEYS[1] - writer lock name
    # KEYS[2] - writer semaphore name
    # KEYS[3] - reader lock name
    # ARGV[1] - token
    # ARGV[2] - expiration
    # return 1 if the lock was acquired, otherwise 0
    LUA_ACQUIRE_READER_SCRIPT = """
        local token = ARGV[1]

        local timespec = redis.call('time')
        local time = timespec[1] + 1e-6 * timespec[2]

        redis.call('zremrangebyscore', KEYS[2], 0, time)
        redis.call('zremrangebyscore', KEYS[3], 0, time)

        local locked = redis.call('exists', KEYS[1]) > 0 or
            redis.call('zcard', KEYS[2]) > 0
        if locked then
            return 0
        end

        local expiry = time + ARGV[2]
        redis.call('zadd', KEYS[3], expiry, token)

        return 1
    """

    # KEYS[1] - writer lock name
    # KEYS[2] - writer semaphore name
    # KEYS[3] - reader lock name
    # ARGV[1] - token
    # ARGV[2] - expiration
    # ARGV[3] - sempahore expiration (or 0 to release the sempahore)
    # ARGV[4] - max writers
    #
    # NOTE: return codes:
    # - 0: Lock was acquired
    # - 1: Blocked
    # - 2: Didn't block
    LUA_ACQUIRE_WRITER_SCRIPT = """
        local writer_key = KEYS[1]
        local semaphore_key = KEYS[2]
        local reader_key = KEYS[3]
        local token = ARGV[1]
        local expiration = tonumber(ARGV[2])
        local semaphore_expiration = tonumber(ARGV[3])
        local max_writers = tonumber(ARGV[4])

        local timespec = redis.call('time')
        local time = timespec[1] + 1e-6 * timespec[2]
        redis.call('zremrangebyscore', KEYS[2], 0, time)
        redis.call('zremrangebyscore', KEYS[3], 0, time)

        local read_locked = redis.call('zcard', reader_key) > 0
        local write_locked = redis.call('exists', writer_key) > 0
        if read_locked or write_locked then
            local op = 'create'
            if semaphore_expiration == 0 then
                op = 'delete'
            elseif max_writers > 0 then
                local count = redis.call('zcard', semaphore_key)
                if write_locked then
                    count = count + 1
                end
                if count >= max_writers then
                    op = 'update'
                end
            end

            local blocked
            if op == 'update' then
                blocked = redis.call('zadd', semaphore_key, 'XX', 'CH', time + semaphore_expiration, token) > 0
            elseif op == 'create' then
                redis.call('zadd', semaphore_key, time + semaphore_expiration, token)
                blocked = true
            else
                redis.call('zrem', semaphore_key, token)
                blocked = false
            end

            if blocked then
                return 1
            else
                return 2
            end
        end

        redis.call('zrem', semaphore_key, token)
        redis.call('set', writer_key, token, 'PX', math.floor(1000 * expiration))

        return 0
    """

    # KEYS[1] - writer lock name
    # ARGV[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_WRITER_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """

    # KEYS[1] - reader lock name
    # ARGV[1] - token
    # ARGV[2] - expiration
    # return 1 if the lock was reacquired, otherwise 0
    LUA_REACQUIRE_READER_SCRIPT = """
        local token = ARGV[1]

        local timespec = redis.call('time')
        local time = timespec[1] + 1e-6 * timespec[2]
        local expiry = time + ARGV[2]
        return redis.call('zadd', KEYS[1], 'XX', 'CH', expiry, token) > 0
    """

    # KEYS[1] - writer lock name
    # ARGV[1] - token
    # ARGV[2] - expiration
    # return 1 if the lock was reacquired, otherwise 0
    LUA_REACQUIRE_WRITER_SCRIPT = """
        local token = ARGV[1]
        local value = redis.call('get', KEYS[1])
        if value == token then
            redis.call('pexpire', KEYS[1], math.floor(1000 * ARGV[2]))
            return 1
        else
            return 0
        end
    """

    def __init__(
        self,
        redis: Redis,
        prefix: str,
        timeout: Number,
        sleep: Number = 0.1,
        blocking: bool = True,
        blocking_timeout: Optional[Number] = None,
        max_writers: Optional[Number] = None,
    ) -> None:
        """Construct a new lock.

        The `RwLock` object only identifies a lock; use ``read`` or
        ``write`` to construct a guard that can be waited on to acquire
        the lock.

        ``prefix``: The prefix to use for keys created by this lock. All
        keys beginning with this prefix should be treated as *reserved*
        by the lock.

        ``timeout``: The expiration on leases held by readers and
        writers. This prevents deadlocks but may lead to unsynchronized
        access if the timeout elapses while a process believes it is
        still holding the lock. Use `reacquire()` to refresh the lease
        when holding the lock for long periods of time.

        ``sleep``: The time in seconds to wait between attempts to
        acquire the lock while spinning.

        ``blocking``: If `True`, then attempting to acquire the lock
        will block the current thread if the lock is contended.

        ``blocking_timeout``: If ``blocking`` is `True`, the maximum
        amount of time to wait for the lock to be released when
        contended. If `None`, blocks forever.

        ``max_writers``: If set to a positive number, the maximum number
        of writers that may contend the lock. For example, if set to 1,
        then only one writer may hold or wait on the lock at a time. Any
        additional writers will fail to acquire the lock and raise
        ``LockMaxWritersError``.
        """
        self.redis = redis
        self.prefix = prefix
        if timeout <= 1e-3:
            raise ValueError('Timeout must be at least 1ms')
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.max_writers = max_writers or 0
        self._register_scripts()

    def _register_scripts(self) -> None:
        cls = self.__class__
        client = self.redis
        if cls.lua_acquire_reader is None:
            cls.lua_acquire_reader = client.register_script(cls.LUA_ACQUIRE_READER_SCRIPT)
        if cls.lua_acquire_writer is None:
            cls.lua_acquire_writer = client.register_script(cls.LUA_ACQUIRE_WRITER_SCRIPT)
        if cls.lua_release_writer is None:
            cls.lua_release_writer = client.register_script(cls.LUA_RELEASE_WRITER_SCRIPT)
        if cls.lua_reacquire_reader is None:
            cls.lua_reacquire_reader = client.register_script(cls.LUA_REACQUIRE_READER_SCRIPT)
        if cls.lua_reacquire_writer is None:
            cls.lua_reacquire_writer = client.register_script(cls.LUA_REACQUIRE_WRITER_SCRIPT)

    def _reader_lock_name(self) -> str:
        return f'{self.prefix}:read'

    def _writer_lock_name(self) -> str:
        return f'{self.prefix}:write'

    def _writer_semaphore_name(self) -> str:
        return f'{self.prefix}:write_semaphore'

    def _make_token(self) -> Any:
        token = self.redis.get_encoder().encode(uuid4().hex)
        return token

    def read(
        self,
        timeout: Optional[Number] = None,
        sleep: Optional[Number] = None,
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[Number] = None,
    ) -> ReadLockGuard:
        """Construct a guard that can be used to acquire the lock in
        shared write mode.

        See ``RwLock`` for documentation on parameters.
        """
        return ReadLockGuard(
            lock=self,
            token=self._make_token(),
            timeout=timeout if timeout is not None else self.timeout,
            sleep=sleep if sleep is not None else self.sleep,
            blocking=blocking if blocking is not None else self.blocking,
            blocking_timeout=blocking_timeout if blocking_timeout is not None else self.blocking_timeout,
        )

    def write(
        self,
        timeout: Optional[Number] = None,
        sleep: Optional[Number] = None,
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[Number] = None,
    ) -> WriteLockGuard:
        """Construct a guard that can be used to acquire the lock in
        exclusive write mode.

        See ``RwLock`` for documentation on other parameters.
        """
        return WriteLockGuard(
            lock=self,
            token=self._make_token(),
            timeout=timeout if timeout is not None else self.timeout,
            sleep=sleep if sleep is not None else self.sleep,
            blocking=blocking if blocking is not None else self.blocking,
            blocking_timeout=blocking_timeout if blocking_timeout is not None else self.blocking_timeout,
        )

    def is_write_locked(self) -> bool:
        """Returns `True` if the lock is currently held by any writer."""
        return bool(self.redis.exists(self._writer_lock_name()))


class BaseLockGuard(abc.ABC):
    lock: RwLock

    token: Any

    timeout: Number
    sleep: Number
    blocking: bool
    blocking_timeout: Optional[Number]

    def __init__(
        self,
        lock: RwLock,
        token: Any,
        timeout: Number,
        sleep: Number,
        blocking: bool,
        blocking_timeout: Optional[Number],
    ) -> None:
        self.lock = lock
        self.token = token
        if timeout <= 1e-3:
            raise ValueError('Timeout must be at least 1ms')
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout

    def __enter__(self) -> Self:
        if not self.acquire():
            raise LockError(
                "Unable to acquire lock within the time specified",
                lock_name=self.lock.prefix,
            )
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.release()

    @property
    def redis(self) -> Redis:
        return self.lock.redis

    @abc.abstractmethod
    def _acquire(self, block_readers: bool) -> bool:
        ...

    @abc.abstractmethod
    def _release(self) -> bool:
        ...

    @abc.abstractmethod
    def _reacquire(self, timeout: Number) -> bool:
        ...

    def acquire(
        self,
        sleep: Optional[Number] = None,
        blocking: Optional[bool] = None,
        blocking_timeout: Optional[Number] = None,
    ) -> bool:
        """Attempts to acquire the lock.

        See ``RwLock`` for documentation on parameters.
        """
        if sleep is None:
            sleep = self.sleep
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = mod_time.monotonic() + blocking_timeout
        while True:
            next_try_at = mod_time.monotonic() + sleep
            stop_trying = not blocking or (
                stop_trying_at is not None and next_try_at > stop_trying_at
            )
            if self._acquire(should_block=not stop_trying):
                return True
            if stop_trying:
                return False
            mod_time.sleep(sleep)

    def release(self) -> None:
        """Releases the lease on the lock.

        Throws ``LockNotOwnedError`` if the lock is no longer owned at
        the time of release.
        """
        if not self._release():
            raise LockNotOwnedError(
                "Cannot release a lock that's no longer owned",
                lock_name=self.lock.prefix,
            )

    def reacquire(self, timeout: Optional[Number] = None) -> bool:
        """Resets the TTL of an already acquired lease back to a timeout
        value.

        When holding the lock for a long amount of time, call this periodically
        to ensure the lease does not expire.
        """
        if timeout is None:
            timeout = self.timeout
        if not self._reacquire(timeout):
            raise LockNotOwnedError(
                "Cannot reacquire a lock that's no longer owned",
                lock_name=self.lock.prefix,
            )
        return True


class ReadLockGuard(BaseLockGuard):
    """A lock guard that will acquire a shared read-mode lease on a
    lock.
    """

    @override
    def _acquire(self, should_block: bool) -> bool:
        return bool(self.lock.lua_acquire_reader(
            keys=[
                self.lock._writer_lock_name(),
                self.lock._writer_semaphore_name(),
                self.lock._reader_lock_name(),
            ],
            args=[self.token, self.timeout],
            client=self.redis,
        ))

    @override
    def _release(self) -> bool:
        return self.redis.zrem(self.lock._reader_lock_name(), self.token)

    @override
    def _reacquire(self, timeout: Number) -> bool:
        result = self.lock.lua_reacquire_reader(
            keys=[self.lock._reader_lock_name()],
            args=[self.token, timeout],
            client=self.redis,
        )
        return bool(result)


class WriteLockGuard(BaseLockGuard):
    """A lock guard that will acquire an exclusive write-mode lease on a
    lock.
    """

    @property
    def _semaphore_timeout(self) -> Optional[Number]:
        # Block readers just long enough to get through a sleep cycle
        return 1.1 * self.sleep

    @override
    def _acquire(self, should_block: bool):
        code = self.lock.lua_acquire_writer(
            keys=[
                self.lock._writer_lock_name(),
                self.lock._writer_semaphore_name(),
                self.lock._reader_lock_name(),
            ],
            args=[
                self.token,
                # Lock timeout
                self.timeout,
                # Writer semaphore timeout
                self._semaphore_timeout if should_block else 0,
                # Max writers
                self.lock.max_writers,
            ],
            client=self.redis,
        )
        if should_block and code == 2:
            raise LockMaxWritersError
        else:
            return code == 0

    @override
    def _release(self) -> bool:
        return bool(self.lock.lua_release_writer(
            keys=[self.lock._writer_lock_name()],
            args=[self.token],
            client=self.redis,
        ))

    @override
    def _reacquire(self, timeout: Number) -> bool:
        return bool(self.lock.lua_reacquire_writer(
            keys=[self.lock._writer_lock_name()],
            args=[self.token, timeout],
            client=self.redis,
        ))
