import contextlib
import threading
import time as mod_time
import uuid

from importlib_resources import files

from redis.exceptions import LockError, LockNotOwnedError
from redis.utils import dummy

try:
    # This should exist on py3.3+
    # See: https://www.python.org/dev/peps/pep-0418/
    now = mod_time.monotonic
except ImportError:
    now = mod_time.time


def exponential_back_off_delay(attempts, last_delay, max_delay=None,
                               initial_delay=0.1, mult_factor=2):
    if attempts == 0 or last_delay is None:
        delay_secs = initial_delay
    else:
        delay_secs = last_delay * mult_factor
    if max_delay is None:
        return max(0, delay_secs)
    else:
        return max(0, min(delay_secs, max_delay))


class ReaderWriterLock(object):
    """
    A shared, distributed reader/writer Lock. Using Redis for
    locking allows the ReaderWriterLock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """
    LUA_PACK = "redis.lua"
    LUA_SCRIPTS = {
        'acquire_write_lock': 'acquire_write_lock.lua',
        'release_write_lock': 'release_write_lock.lua',
        'acquire_read_lock': 'acquire_read_lock.lua',
        'release_read_lock': 'release_read_lock.lua',
    }

    def __init__(self, redis, name, timeout=None, thread_local=True):
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.thread_local = bool(thread_local)
        self.local = threading.local() if self.thread_local else dummy()
        if not self.thread_local:
            self.local.token = str(uuid.uuid4())
        else:
            self.local.token = None
        self._scripts = {}
        self.register_scripts()

    def register_scripts(self):
        for n, fn in self.LUA_SCRIPTS.items():
            n_path = files(self.LUA_PACK).joinpath(fn)
            self._scripts[n] = self.redis.register_script(n_path.read_text())

    def _get_token(self):
        token = self.local.token
        if not token:
            token = str(uuid.uuid4())
            self.local.token = token
        return token

    def _try_get_write_lock(self):
        script = self._scripts["acquire_write_lock"]
        token = self._get_token()
        res = int(script(keys=[self.name], args=[token, self.timeout]))
        if res == -1:
            raise LockAcquireError("Something went wrong"
                                   " acquiring write lock with"
                                   " key '%s' (value setting with"
                                   " timeout %s failed)" % (self.name,
                                                            self.timeout))
        return bool(res)

    def _try_release_write_lock(self):
        script = self._scripts["release_write_lock"]
        token = self._get_token()
        res = int(script(keys=[self.name], args=[token]))
        if res == -1:
            raise LockReleaseError("Key data at '%s' was not deleted"
                                   " correctly" % (self.name))
        if res == 0:
            raise LockReleaseError("Key data at '%s' no longer"
                                   " exists" % (self.name))
        if res == 2:
            raise LockReleaseError("Key data at '%s' switched to"
                                   " reader lock while holding a"
                                   " writer lock" % (self.name))
        if res == 3:
            raise LockReleaseError("Key writer lock owner at '%s' is"
                                   " no longer expected"
                                   " token '%s'" % (self.name, token))
        # Catch all to trap anything not success.
        if res != 1:
            raise LockReleaseError("Unexpected return %s when"
                                   " releasing write"
                                   " lock on '%s'" % (res, self.name))

    def _try_get_read_lock(self):
        script = self._scripts["acquire_read_lock"]
        token = self._get_token()
        res = int(script(keys=[self.name], args=[token, self.timeout]))
        if res == -1:
            raise LockAcquireError("Something went wrong"
                                   " acquiring read lock with"
                                   " key '%s' (value setting with"
                                   " ttl %s failed)" % (self.name,
                                                        self.timeout))
        return bool(res)

    def _try_release_read_lock(self):
        script = self._scripts["release_read_lock"]
        token = self._get_token()
        res = int(script(keys=[self.name], args=[token, self.timeout]))
        if res == -2:
            raise LockReleaseError("Key data at '%s' was not"
                                   " set correctly" % (self.name))
        if res == -1:
            raise LockReleaseError("Key data at '%s' was not"
                                   " deleted correctly" % (self.name))
        if res == 0:
            raise LockReleaseError("Key data at '%s' no longer"
                                   " exists" % (self.name))
        if res == 1:
            raise LockReleaseError("Key data at '%s' switched to"
                                   " writer lock while holding"
                                   " a reader lock" % (self.name))
        if res == 2:
            raise LockReleaseError("Reader token '%s' no longer"
                                   " exists (or never existed)"
                                   " in key data at '%s'" % (token,
                                                             self.name))
        # Catch all to trap anything not success.
        if res not in (3, 4):
            raise LockReleaseError("Unexpected return %s when"
                                   " releasing read lock"
                                   " on '%s'" % (res, self.name))

    @contextlib.contextmanager
    def read_locked(self, timeout=None, delay_func=None):
        if delay_func is None:
            delay_func = exponential_back_off_delay
        attempts = 0
        last_delay_secs = None
        stop_trying_at = None
        started_at = None
        if timeout is not None:
            started_at = now()
            stop_trying_at = started_at + timeout
        while True:
            if stop_trying_at is not None and now() >= stop_trying_at:
                elapsed = now() - started_at
                raise LockAcquireTimeoutError("Read lock not acquired"
                                              " after %s seconds" % (elapsed))
            if not self._try_get_read_lock():
                leftover = None
                if stop_trying_at is not None:
                    leftover = max(0, stop_trying_at - now())
                delay_secs = delay_func(attempts,
                                        last_delay_secs, max_delay=leftover)
                attempts += 1
                mod_time.sleep(delay_secs)
                last_delay_secs = delay_secs
            else:
                break
        try:
            yield
        finally:
            self._try_release_read_lock()

    @contextlib.contextmanager
    def write_locked(self, timeout=None, delay_func=None):
        if delay_func is None:
            delay_func = exponential_back_off_delay
        attempts = 0
        last_delay_secs = None
        stop_trying_at = None
        started_at = None
        if timeout is not None:
            started_at = now()
            stop_trying_at = started_at + timeout
        while True:
            if stop_trying_at is not None and now() >= stop_trying_at:
                elapsed = now() - started_at
                raise LockAcquireTimeoutError("Write lock not acquired"
                                              " after %s seconds" % (elapsed))
            if not self._try_get_write_lock():
                leftover = None
                if stop_trying_at is not None:
                    leftover = max(0, stop_trying_at - now())
                delay_secs = delay_func(attempts,
                                        last_delay_secs, max_delay=leftover)
                attempts += 1
                mod_time.sleep(delay_secs)
                last_delay_secs = delay_secs
            else:
                break
        try:
            yield
        finally:
            self._try_release_write_lock()


class Lock(object):
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.
    """

    lua_release = None
    lua_extend = None
    lua_reacquire = None

    # KEYS[1] - lock name
    # ARGV[1] - token
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
    # ARGV[1] - token
    # ARGV[2] - additional milliseconds
    # ARGV[3] - "0" if the additional time should be added to the lock's
    #           existing ttl or "1" if the existing ttl should be replaced
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

        local newttl = ARGV[2]
        if ARGV[3] == "0" then
            newttl = ARGV[2] + expiration
        end
        redis.call('pexpire', KEYS[1], newttl)
        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - milliseconds
    # return 1 if the locks time was reacquired, otherwise 0
    LUA_REACQUIRE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    """

    def __init__(self, redis, name, timeout=None, sleep=0.1,
                 blocking=True, blocking_timeout=None, thread_local=True):
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

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self.thread_local = bool(thread_local)
        self.local = threading.local() if self.thread_local else dummy()
        self.local.token = None
        self.register_scripts()

    def register_scripts(self):
        cls = self.__class__
        client = self.redis
        if cls.lua_release is None:
            cls.lua_release = client.register_script(cls.LUA_RELEASE_SCRIPT)
        if cls.lua_extend is None:
            cls.lua_extend = client.register_script(cls.LUA_EXTEND_SCRIPT)
        if cls.lua_reacquire is None:
            cls.lua_reacquire = \
                client.register_script(cls.LUA_REACQUIRE_SCRIPT)

    def __enter__(self):
        # force blocking, as otherwise the user would have to check whether
        # the lock was actually acquired or not.
        if self.acquire(blocking=True):
            return self
        raise LockError("Unable to acquire lock within the time specified")

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def acquire(self, blocking=None, blocking_timeout=None, token=None):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.

        ``blocking_timeout`` specifies the maximum number of seconds to
        wait trying to acquire the lock.

        ``token`` specifies the token value to be used. If provided, token
        must be a bytes object or a string that can be encoded to a bytes
        object with the default encoding. If a token isn't specified, a UUID
        will be generated.
        """
        sleep = self.sleep
        if token is None:
            token = uuid.uuid1().hex.encode()
        else:
            encoder = self.redis.connection_pool.get_encoder()
            token = encoder.encode(token)
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = mod_time.time() + blocking_timeout
        while True:
            if self.do_acquire(token):
                self.local.token = token
                return True
            if not blocking:
                return False
            next_try_at = mod_time.time() + sleep
            if stop_trying_at is not None and next_try_at > stop_trying_at:
                return False
            mod_time.sleep(sleep)

    def do_acquire(self, token):
        if self.timeout:
            # convert to milliseconds
            timeout = int(self.timeout * 1000)
        else:
            timeout = None
        if self.redis.set(self.name, token, nx=True, px=timeout):
            return True
        return False

    def locked(self):
        """
        Returns True if this key is locked by any process, otherwise False.
        """
        return self.redis.get(self.name) is not None

    def owned(self):
        """
        Returns True if this key is locked by this lock, otherwise False.
        """
        stored_token = self.redis.get(self.name)
        # need to always compare bytes to bytes
        # TODO: this can be simplified when the context manager is finished
        if stored_token and not isinstance(stored_token, bytes):
            encoder = self.redis.connection_pool.get_encoder()
            stored_token = encoder.encode(stored_token)
        return self.local.token is not None and \
            stored_token == self.local.token

    def release(self):
        "Releases the already acquired lock"
        expected_token = self.local.token
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        self.local.token = None
        self.do_release(expected_token)

    def do_release(self, expected_token):
        if not bool(self.lua_release(keys=[self.name],
                                     args=[expected_token],
                                     client=self.redis)):
            raise LockNotOwnedError("Cannot release a lock"
                                    " that's no longer owned")

    def extend(self, additional_time, replace_ttl=False):
        """
        Adds more time to an already acquired lock.

        ``additional_time`` can be specified as an integer or a float, both
        representing the number of seconds to add.

        ``replace_ttl`` if False (the default), add `additional_time` to
        the lock's existing ttl. If True, replace the lock's ttl with
        `additional_time`.
        """
        if self.local.token is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_extend(additional_time, replace_ttl)

    def do_extend(self, additional_time, replace_ttl):
        additional_time = int(additional_time * 1000)
        if not bool(
            self.lua_extend(
                keys=[self.name],
                args=[
                    self.local.token,
                    additional_time,
                    replace_ttl and "1" or "0"
                ],
                client=self.redis,
            )
        ):
            raise LockNotOwnedError(
                "Cannot extend a lock that's" " no longer owned"
            )
        return True

    def reacquire(self):
        """
        Resets a TTL of an already acquired lock back to a timeout value.
        """
        if self.local.token is None:
            raise LockError("Cannot reacquire an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot reacquire a lock with no timeout")
        return self.do_reacquire()

    def do_reacquire(self):
        timeout = int(self.timeout * 1000)
        if not bool(self.lua_reacquire(keys=[self.name],
                                       args=[self.local.token, timeout],
                                       client=self.redis)):
            raise LockNotOwnedError("Cannot reacquire a lock that's"
                                    " no longer owned")
        return True
