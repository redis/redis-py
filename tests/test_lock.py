from __future__ import with_statement
import pytest
import sys
import time

from redis.client import Lock, LuaLock, LockError
from .conftest import r as _redis_client

class TestLock(object):

    def test_lock(self, r):
        lock = Lock(r, 'foo')
        assert lock.acquire()
        assert r['foo'] == str(Lock.LOCK_FOREVER).encode()
        lock.release()
        assert r.get('foo') is None

    def test_competing_locks(self, r):
        lock1 = Lock(r, 'foo')
        lock2 = Lock(r, 'foo')
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire()
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeouts(self, r):
        lock1 = Lock(r, 'foo', timeout=1)
        lock2 = Lock(r, 'foo')
        assert lock1.acquire()
        now = time.time()
        assert now < lock1.acquired_until < now + 1
        assert lock1.acquired_until == float(r['foo'])
        assert not lock2.acquire(blocking=False)
        time.sleep(2)  # need to wait up to 2 seconds for lock to timeout
        assert lock2.acquire(blocking=False)
        lock2.release()

    def test_non_blocking(self, r):
        lock1 = Lock(r, 'foo')
        assert lock1.acquire(blocking=False)
        assert lock1.acquired_until
        lock1.release()
        assert lock1.acquired_until is None

    def test_context_manager(self, r):
        with Lock(r, 'foo'):
            assert r['foo'] == str(Lock.LOCK_FOREVER).encode()
        assert r.get('foo') is None

    def test_float_timeout(self, r):
        lock1 = Lock(r, 'foo', timeout=1.5)
        lock2 = Lock(r, 'foo', timeout=1.5)
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()

    def test_high_sleep_raises_error(self, r):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            Lock(r, 'foo', timeout=1, sleep=2)


@pytest.fixture()
def rll(request, **kwargs):
    r = _redis_client(request=request)
    LuaLock.register_scripts(r)
    return r


class TestLuaLock(object):

    def test_lock(self, rll):
        lock = LuaLock(rll, 'foo')
        assert lock.acquire()
        assert rll['foo'] == lock.token
        lock.release()
        assert rll.get('foo') is None

    def test_competing_locks(self, rll):
        lock1 = LuaLock(rll, 'foo')
        lock2 = LuaLock(rll, 'foo')
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire()
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeouts(self, rll):
        lock1 = LuaLock(rll, 'foo', timeout=1)
        lock2 = LuaLock(rll, 'foo')
        assert lock1.acquire()
        now = time.time()
        assert rll['foo'] == lock1.token
        assert not lock2.acquire(blocking=False)
        time.sleep(2)  # need to wait up to 2 seconds for lock to timeout
        assert lock2.acquire(blocking=False)
        lock2.release()

    def test_non_blocking(self, rll):
        lock1 = LuaLock(rll, 'foo')
        assert lock1.acquire(blocking=False)
        assert rll['foo'] == lock1.token
        lock1.release()

    def test_context_manager(self, rll):
        lock = LuaLock(rll, 'foo')
        with lock:
            assert rll['foo'] == lock.token
        assert rll.get('foo') is None

    def test_float_timeout(self, rll):
        lock1 = LuaLock(rll, 'foo', timeout=1.5)
        lock2 = LuaLock(rll, 'foo', timeout=1.5)
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()

    def test_high_sleep_raises_error(self, rll):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            LuaLock(rll, 'foo', timeout=1, sleep=2)
