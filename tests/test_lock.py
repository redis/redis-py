from __future__ import with_statement
import pytest
import time

from redis.client import Lock, LockError


class TestLock(object):

    def test_lock(self, r):
        lock = r.lock('foo')
        assert lock.acquire()
        assert r['foo'] == str(Lock.LOCK_FOREVER).encode()
        lock.release()
        assert r.get('foo') is None

    def test_competing_locks(self, r):
        lock1 = r.lock('foo')
        lock2 = r.lock('foo')
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire()
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeouts(self, r):
        lock1 = r.lock('foo', timeout=1)
        lock2 = r.lock('foo')
        assert lock1.acquire()
        now = time.time()
        assert now < lock1.acquired_until < now + 1
        assert lock1.acquired_until == float(r['foo'])
        assert not lock2.acquire(blocking=False)
        time.sleep(2)  # need to wait up to 2 seconds for lock to timeout
        assert lock2.acquire(blocking=False)
        lock2.release()

    def test_non_blocking(self, r):
        lock1 = r.lock('foo')
        assert lock1.acquire(blocking=False)
        assert lock1.acquired_until
        lock1.release()
        assert lock1.acquired_until is None

    def test_context_manager(self, r):
        with r.lock('foo'):
            assert r['foo'] == str(Lock.LOCK_FOREVER).encode()
        assert r.get('foo') is None

    def test_float_timeout(self, r):
        lock1 = r.lock('foo', timeout=1.5)
        lock2 = r.lock('foo', timeout=1.5)
        assert lock1.acquire()
        assert not lock2.acquire(blocking=False)
        lock1.release()

    def test_high_sleep_raises_error(self, r):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            r.lock('foo', timeout=1, sleep=2)
