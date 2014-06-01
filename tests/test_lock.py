from __future__ import with_statement
import pytest
import time

from redis.lock import LockError


class TestLock(object):

    def test_lock(self, sr):
        lock = sr.lock('foo')
        assert lock.acquire(blocking=False)
        assert sr.get('foo') == lock.token
        assert sr.ttl('foo') == -1
        lock.release()
        assert sr.get('foo') is None

    def test_competing_locks(self, sr):
        lock1 = sr.lock('foo')
        lock2 = sr.lock('foo')
        assert lock1.acquire(blocking=False)
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire(blocking=False)
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeout(self, sr):
        lock = sr.lock('foo', timeout=10)
        assert lock.acquire(blocking=False)
        assert 0 < sr.ttl('foo') <= 10
        lock.release()

    def test_float_timeout(self, sr):
        lock = sr.lock('foo', timeout=9.5)
        assert lock.acquire(blocking=False)
        assert 0 < sr.pttl('foo') <= 9500
        lock.release()

    def test_blocking_timeout(self, sr):
        lock1 = sr.lock('foo')
        assert lock1.acquire(blocking=False)
        lock2 = sr.lock('foo', blocking_timeout=0.2)
        start = time.time()
        assert not lock2.acquire()
        assert (time.time() - start) > 0.2
        lock1.release()

    def test_context_manager(self, sr):
        with sr.lock('foo') as lock:
            assert sr.get('foo') == lock.token
        assert sr.get('foo') is None

    def test_high_sleep_raises_error(self, sr):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            sr.lock('foo', timeout=1, sleep=2)

    def test_releasing_unlocked_lock_raises_error(self, sr):
        lock = sr.lock('foo')
        with pytest.raises(LockError):
            lock.release()

    def test_releasing_lock_no_longer_owned_raises_error(self, sr):
        lock = sr.lock('foo')
        lock.acquire(blocking=False)
        # manually change the token
        sr.set('foo', 'a')
        with pytest.raises(LockError):
            lock.release()
        # even though we errored, the token is still cleared
        assert lock.token is None

    def test_extend_lock(self, sr):
        lock = sr.lock('foo', timeout=10)
        assert lock.acquire(blocking=True)
        assert 0 < sr.pttl('foo') <= 10000
        assert lock.extend(10)
        assert 10000 < sr.pttl('foo') < 20000
        lock.release()

    def test_extend_lock_float(self, sr):
        lock = sr.lock('foo', timeout=10.0)
        assert lock.acquire(blocking=True)
        assert 0 < sr.pttl('foo') <= 10000
        assert lock.extend(10.0)
        assert 10000 < sr.pttl('foo') < 20000
        lock.release()

    def test_extending_unlocked_lock_raises_error(self, sr):
        lock = sr.lock('foo', timeout=10)
        with pytest.raises(LockError):
            lock.extend(10)

    def test_extending_lock_with_no_timeout_raises_error(self, sr):
        lock = sr.lock('foo')
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.extend(10)
        lock.release()

    def test_extending_lock_no_longer_owned_raises_error(self, sr):
        lock = sr.lock('foo')
        assert lock.acquire(blocking=False)
        sr.set('foo', 'a')
        with pytest.raises(LockError):
            lock.extend(10)
