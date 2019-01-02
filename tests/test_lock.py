import pytest
import time

from redis.exceptions import LockError, LockNotOwnedError
from redis.lock import Lock


class TestLock(object):
    def get_lock(self, redis, *args, **kwargs):
        kwargs['lock_class'] = Lock
        return redis.lock(*args, **kwargs)

    def test_lock(self, r):
        lock = self.get_lock(r, 'foo')
        assert lock.acquire(blocking=False)
        assert r.get('foo') == lock.local.token
        assert r.ttl('foo') == -1
        lock.release()
        assert r.get('foo') is None

    def test_lock_token(self, r):
        lock = self.get_lock(r, 'foo')
        assert lock.acquire(blocking=False, token='test')
        assert r.get('foo') == b'test'
        assert lock.local.token == 'test'
        assert r.ttl('foo') == -1
        lock.release()
        assert r.get('foo') is None
        assert lock.local.token is None

    def test_locked(self, r):
        lock = self.get_lock(r, 'foo')
        assert lock.locked() is False
        lock.acquire(blocking=False)
        assert lock.locked() is True
        lock.release()
        assert lock.locked() is False

        # Original lock should always return false for locked(), especially
        # when a new lock is acquired
        lock2 = self.get_lock(r, 'foo')
        assert lock.locked() is False
        assert lock2.locked() is False
        lock2.acquire(blocking=False)
        assert lock.locked() is True
        assert lock.locked(True) is False
        assert lock2.locked() is True
        lock2.release()
        assert lock.locked() is False
        assert lock2.locked() is False

    def test_competing_locks(self, r):
        lock1 = self.get_lock(r, 'foo')
        lock2 = self.get_lock(r, 'foo')
        assert lock1.acquire(blocking=False)
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire(blocking=False)
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeout(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        assert lock.acquire(blocking=False)
        assert 8 < r.ttl('foo') <= 10
        lock.release()

    def test_float_timeout(self, r):
        lock = self.get_lock(r, 'foo', timeout=9.5)
        assert lock.acquire(blocking=False)
        assert 8 < r.pttl('foo') <= 9500
        lock.release()

    def test_blocking_timeout(self, r):
        lock1 = self.get_lock(r, 'foo')
        assert lock1.acquire(blocking=False)
        lock2 = self.get_lock(r, 'foo', blocking_timeout=0.2)
        start = time.time()
        assert not lock2.acquire()
        assert (time.time() - start) > 0.2
        lock1.release()

    def test_context_manager(self, r):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        with self.get_lock(r, 'foo', blocking_timeout=0.2) as lock:
            assert r.get('foo') == lock.local.token
        assert r.get('foo') is None

    def test_context_manager_raises_when_locked_not_acquired(self, r):
        r.set('foo', 'bar')
        with pytest.raises(LockError):
            with self.get_lock(r, 'foo', blocking_timeout=0.1):
                pass

    def test_high_sleep_raises_error(self, r):
        "If sleep is higher than timeout, it should raise an error"
        with pytest.raises(LockError):
            self.get_lock(r, 'foo', timeout=1, sleep=2)

    def test_releasing_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, 'foo')
        with pytest.raises(LockError):
            lock.release()

    def test_releasing_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, 'foo')
        lock.acquire(blocking=False)
        # manually change the token
        r.set('foo', 'a')
        with pytest.raises(LockNotOwnedError):
            lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.token is None

    def test_extend_lock(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl('foo') <= 10000
        assert lock.extend(10)
        assert 16000 < r.pttl('foo') <= 20000
        lock.release()

    def test_extend_lock_float(self, r):
        lock = self.get_lock(r, 'foo', timeout=10.0)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl('foo') <= 10000
        assert lock.extend(10.0)
        assert 16000 < r.pttl('foo') <= 20000
        lock.release()

    def test_extending_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        with pytest.raises(LockError):
            lock.extend(10)

    def test_extending_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, 'foo')
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.extend(10)
        lock.release()

    def test_extending_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        assert lock.acquire(blocking=False)
        r.set('foo', 'a')
        with pytest.raises(LockNotOwnedError):
            lock.extend(10)

    def test_reacquire_lock(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        assert lock.acquire(blocking=False)
        assert r.pexpire('foo', 5000)
        assert r.pttl('foo') <= 5000
        assert lock.reacquire()
        assert 8000 < r.pttl('foo') <= 10000
        lock.release()

    def test_reacquiring_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        with pytest.raises(LockError):
            lock.reacquire()

    def test_reacquiring_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, 'foo')
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.reacquire()
        lock.release()

    def test_reacquiring_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, 'foo', timeout=10)
        assert lock.acquire(blocking=False)
        r.set('foo', 'a')
        with pytest.raises(LockNotOwnedError):
            lock.reacquire()


class TestLockClassSelection(object):
    def test_lock_class_argument(self, r):
        class MyLock(object):
            def __init__(self, *args, **kwargs):

                pass
        lock = r.lock('foo', lock_class=MyLock)
        assert type(lock) == MyLock
