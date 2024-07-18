import time

import pytest
from redis.client import Redis
from redis.exceptions import LockError, LockNotOwnedError
from redis.lock import Lock

from .conftest import _get_client


class TestLock:
    @pytest.fixture()
    def r_decoded(self, request):
        return _get_client(Redis, request=request, decode_responses=True)

    def get_lock(self, redis, *args, **kwargs):
        kwargs["lock_class"] = Lock
        return redis.lock(*args, **kwargs)

    def test_lock(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        assert r.get("foo") == lock.local.token
        assert r.ttl("foo") == -1
        lock.release()
        assert r.get("foo") is None

    def test_lock_token(self, r):
        lock = self.get_lock(r, "foo")
        self._test_lock_token(r, lock)

    def test_lock_token_thread_local_false(self, r):
        lock = self.get_lock(r, "foo", thread_local=False)
        self._test_lock_token(r, lock)

    def _test_lock_token(self, r, lock):
        assert lock.acquire(blocking=False, token="test")
        assert r.get("foo") == b"test"
        assert lock.local.token == b"test"
        assert r.ttl("foo") == -1
        lock.release()
        assert r.get("foo") is None
        assert lock.local.token is None

    def test_locked(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.locked() is False
        lock.acquire(blocking=False)
        assert lock.locked() is True
        lock.release()
        assert lock.locked() is False

    def _test_owned(self, client):
        lock = self.get_lock(client, "foo")
        assert lock.owned() is False
        lock.acquire(blocking=False)
        assert lock.owned() is True
        lock.release()
        assert lock.owned() is False

        lock2 = self.get_lock(client, "foo")
        assert lock.owned() is False
        assert lock2.owned() is False
        lock2.acquire(blocking=False)
        assert lock.owned() is False
        assert lock2.owned() is True
        lock2.release()
        assert lock.owned() is False
        assert lock2.owned() is False

    def test_owned(self, r):
        self._test_owned(r)

    def test_owned_with_decoded_responses(self, r_decoded):
        self._test_owned(r_decoded)

    def test_competing_locks(self, r):
        lock1 = self.get_lock(r, "foo")
        lock2 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        assert not lock2.acquire(blocking=False)
        lock1.release()
        assert lock2.acquire(blocking=False)
        assert not lock1.acquire(blocking=False)
        lock2.release()

    def test_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8 < r.ttl("foo") <= 10
        lock.release()

    def test_float_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=9.5)
        assert lock.acquire(blocking=False)
        assert 8 < r.pttl("foo") <= 9500
        lock.release()

    def test_blocking_timeout(self, r):
        lock1 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        bt = 0.4
        sleep = 0.05
        fudge_factor = 0.05
        lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
        start = time.monotonic()
        assert not lock2.acquire()
        # The elapsed duration should be less than the total blocking_timeout
        assert (bt + fudge_factor) > (time.monotonic() - start) > bt - sleep
        lock1.release()

    def test_context_manager(self, r):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        with self.get_lock(r, "foo", blocking_timeout=0.2) as lock:
            assert r.get("foo") == lock.local.token
        assert r.get("foo") is None

    def test_context_manager_blocking_timeout(self, r):
        with self.get_lock(r, "foo", blocking=False):
            bt = 0.4
            sleep = 0.05
            fudge_factor = 0.05
            lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
            start = time.monotonic()
            assert not lock2.acquire()
            # The elapsed duration should be less than the total blocking_timeout
            assert (bt + fudge_factor) > (time.monotonic() - start) > bt - sleep

    def test_context_manager_raises_when_locked_not_acquired(self, r):
        r.set("foo", "bar")
        with pytest.raises(LockError):
            with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass

    def test_high_sleep_small_blocking_timeout(self, r):
        lock1 = self.get_lock(r, "foo")
        assert lock1.acquire(blocking=False)
        sleep = 60
        bt = 1
        lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
        start = time.monotonic()
        assert not lock2.acquire()
        # the elapsed timed is less than the blocking_timeout as the lock is
        # unattainable given the sleep/blocking_timeout configuration
        assert bt > (time.monotonic() - start)
        lock1.release()

    def test_releasing_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        with pytest.raises(LockError):
            lock.release()

    def test_releasing_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        lock.acquire(blocking=False)
        # manually change the token
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.token is None

    def test_extend_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl("foo") <= 10000
        assert lock.extend(10)
        assert 16000 < r.pttl("foo") <= 20000
        lock.release()

    def test_extend_lock_replace_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl("foo") <= 10000
        assert lock.extend(10, replace_ttl=True)
        assert 8000 < r.pttl("foo") <= 10000
        lock.release()

    def test_extend_lock_float(self, r):
        lock = self.get_lock(r, "foo", timeout=10.0)
        assert lock.acquire(blocking=False)
        assert 8000 < r.pttl("foo") <= 10000
        assert lock.extend(10.0)
        assert 16000 < r.pttl("foo") <= 20000
        lock.release()

    def test_extending_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            lock.extend(10)

    def test_extending_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.extend(10)
        lock.release()

    def test_extending_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.extend(10)

    def test_reacquire_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        assert r.pexpire("foo", 5000)
        assert r.pttl("foo") <= 5000
        assert lock.reacquire()
        assert 8000 < r.pttl("foo") <= 10000
        lock.release()

    def test_reacquiring_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            lock.reacquire()

    def test_reacquiring_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert lock.acquire(blocking=False)
        with pytest.raises(LockError):
            lock.reacquire()
        lock.release()

    def test_reacquiring_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert lock.acquire(blocking=False)
        r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            lock.reacquire()

    def test_context_manager_reacquiring_lock_with_no_timeout_raises_error(self, r):
        with self.get_lock(r, "foo", timeout=None, blocking=False) as lock:
            with pytest.raises(LockError):
                lock.reacquire()

    def test_context_manager_reacquiring_lock_no_longer_owned_raises_error(self, r):
        with pytest.raises(LockNotOwnedError):
            with self.get_lock(r, "foo", timeout=10, blocking=False):
                r.set("foo", "a")

    def test_lock_error_gives_correct_lock_name(self, r):
        r.set("foo", "bar")
        with pytest.raises(LockError) as excinfo:
            with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass
            assert excinfo.value.lock_name == "foo"


class TestLockClassSelection:
    def test_lock_class_argument(self, r):
        class MyLock:
            def __init__(self, *args, **kwargs):
                pass

        lock = r.lock("foo", lock_class=MyLock)
        assert isinstance(lock, MyLock)
