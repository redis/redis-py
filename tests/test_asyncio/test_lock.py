import asyncio

import pytest
import pytest_asyncio
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, LockNotOwnedError


class TestLock:
    @pytest_asyncio.fixture()
    async def r_decoded(self, create_redis):
        redis = await create_redis(decode_responses=True)
        yield redis
        await redis.flushall()

    def get_lock(self, redis, *args, **kwargs):
        kwargs["lock_class"] = Lock
        return redis.lock(*args, **kwargs)

    async def test_lock(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        assert await r.get("foo") == lock.local.token
        assert await r.ttl("foo") == -1
        await lock.release()
        assert await r.get("foo") is None

    async def test_lock_token(self, r):
        lock = self.get_lock(r, "foo")
        await self._test_lock_token(r, lock)

    async def test_lock_token_thread_local_false(self, r):
        lock = self.get_lock(r, "foo", thread_local=False)
        await self._test_lock_token(r, lock)

    async def _test_lock_token(self, r, lock):
        assert await lock.acquire(blocking=False, token="test")
        assert await r.get("foo") == b"test"
        assert lock.local.token == b"test"
        assert await r.ttl("foo") == -1
        await lock.release()
        assert await r.get("foo") is None
        assert lock.local.token is None

    async def test_locked(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.locked() is False
        await lock.acquire(blocking=False)
        assert await lock.locked() is True
        await lock.release()
        assert await lock.locked() is False

    async def _test_owned(self, client):
        lock = self.get_lock(client, "foo")
        assert await lock.owned() is False
        await lock.acquire(blocking=False)
        assert await lock.owned() is True
        await lock.release()
        assert await lock.owned() is False

        lock2 = self.get_lock(client, "foo")
        assert await lock.owned() is False
        assert await lock2.owned() is False
        await lock2.acquire(blocking=False)
        assert await lock.owned() is False
        assert await lock2.owned() is True
        await lock2.release()
        assert await lock.owned() is False
        assert await lock2.owned() is False

    async def test_owned(self, r):
        await self._test_owned(r)

    async def test_owned_with_decoded_responses(self, r_decoded):
        await self._test_owned(r_decoded)

    async def test_competing_locks(self, r):
        lock1 = self.get_lock(r, "foo")
        lock2 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        assert not await lock2.acquire(blocking=False)
        await lock1.release()
        assert await lock2.acquire(blocking=False)
        assert not await lock1.acquire(blocking=False)
        await lock2.release()

    async def test_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8 < (await r.ttl("foo")) <= 10
        await lock.release()

    async def test_float_timeout(self, r):
        lock = self.get_lock(r, "foo", timeout=9.5)
        assert await lock.acquire(blocking=False)
        assert 8 < (await r.pttl("foo")) <= 9500
        await lock.release()

    async def test_blocking(self, r):
        blocking = False
        lock = self.get_lock(r, "foo", blocking=blocking)
        assert not lock.blocking

        lock_2 = self.get_lock(r, "foo")
        assert lock_2.blocking

    async def test_blocking_timeout(self, r):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        bt = 0.2
        sleep = 0.05
        lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
        start = asyncio.get_running_loop().time()
        assert not await lock2.acquire()
        # The elapsed duration should be less than the total blocking_timeout
        assert bt >= (asyncio.get_running_loop().time() - start) > bt - sleep
        await lock1.release()

    async def test_context_manager(self, r):
        # blocking_timeout prevents a deadlock if the lock can't be acquired
        # for some reason
        async with self.get_lock(r, "foo", blocking_timeout=0.2) as lock:
            assert await r.get("foo") == lock.local.token
        assert await r.get("foo") is None

    async def test_context_manager_raises_when_locked_not_acquired(self, r):
        await r.set("foo", "bar")
        with pytest.raises(LockError):
            async with self.get_lock(r, "foo", blocking_timeout=0.1):
                pass

    async def test_high_sleep_small_blocking_timeout(self, r):
        lock1 = self.get_lock(r, "foo")
        assert await lock1.acquire(blocking=False)
        sleep = 60
        bt = 1
        lock2 = self.get_lock(r, "foo", sleep=sleep, blocking_timeout=bt)
        start = asyncio.get_running_loop().time()
        assert not await lock2.acquire()
        # the elapsed timed is less than the blocking_timeout as the lock is
        # unattainable given the sleep/blocking_timeout configuration
        assert bt > (asyncio.get_running_loop().time() - start)
        await lock1.release()

    async def test_releasing_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        with pytest.raises(LockError):
            await lock.release()

    async def test_releasing_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        await lock.acquire(blocking=False)
        # manually change the token
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.release()
        # even though we errored, the token is still cleared
        assert lock.local.token is None

    async def test_extend_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8000 < (await r.pttl("foo")) <= 10000
        assert await lock.extend(10)
        assert 16000 < (await r.pttl("foo")) <= 20000
        await lock.release()

    async def test_extend_lock_replace_ttl(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert 8000 < (await r.pttl("foo")) <= 10000
        assert await lock.extend(10, replace_ttl=True)
        assert 8000 < (await r.pttl("foo")) <= 10000
        await lock.release()

    async def test_extend_lock_float(self, r):
        lock = self.get_lock(r, "foo", timeout=10.0)
        assert await lock.acquire(blocking=False)
        assert 8000 < (await r.pttl("foo")) <= 10000
        assert await lock.extend(10.0)
        assert 16000 < (await r.pttl("foo")) <= 20000
        await lock.release()

    async def test_extending_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            await lock.extend(10)

    async def test_extending_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockError):
            await lock.extend(10)
        await lock.release()

    async def test_extending_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.extend(10)

    async def test_reacquire_lock(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        assert await r.pexpire("foo", 5000)
        assert await r.pttl("foo") <= 5000
        assert await lock.reacquire()
        assert 8000 < (await r.pttl("foo")) <= 10000
        await lock.release()

    async def test_reacquiring_unlocked_lock_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        with pytest.raises(LockError):
            await lock.reacquire()

    async def test_reacquiring_lock_with_no_timeout_raises_error(self, r):
        lock = self.get_lock(r, "foo")
        assert await lock.acquire(blocking=False)
        with pytest.raises(LockError):
            await lock.reacquire()
        await lock.release()

    async def test_reacquiring_lock_no_longer_owned_raises_error(self, r):
        lock = self.get_lock(r, "foo", timeout=10)
        assert await lock.acquire(blocking=False)
        await r.set("foo", "a")
        with pytest.raises(LockNotOwnedError):
            await lock.reacquire()


@pytest.mark.onlynoncluster
class TestLockClassSelection:
    def test_lock_class_argument(self, r):
        class MyLock:
            def __init__(self, *args, **kwargs):
                pass

        lock = r.lock("foo", lock_class=MyLock)
        assert isinstance(lock, MyLock)
