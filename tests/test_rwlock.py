import threading
import time as mod_time

import pytest

from redis.client import Redis
from redis.exceptions import LockError
from redis.exceptions import LockMaxWritersError
from redis.exceptions import LockNotOwnedError
from redis.rwlock import RwLock


class TestLock:
    def test_write_lock(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard = lock.write()
        assert guard.acquire(blocking=False)
        assert r.get('foo:write') == guard.token
        assert r.ttl('foo:write') == 10
        guard.release()
        assert r.get('foo:write') is None

    def test_read_lock(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard = lock.read()
        assert guard.acquire(blocking=False)
        score = r.zmscore('foo:read', [guard.token])[0]
        expected = mod_time.time() + 10
        assert 0.999 * expected < score and score < expected
        guard.release()
        assert r.zcard('foo:read') == 0

    def test_multiple_readers(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard1 = lock.read()
        guard2 = lock.read()
        assert guard1.acquire()
        assert guard2.acquire()
        assert r.zcard('foo:read') == 2
        guard1.release()
        assert r.zcard('foo:read') == 1
        guard2.release()
        assert r.zcard('foo:read') == 0

    def test_mutual_exclusion(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard1 = lock.write()
        guard2 = lock.write()
        assert guard1.acquire()
        assert not guard2.acquire(blocking=False)
        guard1.release()
        assert guard2.acquire(blocking=False)
        guard2.release()

    def test_writer_reader_exclusion(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        wguard = lock.write()
        rguard = lock.read()
        assert wguard.acquire()
        assert not rguard.acquire(blocking=False)
        wguard.release()
        assert rguard.acquire(blocking=False)
        rguard.release()

    def test_reader_writer_exclusion(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        wguard = lock.write()
        rguard = lock.read()
        assert rguard.acquire()
        assert not wguard.acquire(blocking=False)
        rguard.release()
        assert wguard.acquire(blocking=False)
        wguard.release()

    def test_context_manager(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        with lock.write() as guard:
            assert r.get('foo:write') == guard.token
        assert r.get('foo:write') is None
        with lock.read():
            assert r.zcard('foo:read') == 1
        assert r.zcard('foo:read') == 0

    def test_reader_blocking_timeout(self, r: Redis):
        lock = RwLock(
            r,
            'foo',
            timeout=10,
            sleep=0.01,
            blocking=True,
            blocking_timeout=0.1,
        )
        with lock.write():
            start = mod_time.time()
            assert not lock.read().acquire()
            end = mod_time.time()
            assert end - start > lock.blocking_timeout - lock.sleep

    def test_writer_blocking_timeout(self, r: Redis):
        lock = RwLock(
            r,
            'foo',
            timeout=10,
            blocking=True,
            sleep=0.01,
            blocking_timeout=0.1,
        )
        with lock.read():
            start = mod_time.time()
            assert not lock.write().acquire()
            end = mod_time.time()
            assert end - start > lock.blocking_timeout - lock.sleep

    def test_writer_waiting_blocks_readers(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        r.zadd('foo:write_semaphore', {'token': mod_time.time() + 10})
        assert not lock.read().acquire(blocking=False)

    def test_read_timeout_then_write(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.03)
        rguard = lock.read()
        wguard = lock.write()
        assert rguard.acquire(blocking=False)
        assert r.zcard('foo:read') == 1
        assert not wguard.acquire(blocking=False)
        mod_time.sleep(0.03)
        assert wguard.acquire(blocking=False)
        assert r.zcard('foo:read') == 0

    def test_read_timeout_then_read(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.03)
        guard1 = lock.read()
        assert guard1.acquire(blocking=False)
        assert r.zcard('foo:read') == 1
        mod_time.sleep(0.03)
        guard2 = lock.read()
        assert guard2.acquire(blocking=False)
        assert r.zcard('foo:read') == 1

    def test_write_waiting_then_read(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=1)
        r.zadd('foo:write_semaphore', {'token': mod_time.time()})
        assert lock.read().acquire(blocking=False)

    def test_write_waiting_then_write(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=1)
        r.zadd('foo:write_semaphore', {'token': mod_time.time()})
        assert lock.write().acquire(blocking=False)

    def test_write_reacquire(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1)
        guard = lock.write()
        guard.acquire()
        mod_time.sleep(0.01)
        ttl = r.pttl('foo:write')
        assert 89 <= ttl and ttl <= 91
        guard.reacquire()
        ttl = r.pttl('foo:write')
        assert ttl >= 99

    def test_write_reacquire_failure(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard1 = lock.write()
        guard1.acquire()
        r.delete('foo:write')
        with pytest.raises(LockNotOwnedError):
            guard1.reacquire()
        guard2 = lock.write()
        guard2.acquire()
        with pytest.raises(LockNotOwnedError):
            guard1.reacquire()

    def test_read_reacquire(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1)
        guard = lock.read()
        guard.acquire()

        mod_time.sleep(10e-3)
        expiry = r.zmscore('foo:read', [guard.token])[0]
        now = mod_time.time()
        assert 89e-3 <= expiry - now and expiry - now < 91e-3

        guard.reacquire()
        expiry = r.zmscore('foo:read', [guard.token])[0]
        now = mod_time.time()
        assert 99e-3 <= expiry - now and expiry - now < 100e-3

    def test_read_release_without_acquire_raises(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard = lock.read()
        with pytest.raises(LockNotOwnedError):
            guard.release()

    def test_read_release_no_longer_owned_raises(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1)
        guard = lock.read()
        assert guard.acquire(blocking=False)
        r.zrem(lock._reader_lock_name(), guard.token)
        with pytest.raises(LockNotOwnedError):
            guard.release()

    def test_write_release_without_acquire_raises(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=10)
        guard = lock.write()
        with pytest.raises(LockNotOwnedError):
            guard.release()

    def test_write_release_no_longer_owned_raises(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1)
        guard = lock.write()
        assert guard.acquire(blocking=False)
        r.set(lock._writer_lock_name(), b'other-token')
        with pytest.raises(LockNotOwnedError):
            guard.release()

    def test_read_reacquire_after_expiration(self, r: Redis):
        """Can reacquire a read lock after expiration of the key still"""
        lock = RwLock(r, 'foo', timeout=0.01)
        guard = lock.read()
        assert guard.acquire(blocking=False)
        mod_time.sleep(0.01)
        assert guard.reacquire()

    def test_read_reacquire_no_longer_owned_raises(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1)
        guard = lock.read()
        assert guard.acquire(blocking=False)
        r.zrem(lock._reader_lock_name(), guard.token)
        with pytest.raises(LockNotOwnedError):
            guard.reacquire()

    def test_read_context_manager_blocking_timeout(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1, sleep=0.01)
        wguard = lock.write()
        assert wguard.acquire(blocking=False)
        with pytest.raises(LockError) as excinfo:
            with lock.read(blocking_timeout=0.05, sleep=0.01):
                pass
        assert excinfo.value.lock_name == 'foo'
        wguard.release()

    def test_write_context_manager_blocking_timeout(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=0.1, sleep=0.01)
        rguard = lock.read()
        assert rguard.acquire(blocking=False)
        with pytest.raises(LockError):
            with lock.write(blocking_timeout=0.05, sleep=0.01):
                pass
        rguard.release()

    def test_unique_writer(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=1, max_writers=1)
        guard1 = lock.write()
        assert guard1.acquire()
        guard2 = lock.write()
        with pytest.raises(LockMaxWritersError):
            guard2.acquire()

    def test_max_writers(self, r: Redis):
        lock = RwLock(r, 'foo', timeout=1, blocking_timeout=50e-3, sleep=10e-3, max_writers=2)
        guard1 = lock.write()
        assert guard1.acquire()
        result = []

        # Spawn a thread to wait on the lock
        def target():
            with lock.write():
                result.append(1)
        thread = threading.Thread(target=target)
        thread.start()

        # Third writer fails
        guard2 = lock.write()
        with pytest.raises(LockMaxWritersError):
            guard2.acquire()

        guard1.release()
        thread.join()

        # Thread acquired after guard1 was released
        assert len(result) == 1
        assert not lock.is_write_locked()
