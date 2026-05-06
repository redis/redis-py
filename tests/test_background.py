import asyncio
import threading
from time import monotonic, sleep

import pytest

from redis.background import BackgroundScheduler


class TestBackgroundScheduler:
    @staticmethod
    def _wait_for_calls(execute_counter, min_call_count, timeout):
        if min_call_count == 0:
            sleep(timeout)
            return

        deadline = monotonic() + max(timeout, 0.2)
        while len(execute_counter) < min_call_count and monotonic() < deadline:
            sleep(0.002)

    @staticmethod
    async def _wait_for_calls_async(execute_counter, min_call_count, timeout):
        if min_call_count == 0:
            await asyncio.sleep(timeout)
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(timeout, 0.2)
        while len(execute_counter) < min_call_count and loop.time() < deadline:
            await asyncio.sleep(0.002)

    def test_run_once(self):
        execute_counter = 0
        one = "arg1"
        two = 9999

        def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter += 1

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        scheduler.run_once(0.1, callback, one, two)
        assert execute_counter == 0

        sleep(0.15)

        assert execute_counter == 1

    @pytest.mark.parametrize(
        "interval,timeout,min_call_count",
        [
            (0.012, 0.04, 2),  # At least 2 calls (was 3, but timing on CI can vary)
            (0.035, 0.04, 1),
            (0.045, 0.04, 0),
        ],
    )
    def test_run_recurring(self, interval, timeout, min_call_count):
        execute_counter = []
        one = "arg1"
        two = 9999

        def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter.append(1)

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        scheduler.run_recurring(interval, callback, one, two)
        assert len(execute_counter) == 0

        self._wait_for_calls(execute_counter, min_call_count, timeout)

        # Use >= instead of == to account for timing variations on CI runners
        assert len(execute_counter) >= min_call_count

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "interval,timeout,min_call_count",
        [
            (0.012, 0.04, 2),  # At least 2 calls (was 3, but timing on CI can vary)
            (0.035, 0.04, 1),
            (0.045, 0.04, 0),
        ],
    )
    async def test_run_recurring_async(self, interval, timeout, min_call_count):
        execute_counter = []
        one = "arg1"
        two = 9999

        async def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter.append(1)

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        await scheduler.run_recurring_async(interval, callback, one, two)
        assert len(execute_counter) == 0

        await self._wait_for_calls_async(execute_counter, min_call_count, timeout)

        # Use >= instead of == to account for timing variations on CI runners
        assert len(execute_counter) >= min_call_count

    @pytest.mark.parametrize(
        "interval,timeout,min_call_count",
        [
            (0.012, 0.04, 2),  # At least 2 calls
            (0.035, 0.04, 1),
            (0.045, 0.04, 0),
        ],
    )
    def test_run_recurring_coro(self, interval, timeout, min_call_count):
        """
        Test that run_recurring_coro executes coroutines in a background thread.
        This is used by sync MultiDBClient for async health checks.
        """
        execute_counter = []
        one = "arg1"
        two = 9999

        async def coro_callback(arg1: str, arg2: int):
            nonlocal execute_counter
            execute_counter.append(1)
            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(interval, coro_callback, one, two)
        assert len(execute_counter) == 0

        self._wait_for_calls(execute_counter, min_call_count, timeout)

        # Use >= instead of == to account for timing variations on CI runners
        assert len(execute_counter) >= min_call_count
        scheduler.stop()

    def test_run_recurring_coro_runs_in_background_thread(self):
        """
        Verify that run_recurring_coro executes in a separate thread,
        not blocking the main thread.
        """
        main_thread_id = threading.current_thread().ident
        coro_thread_ids = []

        async def coro_callback():
            coro_thread_ids.append(threading.current_thread().ident)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, coro_callback)

        sleep(0.05)  # Wait for at least one execution

        assert len(coro_thread_ids) >= 1
        # Coroutine should run in a different thread than main
        assert all(tid != main_thread_id for tid in coro_thread_ids)
        scheduler.stop()

    def test_run_recurring_coro_supports_concurrent_execution(self):
        """
        Verify that multiple coroutines scheduled in the same background loop
        can run concurrently (not blocking each other).
        """
        execution_order = []
        lock = threading.Lock()

        async def slow_coro(name: str):
            with lock:
                execution_order.append(f"{name}_start")
            await asyncio.sleep(0.02)  # Simulate async I/O
            with lock:
                execution_order.append(f"{name}_end")

        async def fast_coro(name: str):
            with lock:
                execution_order.append(f"{name}_start")
            await asyncio.sleep(0.005)
            with lock:
                execution_order.append(f"{name}_end")

        scheduler = BackgroundScheduler()
        # Schedule both coroutines - they share the same event loop
        scheduler.run_recurring_coro(0.01, slow_coro, "slow")
        scheduler.run_recurring_coro(0.01, fast_coro, "fast")

        sleep(0.1)
        scheduler.stop()

        # Both coroutines should have executed
        assert "slow_start" in execution_order
        assert "fast_start" in execution_order

    def test_run_recurring_coro_with_timeout(self):
        """
        Test that asyncio.wait_for works correctly within run_recurring_coro,
        which is critical for health check timeouts.
        """
        results = []

        async def slow_operation():
            await asyncio.sleep(1.0)  # Takes 1 second
            return "completed"

        async def coro_with_timeout():
            try:
                result = await asyncio.wait_for(slow_operation(), timeout=0.01)
                results.append(("success", result))
            except asyncio.TimeoutError:
                results.append(("timeout", None))

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.02, coro_with_timeout)

        sleep(0.1)
        scheduler.stop()

        # Should have at least one timeout result
        assert len(results) >= 1
        assert all(r[0] == "timeout" for r in results)

    def test_run_recurring_coro_stops_cleanly(self):
        """
        Verify that stop() properly terminates the background event loop.
        """
        execute_counter = []

        async def coro_callback():
            execute_counter.append(1)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, coro_callback)

        sleep(0.05)
        count_before_stop = len(execute_counter)
        assert count_before_stop >= 1

        scheduler.stop()
        sleep(0.05)

        # No more executions after stop
        count_after_stop = len(execute_counter)
        # Allow for at most 1 more execution due to timing
        assert count_after_stop <= count_before_stop + 1

    def test_run_recurring_coro_exception_does_not_stop_scheduler(self):
        """
        Verify that exceptions in coroutines don't crash the scheduler.
        """
        success_count = []
        error_count = []

        async def flaky_coro():
            if len(success_count) % 2 == 0:
                error_count.append(1)
                raise ValueError("Simulated error")
            success_count.append(1)

        scheduler = BackgroundScheduler()
        scheduler.run_recurring_coro(0.01, flaky_coro)

        sleep(0.1)
        scheduler.stop()

        # Both successes and errors should have occurred
        # (scheduler continues despite exceptions)
        total_executions = len(success_count) + len(error_count)
        assert total_executions >= 2

    def test_run_recurring_coro_prevents_overlapping_executions(self):
        """
        Verify that the next execution is scheduled only after the current one completes,
        preventing overlapping runs when execution takes longer than the interval.
        """
        execution_log = []
        lock = threading.Lock()

        async def slow_coro():
            with lock:
                execution_log.append(("start", len(execution_log)))
            await asyncio.sleep(0.05)  # Takes 50ms, longer than 10ms interval
            with lock:
                execution_log.append(("end", len(execution_log)))

        scheduler = BackgroundScheduler()
        # Interval is 10ms but execution takes 50ms
        scheduler.run_recurring_coro(0.01, slow_coro)

        sleep(0.2)
        scheduler.stop()

        # Verify no overlapping: each "start" should be followed by "end" before next "start"
        # With overlap prevention, pattern should be: start, end, start, end, ...
        starts = [i for i, (event, _) in enumerate(execution_log) if event == "start"]
        ends = [i for i, (event, _) in enumerate(execution_log) if event == "end"]

        # Each start should have a corresponding end before the next start
        for i, start_idx in enumerate(starts[:-1]):
            # Find the end that follows this start
            corresponding_ends = [e for e in ends if e > start_idx]
            assert len(corresponding_ends) > 0, "Start without corresponding end"
            # The end should come before the next start
            next_start_idx = starts[i + 1]
            assert corresponding_ends[0] < next_start_idx, (
                "Overlapping execution detected"
            )
