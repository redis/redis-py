import asyncio
import logging
import threading
from typing import Any, Callable, Coroutine


class BackgroundScheduler:
    """
    Schedules background tasks execution either in separate thread or in the running event loop.
    """

    def __init__(self):
        self._next_timer = None
        self._event_loops = []
        self._lock = threading.Lock()
        self._stopped = False
        # Dedicated loop for health checks - ensures all health checks use the same loop
        self._health_check_loop: asyncio.AbstractEventLoop | None = None
        self._health_check_thread: threading.Thread | None = None
        # Event to signal when health check loop is ready
        self._health_check_loop_ready = threading.Event()

    def __del__(self):
        self.stop()

    def stop(self):
        """
        Stop all scheduled tasks and clean up resources.
        """
        with self._lock:
            if self._stopped:
                return
            self._stopped = True

            if self._next_timer:
                self._next_timer.cancel()
                self._next_timer = None

            # Stop all event loops
            for loop in self._event_loops:
                if loop.is_running():
                    loop.call_soon_threadsafe(loop.stop)

            self._event_loops.clear()

    def run_once(self, delay: float, callback: Callable, *args):
        """
        Runs callable task once after certain delay in seconds.
        """
        with self._lock:
            if self._stopped:
                return

        # Run loop in a separate thread to unblock main thread.
        loop = asyncio.new_event_loop()

        with self._lock:
            self._event_loops.append(loop)

        thread = threading.Thread(
            target=_start_event_loop_in_thread,
            args=(loop, self._call_later, delay, callback, *args),
            daemon=True,
        )
        thread.start()

    def run_recurring(self, interval: float, callback: Callable, *args):
        """
        Runs recurring callable task with given interval in seconds.
        """
        with self._lock:
            if self._stopped:
                return

        # Run loop in a separate thread to unblock main thread.
        loop = asyncio.new_event_loop()

        with self._lock:
            self._event_loops.append(loop)

        thread = threading.Thread(
            target=_start_event_loop_in_thread,
            args=(loop, self._call_later_recurring, interval, callback, *args),
            daemon=True,
        )
        thread.start()

    def run_recurring_coro(
        self, interval: float, coro: Callable[..., Coroutine[Any, Any, Any]], *args
    ):
        """
        Runs recurring coroutine with given interval in seconds in a background thread.
        Uses a shared event loop to ensure connection pools remain valid across calls.

        This is useful for sync code that needs to run async health checks.
        """
        with self._lock:
            if self._stopped:
                return

        # Use the shared health check loop, creating it if needed
        self._ensure_health_check_loop()

        with self._lock:
            loop = self._health_check_loop

        # Schedule recurring execution in the shared loop
        loop.call_soon_threadsafe(
            self._call_later_recurring_coro, loop, interval, coro, *args
        )

    def run_coro_sync(
        self,
        coro: Callable[..., Coroutine[Any, Any, Any]],
        *args,
        timeout: float | None = 10.0,
    ) -> Any:
        """
        Runs a coroutine synchronously and returns its result.
        Uses the shared health check event loop to ensure connection pools
        created here remain valid for subsequent recurring health checks.

        This is useful for running the initial health check before starting
        recurring checks.

        Args:
            coro: Coroutine function to execute
            *args: Arguments to pass to the coroutine
            timeout: Maximum seconds to wait for the result. None means wait
                forever. Default is 10 seconds to avoid blocking indefinitely
                if the event loop is busy with long-running health checks.

        Returns:
            The result of the coroutine

        Raises:
            TimeoutError: If the coroutine doesn't complete within timeout
            Any exception raised by the coroutine
        """

        with self._lock:
            if self._stopped:
                raise RuntimeError("Scheduler is stopped")

        # Ensure the shared loop exists
        self._ensure_health_check_loop()

        with self._lock:
            loop = self._health_check_loop

        # Submit the coroutine to the shared loop and wait for result
        future = asyncio.run_coroutine_threadsafe(coro(*args), loop)
        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            # Cancel the future to avoid leaving orphaned tasks
            future.cancel()
            raise

    def run_coro_fire_and_forget(
        self, coro: Callable[..., Coroutine[Any, Any, Any]], *args
    ) -> None:
        """
        Schedule a coroutine for execution on the shared health check loop
        without waiting for the result. Exceptions are logged but not raised.

        This is useful for HALF_OPEN recovery health checks that need to run
        on the same event loop where connection pools were created.

        Args:
            coro: Coroutine function to execute
            *args: Arguments to pass to the coroutine
        """
        with self._lock:
            if self._stopped:
                return

        # Ensure the shared loop exists
        self._ensure_health_check_loop()

        with self._lock:
            loop = self._health_check_loop

        def on_complete(future: asyncio.Future):
            """Log any exceptions from the coroutine."""
            if future.cancelled():
                logging.getLogger(__name__).debug("Fire-and-forget coroutine cancelled")
            elif future.exception() is not None:
                logging.getLogger(__name__).debug(
                    "Fire-and-forget coroutine raised exception",
                    exc_info=future.exception(),
                )

        # Schedule on the shared loop without waiting
        future = asyncio.run_coroutine_threadsafe(coro(*args), loop)
        future.add_done_callback(on_complete)

    def _ensure_health_check_loop(self, timeout: float = 5.0):
        """
        Ensure the shared health check loop and thread are running.

        Args:
            timeout: Maximum seconds to wait for the loop to start.

        Raises:
            RuntimeError: If the loop fails to start within the timeout.
        """
        # Fast path: if loop is already running, return immediately
        if self._health_check_loop_ready.is_set():
            with self._lock:
                if (
                    self._health_check_loop is not None
                    and self._health_check_loop.is_running()
                ):
                    return

        with self._lock:
            # Double-check after acquiring the lock
            if (
                self._health_check_loop is not None
                and self._health_check_loop.is_running()
            ):
                return

            # Clear the event - we're about to start a new loop
            self._health_check_loop_ready.clear()

            # Create a new event loop for health checks
            self._health_check_loop = asyncio.new_event_loop()
            self._event_loops.append(self._health_check_loop)

            # Start the loop in a background thread
            self._health_check_thread = threading.Thread(
                target=self._run_health_check_loop,
                daemon=True,
            )
            self._health_check_thread.start()

            # Wait for loop to be running INSIDE the lock with a timeout.
            # This prevents other threads from trying to create another loop
            # before this one is fully started, while avoiding permanent deadlock
            # if the background thread fails to start the loop.
            if not self._health_check_loop_ready.wait(timeout=timeout):
                # Timeout expired - the loop failed to start
                # Clean up the failed loop to allow retry
                failed_loop = self._health_check_loop
                self._health_check_loop = None
                if failed_loop in self._event_loops:
                    self._event_loops.remove(failed_loop)
                try:
                    failed_loop.close()
                except Exception:
                    pass
                raise RuntimeError(
                    f"Health check event loop failed to start within {timeout} seconds"
                )

    def _run_health_check_loop(self):
        """Run the shared health check event loop."""
        asyncio.set_event_loop(self._health_check_loop)

        # Signal that the loop is ready before running
        # Use call_soon to signal after run_forever starts processing
        self._health_check_loop.call_soon(self._health_check_loop_ready.set)

        try:
            self._health_check_loop.run_forever()
        finally:
            try:
                pending = asyncio.all_tasks(self._health_check_loop)
                for task in pending:
                    task.cancel()
                self._health_check_loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            except Exception:
                pass
            finally:
                self._health_check_loop.close()

    def _call_later_recurring_coro(
        self,
        loop: asyncio.AbstractEventLoop,
        interval: float,
        coro: Callable[..., Coroutine[Any, Any, Any]],
        *args,
    ):
        """Schedule first execution of recurring coroutine."""
        with self._lock:
            if self._stopped:
                return
        self._call_later(
            loop, interval, self._execute_recurring_coro, loop, interval, coro, *args
        )

    def _execute_recurring_coro(
        self,
        loop: asyncio.AbstractEventLoop,
        interval: float,
        coro: Callable[..., Coroutine[Any, Any, Any]],
        *args,
    ):
        """
        Executes recurring coroutine with given interval in seconds.
        Schedules next execution only after current one completes to prevent overlap.
        """
        with self._lock:
            if self._stopped:
                return

        def on_complete(task: asyncio.Task):
            """Callback when coroutine completes - schedule next execution."""
            # Log any exceptions (prevents "Task exception was never retrieved")
            if task.cancelled():
                pass  # Task was cancelled, ignore
            elif task.exception() is not None:
                # Log the exception but don't crash the scheduler
                logging.getLogger(__name__).debug(
                    "Background coroutine raised exception",
                    exc_info=task.exception(),
                )

            # Schedule next execution after completion
            with self._lock:
                if self._stopped:
                    return

            self._call_later(
                loop,
                interval,
                self._execute_recurring_coro,
                loop,
                interval,
                coro,
                *args,
            )

        try:
            task = asyncio.ensure_future(coro(*args))
            # Add callback to handle completion and schedule next run
            task.add_done_callback(on_complete)
        except Exception:
            # If scheduling fails (e.g., during shutdown), try to schedule next run anyway
            with self._lock:
                if self._stopped:
                    return
            self._call_later(
                loop,
                interval,
                self._execute_recurring_coro,
                loop,
                interval,
                coro,
                *args,
            )

    async def run_recurring_async(
        self, interval: float, coro: Callable[..., Coroutine[Any, Any, Any]], *args
    ):
        """
        Runs recurring coroutine with given interval in seconds in the current event loop.
        To be used only from an async context. No additional threads are created.

        Prevents overlapping executions by scheduling the next run only after
        the current one completes.

        Raises:
            RuntimeError: If called without a running event loop (programming error)
        """
        with self._lock:
            if self._stopped:
                return

        # This is an async method - it must be awaited in a running event loop.
        # If get_running_loop() raises RuntimeError, let it propagate as that
        # indicates a programming error (calling async method outside async context).
        loop = asyncio.get_running_loop()

        def schedule_next():
            """Schedule the next execution after the current one completes."""
            with self._lock:
                if self._stopped:
                    return
            self._next_timer = loop.call_later(interval, execute_and_reschedule)

        def execute_and_reschedule():
            """Execute the coroutine and schedule next run after completion."""
            with self._lock:
                if self._stopped:
                    return

            def on_complete(task: asyncio.Task):
                """Callback when coroutine completes - schedule next execution."""
                # Log any exceptions (prevents "Task exception was never retrieved")
                if task.cancelled():
                    pass
                elif task.exception() is not None:
                    logging.getLogger(__name__).debug(
                        "Recurring async coroutine raised exception",
                        exc_info=task.exception(),
                    )
                # Schedule next execution AFTER this one completes
                schedule_next()

            try:
                task = asyncio.ensure_future(coro(*args))
                task.add_done_callback(on_complete)
            except Exception:
                # If scheduling fails, still try to schedule next run
                logging.getLogger(__name__).debug(
                    "Failed to schedule recurring async coroutine", exc_info=True
                )
                schedule_next()

        # Schedule first execution
        self._next_timer = loop.call_later(interval, execute_and_reschedule)

    def _call_later(
        self, loop: asyncio.AbstractEventLoop, delay: float, callback: Callable, *args
    ):
        with self._lock:
            if self._stopped:
                return
        self._next_timer = loop.call_later(delay, callback, *args)

    def _call_later_recurring(
        self,
        loop: asyncio.AbstractEventLoop,
        interval: float,
        callback: Callable,
        *args,
    ):
        with self._lock:
            if self._stopped:
                return
        self._call_later(
            loop, interval, self._execute_recurring, loop, interval, callback, *args
        )

    def _execute_recurring(
        self,
        loop: asyncio.AbstractEventLoop,
        interval: float,
        callback: Callable,
        *args,
    ):
        """
        Executes recurring callable task with given interval in seconds.
        """
        with self._lock:
            if self._stopped:
                return

        try:
            callback(*args)
        except Exception:
            # Silently ignore exceptions during shutdown
            pass

        with self._lock:
            if self._stopped:
                return

        self._call_later(
            loop, interval, self._execute_recurring, loop, interval, callback, *args
        )


def _start_event_loop_in_thread(
    event_loop: asyncio.AbstractEventLoop, call_soon_cb: Callable, *args
):
    """
    Starts event loop in a thread and schedule callback as soon as event loop is ready.
    Used to be able to schedule tasks using loop.call_later.

    :param event_loop:
    :return:
    """
    asyncio.set_event_loop(event_loop)
    event_loop.call_soon(call_soon_cb, event_loop, *args)
    try:
        event_loop.run_forever()
    finally:
        try:
            # Clean up pending tasks
            pending = asyncio.all_tasks(event_loop)
            for task in pending:
                task.cancel()
            # Run loop once more to process cancellations
            event_loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        except Exception:
            pass
        finally:
            event_loop.close()
