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
        self, coro: Callable[..., Coroutine[Any, Any, Any]], *args
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

        Returns:
            The result of the coroutine

        Raises:
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
        return future.result()

    def _ensure_health_check_loop(self):
        """Ensure the shared health check loop and thread are running."""
        with self._lock:
            if (
                self._health_check_loop is not None
                and self._health_check_loop.is_running()
            ):
                return

            # Create a new event loop for health checks
            self._health_check_loop = asyncio.new_event_loop()
            self._event_loops.append(self._health_check_loop)

            # Start the loop in a background thread
            self._health_check_thread = threading.Thread(
                target=self._run_health_check_loop,
                daemon=True,
            )
            self._health_check_thread.start()

            # Wait for loop to be running
            while not self._health_check_loop.is_running():
                pass

    def _run_health_check_loop(self):
        """Run the shared health check event loop."""
        asyncio.set_event_loop(self._health_check_loop)
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
            # Schedule the coroutine to run in the event loop
            task = asyncio.ensure_future(coro(*args), loop=loop)
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
        """
        with self._lock:
            if self._stopped:
                return

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()

        wrapped = _async_to_sync_wrapper(loop, coro, *args)

        def tick():
            with self._lock:
                if self._stopped:
                    return
            # Schedule the coroutine
            wrapped()
            # Schedule next tick
            self._next_timer = loop.call_later(interval, tick)

        # Schedule first tick
        self._next_timer = loop.call_later(interval, tick)

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


def _async_to_sync_wrapper(loop, coro_func, *args, **kwargs):
    """
    Wraps an asynchronous function so it can be used with loop.call_later.

    :param loop: The event loop in which the coroutine will be executed.
    :param coro_func: The coroutine function to wrap.
    :param args: Positional arguments to pass to the coroutine function.
    :param kwargs: Keyword arguments to pass to the coroutine function.
    :return: A regular function suitable for loop.call_later.
    """

    def wrapped():
        # Schedule the coroutine in the event loop
        asyncio.ensure_future(coro_func(*args, **kwargs), loop=loop)

    return wrapped
