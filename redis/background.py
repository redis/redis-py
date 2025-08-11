import asyncio
import threading
from typing import Callable

class BackgroundScheduler:
    """
    Schedules background tasks execution either in separate thread or in the running event loop.
    """
    def __init__(self):
        self._next_timer = None

    def __del__(self):
        if self._next_timer:
            self._next_timer.cancel()

    def run_once(self, delay: float, callback: Callable, *args):
        """
        Runs callable task once after certain delay in seconds.
        """
        # Run loop in a separate thread to unblock main thread.
        loop = asyncio.new_event_loop()
        thread = threading.Thread(
            target=_start_event_loop_in_thread,
            args=(loop, self._call_later, delay, callback, *args),
            daemon=True
        )
        thread.start()

    def run_recurring(
            self,
            interval: float,
            callback: Callable,
            *args
    ):
        """
        Runs recurring callable task with given interval in seconds.
        """
        # Run loop in a separate thread to unblock main thread.
        loop = asyncio.new_event_loop()

        thread = threading.Thread(
            target=_start_event_loop_in_thread,
            args=(loop, self._call_later_recurring, interval, callback, *args),
            daemon=True
        )
        thread.start()

    def _call_later(self, loop: asyncio.AbstractEventLoop, delay: float, callback: Callable, *args):
        self._next_timer = loop.call_later(delay, callback, *args)

    def _call_later_recurring(
            self,
            loop: asyncio.AbstractEventLoop,
            interval: float,
            callback: Callable,
            *args
    ):
        self._call_later(
            loop, interval, self._execute_recurring, loop, interval, callback, *args
        )

    def _execute_recurring(
            self,
            loop: asyncio.AbstractEventLoop,
            interval: float,
            callback: Callable,
            *args
    ):
        """
        Executes recurring callable task with given interval in seconds.
        """
        callback(*args)

        self._call_later(
            loop, interval, self._execute_recurring, loop, interval, callback, *args
        )


def _start_event_loop_in_thread(event_loop: asyncio.AbstractEventLoop, call_soon_cb: Callable, *args):
    """
    Starts event loop in a thread and schedule callback as soon as event loop is ready.
    Used to be able to schedule tasks using loop.call_later.

    :param event_loop:
    :return:
    """
    asyncio.set_event_loop(event_loop)
    event_loop.call_soon(call_soon_cb, event_loop, *args)
    event_loop.run_forever()