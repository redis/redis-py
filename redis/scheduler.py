import threading
import time
from typing import Callable


class Scheduler:

    def __init__(self, polling_period: float = 0.1):
        """
        :param polling_period: Period between polling operations.
        Needs to detect when new job has to be scheduled.
        """
        self.polling_period = polling_period

    def run_with_interval(
        self,
        func: Callable[[threading.Event, ...], None],
        interval: float,
        cancel: threading.Event,
        args: tuple = (),
    ) -> threading.Thread:
        """
        Run scheduled execution with given interval
        in a separate thread until cancel event won't be set.
        """
        done = threading.Event()
        thread = threading.Thread(
            target=self._run_timer, args=(func, interval, (done, *args), done, cancel)
        )
        thread.start()
        return thread

    def _get_timer(
        self, func: Callable[[threading.Event, ...], None], interval: float, args: tuple
    ) -> threading.Timer:
        timer = threading.Timer(interval=interval, function=func, args=args)
        return timer

    def _run_timer(
        self,
        func: Callable[[threading.Event, ...], None],
        interval: float,
        args: tuple,
        done: threading.Event,
        cancel: threading.Event,
    ):
        timer = self._get_timer(func, interval, args)
        timer.start()

        while not cancel.is_set():
            if done.is_set():
                done.clear()
                timer.join()
                timer = self._get_timer(func, interval, args)
                timer.start()
            else:
                time.sleep(self.polling_period)

        timer.cancel()
        timer.join()
