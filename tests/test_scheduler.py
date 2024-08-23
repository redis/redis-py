import threading
import time

import pytest
from redis.scheduler import Scheduler


class TestScheduler:
    @pytest.mark.parametrize(
        "polling_period,interval,expected_count",
        [
            (0.001, 0.1, (8, 9)),
            (0.1, 0.2, (3, 4)),
            (0.1, 2, (0, 0)),
        ],
        ids=[
            "small polling period (0.001s)",
            "large polling period (0.1s)",
            "interval larger than timeout - no execution",
        ],
    )
    def test_run_with_interval(self, polling_period, interval, expected_count):
        scheduler = Scheduler(polling_period=polling_period)
        cancel_event = threading.Event()
        counter = 0

        def callback(done: threading.Event):
            nonlocal counter
            counter += 1
            done.set()

        scheduler.run_with_interval(
            func=callback, interval=interval, cancel=cancel_event
        )
        time.sleep(1)
        cancel_event.set()
        cancel_event.wait()
        # Due to flacky nature of test case, provides at least 2 possible values.
        assert counter in expected_count
