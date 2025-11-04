import asyncio
from time import sleep

import pytest

from redis.background import BackgroundScheduler


class TestBackgroundScheduler:
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
        "interval,timeout,call_count",
        [
            (0.012, 0.04, 3),
            (0.035, 0.04, 1),
            (0.045, 0.04, 0),
        ],
    )
    def test_run_recurring(self, interval, timeout, call_count):
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
        scheduler.run_recurring(interval, callback, one, two)
        assert execute_counter == 0

        sleep(timeout)

        assert execute_counter == call_count

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "interval,timeout,call_count",
        [
            (0.012, 0.04, 3),
            (0.035, 0.04, 1),
            (0.045, 0.04, 0),
        ],
    )
    async def test_run_recurring_async(self, interval, timeout, call_count):
        execute_counter = 0
        one = "arg1"
        two = 9999

        async def callback(arg1: str, arg2: int):
            nonlocal execute_counter
            nonlocal one
            nonlocal two

            execute_counter += 1

            assert arg1 == one
            assert arg2 == two

        scheduler = BackgroundScheduler()
        await scheduler.run_recurring_async(interval, callback, one, two)
        assert execute_counter == 0

        await asyncio.sleep(timeout)

        assert execute_counter == call_count
