from collections.abc import Callable, Coroutine, Sequence
from typing import Any, TYPE_CHECKING

import anyio

if TYPE_CHECKING:
    from redis.anyio.client import Pipeline, Redis


def from_url(url, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.anyio.client import Redis

    return Redis.from_url(url, **kwargs)


class pipeline:  # noqa: N801
    def __init__(self, redis_obj: "Redis"):
        self.p: "Pipeline" = redis_obj.pipeline()

    async def __aenter__(self) -> "Pipeline":
        return self.p

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.p.execute()
        del self.p


async def gather(*coros: Coroutine[Any, Any, Any]) -> Sequence[Any]:
    results = [None] * len(coros)

    async def run_coro(coro: Coroutine[Any, Any, Any], index: int) -> None:
        results[index] = await coro

    async with anyio.create_task_group() as tg:
        for i, coro in enumerate(coros):
            tg.start_soon(run_coro, coro, i)

    return results


async def wait_for_condition(cond: anyio.Condition, predicate: Callable) -> None:
    result = predicate()
    while not result:
        await cond.wait()
        result = predicate()

    return result
