from typing import TYPE_CHECKING

import anyio

if TYPE_CHECKING:
    from typing import Any, Awaitable, Callable, List

    from redis.asyncio.client import Pipeline, Redis


def from_url(url, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.

    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.asyncio.client import Redis

    return Redis.from_url(url, **kwargs)


class pipeline:  # noqa: N801
    def __init__(self, redis_obj: "Redis"):
        self.p: "Pipeline" = redis_obj.pipeline()

    async def __aenter__(self) -> "Pipeline":
        return self.p

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.p.execute()
        del self.p


async def anyio_gather(
    *tasks: "Awaitable[Any]", return_exceptions: bool = False
) -> "List[Any]":
    results = [None] * len(tasks)

    async def _wrapper(idx: int, task: "Awaitable[Any]") -> "Any":
        with anyio.CancelScope(shield=True):
            try:
                results[idx] = await task
            except Exception as e:
                if return_exceptions:
                    results[idx] = e
                else:
                    raise

    async with anyio.create_task_group() as tg:
        for task in tasks:
            tg.start_soon(_wrapper, task)

    return results


async def anyio_condition_wait_for(
    condition: anyio.Condition, predicate: "Callable[[], bool]"
):
    """Wait until a predicate becomes true.

    The predicate should be a callable which result will be
    interpreted as a boolean value.  The final predicate value is
    the return value.
    """
    result = predicate()
    while not result:
        await condition.wait()
        result = predicate()
    return result
