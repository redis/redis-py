import asyncio

import pytest


@pytest.mark.asyncio
async def test_usage_counter(create_redis):
    r = await create_redis(decode_responses=True)

    async def dummy_task():
        async with r:
            await asyncio.sleep(0.01)

    tasks = [dummy_task() for _ in range(20)]
    await asyncio.gather(*tasks)

    # After all tasks have completed, the usage counter should be back to zero.
    assert r._usage_counter == 0
