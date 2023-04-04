import asyncio
import sys

import pytest

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster


async def pipe(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float, name=""
):
    while True:
        data = await reader.read(1000)
        if not data:
            break
        await asyncio.sleep(delay)
        writer.write(data)
        await writer.drain()


class DelayProxy:
    def __init__(self, addr, redis_addr, delay: float):
        self.addr = addr
        self.redis_addr = redis_addr
        self.delay = delay

    async def start(self):
        asyncio.get_event_loop()
        self.server = await asyncio.start_server(self.handle, *self.addr)

    async def handle(self, reader, writer):
        # establish connection to redis
        redis_reader, redis_writer = await asyncio.open_connection(*self.redis_addr)
        pipe1 = asyncio.ensure_future(
            pipe(reader, redis_writer, self.delay, "to redis:")
        )
        pipe2 = asyncio.ensure_future(
            pipe(redis_reader, writer, self.delay, "from redis:")
        )
        asyncio.gather(pipe1, pipe2)

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()


@pytest.mark.asyncio
@pytest.mark.onlynoncluster
async def test_standalone(delay=0.05):

    # create a tcp socket proxy that relays data to Redis and back,
    # inserting 0.1 seconds of delay
    dp = DelayProxy(
        addr=("localhost", 5380), redis_addr=("localhost", 6379), delay=delay * 2
    )
    await dp.start()

    for b in [True, False]:
        # note that we connect to proxy, rather than to Redis directly
        async with Redis(host="localhost", port=5380, single_connection_client=b) as r:

            await r.set("foo", "foo")
            await r.set("bar", "bar")

            t = asyncio.ensure_future(r.get("foo"))
            await asyncio.sleep(delay)
            t.cancel()
            try:
                await t
                sys.stderr.write("try again, we did not cancel the task in time\n")
            except asyncio.CancelledError:
                sys.stderr.write(
                    "canceled task, connection is left open with unread response\n"
                )

            assert await r.get("bar") == b"bar"
            assert await r.ping()
            assert await r.get("foo") == b"foo"

    await dp.stop()


@pytest.mark.asyncio
@pytest.mark.onlynoncluster
async def test_standalone_pipeline(delay=0.05):
    dp = DelayProxy(
        addr=("localhost", 5380), redis_addr=("localhost", 6379), delay=delay * 2
    )
    await dp.start()

    async with Redis(host="localhost", port=5380) as r:
        await r.set("foo", "foo")
        await r.set("bar", "bar")

        pipe = r.pipeline()

        pipe2 = r.pipeline()
        pipe2.get("bar")
        pipe2.ping()
        pipe2.get("foo")

        t = asyncio.ensure_future(pipe.get("foo").execute())
        await asyncio.sleep(delay)
        t.cancel()

        pipe.get("bar")
        pipe.ping()
        pipe.get("foo")
        await pipe.reset()

        assert await pipe.execute() in [None, []]

        # validating that the pipeline can be used as it could previously
        pipe.get("bar")
        pipe.ping()
        pipe.get("foo")
        assert await pipe.execute() == [b"bar", True, b"foo"]
        assert await pipe2.execute() == [b"bar", True, b"foo"]

    await dp.stop()


@pytest.mark.asyncio
@pytest.mark.onlycluster
async def test_cluster(request):

    dp = DelayProxy(addr=("localhost", 5381), redis_addr=("localhost", 6372), delay=0.1)
    await dp.start()

    r = RedisCluster.from_url("redis://localhost:5381")
    await r.initialize()
    await r.set("foo", "foo")
    await r.set("bar", "bar")

    t = asyncio.ensure_future(r.get("foo"))
    await asyncio.sleep(0.050)
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pytest.fail("connection is left open with unread response")

    assert await r.get("bar") == b"bar"
    assert await r.ping()
    assert await r.get("foo") == b"foo"

    await dp.stop()
