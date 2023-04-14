import asyncio
import sys
import urllib.parse

import pytest

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import async_timeout


@pytest.fixture
def redis_addr(request):
    redis_url = request.config.getoption("--redis-url")
    scheme, netloc = urllib.parse.urlparse(redis_url)[:2]
    assert scheme == "redis"
    if ":" in netloc:
        return netloc.split(":")
    else:
        return netloc, "6379"


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
        # test that we can connect to redis
        async with async_timeout(2):
            _, redis_writer = await asyncio.open_connection(*self.redis_addr)
        redis_writer.close()
        self.server = await asyncio.start_server(self.handle, *self.addr)
        self.ROUTINE = asyncio.create_task(self.server.serve_forever())

    async def handle(self, reader, writer):
        # establish connection to redis
        redis_reader, redis_writer = await asyncio.open_connection(*self.redis_addr)
        pipe1 = asyncio.create_task(pipe(reader, redis_writer, self.delay, "to redis:"))
        pipe2 = asyncio.create_task(
            pipe(redis_reader, writer, self.delay, "from redis:")
        )
        await asyncio.gather(pipe1, pipe2)

    async def stop(self):
        # clean up enough so that we can reuse the looper
        self.ROUTINE.cancel()
        loop = self.server.get_loop()
        await loop.shutdown_asyncgens()


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("delay", argvalues=[0.05, 0.5, 1, 2])
async def test_standalone(delay, redis_addr):

    # create a tcp socket proxy that relays data to Redis and back,
    # inserting 0.1 seconds of delay
    dp = DelayProxy(addr=("127.0.0.1", 5380), redis_addr=redis_addr, delay=delay * 2)
    await dp.start()

    for b in [True, False]:
        # note that we connect to proxy, rather than to Redis directly
        async with Redis(host="127.0.0.1", port=5380, single_connection_client=b) as r:

            await r.set("foo", "foo")
            await r.set("bar", "bar")

            t = asyncio.create_task(r.get("foo"))
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


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("delay", argvalues=[0.05, 0.5, 1, 2])
async def test_standalone_pipeline(delay, redis_addr):
    dp = DelayProxy(addr=("127.0.0.1", 5380), redis_addr=redis_addr, delay=delay * 2)
    await dp.start()
    for b in [True, False]:
        async with Redis(host="127.0.0.1", port=5380, single_connection_client=b) as r:
            await r.set("foo", "foo")
            await r.set("bar", "bar")

            pipe = r.pipeline()

            pipe2 = r.pipeline()
            pipe2.get("bar")
            pipe2.ping()
            pipe2.get("foo")

            t = asyncio.create_task(pipe.get("foo").execute())
            await asyncio.sleep(delay)
            t.cancel()

            pipe.get("bar")
            pipe.ping()
            pipe.get("foo")
            await pipe.reset()

            # check that the pipeline is empty after reset
            assert await pipe.execute() == []

            # validating that the pipeline can be used as it could previously
            pipe.get("bar")
            pipe.ping()
            pipe.get("foo")
            assert await pipe.execute() == [b"bar", True, b"foo"]
            assert await pipe2.execute() == [b"bar", True, b"foo"]

    await dp.stop()


@pytest.mark.onlycluster
async def test_cluster(request, redis_addr):

    redis_addr = redis_addr[0], 6372  # use the cluster port
    dp = DelayProxy(addr=("127.0.0.1", 5381), redis_addr=redis_addr, delay=0.1)
    await dp.start()

    r = RedisCluster.from_url("redis://127.0.0.1:5381")
    await r.initialize()
    await r.set("foo", "foo")
    await r.set("bar", "bar")

    t = asyncio.create_task(r.get("foo"))
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
