import asyncio
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


class DelayProxy:
    def __init__(self, addr, redis_addr, delay: float):
        self.addr = addr
        self.redis_addr = redis_addr
        self.delay = delay
        self.send_event = asyncio.Event()

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
        pipe1 = asyncio.create_task(
            self.pipe(reader, redis_writer, "to redis:", self.send_event)
        )
        pipe2 = asyncio.create_task(self.pipe(redis_reader, writer, "from redis:"))
        await asyncio.gather(pipe1, pipe2)

    async def stop(self):
        # clean up enough so that we can reuse the looper
        self.ROUTINE.cancel()
        loop = self.server.get_loop()
        await loop.shutdown_asyncgens()

    async def pipe(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        name="",
        event: asyncio.Event = None,
    ):
        while True:
            data = await reader.read(1000)
            if not data:
                break
            # print(f"{name} read {len(data)} delay {self.delay}")
            if event:
                event.set()
            await asyncio.sleep(self.delay)
            writer.write(data)
            await writer.drain()


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

            dp.send_event.clear()
            t = asyncio.create_task(r.get("foo"))
            # Wait until the task has sent, and then some, to make sure it has
            # settled on the read.
            await dp.send_event.wait()
            await asyncio.sleep(0.01)  # a little extra time for prudence
            t.cancel()
            with pytest.raises(asyncio.CancelledError):
                await t

            # make sure that our previous request, cancelled while waiting for
            # a repsponse, didn't leave the connection open andin a bad state
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

            dp.send_event.clear()
            t = asyncio.create_task(pipe.get("foo").execute())
            # wait until task has settled on the read
            await dp.send_event.wait()
            await asyncio.sleep(0.01)
            t.cancel()
            with pytest.raises(asyncio.CancelledError):
                await t

            # we have now cancelled the pieline in the middle of a request, make sure
            # that the connection is still usable
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

    dp.send_event.clear()
    t = asyncio.create_task(r.get("foo"))
    await dp.send_event.wait()
    await asyncio.sleep(0.01)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert await r.get("bar") == b"bar"
    assert await r.ping()
    assert await r.get("foo") == b"foo"

    await dp.stop()
