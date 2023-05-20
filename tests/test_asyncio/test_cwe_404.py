import asyncio
import contextlib

import pytest

from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster
from redis.asyncio.connection import async_timeout


class DelayProxy:
    def __init__(self, addr, redis_addr, delay: float = 0.0):
        self.addr = addr
        self.redis_addr = redis_addr
        self.delay = delay
        self.send_event = asyncio.Event()
        self.server = None
        self.task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()

    async def start(self):
        # test that we can connect to redis
        async with async_timeout(2):
            _, redis_writer = await asyncio.open_connection(*self.redis_addr)
        redis_writer.close()
        self.server = await asyncio.start_server(
            self.handle, *self.addr, reuse_address=True
        )
        self.task = asyncio.create_task(self.server.serve_forever())

    @contextlib.contextmanager
    def set_delay(self, delay: float = 0.0):
        """
        Allow to override the delay for parts of tests which aren't time dependent,
        to speed up execution.
        """
        old_delay = self.delay
        self.delay = delay
        try:
            yield
        finally:
            self.delay = old_delay

    async def handle(self, reader, writer):
        # establish connection to redis
        redis_reader, redis_writer = await asyncio.open_connection(*self.redis_addr)
        try:
            pipe1 = asyncio.create_task(
                self.pipe(reader, redis_writer, "to redis:", self.send_event)
            )
            pipe2 = asyncio.create_task(self.pipe(redis_reader, writer, "from redis:"))
            await asyncio.gather(pipe1, pipe2)
        finally:
            redis_writer.close()

    async def stop(self):
        # clean up enough so that we can reuse the looper
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass
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
async def test_standalone(delay, master_host):

    # create a tcp socket proxy that relays data to Redis and back,
    # inserting 0.1 seconds of delay
    async with DelayProxy(addr=("127.0.0.1", 5380), redis_addr=master_host) as dp:

        for b in [True, False]:
            # note that we connect to proxy, rather than to Redis directly
            async with Redis(
                host="127.0.0.1", port=5380, single_connection_client=b
            ) as r:

                await r.set("foo", "foo")
                await r.set("bar", "bar")

                async def op(r):
                    with dp.set_delay(delay * 2):
                        return await r.get(
                            "foo"
                        )  # <-- this is the operation we want to cancel

                dp.send_event.clear()
                t = asyncio.create_task(op(r))
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


@pytest.mark.onlynoncluster
@pytest.mark.parametrize("delay", argvalues=[0.05, 0.5, 1, 2])
async def test_standalone_pipeline(delay, master_host):
    async with DelayProxy(addr=("127.0.0.1", 5380), redis_addr=master_host) as dp:
        for b in [True, False]:
            async with Redis(
                host="127.0.0.1", port=5380, single_connection_client=b
            ) as r:
                await r.set("foo", "foo")
                await r.set("bar", "bar")

                pipe = r.pipeline()

                pipe2 = r.pipeline()
                pipe2.get("bar")
                pipe2.ping()
                pipe2.get("foo")

                async def op(pipe):
                    with dp.set_delay(delay * 2):
                        return await pipe.get(
                            "foo"
                        ).execute()  # <-- this is the operation we want to cancel

                dp.send_event.clear()
                t = asyncio.create_task(op(pipe))
                # wait until task has settled on the read
                await dp.send_event.wait()
                await asyncio.sleep(0.01)
                t.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await t

                # we have now cancelled the pieline in the middle of a request,
                # make sure that the connection is still usable
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


@pytest.mark.onlycluster
async def test_cluster(master_host):

    delay = 0.1
    cluster_port = 6372
    remap_base = 7372
    n_nodes = 6
    hostname, _ = master_host

    def remap(address):
        host, port = address
        return host, remap_base + port - cluster_port

    proxies = []
    for i in range(n_nodes):
        port = cluster_port + i
        remapped = remap_base + i
        forward_addr = hostname, port
        proxy = DelayProxy(addr=("127.0.0.1", remapped), redis_addr=forward_addr)
        proxies.append(proxy)

    def all_clear():
        for p in proxies:
            p.send_event.clear()

    async def wait_for_send():
        asyncio.wait(
            [p.send_event.wait() for p in proxies], return_when=asyncio.FIRST_COMPLETED
        )

    @contextlib.contextmanager
    def set_delay(delay: float):
        with contextlib.ExitStack() as stack:
            for p in proxies:
                stack.enter_context(p.set_delay(delay))
            yield

    async with contextlib.AsyncExitStack() as stack:
        for p in proxies:
            await stack.enter_async_context(p)

        with contextlib.closing(
            RedisCluster.from_url(
                f"redis://127.0.0.1:{remap_base}", address_remap=remap
            )
        ) as r:
            await r.initialize()
            await r.set("foo", "foo")
            await r.set("bar", "bar")

            async def op(r):
                with set_delay(delay):
                    return await r.get("foo")

            all_clear()
            t = asyncio.create_task(op(r))
            # Wait for whichever DelayProxy gets the request first
            await wait_for_send()
            await asyncio.sleep(0.01)
            t.cancel()
            with pytest.raises(asyncio.CancelledError):
                await t

            # try a number of requests to excercise all the connections
            async def doit():
                assert await r.get("bar") == b"bar"
                assert await r.ping()
                assert await r.get("foo") == b"foo"

            await asyncio.gather(*[doit() for _ in range(10)])
