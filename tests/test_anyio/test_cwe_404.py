import contextlib
from contextlib import AsyncExitStack

import anyio
import pytest
from anyio.abc import ByteReceiveStream, ByteSendStream, ByteStream, TaskGroup

from redis.anyio import Redis
from redis.anyio.cluster import RedisCluster
from redis.anyio.utils import gather

pytestmark = pytest.mark.anyio


class DelayProxy:
    def __init__(self, addr, redis_addr, delay: float = 0.0):
        self.addr = addr
        self.redis_addr = redis_addr
        self.delay = delay
        self.send_event = anyio.Event()

    async def __aenter__(self):
        # test that we can connect to redis
        with anyio.fail_after(2):
            async with await anyio.connect_tcp(*self.redis_addr):
                pass

        async with AsyncExitStack() as stack:
            listener = await anyio.create_tcp_listener(
                local_port=self.addr[1], reuse_port=True
            )
            await stack.enter_async_context(listener)
            task_group = await stack.enter_async_context(anyio.create_task_group())
            task_group.start_soon(listener.serve, self.handle)
            stack.callback(task_group.cancel_scope.cancel)
            self._stack = stack.pop_all()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._stack.__aexit__(exc_type, exc_val, exc_tb)

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

    async def handle(self, stream: ByteStream) -> None:
        # establish connection to redis
        redis_stream = await anyio.connect_tcp(*self.redis_addr)
        async with stream, redis_stream, anyio.create_task_group() as tg:
            tg.start_soon(self.pipe, stream, redis_stream, True)
            tg.start_soon(self.pipe, redis_stream, stream, False)

    async def pipe(
        self,
        reader: ByteReceiveStream,
        writer: ByteSendStream,
        set_event_on_receive: bool,
    ):
        async for data in reader:
            if set_event_on_receive:
                self.send_event.set()

            await anyio.sleep(self.delay)
            await writer.send(data)


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

                dp.send_event = anyio.Event()
                async with anyio.create_task_group() as tg:
                    tg.start_soon(op, r)
                    # Wait until the task has sent, and then some, to make sure it has
                    # settled on the read.
                    await dp.send_event.wait()
                    await anyio.sleep(0.01)  # a little extra time for prudence
                    tg.cancel_scope.cancel()

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

                async def op(r):
                    with dp.set_delay(delay * 2):
                        return await r.get(
                            "foo"
                        )  # <-- this is the operation we want to cancel

                dp.send_event = anyio.Event()
                async with anyio.create_task_group() as tg:
                    tg.start_soon(op, r)
                    # wait until task has settled on the read
                    await dp.send_event.wait()
                    await anyio.sleep(0.01)
                    tg.cancel_scope.cancel()

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
    cluster_port = 16379
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
        proxy = DelayProxy(addr=(hostname, remapped), redis_addr=forward_addr)
        proxies.append(proxy)

    def all_clear():
        for p in proxies:
            p.send_event = anyio.Event()

    async def wait_for_send():
        async def wait_and_cancel_rest(proxy: DelayProxy, tg: TaskGroup) -> None:
            await proxy.send_event.wait()
            tg.cancel_scope.cancel()

        async with anyio.create_task_group() as tg:
            for p in proxies:
                tg.start_soon(wait_and_cancel_rest, p, tg)

    @contextlib.contextmanager
    def set_delay(delay: float):
        with contextlib.ExitStack() as stack:
            for p in proxies:
                stack.enter_context(p.set_delay(delay))
            yield

    async with contextlib.AsyncExitStack() as stack:
        for p in proxies:
            await stack.enter_async_context(p)

        async with RedisCluster.from_url(
            f"redis://{hostname}:{remap_base}", address_remap=remap
        ) as r:
            await r.set("foo", "foo")
            await r.set("bar", "bar")

            async def op(r):
                with set_delay(delay):
                    return await r.get("foo")

            all_clear()
            async with anyio.create_task_group() as tg:
                # Wait for whichever DelayProxy gets the request first
                tg.start_soon(op, r)
                await wait_for_send()
                await anyio.sleep(0.01)
                tg.cancel_scope.cancel()

            # try a number of requests to exercise all the connections
            async def doit():
                assert await r.get("bar") == b"bar"
                assert await r.ping()
                assert await r.get("foo") == b"foo"

            await gather(*[doit() for _ in range(10)])
