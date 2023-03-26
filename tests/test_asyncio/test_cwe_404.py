import asyncio
import sys
import pytest
from redis.asyncio import Redis

async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, delay: float, name=''):
    while data := await reader.read(1000):
        # print(name, 'received:', data)
        await asyncio.sleep(delay)
        writer.write(data)
        await writer.drain()


class DelayProxy:

    def __init__(self, addr, redis_addr, delay: float):
        self.addr = addr
        self.redis_addr = redis_addr
        self.delay = delay

    async def start(self):
        server = await asyncio.start_server(self.handle, *self.addr)
        self.ROUTINE = asyncio.create_task(server.serve_forever())

    async def handle(self, reader, writer):
        # establish connection to redis
        redis_reader, redis_writer = await asyncio.open_connection(*self.redis_addr)
        pipe1 = asyncio.create_task(pipe(reader, redis_writer, self.delay, 'to redis:'))
        pipe2 = asyncio.create_task(pipe(redis_reader, writer, self.delay, 'from redis:'))
        await asyncio.gather(pipe1, pipe2)
        
    async def kill(self):
        self.ROUTINE.cancel()
        
async def test_standalone():
    
    # create a tcp socket proxy that relays data to Redis and back, inserting 0.1 seconds of delay
    dp = DelayProxy(addr=('localhost', 6380), redis_addr=('localhost', 6379), delay=0.1)
    await dp.start()

    for b in [True, False]:
    # note that we connect to proxy, rather than to Redis directly
        async with Redis(host='localhost', port=6380, single_connection_client=b) as r:

            await r.set('foo', 'foo')
            await r.set('bar', 'bar')

            t = asyncio.create_task(r.get('foo'))
            await asyncio.sleep(0.050)
            t.cancel()
            try:
                await t
                sys.stderr.write('try again, we did not cancel the task in time\n')
            except asyncio.CancelledError:
                sys.stderr.write('managed to cancel the task, connection is left open with unread response\n')

            assert await r.get("bar") == b"bar"
            assert await r.ping()
            assert await r.get("foo") == b"foo"
        
    await dp.kill()
    
async def test_standalone_pipeline():
    dp = DelayProxy(addr=('localhost', 6380), redis_addr=('localhost', 6379), delay=0.1)
    await dp.start()
    async with Redis(host='localhost', port=6380) as r:
        await r.set('foo', 'foo')
        await r.set('bar', 'bar')
        
        pipe = r.pipeline()
        pipe.get("bar")
        pipe.ping()
        pipe.get("foo")
        
        pipe2 = r.pipeline()
        pipe2.get("bar")
        pipe2.ping()
        pipe2.get("foo")
        
        t = asyncio.create_task(r.get('foo'))
        await asyncio.sleep(0.050)
        t.cancel()
        assert await pipe.execute() == [b"bar", True, b"foo"]
        assert await pipe2.execute() == [b"bar", True, b"foo"]

    await dp.kill()
