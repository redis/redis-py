"""The async counterparts of the workloads and the app in
tests/test_scenario/scenario_workloads.py: the workers are tasks that share one
client instance. Each workload runs the same commands as its sync counterpart.
"""

import asyncio
from typing import Awaitable, Callable, List

from redis.asyncio import RedisCluster
from tests.test_scenario.scenario_monitor import ClientMonitor, bound_to
from tests.test_scenario.scenario_workloads import (
    BLOCKING_PRODUCER_PAUSE_SECONDS,
    BLOCKING_TIMEOUT,
    INCR_SCRIPT,
    BlockingWorkload,
    MixedWorkload,
    QueueStats,
    SimpleWorkload,
    WorkloadStats,
    validate_counter,
    validate_queue,
)


class AsyncWorkload:
    """The async counterpart of Workload in the sync suite."""

    name: str = ""

    async def round(
        self, app: "AsyncScenarioApp", worker: int, keys: List[str], iteration: int
    ):
        """Run one round of commands over the worker's keys."""
        raise NotImplementedError

    async def validate(self, app: "AsyncScenarioApp"):
        """Check the data the workload wrote, after the app has stopped."""
        stats = app.stats
        for key in set(stats.acked) | set(stats.ambiguous):
            validate_counter(key, int(await app.client.get(key) or 0), stats)


class AsyncSimpleWorkload(AsyncWorkload):
    """The async counterpart of SimpleWorkload: regular data operations."""

    name = SimpleWorkload.name

    async def round(self, app, worker, keys, iteration):
        run = app.run
        for key in keys:
            counter = f"{key}:counter"
            try:
                await run("SET", lambda c: c.set(key, "value"))
                await run("GET", lambda c: c.get(key))
                await app.counted("INCR", counter, lambda c: c.incr(counter))
                await run("HSET", lambda c: c.hset(f"{key}:hash", "field", iteration))
                await run("HGET", lambda c: c.hget(f"{key}:hash", "field"))
                await run("EXPIRE", lambda c: c.expire(f"{key}:hash", 300))
            except Exception:
                continue


class AsyncMixedWorkload(AsyncWorkload):
    """The async counterpart of MixedWorkload: the drill's command mix."""

    name = MixedWorkload.name

    async def round(self, app, worker, keys, iteration):
        run = app.run
        member = f"m{iteration % 20}"
        for key in keys:
            counter = f"{key}:counter"
            script_counter = f"{key}:script-counter"
            hash_key = f"{key}:hash"
            list_key = f"{key}:list"
            stream = f"{key}:stream"
            try:
                await run("SET", lambda c: c.set(key, "value"))
                await run("GET", lambda c: c.get(key))
                await run("EXPIRE", lambda c: c.expire(key, 300))
                await run("TTL", lambda c: c.ttl(key))
                await app.counted("INCR", counter, lambda c: c.incr(counter))
                await run(
                    "HINCRBYFLOAT", lambda c: c.hincrbyfloat(hash_key, "field", 1.5)
                )
                await run("HSETNX", lambda c: c.hsetnx(hash_key, "created", iteration))
                await run("HGET", lambda c: c.hget(hash_key, "field"))
                await run("HLEN", lambda c: c.hlen(hash_key))
                await run("HGETALL", lambda c: c.hgetall(hash_key))
                await run("SADD", lambda c: c.sadd(f"{key}:set", member))
                await run("ZADD", lambda c: c.zadd(f"{key}:zset", {member: iteration}))
                await run("RPUSH", lambda c: c.rpush(list_key, iteration))
                await run("LTRIM", lambda c: c.ltrim(list_key, -50, -1))
                await run("LRANGE", lambda c: c.lrange(list_key, 0, 9))
                await run(
                    "XADD",
                    lambda c: c.xadd(stream, {"f": "v"}, maxlen=100, approximate=True),
                )
                await run("XRANGE", lambda c: c.xrange(stream, count=10))
                await run("XREAD", lambda c: c.xread({stream: "0-0"}, count=10))
                await app.counted(
                    "EVAL",
                    script_counter,
                    lambda c: c.eval(INCR_SCRIPT, 1, script_counter),
                )
            except Exception:
                continue
        if iteration % 5 == 4:
            try:
                await run("EXISTS", lambda c: c.exists(*keys))
                await run("PING", lambda c: c.ping())

                async def pipeline(c):
                    pipe = c.pipeline(transaction=False)
                    for key in keys[:3]:
                        pipe.set(f"{key}:pipe", "value")
                        pipe.get(f"{key}:pipe")
                    return await pipe.execute()

                await run("PIPELINE", pipeline)
            except Exception:
                pass


class AsyncBlockingWorkload(AsyncWorkload):
    """The async counterpart of BlockingWorkload: producers and blocked consumers."""

    name = BlockingWorkload.name

    def __init__(self):
        self.queues = QueueStats()

    async def round(self, app, worker, keys, iteration):
        if worker % 2 == 0:
            await self._produce(app, worker, keys, iteration)
        else:
            await self._consume(app, app.keys[worker - 1])

    async def _produce(self, app, worker, keys, iteration):
        queues = self.queues
        for key in keys:
            queue = BlockingWorkload.queue(key)
            value = f"{worker}-{iteration}"
            try:
                await app.run("RPUSH", lambda c: c.rpush(queue, value))
                queues.pushed[queue].add(value)
            except Exception:
                queues.push_ambiguous[queue].add(value)
        await asyncio.sleep(BLOCKING_PRODUCER_PAUSE_SECONDS)

    async def _consume(self, app, keys):
        queues = self.queues
        for key in keys:
            queue = BlockingWorkload.queue(key)
            try:
                reply = await app.run(
                    "BLPOP", lambda c: c.blpop([queue], timeout=BLOCKING_TIMEOUT)
                )
            except Exception:
                queues.pop_ambiguous[queue] += 1
                continue
            if reply is not None:
                value = reply[1].decode() if isinstance(reply[1], bytes) else reply[1]
                queues.popped[queue].append(value)

    async def validate(self, app):
        queues = self.queues
        problems = []
        for queue in set(queues.pushed) | set(queues.push_ambiguous):
            remaining = [
                v.decode() if isinstance(v, bytes) else v
                for v in await app.client.lrange(queue, 0, -1)
            ]
            problem = validate_queue(queue, remaining, queues)
            if problem:
                problems.append(problem)
        assert not problems, "; ".join(problems[:5])


ASYNC_WORKLOAD_CLASSES = {
    w.name: w for w in (AsyncSimpleWorkload, AsyncMixedWorkload, AsyncBlockingWorkload)
}


class AsyncScenarioApp:
    """The application under test: worker tasks that share one client.

    The async counterpart of ScenarioApp in the sync suite.
    """

    def __init__(
        self,
        client: RedisCluster,
        workload: AsyncWorkload,
        keys: List[List[str]],
        monitor: ClientMonitor,
    ):
        self.client = client
        self.workload = workload
        # keys[i] are the keys of worker i
        self.keys = keys
        self.monitor = monitor
        self.stats = WorkloadStats()
        self._stop = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

    def start(self):
        for index in range(len(self.keys)):
            self._tasks.append(
                asyncio.create_task(self._worker(index), name=f"app_worker_{index}")
            )

    async def stop(self):
        self._stop.set()
        await asyncio.gather(*self._tasks)
        self._tasks = []

    async def run(self, command: str, call: Callable[[RedisCluster], Awaitable]):
        try:
            result = await call(self.client)
            self.stats.ok()
            return result
        except Exception as e:
            self.stats.failed(command, e)
            raise

    async def counted(
        self, command: str, counter: str, call: Callable[[RedisCluster], Awaitable]
    ):
        """Run an increment of counter and record whether it was acknowledged."""
        try:
            await self.run(command, call)
            self.stats.acked[counter] += 1
        except Exception:
            self.stats.ambiguous[counter] += 1
            raise

    async def _worker(self, index: int):
        iteration = 0
        with bound_to(self.monitor):
            while not self._stop.is_set():
                await self.workload.round(self, index, self.keys[index], iteration)
                iteration += 1

    async def validate(self):
        await self.workload.validate(self)
