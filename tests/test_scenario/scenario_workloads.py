"""The workloads and the app of the cluster scenario tests.

The app runs one workload on background workers that share one client instance.
The workloads are independent of any scenario, so every scenario test can run
each of them (WORKLOADS): regular data operations (simple), an application
command mix (mixed), and blocking commands (blocking). The async counterparts are
in tests/test_asyncio/test_scenario/async_scenario_workloads.py.
"""

from collections import Counter, defaultdict
import logging
import threading
import time
from dataclasses import dataclass, field
from threading import Thread
from typing import Any, Callable

from redis import RedisCluster
from redis.asyncio import RedisCluster as AsyncRedisCluster
from redis.exceptions import (
    AuthenticationError,
    ConnectionError,
    RedisError,
    TimeoutError,
)
from tests.test_scenario.maint_notifications_helpers import KeyGenerationHelpers
from tests.test_scenario.scenario_monitor import ClientMonitor, _now, bound_to


CLOSE_TIMEOUT_MESSAGE = "Timed out closing connection"
THROUGHPUT_WINDOW_SECONDS = 5
INCR_SCRIPT = "return redis.call('INCR', KEYS[1])"
# The blocking workload: how long a consumer's BLPOP blocks on an empty queue, and
# how long a producer pauses between its rounds, so the queues stay mostly empty.
BLOCKING_TIMEOUT = 1
BLOCKING_PRODUCER_PAUSE_SECONDS = 0.2


def keys_on_every_primary(
    client: RedisCluster | AsyncRedisCluster, prefix: str
) -> list[str]:
    """One key in the middle of the slots of each primary.

    The slots come from the client's own slot map, not from an even split of the
    slot space, so every primary gets load whatever slot boundaries the database
    uses.
    """
    slots_by_owner: dict[str, list[int]] = defaultdict(list)
    for slot, nodes in client.nodes_manager.slots_cache.items():
        if nodes:
            slots_by_owner[nodes[0].name].append(slot)
    return [
        KeyGenerationHelpers.generate_key(sorted(slots)[len(slots) // 2], prefix)
        for _, slots in sorted(slots_by_owner.items())
    ]


# The app: the workers and the client they share.


@dataclass
class CommandError:
    at: float
    error_type: str
    message: str
    command: str
    is_redis_error: bool
    is_transient: bool


@dataclass
class WorkloadStats:
    """Results of the app's workers, shared between them."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    errors: list[CommandError] = field(default_factory=list)
    ok_per_second: dict[int, int] = field(default_factory=lambda: defaultdict(int))
    started_at: float = field(default_factory=_now)
    # counter key -> acknowledged increments, and increments that raised (and so
    # may or may not have been applied)
    acked: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    ambiguous: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def ok(self, count: int = 1):
        with self.lock:
            self.ok_per_second[int(_now() - self.started_at)] += count

    def failed(self, command: str, error: BaseException):
        is_transient = isinstance(error, (ConnectionError, TimeoutError)) and not (
            isinstance(error, AuthenticationError)
            or CLOSE_TIMEOUT_MESSAGE in str(error)
        )
        entry = CommandError(
            _now(),
            type(error).__name__,
            str(error),
            command,
            isinstance(error, RedisError),
            is_transient,
        )
        with self.lock:
            self.errors.append(entry)
        logging.warning(f"{command} failed: {entry.error_type}: {entry.message}")

    def commands_between(self, start: float, end: float) -> int:
        first = int(start - self.started_at)
        last = int(end - self.started_at)
        with self.lock:
            ok = sum(n for s, n in self.ok_per_second.items() if first <= s <= last)
            failed = sum(1 for e in self.errors if start <= e.at <= end)
        return ok + failed

    def errors_between(self, start: float, end: float) -> int:
        with self.lock:
            return sum(1 for e in self.errors if start <= e.at <= end)

    def throughput(self, start: float, end: float) -> float:
        first = int(start - self.started_at)
        last = int(end - self.started_at)
        with self.lock:
            ok = sum(n for s, n in self.ok_per_second.items() if first <= s < last)
        return ok / max(end - start, 1)

    def recent_throughput(self) -> float:
        return self.throughput(_now() - THROUGHPUT_WINDOW_SECONDS, _now())


class Workload:
    """A command mix the app's workers run, and the data checks that go with it.

    The workloads run over plain keys, one key per primary for each worker, so
    they work against cluster and standalone clients alike. Each stack has its own
    implementation of every workload; WORKLOADS names them for the
    parametrization.
    """

    name: str = ""

    def round(self, app: "ScenarioApp", worker: int, keys: list[str], iteration: int):
        """Run one round of commands over the worker's keys."""
        raise NotImplementedError

    def validate(self, app: "ScenarioApp"):
        """Check the data the workload wrote, after the app has stopped."""
        validate_counters(app.stats, app.client.get)


class SimpleWorkload(Workload):
    """Regular data operations: plain non-blocking commands, each on one key.

    SET, GET, INCR, HSET, HGET and EXPIRE. No transactions, pipelines, scripts or
    pub/sub. INCR counts its increments, so the counter shows a lost write or one
    that ran twice.
    """

    name = "simple"

    def round(self, app, worker, keys, iteration):
        run = app.run
        for key in keys:
            counter = f"{key}:counter"
            try:
                run("SET", lambda c: c.set(key, "value"))
                run("GET", lambda c: c.get(key))
                app.counted("INCR", counter, lambda c: c.incr(counter))
                run("HSET", lambda c: c.hset(f"{key}:hash", "field", iteration))
                run("HGET", lambda c: c.hget(f"{key}:hash", "field"))
                run("EXPIRE", lambda c: c.expire(f"{key}:hash", 300))
            except Exception:
                continue


class MixedWorkload(Workload):
    """The command mix of the drill's application.

    Over every key: strings (SET, GET, EXPIRE, TTL, INCR), hashes (HINCRBYFLOAT,
    HSETNX, HGET, HLEN, HGETALL), a set (SADD), a sorted set (ZADD), a list
    (RPUSH, LTRIM, LRANGE), a stream (XADD, XRANGE, a non-blocking XREAD) and a
    script (EVAL). Every fifth round a multi-key EXISTS, which a cluster client
    runs as an internal pipeline over the slots, a PING and a non-transactional
    pipeline. INCR and the script count their increments. The collections stay
    small: the list and the stream are trimmed and the set members repeat.
    """

    name = "mixed"

    def round(self, app, worker, keys, iteration):
        run = app.run
        member = f"m{iteration % 20}"
        for key in keys:
            counter = f"{key}:counter"
            script_counter = f"{key}:script-counter"
            hash_key = f"{key}:hash"
            list_key = f"{key}:list"
            stream = f"{key}:stream"
            try:
                run("SET", lambda c: c.set(key, "value"))
                run("GET", lambda c: c.get(key))
                run("EXPIRE", lambda c: c.expire(key, 300))
                run("TTL", lambda c: c.ttl(key))
                app.counted("INCR", counter, lambda c: c.incr(counter))
                run("HINCRBYFLOAT", lambda c: c.hincrbyfloat(hash_key, "field", 1.5))
                run("HSETNX", lambda c: c.hsetnx(hash_key, "created", iteration))
                run("HGET", lambda c: c.hget(hash_key, "field"))
                run("HLEN", lambda c: c.hlen(hash_key))
                run("HGETALL", lambda c: c.hgetall(hash_key))
                run("SADD", lambda c: c.sadd(f"{key}:set", member))
                run("ZADD", lambda c: c.zadd(f"{key}:zset", {member: iteration}))
                run("RPUSH", lambda c: c.rpush(list_key, iteration))
                run("LTRIM", lambda c: c.ltrim(list_key, -50, -1))
                run("LRANGE", lambda c: c.lrange(list_key, 0, 9))
                run(
                    "XADD",
                    lambda c: c.xadd(stream, {"f": "v"}, maxlen=100, approximate=True),
                )
                run("XRANGE", lambda c: c.xrange(stream, count=10))
                run("XREAD", lambda c: c.xread({stream: "0-0"}, count=10))
                app.counted(
                    "EVAL",
                    script_counter,
                    lambda c: c.eval(INCR_SCRIPT, 1, script_counter),
                )
            except Exception:
                continue
        if iteration % 5 == 4:
            try:
                run("EXISTS", lambda c: c.exists(*keys))
                run("PING", lambda c: c.ping())

                def pipeline(c):
                    pipe = c.pipeline(transaction=False)
                    for key in keys[:3]:
                        pipe.set(f"{key}:pipe", "value")
                        pipe.get(f"{key}:pipe")
                    return pipe.execute()

                run("PIPELINE", pipeline)
            except Exception:
                pass


@dataclass
class QueueStats:
    """What the blocking workload pushed to and popped from its queues."""

    lock: threading.Lock = field(default_factory=threading.Lock)
    # queue -> values whose RPUSH was acknowledged, and values whose RPUSH raised
    # (and so may or may not be in the queue)
    pushed: dict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    push_ambiguous: dict[str, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    # queue -> values BLPOP returned, and BLPOPs that raised (and so may have
    # taken a value whose reply was lost)
    popped: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    pop_ambiguous: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    # queue -> value -> when its RPUSH started and ended, and the (value, start,
    # end) of every BLPOP that returned one, for the report of a duplicate
    push_spans: dict[str, dict[str, tuple[float, float]]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    pop_spans: dict[str, list[tuple[str, float, float]]] = field(
        default_factory=lambda: defaultdict(list)
    )


def _activity(monitor: ClientMonitor, start: float, end: float) -> str:
    """What the client did to its connections between start and end."""
    return (
        f"drops {monitor.by_address('drop', start, end)}, failed connects "
        f"{monitor.by_address('connect_failure', start, end)}, refreshes "
        f"{monitor.count('refresh', start=start, end=end)}"
    )


def describe_duplicates(
    queue: str,
    remaining: list[str],
    queues: QueueStats,
    monitor: ClientMonitor | None = None,
    since: float = 0.0,
) -> str:
    """For each value found twice in queue: where it was found, and its RPUSH and
    BLPOPs, with what the client did to its connections meanwhile. Times are in
    seconds after since.

    A value popped twice, or popped and still queued, after a push that saw a
    connection drop points at a push the client sent again; one whose push saw
    nothing, popped across a failover, points at a pop the server lost.
    """
    popped = Counter(queues.popped[queue])
    queued = Counter(remaining)
    reports = []
    for value in sorted(v for v in popped | queued if popped[v] + queued[v] > 1):
        push = queues.push_spans[queue].get(value)
        acked = value in queues.pushed[queue]
        report = (
            f"{value}: popped {popped[value]}x, still queued {queued[value]}x; "
            f"RPUSH {'acknowledged' if acked else 'raised'}"
        )
        if push is not None:
            report += f" at {push[0] - since:.2f}-{push[1] - since:.2f}s"
            if monitor is not None:
                report += f" ({_activity(monitor, *push)})"
        for popped_value, start, end in queues.pop_spans[queue]:
            if popped_value == value:
                report += f"; BLPOP at {start - since:.2f}-{end - since:.2f}s"
                if monitor is not None:
                    report += f" ({_activity(monitor, start, end)})"
        reports.append(report)
    return "; ".join(reports[:3])


def validate_queue(
    queue: str,
    remaining: list[str],
    queues: QueueStats,
    monitor: ClientMonitor | None = None,
    since: float = 0.0,
) -> str | None:
    """Check one queue of the blocking workload. Returns the problem, if any.

    monitor and since only add detail to the report of a duplicate; see
    describe_duplicates.
    """
    pushed = queues.pushed[queue]
    maybe_pushed = pushed | queues.push_ambiguous[queue]
    popped = queues.popped[queue]
    if len(popped) + len(remaining) != len(set(popped) | set(remaining)):
        return (
            f"{queue}: a value was popped or queued twice, so a push ran twice or "
            f"the server lost a pop: "
            + describe_duplicates(queue, remaining, queues, monitor, since)
        )
    seen = set(popped) | set(remaining)
    unknown = seen - maybe_pushed
    if unknown:
        return f"{queue}: values that were never pushed: {sorted(unknown)[:5]}"
    missing = pushed - seen
    if len(missing) > queues.pop_ambiguous[queue]:
        return (
            f"{queue}: {len(missing)} acknowledged values are gone, but only "
            f"{queues.pop_ambiguous[queue]} BLPOPs raised: values were lost"
        )
    return None


class BlockingWorkload(Workload):
    """Blocking commands: producers push to queues, consumers block on them.

    Even workers are producers: they RPUSH a unique value to a queue on every
    primary, then pause, so the queues are empty most of the time. Odd workers are
    consumers: they BLPOP the queues of the producer before them, with a short
    timeout, so most of their time is spent blocked on a connection. At the end,
    every acknowledged value must have been popped exactly once or still be in its
    queue, apart from the values a BLPOP that raised may have taken.
    """

    name = "blocking"

    def __init__(self):
        self.queues = QueueStats()

    @staticmethod
    def queue(key: str) -> str:
        return f"{key}:queue"

    def round(self, app, worker, keys, iteration):
        if worker % 2 == 0:
            self._produce(app, worker, keys, iteration)
        else:
            self._consume(app, app.keys[worker - 1])

    def _produce(self, app, worker, keys, iteration):
        queues = self.queues
        for key in keys:
            queue = self.queue(key)
            value = f"{worker}-{iteration}"
            started = _now()
            try:
                app.run("RPUSH", lambda c: c.rpush(queue, value))
                with queues.lock:
                    queues.pushed[queue].add(value)
            except Exception:
                with queues.lock:
                    queues.push_ambiguous[queue].add(value)
            with queues.lock:
                queues.push_spans[queue][value] = (started, _now())
        time.sleep(BLOCKING_PRODUCER_PAUSE_SECONDS)

    def _consume(self, app, keys):
        queues = self.queues
        for key in keys:
            queue = self.queue(key)
            started = _now()
            try:
                reply = app.run(
                    "BLPOP", lambda c: c.blpop([queue], timeout=BLOCKING_TIMEOUT)
                )
            except Exception:
                with queues.lock:
                    queues.pop_ambiguous[queue] += 1
                continue
            if reply is not None:
                value = reply[1].decode() if isinstance(reply[1], bytes) else reply[1]
                with queues.lock:
                    queues.popped[queue].append(value)
                    queues.pop_spans[queue].append((value, started, _now()))

    def validate(self, app):
        queues = self.queues
        problems = []
        for queue in set(queues.pushed) | set(queues.push_ambiguous):
            remaining = [
                v.decode() if isinstance(v, bytes) else v
                for v in app.client.lrange(queue, 0, -1)
            ]
            problem = validate_queue(
                queue, remaining, queues, app.monitor, app.stats.started_at
            )
            if problem:
                problems.append(problem)
        assert not problems, "; ".join(problems[:5])


WORKLOAD_CLASSES = {
    w.name: w for w in (SimpleWorkload, MixedWorkload, BlockingWorkload)
}
WORKLOADS = list(WORKLOAD_CLASSES)


class ScenarioApp:
    """The application under test: workers that share one client.

    Each worker runs rounds of the workload over its keys until the app stops. The
    workers record what the client does into monitor, the client's own monitor.
    """

    def __init__(
        self,
        client: RedisCluster,
        workload: Workload,
        keys: list[list[str]],
        monitor: ClientMonitor,
    ):
        self.client = client
        self.workload = workload
        # keys[i] are the keys of worker i
        self.keys = keys
        self.monitor = monitor
        self.stats = WorkloadStats()
        self._stop = threading.Event()
        self._threads: list[Thread] = []

    def start(self):
        for index in range(len(self.keys)):
            thread = Thread(
                target=self._worker,
                args=(index,),
                name=f"app_worker_{index}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self):
        self._stop.set()
        for thread in self._threads:
            thread.join()
        self._threads = []

    def run(self, command: str, call: Callable[[RedisCluster], Any]):
        try:
            result = call(self.client)
            self.stats.ok()
            return result
        except Exception as e:
            self.stats.failed(command, e)
            raise

    def counted(self, command: str, counter: str, call: Callable[[RedisCluster], Any]):
        """Run an increment of counter and record whether it was acknowledged."""
        stats = self.stats
        try:
            self.run(command, call)
            with stats.lock:
                stats.acked[counter] += 1
        except Exception:
            with stats.lock:
                stats.ambiguous[counter] += 1
            raise

    def _worker(self, index: int):
        iteration = 0
        with bound_to(self.monitor):
            while not self._stop.is_set():
                self.workload.round(self, index, self.keys[index], iteration)
                iteration += 1

    def validate(self):
        self.workload.validate(self)


def validate_counters(stats: WorkloadStats, get: Callable[[str], Any]):
    """Check every counter of the workload; get reads a key."""
    for key in set(stats.acked) | set(stats.ambiguous):
        validate_counter(key, int(get(key) or 0), stats)


def validate_counter(key: str, value: int, stats: WorkloadStats):
    acked = stats.acked[key]
    ambiguous = stats.ambiguous[key]
    assert acked <= value <= acked + ambiguous, (
        f"{key} is {value}, expected {acked} acknowledged increments plus "
        f"at most {ambiguous} that raised: a write was lost or ran twice"
    )
