"""Simulation of a pool of workers reading a cached value which
occasionally must be replaced when it expires.
"""

import random
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from numbers import Number
from pathlib import Path
from typing import Literal
from typing import Optional
from typing import Self
from typing import TextIO
from typing import TypeAlias
from typing import Union

import pandas as pd

from redis.client import Redis
from redis.exceptions import LockMaxWritersError
from redis.rwlock import RwLock


def _now() -> datetime:
    return datetime.now()


AcquireStatus: TypeAlias = Union[
    Literal['success'],
    Literal['timeout'],
    Literal['aborted'],
]


@dataclass
class InvocationMetric:
    timestamp: Optional[datetime] = None
    read_acquire_time: Optional[float] = None
    read_acquire_status: Optional[AcquireStatus] = None
    read_release_time: Optional[float] = None
    write_acquire_time: Optional[float] = None
    write_acquire_status: Optional[AcquireStatus] = None
    write_release_time: Optional[float] = None


@dataclass
class TimeSeriesMetric:
    timestamp: datetime
    num_readers: int
    num_waiting_writers: int

    @staticmethod
    def collect(lock: RwLock) -> Self:
        metric = TimeSeriesMetric(
            timestamp=_now(),
            num_readers=lock.redis.zcard(lock._reader_lock_name()),
            num_waiting_writers=lock.redis.zcard(lock._writer_semaphore_name()),
        )
        assert metric.num_waiting_writers <= 1
        return metric


class Worker:
    # Keys used:
    # - worker_invocations: Total worker invocations
    # - current_key: Holds the read counter key. Value is a random
    #   UUID4. Expires after `ttl` seconds.
    # - previous_key: Previous value of current_key. Does not expire.
    # - total: Total of all increments to the read counter. Should equal
    #   worker_invocations at the end.

    lock: RwLock
    ttl: float
    io_time: float
    metrics: list[InvocationMetric]
    series: list[TimeSeriesMetric]

    def __init__(
        self,
        lock: RwLock,
        ttl: float,
        io_time: float,
    ) -> None:
        self.lock = lock
        self.ttl = ttl
        self.io_time = io_time
        self.metrics = []
        self.series = []

    @property
    def redis(self) -> Redis:
        return self.lock.redis

    def rand_io_time(self) -> float:
        mean = self.io_time
        std = mean
        shape = mean**2 / std**2
        scale = std**2 / mean
        return random.gammavariate(shape, scale)

    def replace_key(self, metric: InvocationMetric) -> None:
        write_guard = self.lock.write()

        # Acquire lock for writing
        acquire_start = time.perf_counter()
        try:
            acquired = write_guard.acquire()
        except LockMaxWritersError:
            # Another worker has the lock; abort
            metric.write_acquire_status = 'aborted'
            return
        metric.write_acquire_time = time.perf_counter() - acquire_start
        self.series.append(TimeSeriesMetric.collect(self.lock))
        if not acquired:
            metric.write_acquire_status = 'timeout'
            return

        metric.write_acquire_status = 'success'

        def release() -> None:
            release_start = time.perf_counter()
            write_guard.release()
            metric.write_release_time = time.perf_counter() - release_start
            self.series.append(TimeSeriesMetric.collect(self.lock))

        if self.redis.exists('current_key'):
            release()
            return

        # Update total with writes to previous key
        previous_key: bytes = self.redis.get('previous_key')
        if previous_key:
            previous_value = self.redis.get(previous_key)
            if previous_value:
                self.redis.incrby('total', int(previous_value))

        # Pretend to do I/O
        time.sleep(self.rand_io_time())

        # Update keys
        new_key = f'cache:{uuid.uuid4().hex}'
        self.redis.set('current_key', new_key, px=int(self.ttl * 1000))
        self.redis.set('previous_key', new_key)

        release()

    def work_inner(self, metric: InvocationMetric) -> None:
        read_guard = self.lock.read()

        # Acquire lock for reading
        acquire_start = time.perf_counter()
        acquired = read_guard.acquire()
        metric.read_acquire_time = time.perf_counter() - acquire_start
        self.series.append(TimeSeriesMetric.collect(self.lock))
        if not acquired:
            metric.read_acquire_status = 'timeout'
            return
        metric.read_acquire_status = 'success'

        def release() -> None:
            release_start = time.perf_counter()
            read_guard.release()
            metric.read_release_time = time.perf_counter() - release_start
            self.series.append(TimeSeriesMetric.collect(self.lock))

        current_key = self.redis.get('current_key')
        if current_key:
            # Key exists; simulate I/O and bump counters
            time.sleep(self.rand_io_time())

            self.redis.incr(current_key)
            self.redis.incr('worker_invocations')

            release()
        else:
            # Key does not exist; release lock and try to update key
            release()
            self.replace_key(metric)

    def work(self) -> None:
        metric = InvocationMetric()
        self.work_inner(metric)
        metric.timestamp = _now()
        self.metrics.append(metric)

    def loop(self, stop_at: float) -> None:
        while time.time() < stop_at:
            self.work()


def write_headers(csv_file: TextIO) -> None:
    headers = [
        'timestamp',
        'num_readers',
        'num_waiting_writers',
        'num_workers',
        'ttl',
    ]
    df = pd.DataFrame(columns=headers)
    df.to_csv(csv_file, mode='w', header=True, index=False)


def write_time_series(
    csv_file: TextIO,
    n: int,
    ttl: Number,
    time_series: list[TimeSeriesMetric],
) -> None:
    ts_records = [
        {
            'timestamp': metric.timestamp.isoformat(),
            'num_readers': metric.num_readers,
            'num_waiting_writers': metric.num_waiting_writers,
            'num_workers': n,
            'ttl': ttl,
        }
        for metric in time_series
    ]
    ts_df = pd.DataFrame(ts_records)
    ts_df.to_csv(csv_file, mode='a', header=False, index=False)


def plot_series(path: str):
    """Import and run this inside a notebook to visualize time series."""
    import matplotlib.pyplot as plt

    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')

    for (workers, ttl), group in df.groupby(['num_workers', 'ttl'], sort=True):
        group = group.sort_values('timestamp')
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(group['timestamp'], group['num_readers'], label='num_readers')
        ax.plot(group['timestamp'], group['num_waiting_writers'], label='num_waiting_writers')
        ax.set_title(f'num_workers={workers}, ttl={ttl}')
        ax.set_xlabel('Time')
        ax.set_ylabel('Count')
        ax.legend()
        ax.grid(alpha=0.3)
        plt.show()


def display_metrics(
    n: int,
    ttl: Number,
    invocation_metrics: list[InvocationMetric],
) -> None:
    inv_df = pd.DataFrame.from_records([
        {
            'timestamp': metric.timestamp.isoformat() if metric.timestamp else None,
            'read_acquire_time': metric.read_acquire_time,
            'read_release_time': metric.read_release_time,
            'write_acquire_time': metric.write_acquire_time,
            'write_release_time': metric.write_release_time,
            'read_acquire_status': metric.read_acquire_status,
            'write_acquire_status': metric.write_acquire_status,
        }
        for metric in invocation_metrics
    ])
    metric_columns = [
        'read_acquire_time',
        'read_release_time',
        'write_acquire_time',
        'write_release_time',
    ]

    stats_df = pd.DataFrame(index=metric_columns)
    inv_df[metric_columns] = inv_df[metric_columns].apply(pd.to_numeric, errors='coerce')
    stats_df['min'] = inv_df[metric_columns].min()
    stats_df['mean'] = inv_df[metric_columns].mean()
    stats_df['p95'] = inv_df[metric_columns].quantile(0.95)
    stats_df['max'] = inv_df[metric_columns].max()

    cols = ('read_acquire_status', 'write_acquire_status')
    percentages = {}
    for col in cols:
        mask = inv_df[col].notna()
        percentages[col] = inv_df[mask][col].value_counts()
    status_df = pd.DataFrame(percentages).T.fillna(0)
    status_df = status_df.reindex(columns=['success', 'timeout', 'aborted'], fill_value=0)

    print(stats_df.to_string(float_format=lambda x: f'{1e3 * x:.2f}ms'))
    print(status_df.to_string(float_format=lambda x: f'{x:.0f}'))
    print()


def main() -> None:
    num_workers = [1, 2, 4, 8]
    ttl_values = [0.05, 0.1, 0.25, 0.5, 1]
    duration = 5
    io_time = 0.025
    cache_dir = Path('.cache')
    cache_dir.mkdir(exist_ok=True)
    csv_path = cache_dir / 'rwlock_cache.csv'
    csv_file = open(csv_path, 'w')
    write_headers(csv_file)

    for n in num_workers:
        for ttl in ttl_values:
            redis = Redis(db=11)
            redis.flushdb()

            lock = RwLock(
                redis=redis,
                prefix='lock',
                timeout=10,
                sleep=io_time,
                blocking_timeout=1,
                max_writers=1,
            )

            stop_at = time.time() + duration

            # Spawn workers
            workers = [Worker(lock=lock, ttl=ttl, io_time=io_time) for _ in range(n)]
            threads = [
                threading.Thread(target=worker.loop, args=(stop_at,), daemon=True) for worker in workers
            ]
            for thread in threads:
                thread.start()

            # Gather series metrics
            time_series = []
            while time.time() < stop_at:
                time_series.append(TimeSeriesMetric.collect(lock))
                time.sleep(0.01)

            # Wait for workers
            for thread in threads:
                thread.join()

            # Verify that total == # invocations
            total = int(redis.get('total') or 0)
            total += int(redis.get(redis.get('previous_key')) or 0)
            worker_invocations = int(redis.get('worker_invocations') or 0)
            assert worker_invocations == total

            # Write time series data
            for worker in workers:
                time_series.extend(worker.series)
            write_time_series(csv_file, n, ttl, time_series)

            # Print stats
            print(f'n = {n}, ttl = {ttl}')
            writes = len(redis.keys('cache:*'))
            print(f'iops: {(writes + worker_invocations) / duration:.2f}')

            # Display metrics
            invocation_metrics = [metric for worker in workers for metric in worker.metrics]
            display_metrics(n, ttl, invocation_metrics)


if __name__ == '__main__':
    main()
