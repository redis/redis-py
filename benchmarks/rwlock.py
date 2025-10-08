"""Simulation of readers and writers sharing ownership of a lock."""

import itertools
import json
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from random import Random
from typing import Literal
from typing import Optional
from typing import Self
from typing import TypeAlias
from typing import Union

import click
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
class Parameters:
    duration: float
    seed: int
    num_workers: int
    wr_ratio: float
    io_duration: float
    max_writers: int

    def to_dict(self) -> dict:
        return {
            'duration': self.duration,
            'seed': self.seed,
            'num_workers': self.num_workers,
            'wr_ratio': self.wr_ratio,
            'io_duration': self.io_duration,
            'max_writers': self.max_writers,
        }

    def to_string(self) -> str:
        return ', '.join([
            f'duration = {self.duration}',
            f'seed = {self.seed}',
            f'num_workers = {self.num_workers}',
            f'wr_ratio = {self.wr_ratio}',
            f'io_duration = {self.io_duration}',
            f'max_writers = {self.max_writers}',
        ])


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
    readers: int
    writers: int
    locked: int

    @staticmethod
    def collect(lock: RwLock) -> Self:
        return TimeSeriesMetric(
            timestamp=_now(),
            readers=int(lock.redis.zcard(lock._reader_lock_name())),
            writers=int(lock.redis.zcard(lock._writer_semaphore_name())),
            locked=int(lock.redis.get('locked') or 0),
        )

    def to_row(self) -> tuple[str, int, int, int]:
        return (self.timestamp.isoformat(), self.readers, self.writers, self.locked)


class Worker:
    # Keys used:
    # - locked
    # - readers
    # - writers
    # - total_reads
    # - total_writes

    lock: RwLock
    random: Random
    wr_ratio: float
    io_duration: float
    metrics: list[InvocationMetric]
    series: list[TimeSeriesMetric]

    def __init__(
        self,
        lock: RwLock,
        params: Parameters,
    ) -> None:
        self.lock = lock
        self.random = Random(params.seed or None)
        self.wr_ratio = params.wr_ratio
        self.io_duration = params.io_duration
        self.metrics = []
        self.series = []

    @property
    def redis(self) -> Redis:
        return self.lock.redis

    def rand_io_time(self) -> float:
        if not self.io_duration:
            return 0
        mean = self.io_duration
        std = mean
        shape = mean**2 / std**2
        scale = std**2 / mean
        return self.random.gammavariate(shape, scale)

    def wait_for_io(self) -> None:
        if self.io_duration:
            time.sleep(self.rand_io_time())

    def do_write(self, metric: InvocationMetric) -> None:
        write_guard = self.lock.write()

        acquire_start = time.perf_counter()
        try:
            acquired = write_guard.acquire()
        except LockMaxWritersError:
            # Hit max workers; abort gracefully
            metric.write_acquire_status = 'aborted'
            return
        metric.write_acquire_time = time.perf_counter() - acquire_start
        self.series.append(TimeSeriesMetric.collect(self.lock))
        if not acquired:
            metric.write_acquire_status = 'timeout'
            return

        self.redis.set('locked', 1)
        metric.write_acquire_status = 'success'
        assert int(self.redis.get('readers') or 0) == 0

        try:
            self.wait_for_io()
            self.redis.incr('total_reads')
        finally:
            self.redis.set('locked', 0)
            release_start = time.perf_counter()
            write_guard.release()
            metric.write_release_time = time.perf_counter() - release_start
            self.series.append(TimeSeriesMetric.collect(self.lock))

    def do_read(self, metric: InvocationMetric) -> None:
        read_guard = self.lock.read()

        acquire_start = time.perf_counter()
        acquired = read_guard.acquire()
        metric.read_acquire_time = time.perf_counter() - acquire_start
        self.series.append(TimeSeriesMetric.collect(self.lock))
        if not acquired:
            metric.read_acquire_status = 'timeout'
            return
        metric.read_acquire_status = 'success'

        assert not int(self.redis.get('locked') or 0)

        self.redis.incr('readers')

        try:
            self.wait_for_io()
            self.redis.incr('total_writes')
        finally:
            self.redis.decr('readers')
            release_start = time.perf_counter()
            read_guard.release()
            metric.read_release_time = time.perf_counter() - release_start
            self.series.append(TimeSeriesMetric.collect(self.lock))

    def work(self) -> None:
        metric = InvocationMetric()

        if self.random.random() < self.wr_ratio:
            self.do_write(metric)
        else:
            self.do_read(metric)

        metric.timestamp = _now()
        self.metrics.append(metric)

    def loop(self, stop_at: float) -> None:
        while time.time() < stop_at:
            self.work()


def plot_series(path: str):
    """Import and run this inside a notebook to visualize time series."""
    import matplotlib.pyplot as plt

    data = json.load(open(path))

    # Detect sweep parameters
    values: defaultdict[str, set[Union[float, int]]] = defaultdict(lambda: set())
    params: dict[str, Union[float, int]]
    for ts in data:
        params = ts['params']
        for k, v in params.items():
            values[k].add(v)
    sweep_params = [k for k, v in values.items() if len(v) > 1]

    for ts in data:
        params = ts['params']
        df = pd.DataFrame(
            ts['data'],
            columns=['timestamp', 'readers', 'writers', 'locked'],
        )
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        df.sort_values('timestamp', inplace=True)
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(df['timestamp'], df['readers'], label='readers')
        ax.plot(df['timestamp'], df['writers'], label='writers')
        ax.plot(df['timestamp'], df['locked'], label='locked')
        title = ', '.join([
            f'{k}={params[k]}'
            for k in sweep_params
        ])
        ax.set_title(title)
        ax.set_xlabel('Time')
        ax.set_ylabel('Count')
        ax.legend()
        ax.grid(alpha=0.3)
        plt.show()


def display_metrics(invocation_metrics: list[InvocationMetric]) -> None:
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


@dataclass
class Output:
    params: Parameters
    total_reads: int
    total_writes: int
    invocation_metrics: list[InvocationMetric]
    time_series_metrics: list[TimeSeriesMetric]


def run(redis: Redis, params: Parameters) -> Output:
    lock = RwLock(
        redis=redis,
        prefix='lock',
        timeout=10,
        sleep=max(params.io_duration, 1e-3),
        blocking_timeout=1,
        max_writers=params.max_writers,
    )

    stop_at = time.time() + params.duration

    # Spawn workers
    workers = [Worker(lock, params) for _ in range(params.num_workers)]
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

    # Aggregate metrics
    for worker in workers:
        time_series.extend(worker.series)
    invocation_metrics = [metric for worker in workers for metric in worker.metrics]

    total_reads = int(redis.get('total_reads') or 0)
    total_writes = int(redis.get('total_writes') or 0)

    return Output(
        params=params,
        total_reads=total_reads,
        total_writes=total_writes,
        invocation_metrics=invocation_metrics,
        time_series_metrics=time_series,
    )


@click.command()
@click.option('--duration', type=str, default='5')
@click.option('--seed', type=str, default='0')
@click.option('--num-workers', type=str, default='1')
@click.option('--wr-ratio', type=str, default='0.1')
@click.option('--io-duration', type=str, default='0.025')
@click.option('--max-writers', type=str, default='0')
def benchmark(
    duration: str,
    seed: str,
    num_workers: str,
    wr_ratio: str,
    io_duration: str,
    max_writers: str,
):
    def parse_int(arg: str) -> list[int]:
        return list([int(x) for x in arg.split(',')])

    def parse_float(arg: str) -> list[float]:
        return list([float(x) for x in arg.split(',')])

    duration = parse_int(duration)
    seed = parse_int(seed)
    num_workers = parse_int(num_workers)
    wr_ratio = parse_float(wr_ratio)
    io_duration = parse_float(io_duration)
    max_writers = parse_int(max_writers)

    time_series = []

    for opts in itertools.product(
        duration,
        seed,
        num_workers,
        wr_ratio,
        io_duration,
        max_writers,
    ):
        redis = Redis(db=11)
        redis.flushdb()

        params = Parameters(*opts)
        output = run(redis, params)

        time_series.append({
            'params': output.params.to_dict(),
            'data': [ts.to_row() for ts in output.time_series_metrics],
        })

        # Print stats
        reads = output.total_reads
        writes = output.total_writes
        print(params.to_string())
        print(f'iops: {(reads + writes) / params.duration:.2f}')

        # Display metrics
        display_metrics(output.invocation_metrics)

    out_dir = Path('.cache')
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / 'rwlock.json'
    out_path = open(out_path, 'w')
    json.dump(time_series, out_path)


if __name__ == '__main__':
    benchmark()
