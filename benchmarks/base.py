import functools
import itertools
import timeit

import redis


class Benchmark:
    ARGUMENTS = ()

    def __init__(self):
        self._client = None

    def get_client(self, **kwargs):
        # eventually make this more robust and take optional args from
        # argparse
        if self._client is None or kwargs:
            defaults = {"db": 9}
            defaults.update(kwargs)
            pool = redis.ConnectionPool(**kwargs)
            self._client = redis.Redis(connection_pool=pool)
        return self._client

    def setup(self, **kwargs):
        pass

    def run(self, **kwargs):
        pass

    def run_benchmark(self):
        group_names = [group["name"] for group in self.ARGUMENTS]
        group_values = [group["values"] for group in self.ARGUMENTS]
        for value_set in itertools.product(*group_values):
            pairs = list(zip(group_names, value_set))
            arg_string = ", ".join(f"{p[0]}={p[1]}" for p in pairs)
            print(f"Benchmark: {arg_string}... ", end="")
            kwargs = dict(pairs)
            setup = functools.partial(self.setup, **kwargs)
            run = functools.partial(self.run, **kwargs)
            t = timeit.timeit(stmt=run, setup=setup, number=1000000)
            print(f"{t:f}")
