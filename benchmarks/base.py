import functools
import itertools
import redis
import sys
import timeit


class Benchmark:
    ARGUMENTS = ()

    def __init__(self):
        self._client = None

    def get_client(self, **kwargs):
        # eventually make this more robust and take optional args from
        # argparse
        if self._client is None or kwargs:
            defaults = {
                'db': 9
            }
            defaults.update(kwargs)
            pool = redis.ConnectionPool(**kwargs)
            self._client = redis.Redis(connection_pool=pool)
        return self._client

    def setup(self, **kwargs):
        pass

    def run(self, **kwargs):
        pass

    def run_benchmark(self):
        group_names = [group['name'] for group in self.ARGUMENTS]
        group_values = [group['values'] for group in self.ARGUMENTS]
        for value_set in itertools.product(*group_values):
            pairs = list(zip(group_names, value_set))
            arg_string = ', '.join(['%s=%s' % (p[0], p[1]) for p in pairs])
            sys.stdout.write('Benchmark: %s... ' % arg_string)
            sys.stdout.flush()
            kwargs = dict(pairs)
            setup = functools.partial(self.setup, **kwargs)
            run = functools.partial(self.run, **kwargs)
            t = timeit.timeit(stmt=run, setup=setup, number=1000)
            sys.stdout.write('%f\n' % t)
            sys.stdout.flush()
