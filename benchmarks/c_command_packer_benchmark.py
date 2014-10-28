import sys
sys.path.insert(0, '..')

import patch_socket

import redis
print(redis.__file__)

try:
    import redis._pack
except ImportError:
    print('using pure python _pack_command')
else:
    print('using cython _pack_command')
from redis.connection import Connection


class DummyConnectionPool(object):
    def __init__(self):
        self.conn = Connection()

    def get_connection(self, *args, **kwargs):
        return self.conn

    def release(self, conn):
        pass

    def disconnect(self):
        pass

    def reinstantiate(self):
        pass

pool = DummyConnectionPool()
pool.conn.connect()
rds = redis.StrictRedis(connection_pool=pool)


def bench():
    'the operation for benchmark'
    rds.set('test', 100)


def run_with_recording(sock, func):
    sock.start_record()
    func()


def run_with_replay(sock, func):
    sock.start_replay()
    func()

# record once
run_with_recording(pool.conn._sock, bench)

import timeit
timeit.main(['-s', 'from __main__ import run_with_replay, pool, bench',
            '-n', '10000', 'run_with_replay(pool.conn._sock, bench)'])

import cProfile
if sys.version_info[0] >= 3:
    xrange = range
cProfile.run('for i in xrange(10000):run_with_replay(pool.conn._sock, bench)',
             sort='time')
