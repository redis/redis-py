import redis
import time
from functools import wraps
from argparse import ArgumentParser


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-n',
                        type=int,
                        help='Total number of requests (default 100000)',
                        default=100000)
    parser.add_argument('-P',
                        type=int,
                        help=('Pipeline <numreq> requests.'
                              ' Default 1 (no pipeline).'),
                        default=1)
    parser.add_argument('-s',
                        type=int,
                        help='Data size of SET/GET value in bytes (default 2)',
                        default=2)

    args = parser.parse_args()
    return args


def run():
    args = parse_args()
    r = redis.Redis()
    r.flushall()
    set_str(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    set_int(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    get_str(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    get_int(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    incr(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    lpush(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    lrange_300(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    lpop(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)
    hmset(conn=r, num=args.n, pipeline_size=args.P, data_size=args.s)


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        ret = func(*args, **kwargs)
        duration = time.monotonic() - start
        if 'num' in kwargs:
            count = kwargs['num']
        else:
            count = args[1]
        print('{} - {} Requests'.format(func.__name__, count))
        print('Duration  = {}'.format(duration))
        print('Rate = {}'.format(count/duration))
        print()
        return ret
    return wrapper


@timer
def set_str(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    format_str = '{:0<%d}' % data_size
    set_data = format_str.format('a')
    for i in range(num):
        conn.set('set_str:%d' % i, set_data)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def set_int(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    format_str = '{:0<%d}' % data_size
    set_data = int(format_str.format('1'))
    for i in range(num):
        conn.set('set_int:%d' % i, set_data)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def get_str(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    for i in range(num):
        conn.get('set_str:%d' % i)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def get_int(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    for i in range(num):
        conn.get('set_int:%d' % i)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def incr(conn, num, pipeline_size, *args, **kwargs):
    if pipeline_size > 1:
        conn = conn.pipeline()

    for i in range(num):
        conn.incr('incr_key')
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def lpush(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    format_str = '{:0<%d}' % data_size
    set_data = int(format_str.format('1'))
    for i in range(num):
        conn.lpush('lpush_key', set_data)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def lrange_300(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    for i in range(num):
        conn.lrange('lpush_key', i, i+300)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


@timer
def lpop(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()
    for i in range(num):
        conn.lpop('lpush_key')
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()
    if pipeline_size > 1:
        conn.execute()


@timer
def hmset(conn, num, pipeline_size, data_size):
    if pipeline_size > 1:
        conn = conn.pipeline()

    set_data = {'str_value': 'string',
                'int_value': 123456,
                'float_value': 123456.0}
    for i in range(num):
        conn.hmset('hmset_key', set_data)
        if pipeline_size > 1 and i % pipeline_size == 0:
            conn.execute()

    if pipeline_size > 1:
        conn.execute()


if __name__ == '__main__':
    run()
