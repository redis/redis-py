from datetime import datetime
from redis import Redis, RedisError, ReadOnlyError, retry
import time
import sys
import random

from redis.backoff import NoBackoff


class Config:
    default_host = "localhost"
    master_host = (
        "ezbenchgc0hng9j-m5-large-6-x-001.eka2zi.0001.use1devo.elmo-dev.amazonaws.com"
    )
    replica_host = "xxx.xx.0001.xxxx.cache.amazonaws.com"
    redis_db = 0
    socket_conn_timeout = 10
    request_delay_sec = 0.1


def get_redis_client():
    retry_obj = retry.Retry(NoBackoff(), 1)
    return Redis(
        host=Config.default_host,
        port=1111,
        db=Config.redis_db,
        socket_connect_timeout=Config.socket_conn_timeout,
        retry=retry_obj,
        retry_on_error=[ReadOnlyError],
    )


def get_random_key_value():
    val = time.time()
    key = "test_key_" + str(random.randint(0, 100))
    return key, val


r = get_redis_client()
print("connection made")
print(f"flush db result={r.flushdb()}")

flag = False

while True:
    try:
        if flag:
            print("beat:", time.time())
        r.set(*get_random_key_value())
        flag = False
        time.sleep(Config.request_delay_sec)
    except RedisError as re:
        print(datetime.now(), "Error:", type(re), re)
        flag = True
        # sys.exit()
    except KeyboardInterrupt:
        print("Stopping loop execution")
        sys.exit()
